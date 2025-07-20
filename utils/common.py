"""
Oktabot Common Utilities Module
Centralized components for rate limiting, error handling, and asset verification
"""

import os
import sys
import time
import json
import hashlib
import datetime
import logging
import threading
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple, Any
from enum import Enum
import subprocess
from PIL import Image
from mutagen.wave import WAVE

# Gerekli kütüphaneleri kontrol et
try:
    from PIL import Image
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False
    print("⚠️ UYARI: PIL (Pillow) kütüphanesi yüklü değil!")

try:
    from mutagen.wave import WAVE
    MUTAGEN_AVAILABLE = True
except ImportError:
    MUTAGEN_AVAILABLE = False
    print("⚠️ UYARI: mutagen kütüphanesi yüklü değil!")

class ErrorType(Enum):
    """Error classification for intelligent retry strategies"""
    QUOTA = "quota"
    NETWORK = "network"
    DISK = "disk"
    CONFIG = "config"
    CODE_BUG = "code_bug"
    SYSTEM = "system"

class RateLimiter:
    """
    Thread-safe unified rate limiting implementation for all modules.
    """
    
    def __init__(self, requests_per_minute: int, cooldown_seconds: float):
        self.requests_per_minute = requests_per_minute
        self.cooldown_seconds = cooldown_seconds
        self.last_request_time = 0
        self.request_count = 0
        self.minute_start = time.time()
        self._lock = threading.Lock()  # Thread-safety için eklendi
    
    def wait_if_needed(self) -> None:
        """Thread-safe rate limiting enforcement."""
        with self._lock:  # Critical section protection
            current_time = time.time()
            
            # Reset counter every minute
            if current_time - self.minute_start >= 60:
                self.request_count = 0
                self.minute_start = current_time
            
            # Check if we've hit the per-minute limit
            if self.request_count >= self.requests_per_minute:
                wait_time = 60 - (current_time - self.minute_start)
                if wait_time > 0:
                    print(f"⏱️ Rate limit reached. Waiting {wait_time:.1f} seconds...")
                    time.sleep(wait_time)
                    self.request_count = 0
                    self.minute_start = time.time()
            
            # Enforce cooldown between requests
            time_since_last = current_time - self.last_request_time
            if time_since_last < self.cooldown_seconds:
                wait_time = self.cooldown_seconds - time_since_last
                print(f"⏳ Cooldown period. Waiting {wait_time:.1f} seconds...")
                time.sleep(wait_time)
            
            self.request_count += 1
            self.last_request_time = time.time()

class ErrorClassifier:
    """
    Intelligently classifies errors to determine retry strategy.
    System errors = retry, Code bugs = immediate stop
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.quota_keywords = config.get("quota_errors", [
            "quota", "rate limit", "429", "too many requests", "resource exhausted"
        ])
        self.network_keywords = [
            "connection", "timeout", "network", "dns", "ssl", "certificate"
        ]
        self.disk_keywords = [
            "no space", "disk full", "permission denied", "file not found"
        ]
        self.system_keywords = [
            "memory", "ram", "cpu", "system", "os error"
        ]
    
    def classify_error(self, error_message: str, exception_type: type = None) -> ErrorType:
        """Classifies an error based on message content and exception type."""
        error_str = str(error_message).lower()
        
        # Check for quota errors first (most common)
        if any(keyword in error_str for keyword in self.quota_keywords):
            return ErrorType.QUOTA
        
        # Network related errors
        if any(keyword in error_str for keyword in self.network_keywords):
            return ErrorType.NETWORK
        
        # Disk/filesystem errors
        if any(keyword in error_str for keyword in self.disk_keywords):
            return ErrorType.DISK
        
        # System resource errors
        if any(keyword in error_str for keyword in self.system_keywords):
            return ErrorType.SYSTEM
        
        # Exception type based classification
        if exception_type:
            if issubclass(exception_type, (ConnectionError, TimeoutError)):
                return ErrorType.NETWORK
            if issubclass(exception_type, (OSError, IOError, PermissionError)):
                return ErrorType.DISK
            if issubclass(exception_type, (MemoryError,)):
                return ErrorType.SYSTEM
            if issubclass(exception_type, (FileNotFoundError, json.JSONDecodeError, 
                                         KeyError, ValueError, TypeError)):
                return ErrorType.CONFIG
        
        # Default to code bug if nothing else matches
        return ErrorType.CODE_BUG
    
    def is_retryable(self, error_type: ErrorType) -> bool:
        """Determines if an error type should be retried."""
        retryable_types = {ErrorType.QUOTA, ErrorType.NETWORK, ErrorType.DISK, ErrorType.SYSTEM}
        return error_type in retryable_types

class AssetVerifier:
    """
    Comprehensive asset verification to prevent duplicate AI requests.
    Validates file existence, integrity, and quality.
    """
    
    def __init__(self, config: Dict[str, Any]):
        verification_config = config.get("asset_verification", {})
        self.min_image_size_kb = verification_config.get("min_image_size_kb", 1)
        self.min_audio_duration = verification_config.get("min_audio_duration_seconds", 0.1)
        self.max_file_age_days = verification_config.get("max_file_age_days", 30)
    
    def verify_image_file(self, file_path: str) -> Tuple[bool, str]:
        """Verifies image file validity and quality."""
        if not os.path.exists(file_path):
            return False, f"Image file not found: {file_path}"
        
        try:
            # Check file size
            file_size_kb = os.path.getsize(file_path) / 1024
            if file_size_kb < self.min_image_size_kb:
                return False, f"Image too small: {file_size_kb:.1f}KB < {self.min_image_size_kb}KB"
            
            # Check if image can be opened and has valid dimensions
            if not PIL_AVAILABLE:
                return False, "PIL kütüphanesi yüklü değil"
            with Image.open(file_path) as img:
                width, height = img.size
                if width < 100 or height < 100:
                    return False, f"Image resolution too low: {width}x{height}"
            
            # Check file age
            file_age_days = (time.time() - os.path.getmtime(file_path)) / (24 * 3600)
            if file_age_days > self.max_file_age_days:
                return False, f"Image file too old: {file_age_days:.1f} days"
            
            return True, "Valid image file"
            
        except Exception as e:
            return False, f"Image verification failed: {e}"
    
    def verify_audio_file(self, file_path: str) -> Tuple[bool, str]:
        """Verifies audio file validity and quality."""
        if not os.path.exists(file_path):
            return False, f"Audio file not found: {file_path}"
        
        try:
            # Check file size
            file_size_kb = os.path.getsize(file_path) / 1024
            if file_size_kb < 1:
                return False, f"Audio file too small: {file_size_kb:.1f}KB"
            
            # Check audio properties
            if not MUTAGEN_AVAILABLE:
                return False, "mutagen kütüphanesi yüklü değil"
            audio = WAVE(file_path)
            if audio.info.length < self.min_audio_duration:
                return False, f"Audio too short: {audio.info.length:.2f}s < {self.min_audio_duration}s"
            
            # Check file age
            file_age_days = (time.time() - os.path.getmtime(file_path)) / (24 * 3600)
            if file_age_days > self.max_file_age_days:
                return False, f"Audio file too old: {file_age_days:.1f} days"
            
            return True, f"Valid audio file ({audio.info.length:.2f}s)"
            
        except Exception as e:
            return False, f"Audio verification failed: {e}"
    
    def calculate_file_hash(self, file_path: str) -> Optional[str]:
        """Calculates SHA256 hash of a file for integrity checking."""
        if not os.path.exists(file_path):
            return None
        
        try:
            sha256_hash = hashlib.sha256()
            with open(file_path, "rb") as f:
                for byte_block in iter(lambda: f.read(4096), b""):
                    sha256_hash.update(byte_block)
            return sha256_hash.hexdigest()
        except Exception as e:
            print(f"⚠️ Warning: Could not calculate hash for {file_path}: {e}")
            return None

class AtomicFileWriter:
    """
    Safe file operations to prevent race conditions and corruption.
    Uses temporary files and atomic renames.
    """
    
    @staticmethod
    def write_json(file_path: str, data: Dict[str, Any]) -> bool:
        """Atomically writes JSON data to file."""
        temp_path = f"{file_path}.tmp"
        try:
            # Write to temporary file first
            with open(temp_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
    
            # Windows uyumlu atomic rename
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                except OSError:
                    # Dosya kullanımda olabilir, kısa bekle
                    import time
                    time.sleep(0.1)
                    os.remove(file_path)
    
            os.rename(temp_path, file_path)
            return True
            
        except Exception as e:
            # Clean up temp file if it exists
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except:
                    pass
            print(f"❌ Error writing {file_path}: {e}")
            return False
    
    @staticmethod
    def read_json(file_path: str) -> Optional[Dict[str, Any]]:
        """Safely reads JSON file with error handling."""
        if not os.path.exists(file_path):
            return None
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"❌ Error reading {file_path}: {e}")
            return None

class RetryHandler:
    """
    Intelligent retry mechanism with exponential backoff.
    Only retries system errors, never code bugs.
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.retry_policies = config.get("retry_policies", {})
        self.error_classifier = ErrorClassifier(config)
    
    def execute_with_retry(self, func, *args, **kwargs):
        """
        Executes a function with intelligent retry logic.
        
        Args:
            func: Function to execute
            *args, **kwargs: Arguments for the function
            
        Returns:
            Function result
            
        Raises:
            Exception: If max retries exceeded or non-retryable error
        """
        last_exception = None
        
        for attempt in range(self._get_max_retries(ErrorType.SYSTEM)):
            try:
                return func(*args, **kwargs)
                
            except Exception as e:
                last_exception = e
                error_type = self.error_classifier.classify_error(str(e), type(e))
                
                # Never retry code bugs
                if error_type == ErrorType.CODE_BUG:
                    print(f"❌ CODE BUG DETECTED - NO RETRY: {e}")
                    raise e
                
                # Check if error is retryable
                if not self.error_classifier.is_retryable(error_type):
                    print(f"❌ NON-RETRYABLE ERROR: {e}")
                    raise e
                
                # Calculate wait time and retry
                if attempt < self._get_max_retries(error_type) - 1:
                    wait_time = self._calculate_wait_time(error_type, attempt)
                    print(f"⚠️ {error_type.value.upper()} ERROR (Attempt {attempt + 1}): {e}")
                    print(f"⏳ Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    print(f"❌ MAX RETRIES EXCEEDED for {error_type.value}: {e}")
                    raise e
        
        # Should never reach here, but just in case
        raise last_exception
    
    def _get_max_retries(self, error_type: ErrorType) -> int:
        """Gets max retries for error type from config."""
        policy_name = f"{error_type.value}_errors"
        policy = self.retry_policies.get(policy_name, 
                                       self.retry_policies.get("system_errors", {}))
        return policy.get("max_retries", 3)
    
    def _calculate_wait_time(self, error_type: ErrorType, attempt: int) -> float:
        """Calculates wait time with exponential backoff."""
        policy_name = f"{error_type.value}_errors"
        policy = self.retry_policies.get(policy_name, 
                                       self.retry_policies.get("system_errors", {}))
        
        base_delay = policy.get("base_delay_seconds", 30)
        use_exponential = policy.get("exponential_backoff", True)
        
        if use_exponential:
            return base_delay * (2 ** attempt)
        else:
            return base_delay

def log_message(message: str, level: str = "INFO", log_file: str = "oktabot.log"):
    """Unified logging system for all modules."""
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_entry = f"[{timestamp}][{level}] {message}"
    
    # Print to console
    print(log_entry)
    
    # Write to log file
    try:
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(log_entry + "\n")
    except Exception as e:
        print(f"⚠️ Warning: Could not write to log file: {e}")

def safe_subprocess_run(command_list: List[str], description: str = "command", 
                       timeout: int = 300) -> Tuple[bool, str]:
    """
    Safe subprocess execution with enhanced timeout and error handling.
    
    Args:
        command_list: Command and arguments
        description: Description for logging
        timeout: Timeout in seconds (default 5 minutes)
    
    Returns:
        Tuple[bool, str]: (success, output_or_error)
    """
    try:
        # Ensure all arguments are strings
        command_list = [str(arg) for arg in command_list]
        
        log_message(f"Running: {' '.join(command_list)}")
        
        # Enhanced error handling with retries for network operations
        max_retries = 3
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                result = subprocess.run(
                    command_list,
                    capture_output=True,
                    text=True,
                    check=False,
                    encoding='utf-8',
                    errors='replace',
                    timeout=timeout,
                    env=os.environ.copy()  # Copy environment variables
                )
                
                if result.returncode == 0:
                    log_message(f"✅ {description} completed successfully")
                    return True, result.stdout
                else:
                    # Check if it's a retryable error
                    error_msg = result.stderr.lower()
                    if any(keyword in error_msg for keyword in ['network', 'connection', 'timeout']):
                        if attempt < max_retries - 1:
                            log_message(f"⚠️ Network error, retrying in {retry_delay}s (attempt {attempt + 1}/{max_retries})", "WARNING")
                            time.sleep(retry_delay)
                            continue
                    
                    error_msg = f"Command failed (code {result.returncode}): {result.stderr}"
                    log_message(f"❌ {description} failed: {error_msg}", "ERROR")
                    return False, error_msg
                    
            except subprocess.TimeoutExpired:
                if attempt < max_retries - 1:
                    log_message(f"⏱️ Command timed out, retrying (attempt {attempt + 1}/{max_retries})", "WARNING")
                    time.sleep(retry_delay)
                    continue
                else:
                    error_msg = f"Command timed out after {timeout} seconds: {' '.join(command_list)}"
                    log_message(error_msg, "ERROR")
                    return False, error_msg
            
            except Exception as e:
                if attempt < max_retries - 1:
                    log_message(f"⚠️ Command error, retrying: {e} (attempt {attempt + 1}/{max_retries})", "WARNING")
                    time.sleep(retry_delay)
                    continue
                else:
                    error_msg = f"Unexpected error running {description}: {e}"
                    log_message(error_msg, "ERROR")
                    return False, error_msg
        
        # Should not reach here
        return False, "Max retries exceeded"
            
    except FileNotFoundError:
        error_msg = f"Command not found: {command_list[0]}"
        log_message(error_msg, "ERROR")
        return False, error_msg
    except Exception as e:
        error_msg = f"Unexpected error running {description}: {e}"
        log_message(error_msg, "ERROR")
        return False, error_msg
