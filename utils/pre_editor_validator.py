"""
Pre-Editor Asset Validator
Ensures ALL required assets are present and valid before editor starts.
Prevents costly duplicate AI requests and guarantees successful video production.
"""

import os
import json
from typing import Dict, List, Tuple, Any
from utils.common import AssetVerifier, log_message

class PreEditorValidationError(Exception):
    """Raised when asset validation fails before editor can start."""
    pass

class PreEditorValidator:
    """
    Comprehensive asset validation before editor execution.
    Guarantees that ALL required files exist and are valid.
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.asset_verifier = AssetVerifier(config)
        self.validation_enabled = config.get("asset_verification", {}).get("verify_before_editor", True)
    
    def validate_all_assets(self, project_file_path: str, audio_folder: str, 
                          image_folder: str) -> Tuple[bool, List[str]]:
        """
        Validates ALL required assets for video production.
        
        Args:
            project_file_path: Path to project.json
            audio_folder: Folder containing audio files
            image_folder: Folder containing image files
        
        Returns:
            Tuple[bool, List[str]]: (all_valid, list_of_issues)
        """
        if not self.validation_enabled:
            log_message("⚠️ Asset validation disabled in config", "WARNING")
            return True, []
        
        log_message("🔍 Starting comprehensive asset validation...")
        
        issues = []
        
        # 1. Validate project file
        project_valid, project_issues = self._validate_project_file(project_file_path)
        if not project_valid:
            issues.extend(project_issues)
            return False, issues
        
        # 2. Get required assets from project file
        try:
            with open(project_file_path, 'r', encoding='utf-8') as f:
                project_data = json.load(f)
        except Exception as e:
            issues.append(f"Cannot read project file: {e}")
            return False, issues
        
        required_assets = self._extract_required_assets(project_data)
        
        # 3. Validate audio assets
        audio_valid, audio_issues = self._validate_audio_assets(
            required_assets['audio'], audio_folder
        )
        if not audio_valid:
            issues.extend(audio_issues)
        
        # 4. Validate image assets  
        image_valid, image_issues = self._validate_image_assets(
            required_assets['images'], image_folder
        )
        if not image_valid:
            issues.extend(image_issues)
        
        # 5. Validate music (if configured)
        music_valid, music_issues = self._validate_music_assets()
        if not music_valid:
            issues.extend(music_issues)
        
        all_valid = len(issues) == 0
        
        if all_valid:
            log_message("✅ ALL ASSETS VALIDATED - READY FOR EDITOR!", "SUCCESS")
        else:
            log_message(f"❌ Asset validation failed: {len(issues)} issues found", "ERROR")
            for issue in issues:
                log_message(f"  - {issue}", "ERROR")
        
        return all_valid, issues
    
    def _validate_project_file(self, project_file_path: str) -> Tuple[bool, List[str]]:
        """Validates project file structure and content."""
        issues = []
        
        if not os.path.exists(project_file_path):
            issues.append(f"Project file not found: {project_file_path}")
            return False, issues
        
        try:
            with open(project_file_path, 'r', encoding='utf-8') as f:
                project_data = json.load(f)
        except json.JSONDecodeError as e:
            issues.append(f"Invalid JSON in project file: {e}")
            return False, issues
        except Exception as e:
            issues.append(f"Cannot read project file: {e}")
            return False, issues
        
        # Validate required structure
        required_keys = ["story_structure", "youtube_metadata", "ffmpeg_settings"]
        for key in required_keys:
            if key not in project_data:
                issues.append(f"Missing required key in project file: {key}")
        
        # Validate story structure
        story_structure = project_data.get("story_structure", {})
        required_sections = ["intro", "development", "conclusion"]
        for section in required_sections:
            if section not in story_structure:
                issues.append(f"Missing story section: {section}")
                continue
            
            section_data = story_structure[section]
            if "paragraphs" not in section_data:
                issues.append(f"Missing paragraphs in section: {section}")
                continue
            
            for paragraph in section_data["paragraphs"]:
                if "segments" not in paragraph:
                    issues.append(f"Missing segments in paragraph: {paragraph.get('paragraph_id', 'unknown')}")
                    continue
                
                for segment in paragraph["segments"]:
                    required_segment_keys = ["segment_id", "text", "visual_prompt"]
                    for seg_key in required_segment_keys:
                        if seg_key not in segment:
                            issues.append(f"Missing {seg_key} in segment: {segment.get('segment_id', 'unknown')}")
        
        return len(issues) == 0, issues
    
    def _extract_required_assets(self, project_data: Dict[str, Any]) -> Dict[str, List[str]]:
        """Extracts list of required asset files from project data."""
        required_assets = {
            "audio": [],
            "images": []
        }
        
        story_structure = project_data.get("story_structure", {})
        for section_data in story_structure.values():
            for paragraph in section_data.get("paragraphs", []):
                for segment in paragraph.get("segments", []):
                    segment_id = segment.get("segment_id")
                    if segment_id:
                        required_assets["audio"].append(f"{segment_id}.wav")
                        required_assets["images"].append(f"{segment_id}.png")
        
        return required_assets
    
    def _validate_audio_assets(self, required_files: List[str], 
                             audio_folder: str) -> Tuple[bool, List[str]]:
        """Validates all required audio files."""
        issues = []
        
        if not os.path.exists(audio_folder):
            issues.append(f"Audio folder not found: {audio_folder}")
            return False, issues
        
        log_message(f"📀 Validating {len(required_files)} audio files...")
        
        for audio_file in required_files:
            audio_path = os.path.join(audio_folder, audio_file)
            is_valid, message = self.asset_verifier.verify_audio_file(audio_path)
            
            if not is_valid:
                issues.append(f"Audio: {message}")
            else:
                log_message(f"  ✅ {audio_file}: {message}")
        
        return len(issues) == 0, issues
    
    def _validate_image_assets(self, required_files: List[str], 
                             image_folder: str) -> Tuple[bool, List[str]]:
        """Validates all required image files."""
        issues = []
        
        if not os.path.exists(image_folder):
            issues.append(f"Image folder not found: {image_folder}")
            return False, issues
        
        log_message(f"🖼️ Validating {len(required_files)} image files...")
        
        for image_file in required_files:
            image_path = os.path.join(image_folder, image_file)
            is_valid, message = self.asset_verifier.verify_image_file(image_path)
            
            if not is_valid:
                issues.append(f"Image: {message}")
            else:
                log_message(f"  ✅ {image_file}: {message}")
        
        return len(issues) == 0, issues
    
    def _validate_music_assets(self) -> Tuple[bool, List[str]]:
        """Validates music folder and files."""
        issues = []
        
        music_folder = self.config.get("music_folder_path", "./music")
        
        if not os.path.exists(music_folder):
            issues.append(f"Music folder not found: {music_folder}")
            return False, issues
        
        # Check for music files
        music_extensions = ['.mp3', '.wav', '.m4a', '.aac']
        music_files = []
        
        try:
            for file in os.listdir(music_folder):
                if any(file.lower().endswith(ext) for ext in music_extensions):
                    music_files.append(file)
        except Exception as e:
            issues.append(f"Cannot read music folder: {e}")
            return False, issues
        
        if not music_files:
            issues.append(f"No music files found in: {music_folder}")
            return False, issues
        
        log_message(f"🎵 Found {len(music_files)} music files in {music_folder}")
        return True, []
    
    def generate_missing_asset_report(self, issues: List[str]) -> Dict[str, List[str]]:
        """
        Generates a categorized report of missing assets.
        
        Returns:
            Dict with categories: audio_missing, images_missing, other_issues
        """
        report = {
            "audio_missing": [],
            "images_missing": [],
            "other_issues": []
        }
        
        for issue in issues:
            if "Audio:" in issue and "not found" in issue:
                report["audio_missing"].append(issue.replace("Audio: Audio file not found: ", ""))
            elif "Image:" in issue and "not found" in issue:
                report["images_missing"].append(issue.replace("Image: Image file not found: ", ""))
            else:
                report["other_issues"].append(issue)
        
        return report
    
    def suggest_recovery_actions(self, issues: List[str]) -> List[str]:
        """Suggests specific actions to fix validation issues."""
        suggestions = []
        
        report = self.generate_missing_asset_report(issues)
        
        if report["audio_missing"]:
            suggestions.append("🎤 Run narrator module to generate missing audio files")
            
        if report["images_missing"]:
            suggestions.append("🎨 Run image_director module to generate missing images")
            
        if report["other_issues"]:
            if any("Music folder" in issue for issue in report["other_issues"]):
                suggestions.append("🎵 Create music folder and add background music files")
            
            if any("Project file" in issue for issue in report["other_issues"]):
                suggestions.append("📋 Fix project.json file structure or regenerate with director module")
        
        return suggestions