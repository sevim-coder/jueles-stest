# producer.py (Enhanced with Smart Asset Management and Robust Error Handling)

import os
import sys
import json
import argparse
import datetime
import shutil
from typing import Optional, List, Dict, Any

# Import our new centralized utilities
sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))
from utils.common import (
    log_message, safe_subprocess_run, AtomicFileWriter, 
    RetryHandler, AssetVerifier, ErrorType
)
from utils.pre_editor_validator import PreEditorValidator, PreEditorValidationError

class Producer:
    """
    Enhanced Oktabot video production system with smart asset management.
    Features:
    - Intelligent asset verification before editor
    - Zero duplicate AI requests
    - Robust error handling with smart retry
    - Resume capability
    - Cost optimization
    """
    
    def __init__(self, config_path="config.json", weekly_guide_path=None):
        self.config = self._load_config(config_path)
        self.config_path = config_path
        self.weekly_guide = self._load_config(weekly_guide_path) if weekly_guide_path else None
        
        # Initialize components
        self.retry_handler = RetryHandler(self.config)
        self.asset_verifier = AssetVerifier(self.config)
        self.pre_editor_validator = PreEditorValidator(self.config)
        
        # Project state
        self.project_path = ""
        self.status = {}
        self.hash_status = {}
        self.is_manual = not bool(self.weekly_guide)
        self.channel_name = ""
        self.video_topic = ""
        self.target_char_count = 0

    def _load_config(self, file_path: str) -> Optional[Dict[str, Any]]:
        """Loads configuration with enhanced error handling."""
        if not file_path:
            return None
            
        config = AtomicFileWriter.read_json(file_path)
        if config is None:
            log_message(f"❌ CRITICAL: Cannot load config from {file_path}", "CRITICAL")
            sys.exit(1)
        
        log_message(f"✅ Configuration loaded from {file_path}")
        return config

    def select_channel_manually(self) -> str:
        """Enhanced channel selection with better error handling."""
        print("\n🎬 CHANNEL SELECTION")
        print("=" * 40)
        channels = list(self.config["channels"].keys())
        for i, name in enumerate(channels, 1):
            print(f"  {i}. {name}")
        print("=" * 40)
        
        while True:
            try:
                choice = input(f"Which channel? (1-{len(channels)}): ").strip()
                choice_int = int(choice)
                if 1 <= choice_int <= len(channels):
                    selected = channels[choice_int - 1]
                    log_message(f"Selected channel: {selected}")
                    return selected
                else:
                    print(f"❌ Please enter 1-{len(channels)}")
            except (ValueError, IndexError):
                print(f"❌ Please enter a valid number (1-{len(channels)})")
            except KeyboardInterrupt:
                log_message("Operation cancelled by user")
                sys.exit(1)

    def check_incomplete_projects(self, channel_slug: str) -> List[Dict[str, str]]:
        """Enhanced incomplete project detection."""
        channel_folder = os.path.join("channels", channel_slug)
        if not os.path.exists(channel_folder):
            return []
        
        incomplete_projects = []
        try:
            for project_name in os.listdir(channel_folder):
                project_path = os.path.join(channel_folder, project_name)
                if not os.path.isdir(project_path):
                    continue
                
                status_data = AtomicFileWriter.read_json(
                    os.path.join(project_path, "status.json")
                )
                
                if status_data:
                    completed_steps = status_data.get("completed_steps", [])
                    if completed_steps and "upload" not in completed_steps:
                        incomplete_projects.append({
                            "name": project_name,
                            "path": project_path,
                            "last_step": completed_steps[-1] if completed_steps else "Not started"
                        })
                        
        except OSError as e:
            log_message(f"Warning: Error reading channel folder {channel_folder}: {e}", "WARNING")
        
        return incomplete_projects

    def select_incomplete_project(self, incomplete_projects: List[Dict[str, str]]) -> Optional[Dict[str, str]]:
        """Enhanced project selection interface."""
        print("\n📋 INCOMPLETE PROJECTS FOUND!")
        print("=" * 50)
        for i, project in enumerate(incomplete_projects, 1):
            print(f"  {i}. {project['name']} (Last: {project['last_step']})")
        print("  0. Start new project")
        print("=" * 50)
        
        while True:
            try:
                choice = input("Continue which project? (0 for new): ").strip()
                choice_int = int(choice)
                if choice_int == 0:
                    return None
                if 1 <= choice_int <= len(incomplete_projects):
                    return incomplete_projects[choice_int - 1]
                print(f"❌ Please enter 0-{len(incomplete_projects)}")
            except ValueError:
                print("❌ Please enter a valid number")
            except KeyboardInterrupt:
                log_message("Operation cancelled by user")
                sys.exit(1)

    def load_status(self):
        """Enhanced status loading with atomic operations."""
        status_file = os.path.join(self.project_path, "status.json")
        self.status = AtomicFileWriter.read_json(status_file) or {"completed_steps": []}
        log_message("Status loaded successfully")

    def load_hash_status(self):
        """Enhanced hash status loading."""
        hash_file = os.path.join(self.project_path, "integrity.json")
        self.hash_status = AtomicFileWriter.read_json(hash_file) or {"file_hashes": {}}
        log_message("Hash status loaded successfully")

    def save_status(self) -> bool:
        """Enhanced status saving with atomic operations."""
        status_file = os.path.join(self.project_path, "status.json")
        return AtomicFileWriter.write_json(status_file, self.status)

    def save_hash_status(self) -> bool:
        """Enhanced hash status saving."""
        hash_file = os.path.join(self.project_path, "integrity.json")
        return AtomicFileWriter.write_json(hash_file, self.hash_status)

    def is_step_complete(self, step_name: str) -> bool:
        """Checks if a production step is complete."""
        return step_name in self.status.get("completed_steps", [])

    def complete_step(self, step_name: str, files_to_hash: List[str] = None):
        """Enhanced step completion with file integrity tracking."""
        if step_name not in self.status["completed_steps"]:
            self.status["completed_steps"].append(step_name)
        
        # Calculate hashes for important files
        if files_to_hash:
            for file_path in files_to_hash:
                if os.path.exists(file_path):
                    file_hash = self.asset_verifier.calculate_file_hash(file_path)
                    if file_hash:
                        self.hash_status["file_hashes"][file_path] = file_hash
        
        # Save status atomically
        if not self.save_status():
            log_message(f"⚠️ Warning: Could not save status for step {step_name}", "WARNING")
        
        if files_to_hash and not self.save_hash_status():
            log_message(f"⚠️ Warning: Could not save hash status for step {step_name}", "WARNING")
        
        log_message(f"✅ Step completed: {step_name}")

    def verify_file_integrity(self) -> bool:
        """Enhanced file integrity verification."""
        if not self.hash_status.get("file_hashes"):
            return True
        
        log_message("🔍 Verifying file integrity...")
        
        for file_path, saved_hash in self.hash_status["file_hashes"].items():
            if not os.path.exists(file_path):
                log_message(f"❌ Missing file: {file_path}", "WARNING")
                return False
            
            current_hash = self.asset_verifier.calculate_file_hash(file_path)
            if current_hash != saved_hash:
                log_message(f"❌ File modified: {file_path}", "WARNING")
                return False
        
        log_message("✅ All files verified")
        return True

    def run_module_with_retry(self, module_name: str, command_args: List[str]) -> bool:
        """Runs a module with intelligent retry logic."""
        def run_command():
            success, output = safe_subprocess_run(command_args, f"{module_name} module")
            if not success:
                raise Exception(f"{module_name} failed: {output}")
            return output
        
        try:
            result = self.retry_handler.execute_with_retry(run_command)
            log_message(f"✅ {module_name} completed successfully")
            return True
        except Exception as e:
            log_message(f"❌ {module_name} failed permanently: {e}", "ERROR")
            return False

    def setup_project(self) -> bool:
        """Enhanced project setup with better error handling."""
        try:
            if self.is_manual:
                log_message("🎬 Starting Manual Mode...")
                self.channel_name = self.select_channel_manually()
                channel_slug = self.config["channels"][self.channel_name]["slug"]
                
                incomplete_projects = self.check_incomplete_projects(channel_slug)
                selected_project = None
                if incomplete_projects:
                    selected_project = self.select_incomplete_project(incomplete_projects)
                
                if selected_project:
                    self.project_path = selected_project["path"]
                    log_message(f"Resuming project: {self.project_path}")
                else:
                    # New project setup
                    self.video_topic = input("Video topic: ").strip()
                    try:
                        self.target_char_count = int(input("Target character count: ").strip())
                    except ValueError:
                        log_message("❌ Character count must be a number!", "CRITICAL")
                        return False
                    
                    if not self.video_topic or not self.target_char_count:
                        log_message("❌ Topic and character count required!", "CRITICAL")
                        return False
                    
                    project_slug = self._slugify(self.video_topic)[:50]
                    self.project_path = os.path.join("channels", channel_slug, project_slug)
            else:
                log_message("🤖 Starting Automatic Mode...")
                today = datetime.datetime.now().strftime("%A").lower()
                if today not in self.weekly_guide:
                    log_message(f"No task for {today} in weekly_guide.json")
                    return False
                
                task = self.weekly_guide[today]
                self.channel_name = task["channel_name"]
                self.video_topic = task["video_topic"]
                self.target_char_count = task["target_char_count"]
                
                channel_slug = self.config["channels"][self.channel_name]["slug"]
                project_slug = self._slugify(self.video_topic)[:50]
                self.project_path = os.path.join("channels", channel_slug, project_slug)

            os.makedirs(self.project_path, exist_ok=True)
            log_message(f"Project path: {self.project_path}")
            return True
            
        except Exception as e:
            log_message(f"❌ Project setup failed: {e}", "CRITICAL")
            return False

    def _slugify(self, text: str) -> str:
        """Converts text to URL-friendly slug."""
        return "".join(c for c in text if c.isalnum() or c in " _-").rstrip().replace(" ", "_")

    def run_pipeline(self):
        """Enhanced main production pipeline with smart asset management."""
        log_message("🚀 Starting Oktabot Production Pipeline")
        
        # Setup project
        if not self.setup_project():
            return
        
        self.load_status()
        self.load_hash_status()
        
        # Verify file integrity if resuming
        if self.status["completed_steps"] and not self.verify_file_integrity():
            reset = 'y'
            if self.is_manual:
                reset = input("⚠️ Files modified. Reset project? (y/N): ").lower()
            else:
                log_message("File integrity check failed. Resetting project...", "WARNING")
            
            if reset == 'y':
                log_message("Resetting project folder...")
                shutil.rmtree(self.project_path)
                os.makedirs(self.project_path, exist_ok=True)
                self.status = {"completed_steps": []}
                self.hash_status = {"file_hashes": {}}
            else:
                log_message("Operation cancelled")
                return

        # Define file paths
        script_path = os.path.join(self.project_path, "script.txt")
        project_file_path = os.path.join(self.project_path, "project.json")
        audio_folder = os.path.join(self.project_path, "audio")
        image_folder = os.path.join(self.project_path, "images")
        final_video_path = os.path.join(self.project_path, "final_video.mp4")

        try:
            # Step 1: Scriptwriting
            if not self.is_step_complete("scriptwriting"):
                log_message("🎯 Step 1: Scriptwriting")
                command = [
                    "python", "modules/scriptwriter.py",
                    self.channel_name, self.video_topic, str(self.target_char_count),
                    "--output_path", script_path,
                    "--config_path", self.config_path
                ]
                if not self.run_module_with_retry("scriptwriter", command):
                    raise Exception("Scriptwriter module failed")
                self.complete_step("scriptwriting", [script_path])
            else:
                log_message("✅ Scriptwriting already complete")

            # Step 2: Direction  
            if not self.is_step_complete("direction"):
                log_message("🎯 Step 2: Direction")
                command = [
                    "python", "modules/director.py",
                    script_path, project_file_path,
                    "--channel_name", self.channel_name,
                    "--config_path", self.config_path
                ]
                if not self.run_module_with_retry("director", command):
                    raise Exception("Director module failed")
                self.complete_step("direction", [project_file_path])
            else:
                log_message("✅ Direction already complete")

            # Step 3: Asset Production (Audio & Images)
            if not self.is_step_complete("asset_production"):
                log_message("🎯 Step 3: Asset Production")
                os.makedirs(audio_folder, exist_ok=True)
                os.makedirs(image_folder, exist_ok=True)
                
                # 3a: Narration
                log_message("🎙️ Step 3a: Narration")
                command = [
                    "python", "modules/narrator.py",
                    project_file_path, audio_folder,
                    "--config_path", self.config_path
                ]
                if not self.run_module_with_retry("narrator", command):
                    raise Exception("Narrator module failed")
                
                # 3b: Image Generation
                log_message("🎨 Step 3b: Image Generation")
                command = [
                    "python", "modules/image_director.py",
                    project_file_path, image_folder,
                    "--config_path", self.config_path
                ]
                if not self.run_module_with_retry("image_director", command):
                    raise Exception("Image Director module failed")
                
                # Collect all generated files for hash tracking
                audio_files = [os.path.join(audio_folder, f) for f in os.listdir(audio_folder) 
                             if f.endswith('.wav')]
                image_files = [os.path.join(image_folder, f) for f in os.listdir(image_folder) 
                             if f.endswith('.png')]
                
                self.complete_step("asset_production", audio_files + image_files)
            else:
                log_message("✅ Asset Production already complete")

            # CRITICAL: Pre-Editor Asset Validation
            log_message("🔍 Step 4: Pre-Editor Asset Validation")
            validation_success, issues = self.pre_editor_validator.validate_all_assets(
                project_file_path, audio_folder, image_folder
            )
            
            if not validation_success:
                log_message("❌ CRITICAL: Asset validation failed before editor!", "CRITICAL")
                log_message("Issues found:", "ERROR")
                for issue in issues:
                    log_message(f"  - {issue}", "ERROR")
                
                # Generate recovery suggestions
                suggestions = self.pre_editor_validator.suggest_recovery_actions(issues)
                log_message("💡 Suggested recovery actions:", "INFO")
                for suggestion in suggestions:
                    log_message(f"  - {suggestion}", "INFO")
                
                raise PreEditorValidationError(f"Asset validation failed: {len(issues)} issues")

            # Step 5: Editing (Only runs if ALL assets are validated)
            if not self.is_step_complete("editing"):
                log_message("🎯 Step 5: Video Editing & Assembly")
                command = [
                    "python", "modules/editor.py",
                    project_file_path, audio_folder, image_folder, final_video_path,
                    "--config_path", self.config_path
                ]
                if not self.run_module_with_retry("editor", command):
                    raise Exception("Editor module failed")
                self.complete_step("editing", [final_video_path])
            else:
                log_message("✅ Editing already complete")

            # Step 6: Upload
            if not self.is_step_complete("upload"):
                log_message("🎯 Step 6: YouTube Upload")
                if not os.path.exists("service_account.json"):
                    raise Exception("YouTube service account file not found")
                
                command = [
                    "python", "modules/uploader.py",
                    final_video_path, project_file_path
                ]
                if not self.run_module_with_retry("uploader", command):
                    raise Exception("Uploader module failed")
                self.complete_step("upload")
            else:
                log_message("✅ Upload already complete")

            # Success!
            log_message("🎉 PRODUCTION COMPLETED SUCCESSFULLY!", "SUCCESS")
            print(f"\n{'='*60}")
            print("🎬 VIDEO PRODUCTION COMPLETE!")
            print(f"{'='*60}")
            print(f"📁 Project: {self.project_path}")
            print(f"🎥 Video: {final_video_path}")
            print(f"📊 Steps: {', '.join(self.status['completed_steps'])}")
            print(f"{'='*60}")

        except PreEditorValidationError as e:
            log_message(f"❌ ASSET VALIDATION ERROR: {e}", "CRITICAL")
            log_message("Fix missing assets and restart to continue", "INFO")
            sys.exit(1)
        except Exception as e:
            log_message(f"❌ PRODUCTION FAILED: {e}", "CRITICAL")
            print(f"\n💡 TROUBLESHOOTING TIPS:")
            print(f"1. Check log file for details")
            print(f"2. Test individual modules")
            print(f"3. Verify API keys and config")
            print(f"4. Check project folder: {self.project_path}")
            sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Enhanced Oktabot Production System with Smart Asset Management"
    )
    parser.add_argument("--guide", help="Path to weekly_guide.json for automatic mode")
    args = parser.parse_args()
    
    # Initialize logging
    log_message("="*60)
    log_message("🚀 OKTABOT ENHANCED PRODUCTION SYSTEM")
    log_message("="*60)
    
    producer = Producer(config_path="config.json", weekly_guide_path=args.guide)
    producer.run_pipeline()