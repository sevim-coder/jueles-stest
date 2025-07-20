# modules/uploader.py (Enhanced with centralized utils and robust error handling)

import os
import sys
import json
import argparse
import pickle
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials

# Ana proje klasörünü sys.path'e ekle
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)
from utils.common import log_message, RetryHandler, ErrorClassifier, AtomicFileWriter, AssetVerifier

def log_error(message):
    """Prints error messages to stderr."""
    log_message(message, "ERROR")
    print(message, file=sys.stderr)

class Uploader:
    """
    Enhanced YouTube uploader with centralized utils and smart error handling.
    Handles authentication, metadata processing, and reliable video upload.
    """
    
    # YouTube API scopes and settings
    SCOPES = ["https://www.googleapis.com/auth/youtube.upload"]
    API_SERVICE_NAME = "youtube"
    API_VERSION = "v3"

    def __init__(self, client_secrets_file="service_account.json", config_path="config.json"):
        log_message("📤 Enhanced Uploader initializing...")
        
        # Load configuration
        self.config_data = AtomicFileWriter.read_json(config_path)
        if not self.config_data:
            log_error(f"❌ CRITICAL: Cannot load config from {config_path}")
            sys.exit(1)
        
        # Initialize centralized components
        self.retry_handler = RetryHandler(self.config_data)
        self.error_classifier = ErrorClassifier(self.config_data)
        self.asset_verifier = AssetVerifier(self.config_data)
        
        # Validate client secrets file
        self.client_secrets_file = client_secrets_file
        if not os.path.exists(self.client_secrets_file):
            log_error(f"❌ CRITICAL: Client secrets file not found: {self.client_secrets_file}")
            sys.exit(1)
        
        # Initialize YouTube service
        self.youtube_service = self._get_authenticated_service()
        log_message("✅ Enhanced Uploader ready for publishing")

    def _get_authenticated_service(self):
        """Enhanced YouTube API authentication with retry capability."""
        log_message("🔐 Authenticating with YouTube API...")
        
        def authenticate():
            creds = None
            token_pickle_path = 'token.pickle'

            # Load existing credentials
            if os.path.exists(token_pickle_path):
                try:
                    with open(token_pickle_path, 'rb') as token:
                        creds = pickle.load(token)
                    log_message("🔑 Existing credentials loaded")
                except Exception as e:
                    log_message(f"⚠️ Could not load existing credentials: {e}", "WARNING")
                    creds = None
            
            # Refresh or get new credentials
            if not creds or not creds.valid:
                if creds and creds.expired and creds.refresh_token:
                    try:
                        creds.refresh(Request())
                        log_message("🔄 Credentials refreshed")
                    except Exception as e:
                        log_message(f"⚠️ Could not refresh credentials: {e}", "WARNING")
                        creds = None
                
                if not creds:
                    log_message("🆕 Starting new OAuth flow...")
                    flow = InstalledAppFlow.from_client_secrets_file(
                        self.client_secrets_file, self.SCOPES
                    )
                    creds = flow.run_local_server(port=0)
                    log_message("✅ New credentials obtained")
                
                # Save credentials for next run
                try:
                    with open(token_pickle_path, 'wb') as token:
                        pickle.dump(creds, token)
                    log_message("💾 Credentials saved for future use")
                except Exception as e:
                    log_message(f"⚠️ Could not save credentials: {e}", "WARNING")
            
            # Build and return service
            service = build(self.API_SERVICE_NAME, self.API_VERSION, credentials=creds)
            log_message("✅ YouTube API service ready")
            return service

        # Use retry handler for authentication
        try:
            return self.retry_handler.execute_with_retry(authenticate)
        except Exception as e:
            log_error(f"❌ CRITICAL: YouTube authentication failed: {e}")
            sys.exit(1)

    def read_project_file(self, project_file_path: str) -> dict:
        """Enhanced project file reading with validation."""
        log_message(f"📖 Reading project metadata: {project_file_path}")
        
        if not os.path.exists(project_file_path):
            log_error(f"❌ CRITICAL: Project file not found: {project_file_path}")
            sys.exit(1)
        
        project_data = AtomicFileWriter.read_json(project_file_path)
        if not project_data:
            log_error(f"❌ CRITICAL: Could not read project file: {project_file_path}")
            sys.exit(1)
        
        # Validate required metadata structure
        if "youtube_metadata" not in project_data:
            log_error("❌ CRITICAL: 'youtube_metadata' section missing from project file")
            sys.exit(1)
        
        log_message("✅ Project metadata loaded and validated")
        return project_data

    def _validate_video_file(self, video_path: str) -> dict:
        """Comprehensive video file validation."""
        log_message(f"🔍 Validating video file: {video_path}")
        
        if not os.path.exists(video_path):
            log_error(f"❌ CRITICAL: Video file not found: {video_path}")
            sys.exit(1)

        try:
            file_size = os.path.getsize(video_path)
            if file_size == 0:
                log_error(f"❌ CRITICAL: Video file is empty: {video_path}")
                sys.exit(1)
            
            file_size_mb = file_size / (1024 * 1024)
            
            # Check file size limits (YouTube allows up to 256GB, but let's be reasonable)
            if file_size_mb > 10240:  # 10GB limit
                log_error(f"❌ CRITICAL: Video file too large: {file_size_mb:.2f}MB (max 10GB)")
                sys.exit(1)
            
            if file_size_mb < 1:  # Minimum 1MB
                log_error(f"❌ CRITICAL: Video file too small: {file_size_mb:.2f}MB (min 1MB)")
                sys.exit(1)
            
            log_message(f"✅ Video file validated: {file_size_mb:.2f}MB")
            
            return {
                "size_bytes": file_size,
                "size_mb": file_size_mb,
                "path": video_path
            }
            
        except Exception as e:
            log_error(f"❌ CRITICAL: Cannot access video file: {e}")
            sys.exit(1)

    def _validate_and_prepare_metadata(self, project_data: dict) -> dict:
        """Enhanced metadata validation and preparation."""
        log_message("📋 Validating and preparing upload metadata...")
        
        metadata = project_data.get("youtube_metadata", {})
        
        # Required fields validation
        required_fields = ["title", "description"]
        for field in required_fields:
            if not metadata.get(field):
                log_error(f"❌ CRITICAL: Required metadata field '{field}' is missing or empty")
                sys.exit(1)
        
        # Category mapping
        category_map = {
            "Film & Animation": "1",
            "Autos & Vehicles": "2", 
            "Music": "10",
            "Pets & Animals": "15",
            "Sports": "17",
            "Gaming": "20",
            "People & Blogs": "22",
            "Comedy": "23",
            "Entertainment": "24",
            "News & Politics": "25",
            "Howto & Style": "26",
            "Education": "27",
            "Science & Technology": "28",
        }
        
        # Get category from config defaults if not specified
        category_name = metadata.get("category") or self.config_data.get("youtube_defaults", {}).get("category", "Education")
        category_id = category_map.get(category_name, "27")  # Default to Education
        
        # Privacy status validation
        valid_privacy_statuses = ["private", "public", "unlisted"]
        privacy_status = metadata.get("privacy_status") or self.config_data.get("youtube_defaults", {}).get("privacy_status", "private")
        
        if privacy_status not in valid_privacy_statuses:
            log_error(f"❌ CRITICAL: Invalid privacy status '{privacy_status}'. Must be: {valid_privacy_statuses}")
            sys.exit(1)
        
        # Prepare upload body
        upload_body = {
            "snippet": {
                "title": metadata.get("title"),
                "description": metadata.get("description"),
                "tags": metadata.get("tags", []),
                "categoryId": category_id
            },
            "status": {
                "privacyStatus": privacy_status,
                "selfDeclaredMadeForKids": False  # Assume content is not for kids
            }
        }
        
        log_message(f"✅ Metadata prepared:")
        log_message(f"  📺 Title: {upload_body['snippet']['title']}")
        log_message(f"  🔒 Privacy: {upload_body['status']['privacyStatus']}")
        log_message(f"  📂 Category: {category_name} (ID: {category_id})")
        log_message(f"  🏷️ Tags: {len(upload_body['snippet']['tags'])} tags")
        
        return upload_body

    def upload_video(self, video_path: str, project_file_path: str):
        """Enhanced video upload with comprehensive error handling and retry."""
        log_message("🚀 Starting enhanced video upload process...")
        
        # Validate video file
        video_info = self._validate_video_file(video_path)
        
        # Read and validate project data
        project_data = self.read_project_file(project_file_path)
        
        # Prepare metadata
        upload_body = self._validate_and_prepare_metadata(project_data)
        
        def perform_upload():
            """Inner upload function for retry mechanism."""
            log_message(f"📤 Starting upload: '{upload_body['snippet']['title']}'")
            log_message(f"   📊 File size: {video_info['size_mb']:.2f}MB")
            
            # Create media upload object
            try:
                media = MediaFileUpload(
                    video_path, 
                    chunksize=-1,  # Upload entire file at once for files < 8MB
                    resumable=True
                )
            except Exception as e:
                raise Exception(f"Failed to create media upload object: {e}")
            
            # Create upload request
            try:
                request = self.youtube_service.videos().insert(
                    part=",".join(upload_body.keys()),
                    body=upload_body,
                    media_body=media
                )
            except Exception as e:
                raise Exception(f"Failed to create upload request: {e}")
            
            # Execute upload with progress tracking
            response = None
            last_progress = 0
            
            try:
                while response is None:
                    status, response = request.next_chunk()
                    if status:
                        progress = int(status.progress() * 100)
                        if progress != last_progress:
                            log_message(f"  📈 Upload progress: {progress}%")
                            last_progress = progress

                if not response or not response.get('id'):
                    raise Exception("Upload completed but no video ID received")

                video_id = response.get('id')
                video_url = f"https://www.youtube.com/watch?v={video_id}"
                
                log_message("🎉 UPLOAD COMPLETE!")
                log_message(f"✅ Video ID: {video_id}")
                log_message(f"🔗 URL: {video_url}")
                
                return {
                    "video_id": video_id,
                    "video_url": video_url,
                    "upload_success": True
                }
                
            except Exception as e:
                raise Exception(f"Upload execution failed: {e}")

        # Use retry handler for upload
        try:
            result = self.retry_handler.execute_with_retry(perform_upload)
            
            # Save upload results to project folder if possible
            try:
                project_folder = os.path.dirname(project_file_path)
                upload_info_path = os.path.join(project_folder, "upload_info.json")
                AtomicFileWriter.write_json(upload_info_path, result)
                log_message(f"📄 Upload info saved: {upload_info_path}")
            except Exception as e:
                log_message(f"⚠️ Could not save upload info: {e}", "WARNING")
            
            return result
            
        except Exception as e:
            log_error(f"❌ CRITICAL: Video upload failed permanently: {e}")
            sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Enhanced YouTube Uploader with centralized utils and robust error handling"
    )
    parser.add_argument("video_path", help="Path to final video file")
    parser.add_argument("project_file", help="Path to project.json with metadata")
    # Config dosyasını proje root'undan otomatik bul
    default_config = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config.json")
    parser.add_argument("--config_path", default=default_config, help="Config file path")
    args = parser.parse_args()

    try:
        log_message("📤 ENHANCED UPLOADER MODULE STARTED")
        log_message("="*60)
        
        uploader = Uploader(config_path=args.config_path)
        result = uploader.upload_video(args.video_path, args.project_file)
        
        log_message("="*60)
        log_message("🎉 PUBLISHING PROCESS COMPLETE!")
        log_message(f"🔗 Your video: {result['video_url']}")
        log_message("="*60)
        
    except KeyboardInterrupt:
        log_error("\n⏹️ Upload cancelled by user")
        sys.exit(1)
    except Exception as e:
        log_error(f"\n❌ Critical error in Enhanced Uploader: {e}")
        sys.exit(1)
