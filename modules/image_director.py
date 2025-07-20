# modules/image_director.py (Enhanced with centralized utils and smart asset management)

import os
import sys
import json
import argparse
from google import genai
from google.genai import types

# Ana proje klasörünü sys.path'e ekle
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)
from utils.common import RateLimiter, ErrorClassifier, RetryHandler, AssetVerifier, log_message

def log_error(message):
    """Prints error messages to stderr."""
    log_message(message, "ERROR")
    print(message, file=sys.stderr)

class ImageDirector:
    """
    Enhanced Image Director with centralized utils and smart asset management.
    Only generates missing/corrupt images to save costs and time.
    """
    
    def __init__(self, config_data):
        log_message("🎨 Image Director initializing with smart asset management...")
        
        api_key = os.getenv("GEMINI_API_KEY")
        if not api_key:
            log_error("❌ CRITICAL: GEMINI_API_KEY environment variable not found!")
            sys.exit(1)
        
        try:
            self.client = genai.Client(api_key=api_key)
            log_message("🤖 AI image generation system ready")
        except Exception as e:
            log_error(f"❌ CRITICAL: Could not initialize AI client: {e}")
            sys.exit(1)
        
        # Setup components using centralized utils
        image_config = config_data.get("ai_models", {}).get("image_director", {})
        self.model_name = image_config.get("model_name", "imagen-3.0-generate-002")
        rate_limit = image_config.get("rate_limit", {})
        
        self.rate_limiter = RateLimiter(
            requests_per_minute=rate_limit.get("requests_per_minute", 5),
            cooldown_seconds=rate_limit.get("cooldown_seconds", 12)
        )
        
        self.error_classifier = ErrorClassifier(config_data)
        self.retry_handler = RetryHandler(config_data)
        self.asset_verifier = AssetVerifier(config_data)
        
        log_message(f"⚙️ Image Director ready - Model: {self.model_name}")
        log_message(f"⏱️ Rate limit: {rate_limit.get('requests_per_minute', 5)} req/min, {rate_limit.get('cooldown_seconds', 12)}s cooldown")

    def read_project_file(self, project_file_path: str) -> dict:
        """Reads project file with enhanced error handling."""
        log_message(f"📖 Reading project file: {project_file_path}")
        
        if not os.path.exists(project_file_path):
            log_error(f"❌ CRITICAL: Project file not found: {project_file_path}")
            sys.exit(1)
        
        try:
            with open(project_file_path, 'r', encoding='utf-8') as f:
                project_data = json.load(f)
            log_message("✅ Project file loaded successfully")
            return project_data
        except json.JSONDecodeError as e:
            log_error(f"❌ CRITICAL: Invalid JSON in project file: {e}")
            sys.exit(1)
        except Exception as e:
            log_error(f"❌ CRITICAL: Failed to read project file: {e}")
            sys.exit(1)

    def list_required_images(self, project_data: dict) -> list:
        """Extracts required images from project data."""
        required_images = []
        try:
            story_structure = project_data["story_structure"]
            for section_data in story_structure.values():
                for paragraph in section_data["paragraphs"]:
                    for segment in paragraph["segments"]:
                        required_images.append({
                            "id": segment["segment_id"],
                            "prompt": segment["visual_prompt"],
                            "aspect_ratio": segment.get("aspect_ratio", "16:9")
                        })
            return required_images
        except KeyError as e:
            log_error(f"❌ CRITICAL: Malformed project file, missing key: {e}")
            sys.exit(1)

    def check_existing_images(self, image_folder: str, required_images: list) -> list:
        """Smart checking for existing images using centralized AssetVerifier."""
        log_message(f"🔍 Checking existing images in: {image_folder}")
        
        images_to_generate = []
        existing_count = 0
        
        for image_info in required_images:
            file_name = f"{image_info['id']}.png"
            file_path = os.path.join(image_folder, file_name)
            
            # Use centralized asset verification
            is_valid, message = self.asset_verifier.verify_image_file(file_path)
            
            if is_valid:
                log_message(f"  ✅ {file_name}: {message}")
                existing_count += 1
            else:
                if "not found" in message:
                    log_message(f"  ❌ Missing: {file_name}")
                else:
                    log_message(f"  ⚠️ Invalid: {file_name} - {message}")
                images_to_generate.append(image_info)
        
        log_message("\n📊 IMAGE STATUS REPORT:")
        log_message(f"  ✅ Valid existing images: {existing_count}")
        log_message(f"  🎯 Images to generate: {len(images_to_generate)}")
        
        if len(images_to_generate) == 0:
            log_message("🎉 ALL IMAGES ALREADY EXIST AND ARE VALID!")
            log_message("💰 Significant API cost savings achieved!")
        
        return images_to_generate

    def generate_single_image(self, image_info: dict, image_folder: str) -> bool:
        """Generates single image with smart retry."""
        image_id = image_info["id"]
        prompt = image_info["prompt"]
        aspect_ratio = image_info["aspect_ratio"]
        
        # Validate aspect ratio
        valid_ratios = ["1:1", "4:3", "3:4", "16:9", "9:16"]
        if aspect_ratio not in valid_ratios:
            log_message(f"⚠️ Invalid aspect_ratio '{aspect_ratio}' for {image_id}, using '16:9'", "WARNING")
            aspect_ratio = "16:9"
        
        output_filename = f"{image_id}.png"
        output_path = os.path.join(image_folder, output_filename)
        
        def generate_image():
            """Inner function for retry mechanism."""
            self.rate_limiter.wait_if_needed()
            
            log_message(f"🖼️ Generating: {image_id} [{aspect_ratio}] -> \"{prompt[:50]}...\"")
            
            response = self.client.models.generate_images(
                model=self.model_name,
                prompt=prompt,
                config=types.GenerateImagesConfig(
                    number_of_images=1,
                    aspect_ratio=aspect_ratio
                )
            )
            
            if not response.generated_images:
                raise Exception(f"AI returned no images for: {image_id}")
            
            generated_image = response.generated_images[0]
            generated_image.image.save(output_path)
            
            # Verify the generated image
            is_valid, message = self.asset_verifier.verify_image_file(output_path)
            if not is_valid:
                raise Exception(f"Generated image failed verification: {message}")
            
            log_message(f"✅ Image generated and verified: {output_filename}")
            return True
        
        # Use centralized retry handler
        try:
            return self.retry_handler.execute_with_retry(generate_image)
        except Exception as e:
            log_error(f"❌ CRITICAL: Image generation failed for {image_id}: {e}")
            return False

    def generate_all_images(self, project_file_path: str, image_folder: str):
        """Smart image generation with comprehensive asset management."""
        log_message("🎬 Starting smart image generation process...")
        
        project_data = self.read_project_file(project_file_path)
        
        # Ensure output folder exists
        log_message(f"📁 Preparing image folder: {image_folder}")
        os.makedirs(image_folder, exist_ok=True)
        
        # Get required images
        required_images = self.list_required_images(project_data)
        log_message(f"📋 Total images required: {len(required_images)}")
        
        # Smart checking for existing images
        images_to_generate = self.check_existing_images(image_folder, required_images)

        if not images_to_generate:
            log_message("⚡ Image generation skipped - all images already exist!")
            return

        log_message(f"\n🎨 Generating {len(images_to_generate)} missing images...")
        log_message("="*60)
        
        success_count = 0
        failed_images = []
        
        for i, image_info in enumerate(images_to_generate, 1):
            log_message(f"\n🖼️ Processing ({i}/{len(images_to_generate)}): {image_info['id']}")
            
            if self.generate_single_image(image_info, image_folder):
                success_count += 1
            else:
                failed_images.append(image_info['id'])
                log_error(f"❌ Failed to generate: {image_info['id']}")
                # Don't exit immediately - try to generate as many as possible
            
            progress = (i / len(images_to_generate)) * 100
            log_message(f"    📈 Progress: {progress:.1f}%")

        # Final report
        log_message("\n" + "="*60)
        log_message("🎉 IMAGE GENERATION PROCESS COMPLETE!")
        log_message("="*60)
        log_message(f"  🎨 Successfully generated: {success_count}")
        log_message(f"  ❌ Failed: {len(failed_images)}")
        if failed_images:
            log_message(f"  📋 Failed images: {', '.join(failed_images)}")
        log_message(f"  💰 API cost optimization: Skipped existing valid images")
        log_message("="*60)
        
        # Exit with error if any images failed (this will trigger retry at producer level)
        if failed_images:
            log_error(f"❌ CRITICAL: {len(failed_images)} images failed to generate")
            sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Enhanced Image Director with smart asset management and centralized utils"
    )
    parser.add_argument("project_file", help="Path to project.json")
    parser.add_argument("image_folder", help="Image output folder")
    # Config dosyasını proje root'undan otomatik bul
    default_config = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config.json")
    parser.add_argument("--config_path", default=default_config, help="Config file path")
    args = parser.parse_args()
    
    try:
        log_message("🎨 ENHANCED IMAGE DIRECTOR MODULE STARTED")
        log_message("="*60)
        
        # Load config
        with open(args.config_path, 'r', encoding='utf-8') as f:
            config_data = json.load(f)
        
        image_director = ImageDirector(config_data)
        image_director.generate_all_images(args.project_file, args.image_folder)
        
    except KeyboardInterrupt:
        log_error("\n⏹️ Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        log_error(f"\n❌ Unexpected error in Image Director: {e}")
        sys.exit(1)
