# modules/narrator.py (Enhanced with centralized utils and smart asset checking)

import os
import sys
import json
import argparse
import wave
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

class Narrator:
    """
    Enhanced Narrator with centralized utils and smart asset management.
    Only generates missing/corrupt audio files to save costs and time.
    """
    def __init__(self, config_data):
        log_message("🎙️ Narrator initializing with smart asset management...")
        
        api_key = os.getenv("GEMINI_API_KEY")
        if not api_key:
            log_error("❌ CRITICAL: GEMINI_API_KEY environment variable not found!")
            sys.exit(1)
        
        try:
            self.client = genai.Client(api_key=api_key)
            log_message("🤖 AI voice synthesis ready")
        except Exception as e:
            log_error(f"❌ CRITICAL: Could not initialize AI client: {e}")
            sys.exit(1)
        
        # Setup components using centralized utils
        narrator_config = config_data.get("ai_models", {}).get("narrator", {})
        self.model_name = narrator_config.get("model_name", "gemini-2.5-flash-tts")
        rate_limit = narrator_config.get("rate_limit", {})
        
        self.rate_limiter = RateLimiter(
            requests_per_minute=rate_limit.get("requests_per_minute", 10),
            cooldown_seconds=rate_limit.get("cooldown_seconds", 6)
        )
        
        self.error_classifier = ErrorClassifier(config_data)
        self.retry_handler = RetryHandler(config_data)
        self.asset_verifier = AssetVerifier(config_data)
        
        log_message(f"⚙️ Narrator ready - Model: {self.model_name}")
        log_message(f"⏱️ Rate limit: {rate_limit.get('requests_per_minute', 10)} req/min, {rate_limit.get('cooldown_seconds', 6)}s cooldown")
    
    def _save_wave_file(self, filename: str, pcm_data: bytes, channels: int = 1, 
                       rate: int = 24000, sample_width: int = 2):
        """Safe WAV file creation with error handling."""
        try:
            with wave.open(filename, "wb") as wf:
                wf.setnchannels(channels)
                wf.setsampwidth(sample_width)
                wf.setframerate(rate)
                wf.writeframes(pcm_data)
            log_message(f"✅ WAV file created: {os.path.basename(filename)}")
        except Exception as e:
            log_error(f"❌ CRITICAL: Failed to create WAV file {filename}: {e}")
            raise

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

    def _extract_required_audio(self, project_data: dict) -> list:
        """Extracts required audio segments from project data."""
        required_audio = []
        try:
            story_structure = project_data["story_structure"]
            for section_data in story_structure.values():
                for paragraph in section_data["paragraphs"]:
                    for segment in paragraph["segments"]:
                        required_audio.append({
                            "id": segment["segment_id"],
                            "text": segment["text"]
                        })
            return required_audio
        except KeyError as e:
            log_error(f"❌ CRITICAL: Malformed project file, missing key: {e}")
            sys.exit(1)

    def _check_existing_audio(self, audio_folder: str, required_audio: list) -> list:
        """Smart checking for existing audio files using centralized AssetVerifier."""
        log_message(f"🔍 Checking existing audio in: {audio_folder}")
        
        to_narrate = []
        existing_count = 0
        
        for audio_info in required_audio:
            file_name = f"{audio_info['id']}.wav"
            file_path = os.path.join(audio_folder, file_name)
            
            # Use centralized asset verification
            is_valid, message = self.asset_verifier.verify_audio_file(file_path)
            
            if is_valid:
                log_message(f"  ✅ {file_name}: {message}")
                existing_count += 1
            else:
                if "not found" in message:
                    log_message(f"  ❌ Missing: {file_name}")
                else:
                    log_message(f"  ⚠️ Invalid: {file_name} - {message}")
                to_narrate.append(audio_info)
        
        log_message("\n📊 AUDIO STATUS REPORT:")
        log_message(f"  ✅ Valid existing files: {existing_count}")
        log_message(f"  🎯 Files to generate: {len(to_narrate)}")
        
        if len(to_narrate) == 0:
            log_message("🎉 ALL AUDIO FILES ALREADY EXIST AND ARE VALID!")
            log_message("💰 Significant API cost savings achieved!")
        
        return to_narrate

    def narrate_segment(self, text: str, voice_name: str, output_path: str) -> bool:
        """Narrates single segment with smart retry."""
        def generate_audio():
            """Inner function for retry mechanism."""
            self.rate_limiter.wait_if_needed()
            
            log_message(f"🎤 Generating: \"{text[:50]}...\" with {voice_name}")
            
            response = self.client.models.generate_content(
                model=self.model_name,
                contents=f'"{text}"',
                config=types.GenerateContentConfig(
                    response_modalities=["AUDIO"],
                    speech_config=types.SpeechConfig(
                        voice_config=types.VoiceConfig(
                            prebuilt_voice_config=types.PrebuiltVoiceConfig(
                                voice_name=voice_name
                            )
                        )
                    ),
                )
            )
            
            if not response.candidates or not response.candidates[0].content.parts:
                raise Exception(f"AI returned no audio for text: '{text[:50]}...'")
            
            audio_data = response.candidates[0].content.parts[0].inline_data.data
            self._save_wave_file(output_path, audio_data)
            
            # Verify the generated file
            is_valid, message = self.asset_verifier.verify_audio_file(output_path)
            if not is_valid:
                raise Exception(f"Generated audio file failed verification: {message}")
            
            log_message(f"✅ Audio generated and verified: {os.path.basename(output_path)}")
            return True
        
        # Use centralized retry handler
        try:
            return self.retry_handler.execute_with_retry(generate_audio)
        except Exception as e:
            log_error(f"❌ CRITICAL: Audio generation failed for {os.path.basename(output_path)}: {e}")
            return False

    def narrate_all_segments(self, project_file_path: str, output_folder: str):
        """Smart narration with comprehensive asset management."""
        log_message("🎬 Starting smart narration process...")
        
        project_data = self.read_project_file(project_file_path)
        
        # Ensure output folder exists
        log_message(f"📁 Preparing output folder: {output_folder}")
        os.makedirs(output_folder, exist_ok=True)
        
        # Get narrator from project
        try:
            voice_name = project_data["youtube_metadata"]["narrator"]
            log_message(f"🎭 Using narrator: {voice_name}")
        except KeyError:
            log_error("❌ CRITICAL: 'youtube_metadata.narrator' not found in project file!")
            sys.exit(1)
        
        # Get required audio files
        required_audio = self._extract_required_audio(project_data)
        log_message(f"📋 Total segments required: {len(required_audio)}")
        
        # Smart checking for existing files
        segments_to_narrate = self._check_existing_audio(output_folder, required_audio)

        if not segments_to_narrate:
            log_message("⚡ Narration skipped - all files already exist!")
            return

        log_message(f"\n▶️ Generating {len(segments_to_narrate)} missing audio files...")
        log_message("="*60)
        
        success_count = 0
        failed_segments = []
        
        for i, segment_info in enumerate(segments_to_narrate, 1):
            output_filename = f"{segment_info['id']}.wav"
            output_path = os.path.join(output_folder, output_filename)
            
            log_message(f"\n🎙️ Processing ({i}/{len(segments_to_narrate)}): {segment_info['id']}")
            
            if self.narrate_segment(segment_info['text'], voice_name, output_path):
                success_count += 1
            else:
                failed_segments.append(segment_info['id'])
                log_error(f"❌ Failed to generate: {segment_info['id']}")
                # Don't exit immediately - try to generate as many as possible
            
            progress = (i / len(segments_to_narrate)) * 100
            log_message(f"    📈 Progress: {progress:.1f}%")

        # Final report
        log_message("\n" + "="*60)
        log_message("🎉 NARRATION PROCESS COMPLETE!")
        log_message("="*60)
        log_message(f"  🎤 Successfully generated: {success_count}")
        log_message(f"  ❌ Failed: {len(failed_segments)}")
        if failed_segments:
            log_message(f"  📋 Failed segments: {', '.join(failed_segments)}")
        log_message(f"  💰 API cost optimization: Skipped existing valid files")
        log_message("="*60)
        
        # Exit with error if any segments failed (this will trigger retry at producer level)
        if failed_segments:
            log_error(f"❌ CRITICAL: {len(failed_segments)} audio segments failed to generate")
            sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Enhanced Narrator with smart asset management and centralized utils"
    )
    parser.add_argument("project_file", help="Path to project.json")
    parser.add_argument("output_folder", help="Audio output folder")
    # Config dosyasını proje root'undan otomatik bul
    default_config = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config.json")
    parser.add_argument("--config_path", default=default_config, help="Config file path")
    args = parser.parse_args()
    
    try:
        log_message("🎙️ ENHANCED NARRATOR MODULE STARTED")
        log_message("="*60)
        
        # Load config
        with open(args.config_path, 'r', encoding='utf-8') as f:
            config_data = json.load(f)
        
        narrator = Narrator(config_data)
        narrator.narrate_all_segments(args.project_file, args.output_folder)
        
    except KeyboardInterrupt:
        log_error("\n⏹️ Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        log_error(f"\n❌ Unexpected error in Narrator: {e}")
        sys.exit(1)
