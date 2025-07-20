# modules/editor.py (Enhanced with centralized utils and robust asset management)

import os
import sys
import json
import random
import shutil
import time
import argparse
import subprocess
import glob

# Ana proje klasörünü sys.path'e ekle
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)
from utils.common import (
    log_message, safe_subprocess_run, AssetVerifier, 
    RetryHandler, ErrorClassifier, AtomicFileWriter
)

def log_error(message):
    """Prints error messages to stderr."""
    log_message(message, "ERROR")
    print(message, file=sys.stderr)

class QualityControl:
    """Enhanced quality control using centralized AssetVerifier."""

    def __init__(self, asset_verifier: AssetVerifier):
        self.asset_verifier = asset_verifier

    @staticmethod
    def check_disk_space(required_mb=2000):
        """Checks for sufficient disk space."""
        try:
            _, _, free = shutil.disk_usage(".")
            free_mb = free / (1024 * 1024)
            # Güvenlik marjı ekle (%20 extra)
            safe_required = required_mb * 1.2
            if free_mb < safe_required:
                log_error(f"❌ CRITICAL: Insufficient disk space!")
                log_error(f"   Required: {required_mb}MB (+ %20 güvenlik = {safe_required:.1f}MB)")
                log_error(f"   Available: {free_mb:.1f}MB")
                log_error(f"   Need: {safe_required - free_mb:.1f}MB more")
                sys.exit(1)
            log_message(f"✅ Sufficient disk space: {free_mb:.1f}MB available")
        except Exception as e:
            log_error(f"❌ CRITICAL: Disk space check failed: {e}")
            sys.exit(1)

    def check_audio_file(self, file_path: str) -> float:
        """Validates audio file using centralized verifier."""
        is_valid, message = self.asset_verifier.verify_audio_file(file_path)
        if not is_valid:
            log_error(f"❌ CRITICAL: {message}")
            sys.exit(1)
    
        # Duration'ı AssetVerifier'dan al
        try:
            from mutagen.wave import WAVE  # Sadece burada lokal import
            audio = WAVE(file_path)
            duration = audio.info.length
            log_message(f"  ✅ Audio QC Pass: {os.path.basename(file_path)} ({duration:.2f}s)")
            return duration
        except Exception as e:
            log_error(f"❌ CRITICAL: Could not read audio duration: {file_path} - {e}")
            sys.exit(1)

    def check_image_file(self, file_path: str):
        """Validates image file using centralized verifier."""
        is_valid, message = self.asset_verifier.verify_image_file(file_path)
        if not is_valid:
            log_error(f"❌ CRITICAL: {message}")
            sys.exit(1)
        
        try:
            with Image.open(file_path) as img:
                w, h = img.size
            log_message(f"  ✅ Image QC Pass: {os.path.basename(file_path)} ({w}x{h})")
        except Exception as e:
            log_error(f"❌ CRITICAL: Could not read image properties: {file_path} - {e}")
            sys.exit(1)

class FfmpegRunner:
    """Enhanced FFmpeg wrapper with centralized error handling."""

    def __init__(self, retry_handler: RetryHandler):
        self.retry_handler = retry_handler

    def run_command(self, command_list: list, description: str, is_critical: bool = True) -> bool:
        """Executes FFmpeg with smart retry for system errors."""
        def execute_ffmpeg():
            success, output = safe_subprocess_run(command_list, description)
            if not success:
                # Let the retry handler classify the error
                raise Exception(f"FFmpeg failed for {description}: {output}")
            return True

        try:
            if is_critical:
                # Use retry handler for critical operations
                self.retry_handler.execute_with_retry(execute_ffmpeg)
                log_message(f"✅ FFmpeg success: {description}")
                return True
            else:
                # Direct execution for non-critical operations
                return execute_ffmpeg()
        except Exception as e:
            if is_critical:
                log_error(f"❌ CRITICAL FFmpeg failure: {description} - {e}")
                sys.exit(1)
            else:
                log_message(f"⚠️ Non-critical FFmpeg failure: {description} - {e}", "WARNING")
                return False

class Editor:
    """
    Enhanced Editor with centralized utils and comprehensive asset management.
    Ensures all assets are validated before processing begins.
    """
    def __init__(self, project_path: str, audio_folder: str, image_folder: str, 
                 output_path: str, config_path: str):
        log_message("🎞️ Enhanced Editor initializing...")
        
        # Load config
        self.config_data = AtomicFileWriter.read_json(config_path)
        if not self.config_data:
            log_error(f"❌ CRITICAL: Cannot load config from {config_path}")
            sys.exit(1)
        
        # Initialize centralized components
        self.asset_verifier = AssetVerifier(self.config_data)
        self.retry_handler = RetryHandler(self.config_data)
        self.quality_control = QualityControl(self.asset_verifier)
        self.ffmpeg_runner = FfmpegRunner(self.retry_handler)
        
        # Validate disk space first
        self.quality_control.check_disk_space()
        
        # Load and validate project data
        self.project_data = AtomicFileWriter.read_json(project_path)
        if not self.project_data:
            log_error(f"❌ CRITICAL: Cannot load project file: {project_path}")
            sys.exit(1)
        
        self.audio_folder = audio_folder
        self.image_folder = image_folder
        self.output_path = output_path
        self.music_folder = self._find_music_folder()
        
        self.temp_folder = os.path.join(os.path.dirname(output_path), "temp_clips")
        os.makedirs(self.temp_folder, exist_ok=True)
        
        log_message("✅ Enhanced Editor ready for assembly")

    def __del__(self):
        """Enhanced cleanup with error handling."""
        if hasattr(self, 'temp_folder') and os.path.exists(self.temp_folder):
            try:
                shutil.rmtree(self.temp_folder)
                log_message("🧹 Temporary files cleaned up")
            except Exception as e:
                log_message(f"⚠️ Warning: Could not clean up temp folder: {e}", "WARNING")

    def _find_music_folder(self) -> str:
        """Enhanced music folder detection using config."""
        # Config'dan path al
        config_music_path = self.config_data.get("music_folder_path", "./music")
        
        # Önce config path'ini kontrol et
        if os.path.isabs(config_music_path):
            # Absolute path
            if os.path.exists(config_music_path):
                log_message(f"🎵 Music folder found (config absolute): {config_music_path}")
                return config_music_path
        else:
            # Relative path - project root'tan başla
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            full_music_path = os.path.join(project_root, config_music_path)
            if os.path.exists(full_music_path):
                log_message(f"🎵 Music folder found (config relative): {full_music_path}")
                return full_music_path
        
        # Fallback: project root'ta ara
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        
        try:
            for item in os.listdir(project_root):
                item_path = os.path.join(project_root, item)
                if os.path.isdir(item_path) and 'music' in item.lower():
                    log_message(f"🎵 Music folder found (fallback): {item_path}")
                    return item_path
        except OSError as e:
            log_message(f"⚠️ Error searching for music folder: {e}", "WARNING")
        
        log_message("⚠️ No music folder found - videos will have no background music", "WARNING")
        return None

    def _validate_all_assets_before_start(self) -> bool:
        """Comprehensive asset validation before any processing begins."""
        log_message("🔍 Validating ALL assets before editor starts...")
        
        validation_errors = []
        
        # 1. Validate project structure
        try:
            story_structure = self.project_data["story_structure"]
            required_segments = []
            
            for section_data in story_structure.values():
                for paragraph in section_data["paragraphs"]:
                    for segment in paragraph["segments"]:
                        required_segments.append(segment["segment_id"])
            
            log_message(f"📋 Found {len(required_segments)} segments to process")
            
        except KeyError as e:
            validation_errors.append(f"Invalid project structure: missing {e}")
            
        # 2. Validate all audio files
        log_message("🎵 Validating audio assets...")
        for segment_id in required_segments:
            audio_path = os.path.join(self.audio_folder, f"{segment_id}.wav")
            is_valid, message = self.asset_verifier.verify_audio_file(audio_path)
            if not is_valid:
                validation_errors.append(f"Audio: {message}")
        
        # 3. Validate all image files
        log_message("🖼️ Validating image assets...")
        for segment_id in required_segments:
            image_path = os.path.join(self.image_folder, f"{segment_id}.png")
            is_valid, message = self.asset_verifier.verify_image_file(image_path)
            if not is_valid:
                validation_errors.append(f"Image: {message}")
        
        # 4. Validate music assets
        if self.music_folder:
            music_files = self._get_music_files()
            if not music_files:
                validation_errors.append("No valid music files found in music folder")
        
        # Report results
        if validation_errors:
            log_error("❌ CRITICAL: Asset validation failed!")
            for error in validation_errors:
                log_error(f"  - {error}")
            return False
        
        log_message("✅ ALL ASSETS VALIDATED - READY FOR PROCESSING!")
        return True

    def _get_music_files(self) -> list:
        """Gets list of available music files."""
        if not self.music_folder:
            return []
        
        music_extensions = ['*.mp3', '*.wav', '*.m4a', '*.aac']
        music_files = []
        for ext in music_extensions:
            music_files.extend(glob.glob(os.path.join(self.music_folder, ext)))
        
        return music_files

    def _create_silent_video_clip(self, segment_info: dict, audio_duration: float) -> str:
        """Creates silent video clip with enhanced error handling."""
        image_path = os.path.join(self.image_folder, f"{segment_info['segment_id']}.png")
        self.quality_control.check_image_file(image_path)
        
        # Video duration = audio duration + 1 second for transitions
        video_duration = audio_duration + 1.0
        output_clip_path = os.path.join(self.temp_folder, f"{segment_info['segment_id']}_silent.mp4")
        
        log_message(f"  🎬 Creating clip: {os.path.basename(output_clip_path)} ({video_duration:.2f}s)")
        
        effect = segment_info['internal_effect']
        video_filter = self._build_video_filter(effect, video_duration)
        
        command = [
            'ffmpeg', '-y',
            '-loop', '1', '-i', image_path,
            '-vf', video_filter,
            '-t', str(video_duration),
            '-c:v', 'libx264', '-preset', 'medium', '-crf', '23', '-pix_fmt', 'yuv420p',
            '-an',  # No audio - silent clip
            output_clip_path
        ]
        
        self.ffmpeg_runner.run_command(command, f"silent video clip for {segment_info['segment_id']}")
        return output_clip_path

    def _build_video_filter(self, effect: dict, duration: float) -> str:
        """Enhanced video filter building with validation."""
        base_filter = "scale=1920:1080:force_original_aspect_ratio=decrease,pad=1920:1080:(ow-iw)/2:(oh-ih)/2:black,format=yuv420p"
        
        effect_type = effect.get('effect_type')
        if effect_type == 'still':
            return base_filter
        
        # Zoom effect
        if effect_type == 'zoom':
            direction = effect.get('direction', 'in')
            speed_map = {'very_slow': 0.0005, 'slow': 0.001, 'normal': 0.0015, 'fast': 0.002}
            zoom_rate = speed_map.get(effect.get('speed', 'normal'), 0.0015)
            
            if direction == 'in':
                zoom_expr = f"min(zoom+{zoom_rate},1.5)"
            else:  # direction == 'out'
                zoom_expr = f"max(1.0,zoom-{zoom_rate})"
            
            effect_filter = f"zoompan=z='{zoom_expr}':d={int(duration*30)}:s=1920x1080:fps=30"
            log_message(f"    🔍 Applying Zoom {direction.capitalize()} effect")
            return f"{base_filter},{effect_filter}"

        # Pan effect
        if effect_type == 'pan':
            direction = effect.get('direction', 'left')
            speed_map = {'very_slow': 10, 'slow': 20, 'normal': 30, 'fast': 40}
            pan_speed = speed_map.get(effect.get('speed', 'normal'), 30)
            
            pan_expr_map = {
                'left': f"x='iw/2-(iw/zoom/2) - (t*{pan_speed})':y='ih/2-(ih/zoom/2)'",
                'right': f"x='iw/2-(iw/zoom/2) + (t*{pan_speed})':y='ih/2-(ih/zoom/2)'",
                'up': f"x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2) - (t*{pan_speed})'",
                'down': f"x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2) + (t*{pan_speed})'"
            }
            pan_expr = pan_expr_map.get(direction, "x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)'")
            effect_filter = f"zoompan=z=1.1:{pan_expr}:d={int(duration*30)}:s=1920x1080:fps=30"
            log_message(f"    🔄 Applying Pan {direction.capitalize()} effect")
            return f"{base_filter},{effect_filter}"

        log_message(f"    ⚠️ Unknown effect type: {effect_type}, using still", "WARNING")
        return base_filter

    def _get_video_duration(self, video_path: str) -> float:
        """Gets video duration with enhanced error handling."""
        command = ['ffprobe', '-v', 'error', '-show_entries', 'format=duration', 
                  '-of', 'default=noprint_wrappers=1:nokey=1', video_path]
        try:
            result = subprocess.run(command, capture_output=True, text=True, check=True, timeout=30)
            duration = float(result.stdout.strip())
            log_message(f"  📏 Video duration: {duration:.2f}s")
            return duration
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired, ValueError) as e:
            log_message(f"⚠️ Could not get duration for {os.path.basename(video_path)}: {e}", "WARNING")
            return 0.0

    def _concatenate_silent_videos_with_transitions(self, clip_paths: list, segments_info: list) -> str:
        """Enhanced video concatenation with transitions."""
        log_message("\n🎬 Concatenating silent videos with transitions...")
        
        if len(clip_paths) == 1:
            output_path = os.path.join(self.temp_folder, "concatenated_silent_video.mp4")
            shutil.copy2(clip_paths[0], output_path)
            log_message("  ✅ Single clip, no transitions needed")
            return output_path
        
        transition_map = self.config_data.get('transition_effects_map', {})
        current_video = clip_paths[0]
        
        for i in range(1, len(clip_paths)):
            next_video = clip_paths[i]
            transition_name = segments_info[i-1]['data']['transition_effect']
            ffmpeg_transition = transition_map.get(transition_name, 'fade')
            
            current_duration = self._get_video_duration(current_video)
            transition_offset = max(0, current_duration - 1.0)
            
            output_temp = os.path.join(self.temp_folder, f"transition_step_{i}.mp4")
            
            log_message(f"  🔄 Applying '{transition_name}' transition between clips {i} and {i+1}")
            
            command = [
                'ffmpeg', '-y',
                '-i', current_video,
                '-i', next_video,
                '-filter_complex',
                f'[0:v][1:v]xfade=transition={ffmpeg_transition}:duration=1.0:offset={transition_offset}[v]',
                '-map', '[v]',
                '-c:v', 'libx264', '-preset', 'medium', '-crf', '23',
                output_temp
            ]
            
            self.ffmpeg_runner.run_command(command, f"transition {i}")
            current_video = output_temp
        
        final_video_path = os.path.join(self.temp_folder, "concatenated_silent_video.mp4")
        shutil.move(current_video, final_video_path)
        log_message("✅ Video concatenation complete")
        return final_video_path

    def _concatenate_audio_files(self, segments_info: list) -> str:
        """Enhanced audio concatenation."""
        log_message("\n🎵 Concatenating narrator audio files...")
        
        concat_list_path = os.path.join(self.temp_folder, "audio_concat_list.txt")
        
        # Validate all audio files exist before creating concat list
        missing_audio = []
        for info in segments_info:
            audio_file = os.path.join(self.audio_folder, f"{info['data']['segment_id']}.wav")
            if not os.path.exists(audio_file):
                missing_audio.append(audio_file)
        
        if missing_audio:
            log_error(f"❌ CRITICAL: Missing audio files: {missing_audio}")
            sys.exit(1)
        
        with open(concat_list_path, 'w', encoding='utf-8') as f:
            for info in segments_info:
                audio_file = os.path.join(self.audio_folder, f"{info['data']['segment_id']}.wav")
                f.write(f"file '{os.path.abspath(audio_file)}'\n")
        
        concatenated_audio_path = os.path.join(self.temp_folder, "concatenated_narrator.wav")
        
        command = [
            'ffmpeg', '-y',
            '-f', 'concat',
            '-safe', '0',
            '-i', concat_list_path,
            '-c', 'copy',
            concatenated_audio_path
        ]
        
        self.ffmpeg_runner.run_command(command, "narrator audio concatenation")
        return concatenated_audio_path

    def _prepare_background_music(self, video_duration: float) -> str:
        """Enhanced background music preparation."""
        music_files = self._get_music_files()
        if not music_files:
            log_message("⚠️ No music files available", "WARNING")
            return None
        
        log_message("\n🎵 Preparing background music...")
        
        selected_music = random.choice(music_files)
        log_message(f"🎵 Selected: {os.path.basename(selected_music)}")
        
        # Get music duration
        command = ['ffprobe', '-v', 'error', '-show_entries', 'format=duration', 
                  '-of', 'default=noprint_wrappers=1:nokey=1', selected_music]
        try:
            result = subprocess.run(command, capture_output=True, text=True, check=True, timeout=30)
            music_duration = float(result.stdout.strip())
        except Exception as e:
            log_message(f"⚠️ Could not get music duration, using as-is: {e}", "WARNING")
            return selected_music
        
        prepared_music_path = os.path.join(self.temp_folder, "prepared_background_music.wav")
        
        if music_duration >= video_duration:
            # Trim music
            command = [
                'ffmpeg', '-y',
                '-i', selected_music,
                '-t', str(video_duration),
                '-c:a', 'pcm_s16le',
                prepared_music_path
            ]
            log_message(f"  ✂️ Trimming music to {video_duration:.2f}s")
        else:
            # Loop music
            loop_count = int(video_duration / music_duration) + 1
            command = [
                'ffmpeg', '-y',
                '-stream_loop', str(loop_count),
                '-i', selected_music,
                '-t', str(video_duration),
                '-c:a', 'pcm_s16le',
                prepared_music_path
            ]
            log_message(f"  🔄 Looping music {loop_count} times for {video_duration:.2f}s")
        
        self.ffmpeg_runner.run_command(command, "background music preparation")
        return prepared_music_path

    def run(self):
        """Enhanced main execution with comprehensive validation."""
        log_message("🎬 Enhanced Editor assembly process starting...")
        start_time = time.time()
        
        # CRITICAL: Validate ALL assets before starting
        if not self._validate_all_assets_before_start():
            log_error("❌ CRITICAL: Asset validation failed - cannot proceed with editing")
            sys.exit(1)
        
        # Extract segments information
        log_message("\n📋 Processing project structure...")
        segments_info = []
        for section in self.project_data['story_structure'].values():
            for paragraph in section['paragraphs']:
                for segment in paragraph['segments']:
                    audio_path = os.path.join(self.audio_folder, f"{segment['segment_id']}.wav")
                    duration = self.quality_control.check_audio_file(audio_path)
                    segments_info.append({'data': segment, 'duration': duration})

        log_message(f"✅ Processing {len(segments_info)} validated segments")

        # Create silent video clips
        log_message("\n🎬 Creating silent video clips...")
        clip_paths = []
        for info in segments_info:
            clip_path = self._create_silent_video_clip(info['data'], info['duration'])
            clip_paths.append(clip_path)

        # Concatenate videos with transitions
        concatenated_video_path = self._concatenate_silent_videos_with_transitions(clip_paths, segments_info)

        # Concatenate audio
        concatenated_audio_path = self._concatenate_audio_files(segments_info)

        # Prepare background music
        video_duration = self._get_video_duration(concatenated_video_path)
        background_music_path = self._prepare_background_music(video_duration)

        # Final assembly
        log_message("\n🎭 Final assembly...")
        
        if background_music_path:
            # Video + Narrator + Background Music
            command = [
                'ffmpeg', '-y',
                '-i', concatenated_video_path,
                '-i', concatenated_audio_path,
                '-i', background_music_path,
                '-filter_complex',
                '[2:a]volume=0.15[bg];[1:a][bg]amix=inputs=2:duration=first[aout]',
                '-map', '0:v',
                '-map', '[aout]',
                '-c:v', 'copy',
                '-c:a', 'aac', '-b:a', '192k',
                '-shortest',
                self.output_path
            ]
            log_message("🎵 Final: Video + Narrator + Background Music")
        else:
            # Video + Narrator only
            command = [
                'ffmpeg', '-y',
                '-i', concatenated_video_path,
                '-i', concatenated_audio_path,
                '-map', '0:v',
                '-map', '1:a',
                '-c:v', 'copy',
                '-c:a', 'aac', '-b:a', '192k',
                '-shortest',
                self.output_path
            ]
            log_message("🎤 Final: Video + Narrator only")
        
        self.ffmpeg_runner.run_command(command, "final video assembly")

        # Validate final output
        if os.path.exists(self.output_path):
            file_size = os.path.getsize(self.output_path) / (1024 * 1024)  # MB
            if file_size < 1:
                log_error(f"❌ CRITICAL: Final video file is too small ({file_size:.2f}MB)")
                sys.exit(1)
            log_message(f"✅ Final video created: {file_size:.2f}MB")
        else:
            log_error("❌ CRITICAL: Final video file was not created")
            sys.exit(1)

        total_time = time.time() - start_time
        log_message("\n" + "="*60)
        log_message("🎉 ENHANCED EDITOR WORK COMPLETE!")
        log_message("="*60)
        log_message(f"📹 Output: {self.output_path}")
        log_message(f"⏱️ Total time: {total_time:.2f} seconds")
        log_message(f"📊 Processed {len(segments_info)} segments successfully")
        log_message("="*60)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Enhanced Editor with centralized utils and comprehensive asset validation"
    )
    parser.add_argument("project_path", help="Path to project.json")
    parser.add_argument("audio_folder", help="Audio files folder")
    parser.add_argument("image_folder", help="Image files folder")
    parser.add_argument("output_path", help="Final video output path")
    # Config dosyasını proje root'undan otomatik bul
    default_config = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config.json")
    parser.add_argument("--config_path", default=default_config, help="Config file path")
    args = parser.parse_args()

    try:
        log_message("🎞️ ENHANCED EDITOR MODULE STARTED")
        log_message("="*60)
        
        editor = Editor(
            args.project_path, 
            args.audio_folder, 
            args.image_folder, 
            args.output_path, 
            args.config_path
        )
        editor.run()
        
    except KeyboardInterrupt:
        log_error("\n⏹️ Editing cancelled by user")
        sys.exit(1)
    except Exception as e:
        log_error(f"\n❌ Critical error in Enhanced Editor: {e}")
        sys.exit(1)
