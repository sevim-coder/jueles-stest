# modules/director.py (AI-Driven Creative Director - Christopher Nolan Approach)

import os
import sys
import json
import argparse
from typing import List
from google import genai
from pydantic import BaseModel, Field

# Ana proje klasörünü sys.path'e ekle
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)
from utils.common import RateLimiter, ErrorClassifier, RetryHandler, log_message

def log_error(message):
    """Prints error messages to stderr."""
    log_message(message, "ERROR")
    print(message, file=sys.stderr)

# --- Pydantic Models ---
class InternalEffect(BaseModel):
    effect_type: str = Field(..., description="The type of effect: 'still', 'zoom', or 'pan'. MUST NOT be 'none'.")
    direction: str = Field(..., description="Effect direction: 'in', 'out', 'left', 'right', 'up', 'down', or 'none'. Must be 'none' if type is 'still'.")
    speed: str = Field(..., description="Effect speed: 'very_slow', 'slow', 'normal', 'fast'. Must be 'none' if type is 'still'.")

class Segment(BaseModel):
    segment_id: str = Field(..., description="A unique identifier, e.g., 'I-P1-S1' for Intro, Paragraph 1, Segment 1.")
    text: str = Field(..., description="The narration text for this segment.")
    visual_prompt: str = Field(..., description="A detailed, artistic prompt for the image generation AI.")
    aspect_ratio: str = Field(..., description="The aspect ratio for the visual, e.g., '16:9', '9:16', '1:1'.")
    internal_effect: InternalEffect = Field(..., description="The camera motion effect to apply to the visual.")
    transition_effect: str = Field(..., description="The transition effect to use *after* this segment.")

class Paragraph(BaseModel):
    paragraph_id: str = Field(..., description="A unique identifier for the paragraph, e.g., 'I-P1'.")
    segments: List[Segment]

class Section(BaseModel):
    section_id: str = Field(..., description="The short code for the section: 'I' for Intro, 'D' for Development, 'C' for Conclusion.")
    paragraphs: List[Paragraph]

class StoryStructure(BaseModel):
    intro: Section
    development: Section
    conclusion: Section

class VideoSettings(BaseModel):
    codec: str = "libx264"
    bitrate: str = "8000k"
    fps: int = 30
    resolution: str = "1920x1080"

class AudioSettings(BaseModel):
    codec: str = "aac"
    bitrate: str = "192k"

class FfmpegSettings(BaseModel):
    output_filename: str
    video_settings: VideoSettings = VideoSettings()
    audio_settings: AudioSettings = AudioSettings()

class YoutubeMetadata(BaseModel):
    channel: str
    title: str
    description: str
    narrator: str
    music_style: str
    tags: List[str]
    category: str
    privacy_status: str

class VideoProject(BaseModel):
    ffmpeg_settings: FfmpegSettings
    youtube_metadata: YoutubeMetadata
    story_structure: StoryStructure

class AICreativeDirector:
    """
    AI-Driven Creative Director inspired by Christopher Nolan's approach.
    Makes intelligent creative decisions for each unique project.
    """
    def __init__(self, config_data):
        log_message("🎬 AI Creative Director initializing...")
        log_message("🎭 Christopher Nolan approach: Each project gets unique creative vision")
        
        api_key = os.getenv("GEMINI_API_KEY")
        if not api_key:
            log_error("❌ CRITICAL: GEMINI_API_KEY environment variable not found!")
            sys.exit(1)
        
        try:
            self.client = genai.Client(api_key=api_key)
        except Exception as e:
            log_error(f"❌ CRITICAL: Could not initialize AI client: {e}")
            sys.exit(1)
        
        # Setup components using centralized utils
        director_config = config_data.get("ai_models", {}).get("director", {})
        self.model_name = director_config.get("model_name", "gemini-2.5-pro")
        rate_limit = director_config.get("rate_limit", {})
        
        self.rate_limiter = RateLimiter(
            requests_per_minute=rate_limit.get("requests_per_minute", 15),
            cooldown_seconds=rate_limit.get("cooldown_seconds", 4)
        )
        
        self.error_classifier = ErrorClassifier(config_data)
        self.retry_handler = RetryHandler(config_data)
        self.config = config_data
        
        log_message(f"⚙️ Creative Director ready - Model: {self.model_name}")
        log_message(f"⏱️ Rate limit: {rate_limit.get('requests_per_minute', 15)} req/min, {rate_limit.get('cooldown_seconds', 4)}s cooldown")

    def read_script(self, file_path: str) -> str:
        """Reads the script file with enhanced error handling."""
        log_message(f"📖 Reading script: {file_path}")
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            log_message(f"✅ Script loaded ({len(content)} characters)")
            return content
        except FileNotFoundError:
            log_error(f"❌ CRITICAL: Script file not found: {file_path}")
            sys.exit(1)
        except Exception as e:
            log_error(f"❌ CRITICAL: Failed to read script: {e}")
            sys.exit(1)

    def create_project_file(self, script_text: str, channel_name: str) -> str:
        """AI makes all creative decisions for this unique project."""
        log_message("🎭 AI analyzing script for creative vision...")
        log_message("🎬 Making intelligent decisions: narrator, visuals, music, format...")
        
        # Get channel context
        channel_info = self.config["channels"].get(channel_name, {})
        channel_instruction = channel_info.get("prompt_instruction", "")
        
        # Get available options for AI to choose from
        available_options = self.config["available_options"]
        technical_defaults = self.config["technical_defaults"]
        
        prompt = f"""
        You are a MASTER FILM DIRECTOR with the creative vision of Christopher Nolan.
        Analyze this Turkish script and create a UNIQUE, INTELLIGENT production plan.

        **CHANNEL CONTEXT:**
        - Channel: {channel_name}
        - Channel Style: {channel_instruction}

        **YOUR CREATIVE DECISIONS (Choose intelligently based on content):**

        **1. NARRATOR SELECTION:**
        - Available voices: {[n['name'] + ' (' + n['description'] + ')' for n in available_options['narrators']]}
        - Choose the voice that BEST FITS the content tone and channel
        - Return ONLY the name (e.g., "Callirhoe"), not description

        **2. ASPECT RATIO DECISION (Her segment için ayrı karar ver):**
        - Available ratios: {available_options['aspect_ratios']}
        - 16:9: Landscapes, wide scenes, documentary establishing shots, group scenes
        - 9:16: Portraits, close-ups, vertical objects, mobile-optimized content
        - 1:1: Centered objects, symmetrical compositions, social media focus
        - 4:3: Vintage scenes, classic documentary feel, balanced compositions
        - 3:4: Portrait-oriented, taller than wide
        
        **3. VISUAL STYLE (Content'e göre seç):**
        - Photorealistic: Scientific, historical, documentary
        - Cinematic: Dramatic, epic, movie-like
        - Artistic: Creative, aesthetic, artistic content
        - Cartoon: Fun, child-friendly, playful
        - Illustrated: Educational, explanatory, stylized
        - Choose based on content tone and target audience

        **4. MUSIC STYLE:**
        - Available styles: {available_options['music_styles']}
        - Choose music that complements the mood and content

        **5. INTERNAL EFFECTS:**
        - Types: {available_options['internal_effects']['types']}
        - Directions: {available_options['internal_effects']['directions']}
        - Speeds: {available_options['internal_effects']['speeds']}
        - Use variety and purpose - not random

        **6. TRANSITION EFFECTS:**
        - Available: {available_options['transition_effects']}
        - Use purposefully to enhance storytelling

        **VISUAL PROMPT CREATION RULES:**
        - Each segment needs a DETAILED promts proper to "text"
        - ALWAYS include: "4K ultra high definition, sharp focus, detailed textures, masterpiece quality"
        - Use the chosen visual style consistently
        - NO real people names - use character archetypes or film references
        - Make visuals support and enhance the narrative text
        - Consider the chosen aspect ratio in composition

        **YOUTUBE METADATA INTELLIGENCE:**
        - Create compelling Turkish title that captures essence
        - Write engaging Turkish description with key points
        - Generate relevant Turkish tags for discoverability
        - Use technical defaults: category="{technical_defaults['youtube_category']}", privacy="{technical_defaults['privacy_status']}"

        **OUTPUT REQUIREMENTS:**
        - JSON field names MUST be in ENGLISH
        - Content values remain in Turkish
        - Every segment MUST have all required fields
        - Use hierarchical IDs: Section='I'/'D'/'C', Paragraph='I-P1', Segment='I-P1-S1'
        - Resolution auto-calculated from aspect ratio choice
        - Filename: use channel slug + content identifier

        **TURKISH SCRIPT TO ANALYZE:**
        ---
        {script_text}
        ---

        Create a production plan where EVERY creative choice serves the story.
        Think like a director who tailors every element to enhance the narrative impact.
        """
        
        def generate_content():
            """Inner function for retry mechanism."""
            self.rate_limiter.wait_if_needed()
            
            log_message("🚀 AI analyzing content and making creative decisions...")
            
            response = self.client.models.generate_content(
                model=self.model_name,
                contents=prompt,
                config={
                    "response_mime_type": "application/json",
                    "response_schema": VideoProject,
                },
            )
            
            if not response.text:
                raise Exception("AI returned empty response")
            
            log_message("✅ Creative vision completed by AI")
            return response.text
        
        # Use centralized retry handler
        try:
            return self.retry_handler.execute_with_retry(generate_content)
        except Exception as e:
            log_error(f"❌ CRITICAL: Creative direction failed: {e}")
            sys.exit(1)

    def write_project_file(self, content: str, file_path: str):
        """Writes project file with validation and creative summary."""
        log_message(f"💾 Saving creative vision: {file_path}")
        try:
            # Validate and parse JSON
            project_data = json.loads(content)
            
            # Log creative decisions made by AI
            log_message("🎭 AI CREATIVE DECISIONS SUMMARY:")
            log_message(f"   🎤 Narrator: {project_data['youtube_metadata']['narrator']}")
            log_message(f"   🎵 Music: {project_data['youtube_metadata']['music_style']}")
            log_message(f"   📐 Aspect Ratio: {project_data['story_structure']['intro']['paragraphs'][0]['segments'][0]['aspect_ratio']}")
            log_message(f"   🎬 Title: {project_data['youtube_metadata']['title']}")
            log_message(f"   📱 Resolution: {project_data['ffmpeg_settings']['video_settings']['resolution']}")
            
            # Write with proper formatting
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(project_data, f, ensure_ascii=False, indent=2)
            
            log_message("✅ Creative vision saved successfully!")
        except json.JSONDecodeError as e:
            log_error(f"❌ CRITICAL: Invalid JSON from AI: {e}")
            sys.exit(1)
        except Exception as e:
            log_error(f"❌ CRITICAL: Failed to write project file: {e}")
            sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="AI Creative Director: Intelligent creative decisions for each project"
    )
    parser.add_argument("script_input_path", help="Path to script.txt")
    parser.add_argument("project_output_path", help="Path to save project.json")
    parser.add_argument("--channel_name", required=True, help="Channel name for creative context")
    # Config dosyasını proje root'undan otomatik bul
    default_config = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config.json")
    parser.add_argument("--config_path", default=default_config, help="Config file path")
    args = parser.parse_args()

    try:
        log_message("🎬 AI CREATIVE DIRECTOR STARTED")
        log_message("="*60)
        log_message("🎭 Christopher Nolan Approach: Tailored creative vision for each story")
        log_message("="*60)
        
        # Load config
        with open(args.config_path, 'r', encoding='utf-8') as f:
            config_data = json.load(f)
        
        director = AICreativeDirector(config_data)
        script_content = director.read_script(args.script_input_path)
        json_content = director.create_project_file(script_content, args.channel_name)
        director.write_project_file(json_content, args.project_output_path)
        
        log_message("="*60)
        log_message("🎉 CREATIVE DIRECTION COMPLETE!")
        log_message(f"📁 Unique project vision ready: {args.project_output_path}")
        log_message("🎬 Every creative choice serves the story!")
        log_message("="*60)
        
    except KeyboardInterrupt:
        log_error("\n⏹️ Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        log_error(f"\n❌ Unexpected error in Creative Director: {e}")
        sys.exit(1)
