# modules/scriptwriter.py (Enhanced with centralized utils and smart error handling)

import os
import sys
import json
import argparse
from google import genai
from google.genai import types

# Ana proje klasörünü sys.path'e ekle
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)
from utils.common import RateLimiter, ErrorClassifier, RetryHandler, log_message

def log_error(message):
    """Prints error messages to stderr."""
    log_message(message, "ERROR")
    print(message, file=sys.stderr)

class Scriptwriter:
    """
    Enhanced Scriptwriter with centralized utils and intelligent error handling.
    Generates Turkish video scripts with zero code duplication.
    """
    def __init__(self, config_data):
        log_message("🎭 Scriptwriter initializing with enhanced capabilities...")
        
        api_key = os.getenv("GEMINI_API_KEY")
        if not api_key:
            log_error("❌ CRITICAL: GEMINI_API_KEY environment variable not found!")
            sys.exit(1)
        
        log_message("🔑 API key validated")
        
        try:
            self.client = genai.Client(api_key=api_key)
            log_message("🤖 AI client initialized")
        except Exception as e:
            log_error(f"❌ CRITICAL: Could not initialize AI client: {e}")
            sys.exit(1)
        
        # Setup components using centralized utils
        scriptwriter_config = config_data.get("ai_models", {}).get("scriptwriter", {})
        self.model_name = scriptwriter_config.get("model_name", "gemini-2.5-flash")
        rate_limit = scriptwriter_config.get("rate_limit", {})
        
        self.rate_limiter = RateLimiter(
            requests_per_minute=rate_limit.get("requests_per_minute", 15),
            cooldown_seconds=rate_limit.get("cooldown_seconds", 4)
        )
        
        self.error_classifier = ErrorClassifier(config_data)
        self.retry_handler = RetryHandler(config_data)
        self.config = config_data
        
        log_message(f"⚙️ Scriptwriter ready - Model: {self.model_name}")
        log_message(f"⏱️ Rate limit: {rate_limit.get('requests_per_minute', 15)} req/min, {rate_limit.get('cooldown_seconds', 4)}s cooldown")

    def generate_script(self, channel_name: str, topic: str, target_char_count: int, output_path: str):
        """Generates script with smart retry and error classification."""
        log_message("🎬 Starting script generation...")
        log_message(f"📺 Channel: {channel_name}")
        log_message(f"📝 Topic: {topic}")
        log_message(f"📏 Target length: {target_char_count} characters")
        log_message(f"📁 Output: {output_path}")

        try:
            channel_instruction = self.config["channels"][channel_name]["prompt_instruction"]
            log_message("🎯 Channel-specific instruction loaded")
        except KeyError:
            log_error(f"❌ CRITICAL: Channel '{channel_name}' not found in config")
            sys.exit(1)
        
        system_instruction = f"""
        {channel_instruction}

        Your task is to write a complete YouTube video script in TURKISH based on the topic.
        Target length: approximately {target_char_count} characters.
        You are an expert storyteller with excellent rhetorical skills.

        **MANDATORY RULES:**
        1. Output ONLY the script text in TURKISH. No explanations or comments.
        2. Script must be complete and engaging.
        3. Structure: Clear Introduction, Development, and Conclusion.
        4. Hook the viewer immediately in the introduction.
        5. Maintain momentum throughout development.
        6. End with thought-provoking conclusion.
        7. NO emojis or special characters.
        8. End with proper punctuation ('.', '?', '!').
        """

        def generate_content():
            """Inner function for retry mechanism."""
            self.rate_limiter.wait_if_needed()
            
            log_message("🤖 Sending request to AI...")
            
            response = self.client.models.generate_content(
                model=self.model_name,
                config=types.GenerateContentConfig(system_instruction=system_instruction),
                contents=f"Topic: {topic}. Write the script in Turkish."
            )
            
            if not response.text:
                raise Exception("AI returned empty script")
            
            script_text = response.text.strip()
            log_message(f"✅ Script received ({len(script_text)} characters)")
            return script_text
        
        # Use centralized retry handler
        try:
            script_text = self.retry_handler.execute_with_retry(generate_content)
        except Exception as e:
            log_error(f"❌ CRITICAL: Script generation failed: {e}")
            sys.exit(1)
        
        # Validate script length
        char_difference = abs(len(script_text) - target_char_count)
        char_percentage = (char_difference / target_char_count) * 100
        
        log_message(f"📊 Script length: {len(script_text)} chars (target: {target_char_count})")
        if char_percentage > 20:  # More than 20% difference
            log_message(f"⚠️ Warning: Script length differs by {char_percentage:.1f}%", "WARNING")
        
        # Save script
        try:
            folder_path = os.path.dirname(output_path)
            os.makedirs(folder_path, exist_ok=True)
            
            log_message("💾 Saving script...")
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(script_text)
            
            log_message(f"✅ Script saved: {output_path}")
            
        except Exception as e:
            log_error(f"❌ CRITICAL: Failed to save script: {e}")
            sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Enhanced Scriptwriter with centralized utils and smart error handling"
    )
    parser.add_argument("channel_name", help="Target channel name")
    parser.add_argument("topic", help="Video topic")
    parser.add_argument("target_char_count", type=int, help="Target character count")
    parser.add_argument("--output_path", required=True, help="Output script file path")
    # Config dosyasını proje root'undan otomatik bul
    default_config = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config.json")
    parser.add_argument("--config_path", default=default_config, help="Config file path")
    args = parser.parse_args()
    
    try:
        log_message("🎭 ENHANCED SCRIPTWRITER MODULE STARTED")
        log_message("="*60)
        
        # Load config
        with open(args.config_path, 'r', encoding='utf-8') as f:
            config_data = json.load(f)
        
        scriptwriter = Scriptwriter(config_data)
        scriptwriter.generate_script(
            args.channel_name, 
            args.topic, 
            args.target_char_count, 
            args.output_path
        )
        
        log_message("="*60)
        log_message("🎉 SCRIPT GENERATION COMPLETE!")
        log_message(f"📁 Script ready: {args.output_path}")
        log_message("="*60)
        
    except KeyboardInterrupt:
        log_error("\n⏹️ Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        log_error(f"\n❌ Unexpected error in Scriptwriter: {e}")
        sys.exit(1)
