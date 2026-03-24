import os
import json
from google import genai
from google.genai import types

# Define the system instruction to ensure strict translation
SYSTEM_INSTRUCTION = """
You are a JSON transformation engine. You receive a source JSON object and a target schema. 
Your job is to produce a transformed JSON that matches the target schema exactly.
- Map fields by semantic meaning, not just name (e.g. user_id -> customer_uuid if they mean the same thing)
- If a field has no match, use null
- Coerce types to match the target schema where safe.
- Respond ONLY with valid JSON. No explanation, no markdown, no backticks.
"""

def build_prompt(source: dict, target_schema: dict) -> str:
    return f"""
SOURCE JSON:
{json.dumps(source, indent=2)}

TARGET SCHEMA (keys and types expected):
{json.dumps(target_schema, indent=2)}

Return the transformed JSON now.
"""

def parse_and_clean_response(text: str) -> dict:
    text = text.strip()
    # Attempt to clean markdown backticks if Gemini ignores instructions
    if text.startswith("```json"):
        text = text[7:]
    elif text.startswith("```"):
        text = text[3:]
    if text.endswith("```"):
        text = text[:-3]
    text = text.strip()
    return json.loads(text)

async def transform_json(source: dict, target_schema: dict) -> dict:
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise ValueError("GEMINI_API_KEY environment variable is not set")
    
    # Initialize the new google-genai client
    client = genai.Client(api_key=api_key)
    
    prompt = build_prompt(source, target_schema)
    
    # Configure model to be deterministic and respond in JSON
    config = types.GenerateContentConfig(
        system_instruction=SYSTEM_INSTRUCTION,
        temperature=0.0,
        response_mime_type="application/json",
    )
    
    # Try the call with 1 retry on parse failure as requested
    max_retries = 1
    last_error = None
    
    for attempt in range(max_retries + 1):
        try:
            # Note: We use the synchronous generate_content because the genai client async methods 
            # might require specific async client setup, and typically the standard one is robust enough for API wraps.
            # Using standard generate_content in async context (FastAPI will run it in a threadpool)
            response = client.models.generate_content(
                model='gemini-2.5-flash',
                contents=prompt,
                config=config,
            )
            
            output_json = parse_and_clean_response(response.text)
            
            fields_mapped = 0
            fields_nulled = 0
            
            # Key validation check & Metrics calculation
            if isinstance(output_json, list):
                for i, item in enumerate(output_json):
                    if isinstance(item, dict):
                        for expected_key in target_schema.keys():
                            if expected_key not in item:
                                item[expected_key] = None
                            # Calculate metrics only for the first row to avoid inflated counts
                            if i == 0:
                                if item[expected_key] is not None:
                                    fields_mapped += 1
                                else:
                                    fields_nulled += 1
            elif isinstance(output_json, dict):
                for expected_key in target_schema.keys():
                    if expected_key not in output_json:
                        output_json[expected_key] = None
                    if output_json[expected_key] is not None:
                        fields_mapped += 1
                    else:
                        fields_nulled += 1
                    
            return {
                "transformed": output_json,
                "fields_mapped": fields_mapped,
                "fields_nulled": fields_nulled
            }
            
        except json.JSONDecodeError as e:
            last_error = RuntimeError(f"JSON Parse failed: {str(e)}")
            continue # Retry
        except Exception as e:
            raise RuntimeError(f"Transformation failed: {str(e)}")
            
    if last_error:
        raise last_error
