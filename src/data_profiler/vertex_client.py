import os, re, json
import asyncio
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from typing import Dict, List, Any
import logging
from concurrent.futures import ThreadPoolExecutor
import pandas as pd

logger = logging.getLogger(__name__)

USE_OPENAI = bool(os.getenv("OPENAI_API_KEY"))
LLM_MODEL = os.getenv("LLM_MODEL", "gpt-4o-mini")

# Client caching
_openai_client = None
_vertex_model = None

def get_openai_client():
    global _openai_client
    if _openai_client is None and USE_OPENAI:
        from openai import AsyncOpenAI
        _openai_client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    return _openai_client

def get_vertex_model():
    global _vertex_model
    if _vertex_model is None and not USE_OPENAI:
        try:
            from vertexai import init
            from vertexai.preview.generative_models import GenerativeModel
            PROJECT_ID = os.getenv("GCP_PROJECT")
            LOCATION = os.getenv("LOCATION", "us-central1")
            init(project=PROJECT_ID, location=LOCATION)
            _vertex_model = GenerativeModel("gemini-2.5-flash")
        except Exception as e:
            logger.error(f"Vertex AI initialization failed: {e}")
    return _vertex_model

@retry(
    stop=stop_after_attempt(2),
    wait=wait_exponential(min=1, max=4),
    retry=retry_if_exception_type(Exception)
)
async def _call_llm_optimized(prompt: str):
    """Optimized LLM call with better error handling"""
    if USE_OPENAI:
        client = get_openai_client()
        if not client:
            raise RuntimeError("OpenAI client not available")
        
        try:
            resp = await client.chat.completions.create(
                model=LLM_MODEL,
                messages=[
                    {"role": "system", "content": "You are a data governance expert. Return pure JSON only."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=400,
                timeout=30
            )
            text = resp.choices[0].message.content.strip()
        except Exception as e:
            logger.error(f"OpenAI call failed: {e}")
            raise
    else:
        model = get_vertex_model()
        if not model:
            raise RuntimeError("No LLM available")
        
        try:
            response = model.generate_content(prompt, max_output_tokens=350)
            text = response.text.strip()
        except Exception as e:
            logger.error(f"Vertex AI call failed: {e}")
            raise
    
    # Clean response
    text = re.sub(r"```(json)?", "", text).strip()
    
    try:
        return json.loads(text) if text.startswith("{") else {"raw_output": text}
    except json.JSONDecodeError:
        # Try to extract JSON from text
        json_match = re.search(r'\{.*\}', text, re.DOTALL)
        if json_match:
            try:
                return json.loads(json_match.group())
            except:
                pass
        return {"raw_output": text}

def _create_batched_prompts(samples_by_col: Dict[str, List]) -> List[Dict]:
    """Batch multiple columns into single prompts for efficiency"""
    batches = []
    current_batch = {}
    current_token_estimate = 0
    max_tokens_per_batch = 2000
    
    for col, values in samples_by_col.items():
        col_prompt = f"Column: {col}\nSample values: {values[:8]}\n"
        token_estimate = len(col_prompt.split())
        
        if current_token_estimate + token_estimate > max_tokens_per_batch and current_batch:
            batches.append(current_batch)
            current_batch = {}
            current_token_estimate = 0
        
        current_batch[col] = col_prompt
        current_token_estimate += token_estimate
    
    if current_batch:
        batches.append(current_batch)
    
    return batches

async def call_llm_for_columns_optimized(samples_by_col: dict, max_concurrent: int = 3):
    """
    Optimized LLM calls with batching and rate limiting
    """
    if not samples_by_col:
        return {}
    
    # Create batched prompts
    batched_prompts = _create_batched_prompts(samples_by_col)
    results = {}
    
    # Process batches with concurrency limit
    semaphore = asyncio.Semaphore(max_concurrent)
    
    async def process_batch(batch):
        async with semaphore:
            batch_prompt = "Analyze these dataset columns. For each, return JSON with: classification, profiling_rules (list of {rule,confidence}).\n\n"
            batch_prompt += "\n".join([f"{prompt}" for col, prompt in batch.items()])
            batch_prompt += "\n\nReturn a JSON object mapping column names to their analysis."
            
            try:
                batch_result = await _call_llm_optimized(batch_prompt)
                
                # Parse batch result
                if isinstance(batch_result, dict) and "raw_output" not in batch_result:
                    return batch_result
                else:
                    return {col: batch_result for col in batch.keys()}
                    
            except Exception as e:
                logger.warning(f"LLM batch failed: {e}")
                return {col: {"error": str(e)} for col in batch.keys()}
    
    # Run all batches
    batch_tasks = [process_batch(batch) for batch in batched_prompts]
    batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
    
    # Combine results
    for batch_result in batch_results:
        if isinstance(batch_result, dict):
            results.update(batch_result)
        elif isinstance(batch_result, Exception):
            logger.error(f"Batch processing failed: {batch_result}")
    
    # Ensure all columns have results
    for col in samples_by_col.keys():
        if col not in results:
            results[col] = {"error": "No LLM response"}
    
    return results