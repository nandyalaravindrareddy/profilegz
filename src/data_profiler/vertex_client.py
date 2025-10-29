import os, re, json
import asyncio
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from typing import Dict, List, Any
import logging
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from google.cloud import storage

logger = logging.getLogger(__name__)

USE_OPENAI = bool(os.getenv("OPENAI_API_KEY"))
LLM_MODEL = os.getenv("LLM_MODEL", "gpt-4o-mini")

# Enhanced client caching with model variants
_openai_client = None
_vertex_models = {}

def get_openai_client():
    global _openai_client
    if _openai_client is None and USE_OPENAI:
        from openai import AsyncOpenAI
        _openai_client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    return _openai_client

def get_vertex_model(model_name="gemini-2.5-flash"):
    """Get cached Vertex AI model with support for multiple models"""
    global _vertex_models
    if model_name not in _vertex_models and not USE_OPENAI:
        try:
            from vertexai import init
            from vertexai.preview.generative_models import GenerativeModel
            PROJECT_ID = os.getenv("GCP_PROJECT","custom-plating-475002-j7")
            LOCATION = os.getenv("LOCATION", "us-central1")
            init(project=PROJECT_ID, location=LOCATION)
            _vertex_models[model_name] = GenerativeModel(model_name)
            logger.info(f"✅ Vertex AI model {model_name} initialized successfully")
        except Exception as e:
            logger.error(f"❌ Vertex AI initialization failed for {model_name}: {e}")
    return _vertex_models.get(model_name)

@retry(
    stop=stop_after_attempt(2),
    wait=wait_exponential(min=1, max=4),
    retry=retry_if_exception_type(Exception)
)
async def _call_llm_optimized(prompt: str, model_name: str = None):
    """Enhanced LLM call with model selection and better context handling"""
    if USE_OPENAI:
        client = get_openai_client()
        if not client:
            raise RuntimeError("OpenAI client not available")
        
        try:
            resp = await client.chat.completions.create(
                model=LLM_MODEL,
                messages=[
                    {"role": "system", "content": "You are a data governance and quality expert. Provide concise, actionable insights in JSON format when possible."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=500,
                timeout=30
            )
            text = resp.choices[0].message.content.strip()
        except Exception as e:
            logger.error(f"OpenAI call failed: {e}")
            raise
    else:
        model = get_vertex_model(model_name or "gemini-1.5-flash-001")
        if not model:
            raise RuntimeError("No LLM available")
        
        try:
            # Enhanced generation config for better results
            generation_config = {
                "max_output_tokens": 500,
                "temperature": 0.1,
                "top_p": 0.8,
            }
            response = model.generate_content(prompt, generation_config=generation_config)
            text = response.text.strip()
        except Exception as e:
            logger.error(f"Vertex AI call failed: {e}")
            raise
    
    # Enhanced response cleaning
    text = re.sub(r"```(json)?", "", text).strip()
    
    try:
        return json.loads(text) if text.startswith("{") else {"raw_output": text}
    except json.JSONDecodeError:
        # Try to extract JSON from text with enhanced pattern matching
        json_match = re.search(r'\{[^{}]*\}', text, re.DOTALL)
        if json_match:
            try:
                return json.loads(json_match.group())
            except:
                pass
        return {"raw_output": text, "analysis": text}

async def generate_data_quality_rules_ai(column_name: str, sample_values: List, stats: Dict) -> Dict:
    """Generate AI-powered data quality rules for a column"""
    prompt = f"""
    Analyze this column for data quality rules:
    
    Column: {column_name}
    Sample Values: {sample_values[:10]}
    Statistics: {stats}
    
    Generate 3-5 specific data quality rules with confidence scores (0-1).
    Return JSON format:
    {{
        "quality_rules": [
            {{
                "rule": "description of rule",
                "confidence": 0.95,
                "type": "completeness|validity|consistency|uniqueness"
            }}
        ],
        "data_quality_score": 0.85,
        "recommendations": ["actionable recommendation 1", "recommendation 2"]
    }}
    """
    
    try:
        result = await _call_llm_optimized(prompt, "gemini-1.5-flash-001")
        return result if isinstance(result, dict) else {"quality_rules": [], "error": "Invalid response format"}
    except Exception as e:
        logger.warning(f"AI quality rule generation failed for {column_name}: {e}")
        return {"quality_rules": [], "error": str(e)}

async def generate_column_classification_ai(column_name: str, sample_values: List, basic_stats: Dict) -> Dict:
    """Enhanced AI classification with business context"""
    prompt = f"""
    Classify this data column with business context:
    
    Column Name: {column_name}
    Sample Values: {sample_values[:15]}
    Basic Stats: {basic_stats}
    
    Provide detailed classification in JSON format:
    {{
        "business_classification": "e.g., Customer Identifier, Transaction Amount, Product Category",
        "data_sensitivity": "low|medium|high",
        "privacy_risk": "low|medium|high",
        "suggested_governance_policies": ["policy 1", "policy 2"],
        "compliance_considerations": ["GDPR", "PCI-DSS", "HIPAA"] or ["none"],
        "recommended_anonymization": "none|masking|hashing|encryption"
    }}
    
    Be specific and consider the column name and actual values.
    """
    
    try:
        result = await _call_llm_optimized(prompt, "gemini-1.5-flash-001")
        return result if isinstance(result, dict) else {"business_classification": "Unknown", "data_sensitivity": "low"}
    except Exception as e:
        logger.warning(f"AI classification failed for {column_name}: {e}")
        return {"business_classification": "Unknown", "data_sensitivity": "low"}

async def generate_dataset_summary_ai(profile_results: Dict) -> Dict:
    """Generate AI-powered dataset summary and insights"""
    summary_data = {
        "total_columns": len(profile_results),
        "sensitive_columns": [],
        "quality_issues": [],
        "column_types": {}
    }
    
    for col, data in profile_results.items():
        if col == "_dataset_insights":  # Skip the insights entry itself
            continue
        if data.get("dlp_info_types"):
            summary_data["sensitive_columns"].append(col)
        if data.get("stats", {}).get("null_pct", 0) > 0.1:
            summary_data["quality_issues"].append(col)
        summary_data["column_types"][col] = data.get("inferred_dtype", "unknown")
    
    prompt = f"""
    Analyze this dataset profile and provide executive summary:
    
    Dataset Overview:
    - Total Columns: {summary_data['total_columns']}
    - Sensitive Columns: {len(summary_data['sensitive_columns'])} - {summary_data['sensitive_columns'][:5]}
    - Data Quality Issues: {len(summary_data['quality_issues'])} columns with >10% nulls
    - Column Types: {summary_data['column_types']}
    
    Provide insights in JSON format:
    {{
        "executive_summary": "2-3 sentence overview",
        "key_risks": ["risk1", "risk2", "risk3"],
        "data_quality_score": 0.85,
        "privacy_risk_level": "low|medium|high",
        "recommended_actions": ["action1", "action2", "action3"],
        "potential_use_cases": ["use_case1", "use_case2"]
    }}
    """
    
    try:
        result = await _call_llm_optimized(prompt, "gemini-1.5-flash-001")
        return result if isinstance(result, dict) else {"executive_summary": "Analysis unavailable"}
    except Exception as e:
        logger.warning(f"AI dataset summary failed: {e}")
        return {"executive_summary": "Analysis failed", "error": str(e)}

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
            batch_prompt = "Analyze these dataset columns. For each, return JSON with: classification, profiling_rules (list of {rule,confidence}), data_sensitivity (low|medium|high).\n\n"
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