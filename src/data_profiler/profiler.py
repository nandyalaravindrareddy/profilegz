import math, time, re
import pandas as pd, numpy as np
import asyncio
from concurrent.futures import ProcessPoolExecutor, as_completed
from collections import Counter
from .dlp_client import inspect_table_dlp_optimized
from .vertex_client import call_llm_for_columns_optimized
import numbers, datetime
from functools import lru_cache
import logging

logger = logging.getLogger(__name__)

# Cache for expensive computations
@lru_cache(maxsize=1000)
def cached_shannon_entropy(values_tuple):
    """Cached entropy calculation for repeated patterns"""
    from collections import Counter
    cnt = Counter(values_tuple)
    total = sum(cnt.values()) or 1
    p = [v/total for v in cnt.values()]
    return -sum(x*math.log2(x) for x in p if x>0)

def shannon_entropy_optimized(values):
    """Optimized entropy calculation"""
    if not values:
        return 0.0
    # Convert to tuple for caching
    values_tuple = tuple(str(v) for v in values if pd.notna(v))
    return cached_shannon_entropy(values_tuple)

def infer_dtype_and_stats_optimized(series: pd.Series):
    """Optimized dtype inference with reduced operations"""
    n = len(series)
    if n == 0:
        return {"dtype": "string", "stats": {"null_pct": 1.0, "entropy": 0.0, "distinct_pct": 0.0}}
    
    # Single pass for null analysis
    non_null_mask = series.notna()
    non_null_count = non_null_mask.sum()
    null_pct = round(1 - (non_null_count/n) if n else 1.0, 3)
    
    stats = {"null_pct": null_pct}
    dtype = "string"
    
    if non_null_count > 0:
        non_null_series = series[non_null_mask]
        
        # Numeric detection with single conversion
        num_series = pd.to_numeric(non_null_series, errors="coerce")
        numeric_mask = num_series.notna()
        numeric_count = numeric_mask.sum()
        
        if numeric_count / non_null_count > 0.6:
            numeric_values = num_series[numeric_mask]
            if (numeric_values % 1 == 0).all():
                dtype = "int"
            else:
                dtype = "float"
                
            stats.update({
                "min": float(numeric_values.min()),
                "max": float(numeric_values.max()), 
                "mean": float(numeric_values.mean()),
                "std": float(numeric_values.std())
            })
        else:
            # String-based type detection
            sample_size = min(50, non_null_count)
            sample = non_null_series.sample(sample_size) if sample_size > 0 else non_null_series
            
            # Date detection - FIXED: Handle empty samples
            try:
                dt_series = pd.to_datetime(sample, errors="coerce")
                dt_count = dt_series.notna().sum()
                
                if len(sample) > 0 and dt_count / len(sample) > 0.6:
                    dtype = "date"
                    full_dt = pd.to_datetime(non_null_series, errors="coerce")
                    stats.update({
                        "min_date": str(full_dt.min()),
                        "max_date": str(full_dt.max())
                    })
                else:
                    # Boolean detection
                    str_sample = sample.astype(str).str.lower()
                    if set(str_sample) <= {"true", "false", "0", "1", "yes", "no"}:
                        dtype = "boolean"
                    else:
                        dtype = "string"
            except Exception as e:
                logger.warning(f"Date detection failed: {e}")
                dtype = "string"

    # Optimized statistics calculation
    distinct_count = series.nunique()
    stats["entropy"] = round(shannon_entropy_optimized(series.dropna().tolist()), 3)
    stats["distinct_pct"] = round(distinct_count / non_null_count if non_null_count else 0, 3)
    
    # Cardinality classification
    if stats["distinct_pct"] == 1:
        stats["cardinality"] = "high"
    elif stats["distinct_pct"] >= 0.2:
        stats["cardinality"] = "medium"
    else:
        stats["cardinality"] = "low"

    return {"dtype": dtype, "stats": stats}

def local_rules_optimized(series: pd.Series):
    """Optimized rule generation with reduced operations"""
    n = len(series)
    rules = []
    
    if n == 0:
        return rules
    
    non_null_mask = series.notna()
    non_null_count = non_null_mask.sum()
    non_null_series = series[non_null_mask]
    
    # Completeness rule
    completeness = round(non_null_count / n, 3)
    rules.append({"rule": "Must not be null", "confidence": completeness})
    
    # Uniqueness analysis
    unique_count = non_null_series.nunique()
    unique_ratio = round(unique_count / non_null_count if non_null_count else 0, 3)
    rules.append({"rule": "Should be unique", "confidence": unique_ratio})
    
    # Duplicates analysis (only if not highly unique)
    if unique_ratio < 0.95:
        dup_counts = non_null_series.value_counts()
        dups = dup_counts[dup_counts > 1].head(3).to_dict()
        if dups:
            rules.append({
                "rule": "Contains duplicates (top examples)", 
                "confidence": 0.9, 
                "examples": dups
            })
    
    # Type-specific rules
    if pd.api.types.is_numeric_dtype(non_null_series):
        rules.append({"rule": "Numeric range validation", "confidence": 0.95})
        if (non_null_series < 0).any():
            rules.append({"rule": "Contains negative values", "confidence": 0.8})
    else:
        # String analysis with sampling
        sample_size = min(100, non_null_count)
        str_sample = non_null_series.astype(str).sample(sample_size) if sample_size > 0 else non_null_series.astype(str)
        
        avg_len = int(str_sample.str.len().mean()) if len(str_sample) else 0
        rules.append({"rule": f"Average length ≈ {avg_len} chars", "confidence": 0.8})
        
        # Pattern detection
        if str_sample.str.contains("@").any():
            rules.append({"rule": "Contains email-like patterns", "confidence": 0.98})
        
        # PAN detection with sampling
        pan_matches = str_sample.str.match(r"^[A-Z]{5}\d{4}[A-Z]$").mean()
        if pan_matches > 0.5:
            rules.append({"rule": "Matches PAN format (India)", "confidence": 0.9})

    return rules

def _enhance_classification(col_data: dict) -> str:
    """Enhanced classification logic"""
    # Priority 1: DLP findings
    dlp_types = col_data.get("dlp_info_types", [])
    if dlp_types:
        # Extract primary info type from DLP
        primary_dlp = dlp_types[0].split(' (x')[0]  # Remove count
        return primary_dlp
    
    # Priority 2: LLM classification
    llm_output = col_data.get("llm_output", {})
    if isinstance(llm_output, dict) and llm_output.get("classification"):
        return llm_output["classification"]
    
    # Priority 3: Data type based classification
    dtype = col_data.get("inferred_dtype", "unknown")
    stats = col_data.get("stats", {})
    
    if dtype == "date":
        return "Temporal Data"
    elif dtype in ["int", "float"]:
        if stats.get("distinct_pct", 0) == 1:
            return "Unique Identifier"
        else:
            return "Numeric Data"
    elif dtype == "boolean":
        return "Boolean Flag"
    else:
        return "Text Data"

async def run_profiling_optimized(df: pd.DataFrame, project_id: str, parallel: bool = True, max_workers: int = 4):
    """
    Optimized profiling orchestrator with async operations and batching
    """
    start = time.time()
    columns = list(df.columns)
    results = {}
    
    # Phase 1: Parallel column profiling
    if parallel:
        # Use ProcessPoolExecutor for CPU-bound tasks
        with ProcessPoolExecutor(max_workers=min(max_workers, len(columns))) as executor:
            futures = {
                executor.submit(infer_dtype_and_stats_optimized, df[col]): col 
                for col in columns
            }
            
            for future in as_completed(futures):
                col = futures[future]
                try:
                    prof = future.result(timeout=30)
                    rules = local_rules_optimized(df[col])
                    results[col] = {
                        "inferred_dtype": prof["dtype"],
                        "stats": prof["stats"],
                        "rules": rules,
                        "classification": None
                    }
                except Exception as e:
                    logger.warning(f"Profiling error for {col}: {e}")
                    results[col] = {
                        "inferred_dtype": "string",
                        "stats": {"null_pct": 1.0},
                        "rules": [],
                        "classification": None
                    }
    else:
        # Sequential fallback
        for col in columns:
            prof = infer_dtype_and_stats_optimized(df[col])
            results[col] = {
                "inferred_dtype": prof["dtype"],
                "stats": prof["stats"],
                "rules": local_rules_optimized(df[col]),
                "classification": None
            }
    
    # Phase 2: Async DLP inspection
    try:
        dlp_summary = await inspect_table_dlp_optimized(project_id, df)
    except Exception as e:
        logger.error(f"DLP failed: {e}")
        dlp_summary = {c: {"info_types": [], "samples": [], "categories": []} for c in columns}
    
    # Merge DLP results
    for col, meta in dlp_summary.items():
        if col in results:
            results[col]["dlp_info_types"] = meta.get("info_types", [])
            results[col]["dlp_samples"] = meta.get("samples", [])
            results[col]["categories"] = meta.get("categories", [])
            results[col]["primary_category"] = meta.get("primary_category", "No Category")
    
    # Phase 3: Optimized LLM enrichment (batched)
    text_cols = [
        c for c, v in results.items() 
        if (v["inferred_dtype"] == "string" and 
            not results[c].get("classification") and
            len(df) <= 500)
    ]
    
    if text_cols:
        # Batch samples for LLM
        samples = {
            c: df[c].dropna().astype(str).head(10).tolist()
            for c in text_cols
        }
        try:
            llm_outputs = await call_llm_for_columns_optimized(samples)
            for c, out in llm_outputs.items():
                if c in results:
                    results[c]["llm_output"] = out
        except Exception as e:
            logger.warning(f"LLM enrichment skipped: {e}")
    
    # Enhanced classification after all processing
    for col, meta in results.items():
        meta["classification"] = _enhance_classification(meta)
    
    # Final confidence calculation
    for c, meta in results.items():
        confs = [r.get("confidence", 0.5) for r in meta.get("rules", []) if isinstance(r, dict)]
        meta["overall_confidence"] = round(float(sum(confs)/len(confs)) if confs else 0.5, 3)
    
    total_time = round(time.time() - start, 2)
    logger.info(f"Profiling finished in {total_time}s")
    return results