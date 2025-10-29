import math, time, re
import pandas as pd, numpy as np
import asyncio
from concurrent.futures import ProcessPoolExecutor, as_completed
from collections import Counter
from typing import Dict, List, Any
from .dlp_client import enhance_dlp_findings_with_genai, inspect_table_dlp_optimized
from .vertex_client import (
    call_llm_for_columns_optimized, 
    generate_data_quality_rules_ai,
    generate_column_classification_ai,
    generate_dataset_summary_ai
)
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
    """Optimized dtype inference with enhanced numeric statistics"""
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
                
            # ENHANCED: Add comprehensive numeric statistics
            stats.update({
                "min": float(numeric_values.min()),
                "max": float(numeric_values.max()), 
                "mean": float(numeric_values.mean()),
                "median": float(numeric_values.median()),
                "std": float(numeric_values.std()),
                "q1": float(numeric_values.quantile(0.25)),
                "q3": float(numeric_values.quantile(0.75)),
                "zeros_count": int((numeric_values == 0).sum()),
                "negatives_count": int((numeric_values < 0).sum())
            })
        else:
            # String-based type detection
            sample_size = min(50, non_null_count)
            sample = non_null_series.sample(sample_size) if sample_size > 0 else non_null_series
            
            # Date detection
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
                    if set(str_sample) <= {"true", "false", "0", "1", "yes", "no", "active", "inactive"}:
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
    
async def generate_ai_enhanced_rules(series: pd.Series, col_name: str, basic_stats: Dict):
    """Generate AI-enhanced data quality rules"""
    sample_values = series.dropna().astype(str).head(20).tolist()
    
    try:
        ai_rules = await generate_data_quality_rules_ai(col_name, sample_values, basic_stats)
        return ai_rules.get("quality_rules", [])
    except Exception as e:
        logger.warning(f"AI rule generation failed for {col_name}: {e}")
        return []

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

def data_driven_classification(col_data: dict) -> str:
    """Classification based on data patterns and statistics"""
    col_name = col_data.get('debug_name', '').lower()
    dtype = col_data.get("inferred_dtype", "unknown")
    stats = col_data.get("stats", {})
    
    # Account number detection
    if (any(term in col_name for term in ['account', 'acct', 'acc', 'num', 'no', 'id']) and 
        dtype in ["int", "string"] and 
        stats.get('distinct_pct', 0) > 0.8):
        return "Financial Account Number"
    
    # SAR/Risk indicator detection  
    if (any(term in col_name for term in ['risk', 'flag', 'alert', 'suspicious', 'sar', 'compliance', 'status']) or
        (dtype == "boolean") or
        (stats.get('distinct_pct', 0) <= 3)):  # Low cardinality suggests flags
        return "Risk Indicator"
    
    # Balance/amount detection
    if (any(term in col_name for term in ['balance', 'amount', 'value', 'price', 'cost', 'fee']) and
        dtype in ["int", "float"]):
        return "Transaction Amount"
    
    # Personal information detection
    if any(term in col_name for term in ['birth', 'dob']):
        return "Customer Date of Birth"
    
    if any(term in col_name for term in ['name', 'first', 'last', 'fullname']):
        return "Customer Name"
    
    if any(term in col_name for term in ['email', 'phone', 'contact']):
        return "Customer Contact Information"
    
    if any(term in col_name for term in ['country', 'state', 'city', 'address', 'location']):
        return "Geographical Location"
    
    # Default data type based classification
    if dtype == "date":
        return "Timestamp"
    elif dtype in ["int", "float"]:
        if stats.get('distinct_pct', 0) == 1:
            return "Unique Identifier"
        return "Numeric Measurement"
    elif dtype == "boolean":
        return "Status Flag"
    else:
        if stats.get('distinct_pct', 0) == 1:
            return "Unique Identifier"
        return "Text Data"

# In profiler.py - REPLACE the _enhance_classification function

# In profiler.py - REPLACE the _enhance_classification function

def _enhance_classification(col_data: dict) -> str:
    """ENHANCED classification with better DLP-based logic"""
    
    col_name = col_data.get('debug_name', 'unknown')
    
    # PRIORITY 1: Use primary_category if it's specific
    primary_category = col_data.get("primary_category")
    if primary_category and primary_category not in ["Unknown", "No Category", "Other", "Generic Data", "Business Identifier"]:
        logger.info(f"✅ Using primary_category: {primary_category}")
        return primary_category
    
    # PRIORITY 2: Use DLP info types for SPECIFIC classification
    dlp_info_types = col_data.get("dlp_info_types", [])
    if dlp_info_types:
        dlp_based_class = get_specific_dlp_classification(dlp_info_types, col_name)
        if dlp_based_class != "Business Identifier":
            logger.info(f"✅ Using DLP-based classification: {dlp_based_class}")
            return dlp_based_class
    
    # PRIORITY 3: Use column name patterns for specific classification
    column_based_class = get_column_name_classification(col_name)
    if column_based_class != "Business Identifier":
        logger.info(f"✅ Using column-based classification: {column_based_class}")
        return column_based_class
    
    # PRIORITY 4: Simple data type classification
    classification = get_simple_data_type_classification(col_data)
    logger.info(f"✅ Using data type classification: {classification}")
    return classification

def get_specific_dlp_classification(dlp_info_types: List[str], column_name: str) -> str:
    """Get specific classification based on DLP findings"""
    column_lower = column_name.lower()
    
    # Extract primary DLP types
    for info_type in dlp_info_types:
        clean_type = re.sub(r'\s*\(x?\d+\)', '', info_type).strip().upper()
        
        if "EMAIL" in clean_type:
            return "Customer Contact Information"
        elif "PERSON_NAME" in clean_type:
            return "Customer Name"
        elif "PHONE" in clean_type:
            return "Customer Contact Information"
        elif "LOCATION" in clean_type or "ADDRESS" in clean_type:
            return "Geographical Location"
        elif "CREDIT_CARD" in clean_type:
            return "Payment Information"
        elif "DATE_OF_BIRTH" in clean_type:
            return "Customer Date of Birth"
        elif "IBAN" in clean_type:
            return "Financial Account Number"
        elif "SSN" in clean_type:
            return "National Identification"
    
    return "Business Identifier"

def get_column_name_classification(column_name: str) -> str:
    """Get specific classification based on column name patterns"""
    column_lower = column_name.lower()
    
    if any(term in column_lower for term in ['email', 'e-mail']):
        return "Customer Contact Information"
    elif any(term in column_lower for term in ['phone', 'mobile', 'telephone']):
        return "Customer Contact Information"
    elif any(term in column_lower for term in ['name', 'first', 'last', 'fullname']):
        return "Customer Name"
    elif any(term in column_lower for term in ['country', 'state', 'city', 'address', 'location']):
        return "Geographical Location"
    elif any(term in column_lower for term in ['account', 'acct', 'acc', 'number']):
        return "Financial Account Number"
    elif any(term in column_lower for term in ['balance', 'amount', 'value']):
        return "Transaction Amount"
    elif any(term in column_lower for term in ['dob', 'birth', 'age']):
        return "Customer Date of Birth"
    elif any(term in column_lower for term in ['credit', 'card', 'payment']):
        return "Payment Information"
    elif any(term in column_lower for term in ['ssn', 'social', 'security']):
        return "National Identification"
    elif any(term in column_lower for term in ['passport']):
        return "Government ID"
    elif any(term in column_lower for term in ['aadhaar', 'uidai']):
        return "Government ID"
    elif any(term in column_lower for term in ['pan', 'permanent']):
        return "Tax Identification"
    elif any(term in column_lower for term in ['iban', 'swift']):
        return "Financial Account Number"
    elif any(term in column_lower for term in ['risk', 'sar', 'flag', 'alert', 'suspicious', 'narrative']):
        return "Risk Indicator"
    elif any(term in column_lower for term in ['customer_id', 'user_id', 'client_id']):
        return "Customer Identifier"
    
    return "Business Identifier"

def get_simple_data_type_classification(col_data: dict) -> str:
    """Simple data type based classification"""
    dtype = col_data.get("inferred_dtype", "unknown")
    stats = col_data.get("stats", {})
    
    if dtype == "date":
        return "Timestamp"
    elif dtype in ["int", "float"]:
        if stats.get('distinct_pct', 0) == 1:
            return "Unique Identifier"
        return "Numeric Data"
    elif dtype == "boolean":
        return "Status Flag"
    else:
        if stats.get('distinct_pct', 0) == 1:
            return "Unique Identifier"
        return "Text Data"

def get_dlp_based_classification_simple(dlp_types: List[str], column_name: str) -> str:
    """Simple DLP-based classification"""
    column_lower = column_name.lower()
    
    # Simple mapping
    for dlp_type in dlp_types:
        if "EMAIL" in dlp_type:
            return "Customer Contact Information"
        elif "PERSON_NAME" in dlp_type:
            return "Customer Name"
        elif "PHONE" in dlp_type:
            return "Customer Contact Information"
        elif "LOCATION" in dlp_type or "ADDRESS" in dlp_type:
            return "Geographical Location"
        elif "CREDIT_CARD" in dlp_type:
            return "Payment Information"
        elif "DATE_OF_BIRTH" in dlp_type:
            return "Customer Date of Birth"
    
    # Column name fallbacks
    if any(term in column_lower for term in ['email']):
        return "Customer Contact Information"
    elif any(term in column_lower for term in ['phone']):
        return "Customer Contact Information"
    elif any(term in column_lower for term in ['name']):
        return "Customer Name"
    elif any(term in column_lower for term in ['country', 'city', 'address']):
        return "Geographical Location"
    elif any(term in column_lower for term in ['account']):
        return "Financial Account Number"
    elif any(term in column_lower for term in ['balance', 'amount']):
        return "Transaction Amount"
    elif any(term in column_lower for term in ['dob', 'birth']):
        return "Customer Date of Birth"
    
    return "Business Data"

def simple_data_driven_classification(col_data: dict) -> str:
    """Simple data-driven classification without complex logic"""
    col_name = col_data.get('debug_name', '').lower()
    dtype = col_data.get("inferred_dtype", "unknown")
    
    # Very simple mapping
    if 'email' in col_name:
        return "Customer Contact Information"
    elif 'phone' in col_name:
        return "Customer Contact Information"
    elif 'name' in col_name:
        return "Customer Name"
    elif any(term in col_name for term in ['country', 'city', 'address']):
        return "Geographical Location"
    elif any(term in col_name for term in ['account', 'acct']):
        return "Financial Account Number"
    elif any(term in col_name for term in ['balance', 'amount']):
        return "Transaction Amount"
    elif any(term in col_name for term in ['dob', 'birth']):
        return "Customer Date of Birth"
    elif any(term in col_name for term in ['credit', 'card']):
        return "Payment Information"
    elif any(term in col_name for term in ['risk', 'sar', 'flag']):
        return "Risk Indicator"
    
    # Basic type-based
    if dtype == "date":
        return "Timestamp"
    elif dtype in ["int", "float"]:
        return "Numeric Data"
    elif dtype == "boolean":
        return "Status Flag"
    else:
        return "Text Data"

async def run_profiling_optimized(df: pd.DataFrame, project_id: str, parallel: bool = True, max_workers: int = 4):
    """
    Optimized profiling orchestrator with enhanced AI integration
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
                        "classification": None,
                        "ai_enhanced_rules": []
                    }
                except Exception as e:
                    logger.warning(f"Profiling error for {col}: {e}")
                    results[col] = {
                        "inferred_dtype": "string",
                        "stats": {"null_pct": 1.0},
                        "rules": [],
                        "classification": None,
                        "ai_enhanced_rules": []
                    }
    else:
        # Sequential fallback
        for col in columns:
            prof = infer_dtype_and_stats_optimized(df[col])
            results[col] = {
                "inferred_dtype": prof["dtype"],
                "stats": prof["stats"],
                "rules": local_rules_optimized(df[col]),
                "classification": None,
                "ai_enhanced_rules": []
            }
    
    # Add debug names for classification tracking
    for col, meta in results.items():
        meta["debug_name"] = col
    
    # Phase 2: AI-enhanced DLP inspection (PRIMARY CLASSIFICATION)
    try:
        logger.info("🚀 Starting PRIMARY GenAI classification...")
        dlp_summary = await enhance_dlp_findings_with_genai(project_id, df)
    except Exception as e:
        logger.error(f"DLP failed: {e}")
        dlp_summary = {c: {"info_types": [], "samples": [], "categories": []} for c in columns}
    
    # Merge DLP results with AI enhancements
    for col, meta in dlp_summary.items():
        if col in results:
            results[col]["dlp_info_types"] = meta.get("info_types", [])
            results[col]["dlp_samples"] = meta.get("samples", [])
            results[col]["categories"] = meta.get("categories", [])
            results[col]["primary_category"] = meta.get("primary_category", "No Category")
            results[col]["risk_level"] = meta.get("risk_level", "low")
            results[col]["business_context"] = meta.get("business_context", "Unknown")
            results[col]["compliance_considerations"] = meta.get("compliance_considerations", [])
            results[col]["recommended_handling"] = meta.get("recommended_handling", "standard")
            results[col]["confidence_score"] = meta.get("confidence_score", 0.5)
            results[col]["detected_patterns"] = meta.get("detected_patterns", [])
    
    # Phase 3: AI-enhanced classification and rules
    ai_tasks = []
    for col, data in results.items():
        if len(df) <= 1000:  # Limit AI processing for larger datasets
            sample_values = df[col].dropna().astype(str).head(15).tolist()
            basic_stats = data.get("stats", {})
            
            # Create AI classification task
            ai_tasks.append(
                generate_column_classification_ai(col, sample_values, basic_stats)
            )
        else:
            ai_tasks.append(asyncio.sleep(0))  # Placeholder
    
    # Execute AI classification tasks
    try:
        ai_results = await asyncio.gather(*ai_tasks, return_exceptions=True)
        for i, (col, ai_result) in enumerate(zip(results.keys(), ai_results)):
            if not isinstance(ai_result, Exception) and isinstance(ai_result, dict):
                results[col]["ai_classification"] = ai_result
    except Exception as e:
        logger.warning(f"AI classification batch failed: {e}")
    
    # Phase 4: AI-enhanced quality rules for key columns
    key_columns = [col for col in columns if results[col].get("dlp_info_types") or results[col].get("stats", {}).get("distinct_pct", 0) == 1]
    
    if key_columns and len(df) <= 500:
        rule_tasks = []
        for col in key_columns:
            rule_tasks.append(
                generate_ai_enhanced_rules(df[col], col, results[col].get("stats", {}))
            )
        
        try:
            rule_results = await asyncio.gather(*rule_tasks, return_exceptions=True)
            for i, (col, rule_result) in enumerate(zip(key_columns, rule_results)):
                if not isinstance(rule_result, Exception) and isinstance(rule_result, list):
                    results[col]["ai_enhanced_rules"] = rule_result
        except Exception as e:
            logger.warning(f"AI rule generation failed: {e}")
    
    # Phase 5: Enhanced classification after all processing
    for col, meta in results.items():
        if col == "_dataset_insights":
            continue
        logger.info(f"🎯 Final classification for {col}:")
        meta["classification"] = _enhance_classification(meta)
        logger.info(f"🎯 Result: {meta['classification']}")
        
        # Add AI-derived sensitivity if available
        ai_class = meta.get("ai_classification", {})
        if isinstance(ai_class, dict):
            meta["data_sensitivity"] = ai_class.get("data_sensitivity", "low")
            meta["privacy_risk"] = ai_class.get("privacy_risk", "low")
        else:
            # Use DLP AI classification
            meta["data_sensitivity"] = meta.get("risk_level", "low")
            meta["privacy_risk"] = meta.get("risk_level", "low")
    
    # Phase 6: Generate dataset-level AI insights
    try:
        dataset_insights = await generate_dataset_summary_ai(results)
        results["_dataset_insights"] = dataset_insights
    except Exception as e:
        logger.warning(f"Dataset-level AI insights failed: {e}")
        results["_dataset_insights"] = {"executive_summary": "AI analysis unavailable"}
    
    # Final confidence calculation
    for c, meta in results.items():
        if c == "_dataset_insights":
            continue
            
        confs = [r.get("confidence", 0.5) for r in meta.get("rules", []) if isinstance(r, dict)]
        ai_confs = [r.get("confidence", 0.5) for r in meta.get("ai_enhanced_rules", []) if isinstance(r, dict)]
        
        all_confs = confs + ai_confs
        meta["overall_confidence"] = round(float(sum(all_confs)/len(all_confs)) if all_confs else 0.5, 3)
    
    total_time = round(time.time() - start, 2)
    logger.info(f"Profiling finished in {total_time}s")
    return results