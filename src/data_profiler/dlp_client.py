from google.cloud import dlp_v2
from google.api_core import exceptions as gcp_exceptions
from collections import defaultdict
import math, time, traceback
import asyncio
from concurrent.futures import ThreadPoolExecutor
import logging
from typing import Dict, List, Any
import pandas as pd

logger = logging.getLogger(__name__)

# Optimized info types with better categorization
DIVERSE_INFO_TYPES = [
    "EMAIL_ADDRESS", "PHONE_NUMBER", "PERSON_NAME", 
    "CREDIT_CARD_NUMBER", "US_SOCIAL_SECURITY_NUMBER",
    "IP_ADDRESS", "DATE_OF_BIRTH", "IBAN_CODE", 
    "BANK_ACCOUNT_NUMBER", "ACCOUNT_NUMBER",
    "US_PASSPORT", "US_DRIVERS_LICENSE_NUMBER", 
    "AGE", "GENDER", "STREET_ADDRESS", "CITY",
    "STATE", "COUNTRY", "ZIP_CODE", "IN_PAN", 
    "IN_AADHAAR", "LOCATION", "COORDINATE",
    "VEHICLE_IDENTIFICATION_NUMBER", "MAC_ADDRESS",
    "IMEI_HARDWARE_ID", "URL", "DOMAIN_NAME"
]

CUSTOM_INFO_TYPES = [
    {
        "info_type": {"name": "CUSTOM_PAN_REGEX"},
        "regex": {"pattern": r"[A-Z]{5}\d{4}[A-Z]"}
    },
    {
        "info_type": {"name": "CUSTOM_TXN"},
        "regex": {"pattern": r"TXN\d{6,}"}
    },
]

# Client caching to avoid repeated initialization
_dlp_client = None
_info_type_cache = {}

def get_dlp_client():
    """Get cached DLP client"""
    global _dlp_client
    if _dlp_client is None:
        _dlp_client = dlp_v2.DlpServiceClient()
    return _dlp_client

async def list_region_info_types_optimized(client, project_id: str, region: str = "global"):
    """Cached info type listing"""
    cache_key = f"{project_id}_{region}"
    if cache_key in _info_type_cache:
        return _info_type_cache[cache_key]
    
    try:
        parent = f"projects/{project_id}/locations/{region}"
        response = client.list_info_types(request={"parent": parent})
        types = [it.name for it in response.info_types]
        _info_type_cache[cache_key] = types
        return types
    except Exception as e:
        logger.warning(f"Failed to list info types for {region}: {e}")
        _info_type_cache[cache_key] = None
        return None

def _categorize_finding(info_type: str) -> str:
    """Better categorization of DLP findings"""
    info_type_upper = info_type.upper()
    
    # Personal Identifiers
    if any(term in info_type_upper for term in ['EMAIL', 'PHONE', 'PERSON', 'NAME', 'DOB', 'AGE', 'GENDER']):
        return "Personal Identifiers"
    
    # Government IDs
    elif any(term in info_type_upper for term in ['PAN', 'AADHAAR', 'PASSPORT', 'DRIVERS', 'SSN', 'VOTER']):
        return "Government IDs"
    
    # Financial Information
    elif any(term in info_type_upper for term in ['BANK', 'ACCOUNT', 'CREDIT', 'IBAN', 'FINANCE', 'TRANSACTION']):
        return "Financial Information"
    
    # Location Data
    elif any(term in info_type_upper for term in ['ADDRESS', 'LOCATION', 'CITY', 'STATE', 'COUNTRY', 'ZIP', 'POSTAL']):
        return "Location Data"
    
    # Digital Identifiers
    elif any(term in info_type_upper for term in ['IP', 'USERNAME', 'DEVICE', 'COOKIE', 'TOKEN']):
        return "Digital Identifiers"
    
    # Generic Personal
    elif any(term in info_type_upper for term in ['DATE', 'BIRTH', 'GENDER']):
        return "Personal Data"
    
    else:
        return "Other Sensitive Data"

def _dlp_table_from_col_optimized(col_name, series):
    """Optimized single column table creation"""
    # Sample data for efficiency
    sample_data = series.dropna().astype(str).head(50)
    headers = [{"name": col_name}]
    rows = [{"values": [{"string_value": str(v)}]} for v in sample_data]
    return {"table": {"headers": headers, "rows": rows}}

async def inspect_table_dlp_optimized(project_id: str, df, region: str = "global", max_info_types: int = 100):
    """
    Optimized DLP inspection with better classification
    """
    logger.info("🚀 Starting optimized DLP inspection")
    client = get_dlp_client()
    parent = f"projects/{project_id}"
    
    # Get available types or use diverse set
    try:
        available_types = await list_region_info_types_optimized(client, project_id, region)
        if available_types:
            # Use intersection with diverse types
            info_types = [it for it in DIVERSE_INFO_TYPES if it in available_types]
            # Add additional available types
            for it in available_types:
                if it not in info_types and len(info_types) < max_info_types:
                    info_types.append(it)
        else:
            info_types = DIVERSE_INFO_TYPES.copy()
    except Exception:
        info_types = DIVERSE_INFO_TYPES.copy()
    
    info_types = info_types[:max_info_types]
    
    summary = {col: {"info_types": [], "samples": [], "categories": []} for col in df.columns}
    
    # Process columns individually for better accuracy
    for col in df.columns:
        try:
            item = _dlp_table_from_col_optimized(col, df[col])
            inspect_config = {
                "include_quote": True,
                "min_likelihood": dlp_v2.Likelihood.POSSIBLE,
                "limits": {"max_findings_per_item": 50},
                "info_types": [{"name": n} for n in info_types],
                "custom_info_types": CUSTOM_INFO_TYPES
            }
            
            response = client.inspect_content(
                request={"parent": parent, "inspect_config": inspect_config, "item": item}
            )
            
            findings = getattr(response.result, "findings", []) or []
            type_counter = {}
            categories = set()
            sample_quotes = []

            for f in findings:
                it = f.info_type.name if f.info_type else "UNKNOWN"
                type_counter[it] = type_counter.get(it, 0) + 1
                category = _categorize_finding(it)
                categories.add(category)
                
                if hasattr(f, "quote") and f.quote:
                    if f.quote not in sample_quotes and len(sample_quotes) < 3:
                        sample_quotes.append(f.quote)

            # Store findings with categories
            summary[col]["info_types"] = [f"{t} (x{c})" if c > 1 else t for t, c in sorted(type_counter.items(), key=lambda x: x[1], reverse=True)]
            summary[col]["samples"] = sample_quotes
            summary[col]["categories"] = list(categories)
            summary[col]["primary_category"] = max(categories, key=lambda x: len(x)) if categories else "No Category"

        except Exception as e:
            logger.warning(f"DLP inspection failed for {col}: {e}")
            continue
    
    logger.info("✅ Optimized DLP inspection completed")
    return summary