from google.cloud import dlp_v2
from google.api_core import exceptions as gcp_exceptions
from collections import defaultdict
import math, time, traceback
import asyncio
from concurrent.futures import ThreadPoolExecutor
import logging
from typing import Dict, List, Any
import pandas as pd
import vertexai
from vertexai.preview.generative_models import GenerativeModel
import json
import re
import os

logger = logging.getLogger(__name__)

# Client caching
_dlp_client = None
_vertex_model = None

def get_dlp_client():
    """Get cached DLP client"""
    global _dlp_client
    if _dlp_client is None:
        try:
            _dlp_client = dlp_v2.DlpServiceClient()
            logger.info("✅ DLP client initialized successfully")
        except Exception as e:
            logger.error(f"❌ Failed to initialize DLP client: {e}")
            raise
    return _dlp_client

def get_vertex_model():
    """Get cached Vertex AI model"""
    global _vertex_model
    if _vertex_model is None:
        try:
            PROJECT_ID = os.getenv("GCP_PROJECT","custom-plating-475002-j7")
            if not PROJECT_ID:
                logger.error("❌ GCP_PROJECT environment variable not set")
                return None
                
            LOCATION = os.getenv("LOCATION", "us-central1")
            vertexai.init(project=PROJECT_ID, location=LOCATION)
            _vertex_model = GenerativeModel("gemini-1.5-flash-001")
            logger.info("✅ Vertex AI model initialized successfully")
        except Exception as e:
            logger.error(f"❌ Vertex AI initialization failed: {e}")
    return _vertex_model

def analyze_data_patterns(sample_values: List[str], data_type: str, stats: Dict) -> Dict:
    """Analyze actual data patterns for classification"""
    patterns = {}
    
    if not sample_values:
        return patterns
    
    # Convert to string for pattern analysis
    str_samples = [str(v) for v in sample_values if v is not None and str(v).strip() != '']
    
    if not str_samples:
        return patterns
    
    # Numeric pattern analysis
    if data_type in ["int", "float"]:
        patterns["is_numeric"] = True
        
        # Account number patterns (numeric, specific length ranges)
        sample_lengths = [len(str(v)) for v in str_samples]
        avg_length = sum(sample_lengths) / len(sample_lengths) if sample_lengths else 0
        if 8 <= avg_length <= 17:
            patterns["is_account_like"] = True
        
        # Transaction amount patterns
        if stats and 'min' in stats and 'max' in stats:
            if stats['min'] >= 0 and stats['max'] > 1000:
                patterns["is_transaction_amount"] = True
    
    # Boolean/flag pattern analysis
    unique_vals = set(str_samples)
    if len(unique_vals) <= 3 and all(v.lower() in ['true', 'false', '0', '1', 'y', 'n', 'yes', 'no', 'active', 'inactive'] for v in unique_vals):
        patterns["is_risk_indicator"] = True
    
    # Identifier pattern analysis
    if data_type == "string" and stats and stats.get('distinct_pct', 0) > 0.9:
        patterns["is_identifier"] = True
    
    # Date pattern analysis
    if data_type == "date":
        patterns["is_temporal"] = True
        if any(term in str(sample_values).lower() for term in ['birth', 'dob', 'age']):
            patterns["is_personal_date"] = True
    
    return patterns

# In dlp_client.py - REPLACE the get_enhanced_fallback function

def get_enhanced_fallback(column_name: str, sample_values: List[str], data_type: str, stats: Dict, dlp_findings: List[str]) -> Dict:
    """Enhanced fallback using DLP findings and column name patterns"""
    
    column_lower = column_name.lower()
    
    # EXTRACT DLP INFO TYPES FOR CLASSIFICATION
    dlp_primary_types = []
    for finding in dlp_findings:
        # Extract clean type name (remove counts)
        clean_type = re.sub(r'\s*\(x?\d+\)', '', finding).strip()
        if clean_type and clean_type not in dlp_primary_types:
            dlp_primary_types.append(clean_type)
    
    # PRIORITY 1: Use DLP findings for specific classification
    for dlp_type in dlp_primary_types:
        if "EMAIL" in dlp_type:
            return create_classification_result("Customer Contact Information", "medium")
        elif "PERSON_NAME" in dlp_type:
            return create_classification_result("Customer Name", "medium")
        elif "PHONE" in dlp_type:
            return create_classification_result("Customer Contact Information", "medium")
        elif "LOCATION" in dlp_type or "ADDRESS" in dlp_type:
            return create_classification_result("Geographical Location", "low")
        elif "CREDIT_CARD" in dlp_type:
            return create_classification_result("Payment Information", "high")
        elif "DATE_OF_BIRTH" in dlp_type:
            return create_classification_result("Customer Date of Birth", "medium")
        elif "IBAN" in dlp_type:
            return create_classification_result("Financial Account Number", "high")
    
    # PRIORITY 2: Use column name patterns
    if any(term in column_lower for term in ['email', 'e-mail']):
        return create_classification_result("Customer Contact Information", "medium")
    elif any(term in column_lower for term in ['phone', 'mobile', 'telephone']):
        return create_classification_result("Customer Contact Information", "medium")
    elif any(term in column_lower for term in ['name', 'first', 'last', 'fullname']):
        return create_classification_result("Customer Name", "medium")
    elif any(term in column_lower for term in ['country', 'state', 'city', 'address', 'location']):
        return create_classification_result("Geographical Location", "low")
    elif any(term in column_lower for term in ['account', 'acct', 'acc', 'number']):
        return create_classification_result("Financial Account Number", "medium")
    elif any(term in column_lower for term in ['balance', 'amount', 'value', 'price', 'cost']):
        return create_classification_result("Transaction Amount", "medium")
    elif any(term in column_lower for term in ['dob', 'birth', 'age']):
        return create_classification_result("Customer Date of Birth", "medium")
    elif any(term in column_lower for term in ['credit', 'card', 'payment']):
        return create_classification_result("Payment Information", "high")
    elif any(term in column_lower for term in ['ssn', 'social', 'security']):
        return create_classification_result("National Identification", "high")
    elif any(term in column_lower for term in ['passport']):
        return create_classification_result("Government ID", "high")
    elif any(term in column_lower for term in ['aadhaar', 'uidai']):
        return create_classification_result("Government ID", "high")
    elif any(term in column_lower for term in ['pan', 'permanent']):
        return create_classification_result("Tax Identification", "high")
    elif any(term in column_lower for term in ['iban', 'swift']):
        return create_classification_result("Financial Account Number", "high")
    elif any(term in column_lower for term in ['risk', 'sar', 'flag', 'alert', 'suspicious', 'narrative']):
        return create_classification_result("Risk Indicator", "high")
    elif any(term in column_lower for term in ['customer_id', 'user_id', 'client_id']):
        return create_classification_result("Customer Identifier", "medium")
    
    # PRIORITY 3: Data type based classification
    if data_type == "date":
        return create_classification_result("Timestamp", "low")
    elif data_type in ["int", "float"]:
        if stats and stats.get('distinct_pct', 0) == 1:
            return create_classification_result("Unique Identifier", "medium")
        return create_classification_result("Numeric Data", "low")
    elif data_type == "boolean":
        return create_classification_result("Status Flag", "low")
    else:
        if stats and stats.get('distinct_pct', 0) == 1:
            return create_classification_result("Unique Identifier", "medium")
        return create_classification_result("Text Data", "low")

def create_classification_result(primary_category: str, risk_level: str) -> Dict:
    """Create a standardized classification result"""
    return {
        "primary_category": primary_category,
        "risk_level": risk_level,
        "business_context": f"Classified as {primary_category} based on data patterns",
        "compliance_considerations": ["GDPR"] if risk_level != "low" else [],
        "recommended_handling": "encrypt" if risk_level != "low" else "standard",
        "confidence_score": 0.8,
        "detected_patterns": ["pattern_based_classification"]
    }
    
def is_valid_classification(result: Dict) -> bool:
    """Validate that GenAI returned a meaningful classification"""
    if not result.get("primary_category"):
        return False
    
    poor_classifications = ["Unknown", "Other", "Personal Identifiers", "Generic Data", "No Category"]
    return result["primary_category"] not in poor_classifications

# In dlp_client.py - REPLACE the classify_data_with_genai function

async def classify_data_with_genai(column_name: str, sample_values: List[str], dlp_findings: List[str], data_type: str = None, stats: Dict = None) -> Dict:
    """Use Generative AI as PRIMARY classifier with ENHANCED specificity"""
    
    model = get_vertex_model()
    if not model:
        logger.warning(f"⚠️ Vertex AI model not available for column {column_name}")
        return get_enhanced_fallback(column_name, sample_values, data_type, stats, dlp_findings)
    
    # Enhanced context with data patterns
    sample_text = sample_values[:10] if sample_values else ["No sample values available"]
    
    # Build enhanced context about the data
    data_context = f"Data Type: {data_type}" if data_type else ""
    if stats:
        if 'min' in stats and 'max' in stats:
            data_context += f", Numeric Range: {stats['min']} to {stats['max']}"
        if 'distinct_pct' in stats:
            data_context += f", Distinct Values: {stats['distinct_pct']*100:.1f}%"
        if 'null_pct' in stats:
            data_context += f", Null Percentage: {stats['null_pct']*100:.1f}%"
    
    # EXTRACT DLP INFO TYPES FOR BETTER CONTEXT
    dlp_primary_types = []
    for finding in dlp_findings:
        # Extract clean type name (remove counts)
        clean_type = re.sub(r'\s*\(x?\d+\)', '', finding).strip()
        if clean_type and clean_type not in dlp_primary_types:
            dlp_primary_types.append(clean_type)
    
    prompt = f"""
    Analyze this data column and provide SPECIFIC business classification.
    CRITICAL: Be SPECIFIC, not generic. Use the actual DLP findings and data patterns.

    COLUMN ANALYSIS CONTEXT:
    - Column Name: "{column_name}"
    - Sample Values: {sample_text}
    - Data Characteristics: {data_context}
    - DLP Findings: {dlp_primary_types if dlp_primary_types else "No DLP pattern matches"}

    CLASSIFICATION RULES - BE SPECIFIC:
    - If DLP found EMAIL_ADDRESS → classify as "Customer Contact Information"
    - If DLP found PERSON_NAME → classify as "Customer Name" 
    - If DLP found PHONE_NUMBER → classify as "Customer Contact Information"
    - If DLP found LOCATION/ADDRESS → classify as "Geographical Location"
    - If DLP found CREDIT_CARD_NUMBER → classify as "Payment Information"
    - If DLP found DATE_OF_BIRTH → classify as "Customer Date of Birth"
    
    - If column name contains 'account', 'acct' → classify as "Financial Account Number"
    - If column name contains 'risk', 'sar', 'flag' → classify as "Risk Indicator"
    - If column name contains 'balance', 'amount' → classify as "Transaction Amount"
    - If column name contains 'dob', 'birth' → classify as "Customer Date of Birth"
    - If column name contains 'email' → classify as "Customer Contact Information"
    - If column name contains 'phone' → classify as "Customer Contact Information"
    - If column name contains 'name' → classify as "Customer Name"
    - If column name contains 'country', 'city', 'address' → classify as "Geographical Location"

    Return JSON response:
    {{
        "primary_category": "SPECIFIC classification (not generic)",
        "risk_level": "low/medium/high",
        "business_context": "Brief explanation",
        "compliance_considerations": ["GDPR", "PCI-DSS", "HIPAA"] or ["none"],
        "recommended_handling": "standard/mask/encrypt/restrict",
        "confidence_score": 0.85
    }}

    IMPORTANT: Return ONLY valid JSON. Be SPECIFIC based on DLP findings and column names.
    """
    
    try:
        logger.info(f"🔍 Sending GenAI classification for: {column_name}")
        response = model.generate_content(prompt)
        
        if response.candidates and response.candidates[0].content.parts:
            text = response.candidates[0].content.parts[0].text.strip()
            
            # Clean the response
            text = re.sub(r"```json\n?", "", text)
            text = re.sub(r"```\n?", "", text)
            text = text.strip()
            
            try:
                result = json.loads(text)
                
                # VALIDATE: Ensure specific classification
                primary_category = result.get("primary_category", "")
                if is_too_generic(primary_category):
                    # Use DLP-based classification instead
                    specific_category = get_dlp_based_classification(dlp_primary_types, column_name)
                    result["primary_category"] = specific_category
                    result["confidence_score"] = 0.9  # Higher confidence for DLP-based
                    logger.info(f"🔄 Overridden generic classification to: {specific_category}")
                
                logger.info(f"✅ GenAI classification for {column_name}: {result.get('primary_category', 'Unknown')}")
                return result
                
            except json.JSONDecodeError as e:
                logger.warning(f"⚠️ JSON decode error for {column_name}, using enhanced fallback")
                return get_enhanced_fallback(column_name, sample_values, data_type, stats, dlp_findings)
    
    except Exception as e:
        logger.error(f"❌ GenAI classification failed for {column_name}: {str(e)}")
    
    # Enhanced fallback with data context
    return get_enhanced_fallback(column_name, sample_values, data_type, stats, dlp_findings)

def is_too_generic(classification: str) -> bool:
    """Check if classification is too generic"""
    generic_terms = ["Business Identifier", "Generic Data", "Unknown", "Other", "Personal Identifiers"]
    return classification in generic_terms

def get_dlp_based_classification(dlp_types: List[str], column_name: str) -> str:
    """Get specific classification based on DLP findings"""
    column_lower = column_name.lower()
    
    # Map DLP types to specific classifications
    dlp_mapping = {
        "EMAIL_ADDRESS": "Customer Contact Information",
        "PERSON_NAME": "Customer Name", 
        "PHONE_NUMBER": "Customer Contact Information",
        "LOCATION": "Geographical Location",
        "ADDRESS": "Geographical Location",
        "CREDIT_CARD_NUMBER": "Payment Information",
        "DATE_OF_BIRTH": "Customer Date of Birth",
        "DATE": "Timestamp"
    }
    
    # Check DLP types first
    for dlp_type in dlp_types:
        if dlp_type in dlp_mapping:
            return dlp_mapping[dlp_type]
    
    # Fallback to column name patterns
    if any(term in column_lower for term in ['email', 'e-mail']):
        return "Customer Contact Information"
    elif any(term in column_lower for term in ['phone', 'mobile', 'telephone']):
        return "Customer Contact Information"
    elif any(term in column_lower for term in ['name', 'first', 'last', 'fullname']):
        return "Customer Name"
    elif any(term in column_lower for term in ['country', 'state', 'city', 'address', 'location']):
        return "Geographical Location"
    elif any(term in column_lower for term in ['account', 'acct', 'acc']):
        return "Financial Account Number"
    elif any(term in column_lower for term in ['balance', 'amount', 'value']):
        return "Transaction Amount"
    elif any(term in column_lower for term in ['dob', 'birth', 'age']):
        return "Customer Date of Birth"
    elif any(term in column_lower for term in ['credit', 'card', 'payment']):
        return "Payment Information"
    elif any(term in column_lower for term in ['risk', 'sar', 'flag', 'alert']):
        return "Risk Indicator"
    
    return "Business Data"  # Less generic fallback

async def enhance_dlp_findings_with_genai(project_id: str, df, max_info_types: int = 50):
    """
    Enhanced DLP inspection with Generative AI as PRIMARY classifier
    """
    logger.info("🚀 Starting AI-PRIMARY data classification")
    
    summary = {}
    
    # Process each column with Gen AI FIRST
    for col in df.columns:
        try:
            logger.info(f"🔍 PRIMARY AI classification for: {col}")
            summary[col] = {"info_types": [], "samples": [], "categories": []}
            
            # Get sample data and basic stats for Gen AI
            sample_data = df[col].dropna().head(20)
            sample_values = sample_data.astype(str).tolist()
            
            # Get data type and stats for better context
            from .profiler import infer_dtype_and_stats_optimized
            prof_result = infer_dtype_and_stats_optimized(df[col])
            data_type = prof_result["dtype"]
            stats = prof_result["stats"]
            
            # Run DLP for pattern matching (secondary)
            dlp_info_types = []
            try:
                client = get_dlp_client()
                if client:
                    parent = f"projects/{project_id}"
                    inspect_config = {
                        "include_quote": True,
                        "min_likelihood": dlp_v2.Likelihood.POSSIBLE,
                        "limits": {"max_findings_per_item": 10},
                    }
                    
                    headers = [{"name": col}]
                    rows = [{"values": [{"string_value": str(v)}]} for v in sample_data.head(10)]
                    item = {"table": {"headers": headers, "rows": rows}}
                    
                    response = client.inspect_content(
                        request={"parent": parent, "inspect_config": inspect_config, "item": item}
                    )
                    
                    findings = getattr(response.result, "findings", []) or []
                    type_counter = {}
                    for f in findings:
                        it = f.info_type.name if f.info_type else "UNKNOWN"
                        type_counter[it] = type_counter.get(it, 0) + 1
                    
                    dlp_info_types = [f"{t} (x{c})" if c > 1 else t for t, c in sorted(type_counter.items(), key=lambda x: x[1], reverse=True)]
                    
            except Exception as dlp_error:
                logger.warning(f"DLP skipped for {col}: {dlp_error}")
            
            # PRIMARY: Use Gen AI for classification with enhanced context
            ai_classification = await classify_data_with_genai(
                col, sample_values, dlp_info_types, data_type, stats
            )
            
            # Store PRIMARY Gen AI results
            summary[col]["primary_category"] = ai_classification.get("primary_category", "Unknown")
            summary[col]["risk_level"] = ai_classification.get("risk_level", "low")
            summary[col]["business_context"] = ai_classification.get("business_context", "AI Classification")
            summary[col]["compliance_considerations"] = ai_classification.get("compliance_considerations", [])
            summary[col]["recommended_handling"] = ai_classification.get("recommended_handling", "standard")
            summary[col]["confidence_score"] = ai_classification.get("confidence_score", 0.5)
            summary[col]["detected_patterns"] = ai_classification.get("detected_patterns", [])
            summary[col]["info_types"] = dlp_info_types
            
            # Generate categories from AI classification
            categories = set()
            primary_category = ai_classification.get("primary_category", "Unknown")
            if primary_category and primary_category != "Unknown":
                categories.add(primary_category)
            
            # Add risk-based category
            risk_level = ai_classification.get("risk_level", "low")
            categories.add(f"{risk_level.title()} Risk")
            
            summary[col]["categories"] = list(categories) if categories else ["Unknown"]
            
            logger.info(f"✅ PRIMARY AI classification complete for {col}: {primary_category}")

        except Exception as e:
            logger.error(f"❌ AI classification failed for {col}: {str(e)}")
            # Basic fallback
            summary[col]["primary_category"] = "Unknown"
            summary[col]["risk_level"] = "low"
            summary[col]["categories"] = ["Unknown"]
            summary[col]["info_types"] = []
            continue
    
    logger.info("✅ AI-PRIMARY classification completed")
    return summary

# Legacy function for compatibility
async def inspect_table_dlp_optimized(project_id: str, df, region: str = "global", max_info_types: int = 100):
    """Wrapper for backward compatibility"""
    return await enhance_dlp_findings_with_genai(project_id, df, max_info_types)