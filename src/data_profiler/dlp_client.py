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
            PROJECT_ID = os.getenv("GCP_PROJECT")
            if not PROJECT_ID:
                logger.error("❌ GCP_PROJECT environment variable not set")
                return None
                
            LOCATION = os.getenv("LOCATION", "us-central1")
            vertexai.init(project=PROJECT_ID, location=LOCATION)
            _vertex_model = GenerativeModel("gemini-2.0-flash-001")  # Use stable model name
            logger.info("✅ Vertex AI model initialized successfully")
        except Exception as e:
            logger.error(f"❌ Vertex AI initialization failed: {e}")
    return _vertex_model

def get_specific_classification(column_name: str, dlp_findings: List[str]) -> str:
    """Get specific classification based on column name patterns"""
    column_lower = column_name.lower()
    
    # More specific mappings based on your report
    if any(term in column_lower for term in ['phone', 'mobile', 'telephone']):
        return "Customer Contact Information"
    elif any(term in column_lower for term in ['name', 'firstname', 'lastname', 'fullname']):
        return "Customer Name"
    elif any(term in column_lower for term in ['email', 'e-mail']):
        return "Customer Contact Information"
    elif any(term in column_lower for term in ['customer_id', 'user_id', 'client_id']):
        return "Customer Identifier"
    elif any(term in column_lower for term in ['national_id', 'ssn', 'social', 'passport', 'id_number']):
        return "National Identification Number"
    elif any(term in column_lower for term in ['country', 'state', 'city', 'region']):
        return "Geographical Location"
    elif any(term in column_lower for term in ['address', 'street', 'zip', 'postal']):
        return "Customer Address"
    elif any(term in column_lower for term in ['dob', 'birth', 'date_of_birth']):
        return "Customer Date of Birth"
    else:
        return "Unknown"

def get_fallback_classification(column_name: str, dlp_findings: List[str]) -> Dict:
    """Enhanced fallback classification"""
    specific_classification = get_specific_classification(column_name, dlp_findings)
    
    return {
        "primary_category": specific_classification,
        "risk_level": "high" if "national_id" in column_name.lower() else "medium",
        "business_context": f"Classified as {specific_classification} based on column name patterns",
        "compliance_considerations": ["GDPR"] if specific_classification != "Unknown" else [],
        "recommended_handling": "encrypt" if specific_classification in ["National Identification Number", "Customer Contact Information"] else "mask",
        "confidence_score": 0.7
    }

async def classify_data_with_genai(column_name: str, sample_values: List[str], dlp_findings: List[str]) -> Dict:
    """Use Generative AI to classify and categorize data with DLP findings as context - ENHANCED"""
    
    model = get_vertex_model()
    if not model:
        logger.warning(f"⚠️ Vertex AI model not available for column {column_name}")
        return get_fallback_classification(column_name, dlp_findings)
    
    # Enhanced prompt for better classification
    sample_text = sample_values[:5] if sample_values else ["No sample values available"]
    
    prompt = f"""
    Analyze this data column and provide SPECIFIC business classification based on the column name, sample values, and DLP findings.
    
    COLUMN ANALYSIS REQUEST:
    - Column Name: "{column_name}"
    - Sample Values: {sample_text}
    - DLP Findings: {dlp_findings if dlp_findings else "No DLP findings"}
    
    Provide a SPECIFIC business classification like:
    - "Customer Contact Information", "Customer Name", "National Identification Number", 
    - "Geographical Location", "Customer Address", "Customer Date of Birth",
    - "Customer Identifier", "Financial Account Number", "Transaction Amount"
    
    Avoid generic classifications like "Personal Identifiers" - be specific about what the data represents.
    
    Please provide a JSON response with the following structure:
    {{
        "primary_category": "Specific business classification (e.g., Customer Contact Information)",
        "risk_level": "low/medium/high",
        "business_context": "Brief description of what this data represents in business context",
        "compliance_considerations": ["List", "of", "relevant", "regulations"],
        "recommended_handling": "standard/mask/encrypt/restrict",
        "confidence_score": 0.85
    }}
    
    IMPORTANT: Return ONLY valid JSON, no other text.
    Be specific and accurate in your classification.
    """
    
    try:
        logger.info(f"🔍 Sending GenAI classification request for column: {column_name}")
        response = model.generate_content(prompt)
        
        if response.candidates and response.candidates[0].content.parts:
            text = response.candidates[0].content.parts[0].text.strip()
            logger.info(f"📨 Raw GenAI response for {column_name}: {text[:200]}...")
            
            # Clean the response - remove markdown code blocks
            text = re.sub(r"```json\n?", "", text)
            text = re.sub(r"```\n?", "", text)
            text = text.strip()
            
            try:
                result = json.loads(text)
                # Validate and ensure primary_category is specific
                if result.get("primary_category") in ["Unknown", "Other", "Personal Identifiers"]:
                    # Fallback to more specific classification
                    result["primary_category"] = get_specific_classification(column_name, dlp_findings)
                
                logger.info(f"✅ Successfully parsed GenAI response for {column_name}: {result.get('primary_category', 'Unknown')}")
                return result
            except json.JSONDecodeError as e:
                logger.warning(f"⚠️ JSON decode error for {column_name}: {e}")
                logger.warning(f"📄 Response text was: {text}")
                
                # Try to extract JSON from malformed response
                json_match = re.search(r'\{[^{}]*\{[^{}]*\}[^{}]*\}|\{[^{}]*\}', text, re.DOTALL)
                if json_match:
                    try:
                        result = json.loads(json_match.group())
                        logger.info(f"✅ Recovered JSON from malformed response for {column_name}")
                        return result
                    except:
                        pass
        
        logger.warning(f"⚠️ No valid response from GenAI for {column_name}")
        
    except Exception as e:
        logger.error(f"❌ GenAI classification failed for {column_name}: {str(e)}")
        if hasattr(e, 'details'):
            logger.error(f"🔧 Error details: {e.details}")
    
    # Enhanced fallback classification
    return get_fallback_classification(column_name, dlp_findings)

async def enhance_dlp_findings_with_genai(project_id: str, df, max_info_types: int = 50):
    """
    Enhanced DLP inspection with Generative AI classification
    """
    logger.info("🚀 Starting AI-enhanced DLP inspection")
    client = get_dlp_client()
    
    if not client:
        logger.error("❌ DLP client not available")
        return {col: {"info_types": [], "samples": [], "categories": ["Unknown"]} for col in df.columns}
    
    parent = f"projects/{project_id}"
    
    # Use DLP's comprehensive built-in info types
    inspect_config = {
        "include_quote": True,
        "min_likelihood": dlp_v2.Likelihood.POSSIBLE,
        "limits": {"max_findings_per_item": 20},  # Reduced for efficiency
    }
    
    summary = {}
    
    # Process columns with DLP
    for col in df.columns:
        try:
            logger.info(f"🔍 Processing column: {col}")
            summary[col] = {"info_types": [], "samples": [], "categories": []}
            
            # Create table for DLP inspection
            sample_data = df[col].dropna().astype(str).head(30)  # Reduced sample size
            if len(sample_data) == 0:
                logger.info(f"⚠️ No data in column {col}, skipping DLP inspection")
                continue
                
            headers = [{"name": col}]
            rows = [{"values": [{"string_value": str(v)}]} for v in sample_data]
            item = {"table": {"headers": headers, "rows": rows}}
            
            # Run DLP inspection
            logger.info(f"📊 Running DLP inspection for {col}")
            response = client.inspect_content(
                request={"parent": parent, "inspect_config": inspect_config, "item": item}
            )
            
            findings = getattr(response.result, "findings", []) or []
            logger.info(f"📈 DLP found {len(findings)} findings for {col}")
            
            type_counter = {}
            sample_quotes = []

            for f in findings:
                it = f.info_type.name if f.info_type else "UNKNOWN"
                type_counter[it] = type_counter.get(it, 0) + 1
                
                if hasattr(f, "quote") and f.quote and len(sample_quotes) < 2:
                    sample_quotes.append(f.quote)

            # Store DLP findings
            dlp_info_types = [f"{t} (x{c})" if c > 1 else t for t, c in sorted(type_counter.items(), key=lambda x: x[1], reverse=True)]
            summary[col]["info_types"] = dlp_info_types
            summary[col]["samples"] = sample_quotes
            
            # Enhance with Generative AI classification
            logger.info(f"🧠 Enhancing {col} with GenAI classification")
            sample_values = df[col].dropna().astype(str).head(10).tolist()
            ai_classification = await classify_data_with_genai(col, sample_values, dlp_info_types)
            
            # Store AI classification results
            summary[col]["primary_category"] = ai_classification.get("primary_category", "Unknown")
            summary[col]["risk_level"] = ai_classification.get("risk_level", "low")
            summary[col]["business_context"] = ai_classification.get("business_context", "Unknown")
            summary[col]["compliance_considerations"] = ai_classification.get("compliance_considerations", [])
            summary[col]["recommended_handling"] = ai_classification.get("recommended_handling", "standard")
            summary[col]["confidence_score"] = ai_classification.get("confidence_score", 0.5)
            
            # Generate categories based on AI classification
            categories = set()
            primary_category = ai_classification.get("primary_category", "Unknown")
            if primary_category and primary_category != "Unknown":
                categories.add(primary_category)
            
            # Add risk-based category
            risk_level = ai_classification.get("risk_level", "low")
            categories.add(f"{risk_level.title()} Risk")
            
            # Add compliance-related categories
            compliance_considerations = ai_classification.get("compliance_considerations", [])
            for compliance in compliance_considerations:
                categories.add(f"Regulated: {compliance}")
            
            # Add DLP-based categories if no AI categories
            if not categories and dlp_info_types:
                categories.add("DLP Detected")
                for info_type in dlp_info_types[:2]:  # Add top 2 DLP types as categories
                    clean_type = info_type.split(' (x')[0]
                    categories.add(clean_type)
            
            summary[col]["categories"] = list(categories) if categories else ["Unknown"]
            
            logger.info(f"✅ Completed AI-enhanced classification for {col}: {summary[col]['primary_category']}")

        except Exception as e:
            logger.error(f"❌ DLP inspection failed for {col}: {str(e)}")
            # Fallback: try basic classification even if DLP fails
            try:
                sample_values = df[col].dropna().astype(str).head(5).tolist()
                ai_classification = await classify_data_with_genai(col, sample_values, [])
                summary[col]["primary_category"] = ai_classification.get("primary_category", "Unknown")
                summary[col]["risk_level"] = ai_classification.get("risk_level", "low")
                summary[col]["categories"] = [ai_classification.get("primary_category", "Unknown")]
                summary[col]["business_context"] = ai_classification.get("business_context", "Fallback Classification")
            except Exception as fallback_error:
                logger.error(f"❌ Fallback classification also failed for {col}: {fallback_error}")
                summary[col]["primary_category"] = "Unknown"
                summary[col]["risk_level"] = "low"
                summary[col]["categories"] = ["Unknown"]
                summary[col]["business_context"] = "Classification Failed"
            continue
    
    logger.info("✅ AI-enhanced DLP inspection completed")
    return summary

# Legacy function for compatibility
async def inspect_table_dlp_optimized(project_id: str, df, region: str = "global", max_info_types: int = 100):
    """Wrapper for backward compatibility"""
    return await enhance_dlp_findings_with_genai(project_id, df, max_info_types)