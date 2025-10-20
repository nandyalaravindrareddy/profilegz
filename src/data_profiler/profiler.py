import pandas as pd, numpy as np, math, re, json
from collections import Counter
from .dlp_client import inspect_table_dlp
from .vertex_client import call_vertex_generate
import os

PROJECT_ID = os.getenv('GCP_PROJECT', 'custom-plating-475002-j7')
VERTEX_LOCATION = os.getenv('VERTEX_LOCATION', 'us-central1')
VERTEX_MODEL = os.getenv('VERTEX_MODEL', 'gemini-2.5-flash')

import os

# Check if Vertex AI should be used
use_vertex = os.getenv("USE_VERTEX", "true").lower() in ("true", "1", "yes")


def shannon_entropy(values):
    cnt = Counter(values)
    total = sum(cnt.values()) or 1
    return -sum((v/total) * math.log2(v/total) for v in cnt.values() if v)

def infer_dtype_and_stats(series: pd.Series):
    n = len(series)
    non_null = series.dropna()
    null_pct = round(1.0 - (len(non_null) / n), 3) if n else 1.0
    stats = {'null_pct': null_pct}
    dtype = 'string'
    num = pd.to_numeric(non_null, errors='coerce')
    if len(non_null) and num.notna().mean() > 0.6:
        dtype = 'int' if (num.dropna() % 1 == 0).all() else 'float'
        stats.update({'min': num.min(), 'max': num.max(), 'mean': float(num.mean()), 'std': float(num.std())})
    else:
        try:
            sample = non_null.astype(str).sample(min(50, len(non_null))) if len(non_null) else pd.Series([])
            dt = pd.to_datetime(sample, errors='coerce')
            if len(sample) and dt.notna().mean() > 0.6:
                dtype = 'date'
                stats.update({'min_date': str(dt.min()), 'max_date': str(dt.max())})
            elif len(non_null) and set(non_null.astype(str).str.lower()) <= {'true','false','0','1','yes','no'}:
                dtype = 'boolean'
        except Exception:
            dtype = 'string'
    stats['entropy'] = round(shannon_entropy(non_null.astype(str).tolist()), 3) if len(non_null) else 0.0
    stats['distinct_pct'] = round(non_null.nunique() / len(non_null), 3) if len(non_null) else 0.0
    return {'dtype': dtype, 'stats': stats}

import pandas as pd
import re
import numpy as np

def local_rules(series: pd.Series):
    rules = []
    if not isinstance(series, pd.Series):
        return [{"rule": f"Unsupported input type: {type(series).__name__}", "confidence": 0.0}]

    non_null = series.dropna()
    total = len(series)
    completeness = 1 - (len(series) - len(non_null)) / len(series)
    rules.append({"rule": "Must not be null", "confidence": round(completeness, 2)})

    # === Uniqueness ===
    unique_ratio = len(non_null.unique()) / len(non_null) if len(non_null) else 0
    rules.append({"rule": "Should be unique", "confidence": round(unique_ratio, 2)})

    # === Duplicate Check ===
    duplicate_ratio = round(1 - unique_ratio, 2)
    if duplicate_ratio > 0:
        rules.append({"rule": f"Contains {round(duplicate_ratio*100,1)}% duplicate values", "confidence": 0.9})

    # === Numeric Rules ===
    if pd.api.types.is_numeric_dtype(non_null):
        if (non_null < 0).any():
            rules.append({"rule": "Contains negative values", "confidence": 0.8})
        rules.append({"rule": "Numeric range validation", "confidence": 0.9})
        rules.append({"rule": f"Values range between {non_null.min()} and {non_null.max()}", "confidence": 0.8})
        if non_null.mean() == 0:
            rules.append({"rule": "Possible constant column (mean = 0)", "confidence": 0.7})

    # === String Rules ===
    elif pd.api.types.is_string_dtype(non_null):
        avg_len = non_null.astype(str).map(len).mean()
        rules.append({"rule": f"Average length ≈ {int(avg_len)} chars", "confidence": 0.8})

        # Check for casing consistency
        if non_null.str.isupper().mean() > 0.8:
            rules.append({"rule": "Mostly uppercase text", "confidence": 0.9})
        elif non_null.str.islower().mean() > 0.8:
            rules.append({"rule": "Mostly lowercase text", "confidence": 0.9})

        # Detect special characters
        if non_null.str.contains(r"[^a-zA-Z0-9\s]", regex=True).mean() > 0.2:
            rules.append({"rule": "Contains special characters", "confidence": 0.8})

        # Common business patterns
        if non_null.str.contains(r"@").mean() > 0.5:
            rules.append({"rule": "Contains email-like patterns", "confidence": 0.95})
        if non_null.str.match(r"[A-Z]{5}[0-9]{4}[A-Z]").mean() > 0.5:
            rules.append({"rule": "Matches PAN format", "confidence": 0.9})
        if non_null.str.match(r"[A-Z]{4}0[A-Z0-9]{6}").mean() > 0.5:
            rules.append({"rule": "Matches IFSC code format", "confidence": 0.9})
        if non_null.str.match(r"\b\d{4}\s\d{4}\s\d{4}\b").mean() > 0.5:
            rules.append({"rule": "Matches Aadhaar format", "confidence": 0.9})

    # === Date Rules ===
    elif pd.api.types.is_datetime64_any_dtype(non_null):
        rules.append({"rule": "Date format consistency check", "confidence": 0.9})
        if (non_null < "2000-01-01").any():
            rules.append({"rule": "Contains pre-2000 dates", "confidence": 0.8})
        if (non_null > pd.Timestamp.now()).any():
            rules.append({"rule": "Contains future dates", "confidence": 0.9})

    # === Categorical Rules ===
    unique_count = len(non_null.unique())
    if 1 < unique_count <= 20:
        rules.append({"rule": f"Categorical field with {unique_count} unique values", "confidence": 0.85})
        top_value = non_null.value_counts().idxmax()
        top_ratio = non_null.value_counts().iloc[0] / len(non_null)
        rules.append({"rule": f"Top category '{top_value}' covers {round(top_ratio*100,1)}%", "confidence": 0.85})

    # === Constant Column ===
    if unique_count == 1:
        rules.append({"rule": "Constant value column", "confidence": 1.0})

    # === Text Entropy ===
    try:
        entropy = -np.sum((non_null.value_counts(normalize=True)) * np.log2(non_null.value_counts(normalize=True)))
        if entropy < 1:
            rules.append({"rule": "Low entropy — possible categorical or ID field", "confidence": 0.8})
    except Exception:
        pass

    return rules


def run_profiling(df, project_id=None, sample_rows=200):
    results = {}
    df_sample = df.head(sample_rows)
    print(f"------in run_profiling-----")
    print(PROJECT_ID)
    print('**********')
    dlp_summary = inspect_table_dlp(df_sample, project_id=project_id)
 

    for col in df.columns:
        prof = infer_dtype_and_stats(df[col])
        rules = local_rules(df[col])
        results[col] = {
            'inferred_dtype': prof['dtype'],
            'stats': prof['stats'],
            'rules': rules,
            'dlp_info_types': dlp_summary.get(col, {}).get('info_types', []),
            'dlp_samples': dlp_summary.get(col, {}).get('samples', []),
            'llm_output': None,
            'classification': None,
            'overall_confidence': 0.0
        }

    # Vertex enrichment for columns DLP did not tag
    if use_vertex and PROJECT_ID:
        for col in df.columns:
            if results[col]['dlp_info_types']:
                # prioritize DLP
                results[col]['classification'] = results[col]['dlp_info_types'][0]
                results[col]['overall_confidence'] = 0.95
                continue
            sample_vals = df[col].dropna().astype(str).head(10).tolist()
            if not sample_vals:
                continue
            prompt = (
                "You are a senior data governance expert. Given these sample values for a column, "
                "return strict JSON with fields: classification (one of EMAIL_ADDRESS, PHONE_NUMBER, IN_PAN, US_SOCIAL_SECURITY_NUMBER, NAME, ID, DATE, NUMERIC, BOOLEAN, TEXT, SAR, RESTRICTED), "
                "and profiling_rules (list of {rule, confidence}).\\nSamples:\\n" + str(sample_vals)
            )
            try:
                out = call_vertex_generate(prompt, project=PROJECT_ID, location=VERTEX_LOCATION, model=VERTEX_MODEL)
                cleaned = out.strip()
                # attempt to extract JSON
                import re, json
                m = re.search(r'\\{.*\\}', cleaned, flags=re.S)
                parsed = json.loads(m.group(0)) if m else {'raw': cleaned}
                results[col]['llm_output'] = parsed
                if isinstance(parsed, dict) and parsed.get('classification'):
                    results[col]['classification'] = parsed.get('classification')
                    prs = parsed.get('profiling_rules', [])
                    if prs:
                        results[col]['overall_confidence'] = round(sum([r.get('confidence', 0.5) for r in prs]) / len(prs), 3)
            except Exception as e:
                results[col]['llm_error'] = str(e)

    # fallback: set classification and compute confidence
    for col in df.columns:
        entry = results[col]
        if not entry['classification']:
            entry['classification'] = entry['inferred_dtype']
        if not entry['overall_confidence']:
            local_conf = np.mean([r.get('confidence', 0.5) for r in entry['rules']]) if entry['rules'] else 0.5
            entry['overall_confidence'] = float(round(local_conf, 3))
    return results
