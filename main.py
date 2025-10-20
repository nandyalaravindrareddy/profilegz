import os, io, time
from fastapi import FastAPI, Form
from fastapi.responses import JSONResponse
import pandas as pd
from src.data_profiler.profiler import run_profiling
from google.cloud import storage
import numpy as np
import datetime, numbers
import google.auth

app = FastAPI(title='Data Profiler API')

# --- GCP Configuration ---
PROJECT_ID = os.getenv("GCP_PROJECT", None)

if not PROJECT_ID:
    try:
        creds, project = google.auth.default()
        PROJECT_ID = project
        print(f"🧠 Using GCP project from credentials: {PROJECT_ID}")
        print(f"🔑 Authenticated as: {getattr(creds, 'service_account_email', 'unknown')}")
    except Exception as e:
        PROJECT_ID = "unknown"
        print(f"⚠️ Could not detect project automatically. Falling back to 'unknown'. Error: {e}")
# --- Backend normalization helper (drop into main.py) ---
def normalize_result_table(raw_results, dlp_summary=None):
    """
    Ensure `result_table` is a dict of:
      col -> {
        'inferred_dtype': str,
        'stats': dict,
        'rules': list,
        'classification': str or None,
        'dlp_info_types': list,
        'dlp_samples': list,
        'llm_output': dict or None,
        'overall_confidence': float  # 0.0-1.0
      }
    raw_results: whatever you built earlier (dict)
    dlp_summary: optional dict mapping col -> {'info_types':[], 'samples':[]}
    """
    normalized = {}
    for col, entry in (raw_results.items() if isinstance(raw_results, dict) else []):
        if not isinstance(entry, dict):
            entry = {"inferred_dtype": str(entry), "stats": {}, "rules": []}

        inferred = entry.get("inferred_dtype", entry.get("dtype", "unknown"))
        stats = entry.get("stats", {}) or {}
        rules = entry.get("rules", entry.get("profiling_rules", [])) or []
        llm_output = entry.get("llm_output", entry.get("llm", None))
        classification = entry.get("classification") or None

        # Attach dlp findings if provided
        dlp_info_types = []
        dlp_samples = []
        if dlp_summary and isinstance(dlp_summary, dict):
            col_dlp = dlp_summary.get(col)
            if isinstance(col_dlp, dict):
                dlp_info_types = col_dlp.get("info_types", []) or []
                dlp_samples = col_dlp.get("samples", []) or []

        # If DLP found something, prefer the top DLP info_type as classification
        if dlp_info_types:
            # sometimes DLP returns duplicates; take most common
            from collections import Counter
            top = Counter(dlp_info_types).most_common(1)
            if top:
                classification = top[0][0]

        # compute a simple overall confidence:
        # average of rule confidences (if present) and a boost if DLP exists
        confs = []
        for r in rules:
            try:
                confs.append(float(r.get("confidence", 0.5)))
            except Exception:
                try:
                    confs.append(float(r) if isinstance(r, (int, float)) else 0.5)
                except Exception:
                    confs.append(0.5)
        # fallback default
        if not confs:
            confs = [0.6]

        overall = sum(confs) / len(confs)
        if dlp_info_types:
            # boost slightly if DLP found matches
            overall = min(1.0, overall + 0.15)

        normalized[col] = {
            "inferred_dtype": inferred,
            "stats": stats,
            "rules": rules,
            "classification": classification,
            "dlp_info_types": dlp_info_types,
            "dlp_samples": dlp_samples,
            "llm_output": llm_output,
            "overall_confidence": round(float(overall), 3),
        }
    return normalized


def make_json_safe(obj):
    """Recursively convert numpy, pandas, and datetime objects to JSON-safe Python types."""
    if isinstance(obj, dict):
        return {str(k): make_json_safe(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [make_json_safe(v) for v in obj]
    elif isinstance(obj, (np.integer,)):
        return int(obj)
    elif isinstance(obj, (np.floating, numbers.Real)):
        return float(obj)
    elif isinstance(obj, (np.bool_, bool)):
        return bool(obj)
    elif isinstance(obj, (pd.Timestamp, datetime.date, datetime.datetime)):
        return obj.isoformat()
    elif pd.isna(obj):
        return None
    else:
        return obj


def read_gcs_csv(gcs_path: str, sample_rows: int = 200):
    if not gcs_path.startswith('gs://'):
        # allow local paths in dev
        return pd.read_csv(gcs_path).head(sample_rows)
    client = storage.Client()
    bucket, blob = gcs_path.replace('gs://','').split('/',1)
    data = client.bucket(bucket).blob(blob).download_as_bytes()
    return pd.read_csv(io.BytesIO(data)).head(sample_rows)

@app.post('/profile')
async def profile(gcs_path: str = Form(...), sample_rows: int = Form(5)):
    start_time = time.time()
    try:
        print("------in run_profiling-----")
        print(gcs_path)
        print(PROJECT_ID)

        df = read_gcs_csv(gcs_path, sample_rows)
        print(f"✅ Loaded {len(df)} rows and {len(df.columns)} columns from GCS")

        # --- Run profiling ---
        results = run_profiling(df, project_id=PROJECT_ID)
        # after you compute `results` and `dlp_summary` in your /profile handler:
        final_results = normalize_result_table(results, dlp_summary)
        response = {
            "project": PROJECT_ID,
            "rows_profiled": len(df),
            "columns_profiled": len(df.columns),
            "execution_time_sec": total_time,
            "result_table": final_results,
        }


        # --- Calculate total time ---
        total_time = time.time() - start_time
        print(f"⚡ Total profiling time: {total_time:.2f} sec")


        return JSONResponse(content=make_json_safe(response))


    except Exception as e:
        import traceback
        error_msg = str(e)
        print(f"❌ Error serializing response: {error_msg}")
        print(traceback.format_exc())

        total_time = time.time() - start_time
        return JSONResponse(
            content={
                "error": f"Response serialization failed: {error_msg}",
                "execution_time_sec": round(float(total_time), 2),
            },
            status_code=500,
        )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8080, reload=False)
