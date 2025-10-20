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

        # --- Calculate total time ---
        total_time = time.time() - start_time
        print(f"⚡ Total profiling time: {total_time:.2f} sec")

        # --- Construct final response safely ---
        response_dict = {
            "project": PROJECT_ID,
            "rows_profiled": int(len(df)),
            "columns_profiled": int(len(df.columns)),
            "execution_time_sec": round(float(total_time), 2),
            "result_table": results,
        }

        safe_response = make_json_safe(response_dict)
        return JSONResponse(content=safe_response)

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
