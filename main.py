import os, io, time, traceback
from fastapi import FastAPI, Form
from fastapi.responses import JSONResponse
import pandas as pd
from google.cloud import storage
from src.data_profiler.profiler import run_profiling  # ensure package path matches your project

app = FastAPI(title="Data Profiler API")

PROJECT_ID = os.getenv("GCP_PROJECT", "custom-plating-475002-j7")


def read_gcs_csv(gcs_path: str, sample_rows: int = 200):
    # allow local CSV path for dev convenience
    if not gcs_path.startswith("gs://"):
        return pd.read_csv(gcs_path).head(sample_rows)
    client = storage.Client()
    bucket, blob = gcs_path.replace("gs://", "").split("/", 1)
    data = client.bucket(bucket).blob(blob).download_as_bytes()
    return pd.read_csv(io.BytesIO(data)).head(sample_rows)


def make_json_safe(obj):
    import numpy as np, datetime, numbers, pandas as pd
    if isinstance(obj, dict):
        return {str(k): make_json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [make_json_safe(v) for v in obj]
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating, numbers.Real)):
        return float(obj)
    if isinstance(obj, (np.bool_, bool)):
        return bool(obj)
    if isinstance(obj, (pd.Timestamp, datetime.date, datetime.datetime)):
        return obj.isoformat()
    if pd.isna(obj):
        return None
    return obj


@app.post("/profile")
async def profile(gcs_path: str = Form(...), sample_rows: int = Form(200), parallel: str = Form("true")):
    start_time = time.time()
    try:
        print(f"Loading {gcs_path} (sample_rows={sample_rows})")
        df = read_gcs_csv(gcs_path, int(sample_rows))
        print(f"Loaded {len(df)} rows x {len(df.columns)} cols")

        results = run_profiling(df, project_id=PROJECT_ID, parallel=(str(parallel).lower() in ("true","1","yes")))

        total_time = round(time.time() - start_time, 2)
        resp = {
            "project": PROJECT_ID,
            "rows_profiled": int(len(df)),
            "columns_profiled": int(len(df.columns)),
            "execution_time_sec": total_time,
            "result_table": results,
        }
        return JSONResponse(content=make_json_safe(resp))

    except Exception as e:
        print("ERROR in /profile:", e)
        traceback.print_exc()
        return JSONResponse(status_code=500, content={
            "error": str(e),
            "execution_time_sec": round(time.time() - start_time, 2)
        })


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8080, reload=True)
