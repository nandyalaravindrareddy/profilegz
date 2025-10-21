import os, io, time, traceback
import pandas as pd
import pyarrow.parquet as pq
import fastavro
from google.cloud import storage
from fastapi import FastAPI, Form, BackgroundTasks
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import asyncio
from src.data_profiler.profiler import run_profiling_optimized
from src.data_profiler.dlp_client import inspect_table_dlp_optimized
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PROJECT_ID = os.getenv("GCP_PROJECT", "custom-plating-475002-j7")

app = FastAPI(title="AI + DLP Data Profiler API")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Cache for file metadata to avoid repeated GCS calls
_file_cache = {}
_CACHE_TIMEOUT = 300  # 5 minutes

async def read_gcs_file_optimized(gcs_path: str, sample_rows: int = 200):
    """Optimized file reader with streaming and better memory management"""
    cache_key = f"{gcs_path}_{sample_rows}"
    if cache_key in _file_cache:
        if time.time() - _file_cache[cache_key]['timestamp'] < _CACHE_TIMEOUT:
            return _file_cache[cache_key]['data']
    
    if not gcs_path.startswith("gs://"):
        return _read_local_file(gcs_path, sample_rows)
    
    # GCS file processing with streaming
    client = storage.Client()
    bucket_name, blob_path = gcs_path.replace("gs://", "").split("/", 1)
    blob = client.bucket(bucket_name).blob(blob_path)
    ext = os.path.splitext(blob_path)[1].lower()
    
    # Get file size to optimize reading strategy
    blob.reload()
    file_size = blob.size
    
    if ext == ".csv":
        # For large CSV files, use chunked reading
        if file_size > 10 * 1024 * 1024:  # 10MB
            chunks = []
            stream = blob.open("rb")
            for i, chunk in enumerate(pd.read_csv(stream, chunksize=1000)):
                chunks.append(chunk)
                if len(pd.concat(chunks, ignore_index=True)) >= sample_rows:
                    break
            df = pd.concat(chunks, ignore_index=True).head(sample_rows)
        else:
            data = await asyncio.get_event_loop().run_in_executor(
                None, blob.download_as_bytes
            )
            df = pd.read_csv(io.BytesIO(data)).head(sample_rows)
    
    elif ext == ".parquet":
        # Parquet is columnar - only read needed columns efficiently
        data = await asyncio.get_event_loop().run_in_executor(
            None, blob.download_as_bytes
        )
        # Use pyarrow to read only metadata first
        pf = pq.ParquetFile(io.BytesIO(data))
        df = pf.read().to_pandas().head(sample_rows)
    
    elif ext == ".avro":
        data = await asyncio.get_event_loop().run_in_executor(
            None, blob.download_as_bytes
        )
        bytes_io = io.BytesIO(data)
        reader = fastavro.reader(bytes_io)
        records = [r for _, r in zip(range(sample_rows), reader)]
        df = pd.DataFrame(records)
    
    else:
        raise ValueError(f"Unsupported file type: {ext}")
    
    # Cache the result
    _file_cache[cache_key] = {
        'data': df,
        'timestamp': time.time()
    }
    
    return df

def _read_local_file(file_path: str, sample_rows: int):
    """Optimized local file reading"""
    ext = os.path.splitext(file_path)[1].lower()
    
    if ext == ".csv":
        # Use dtype inference and low_memory for large files
        df = pd.read_csv(file_path, nrows=sample_rows, low_memory=False)
    elif ext == ".parquet":
        df = pd.read_parquet(file_path).head(sample_rows)
    elif ext == ".avro":
        with open(file_path, "rb") as f:
            reader = fastavro.reader(f)
            records = [r for _, r in zip(range(sample_rows), reader)]
            df = pd.DataFrame(records)
    elif ext == ".orc":
        df = pd.read_orc(file_path).head(sample_rows)
    else:
        raise ValueError(f"Unsupported file format: {ext}")
    
    return df

def make_json_safe(obj):
    """Optimized JSON serialization"""
    import numpy as np
    import datetime
    import numbers
    
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
async def profile(
    gcs_path: str = Form(...), 
    sample_rows: int = Form(200), 
    parallel: str = Form("true")
):
    start_time = time.time()
    try:
        logger.info(f"Loading {gcs_path} (sample_rows={sample_rows})")
        
        # Use async file reading
        df = await read_gcs_file_optimized(gcs_path, int(sample_rows))
        logger.info(f"Loaded {len(df)} rows x {len(df.columns)} cols")

        # Run optimized profiling
        results = await run_profiling_optimized(
            df, 
            project_id=PROJECT_ID, 
            parallel=(str(parallel).lower() in ("true","1","yes"))
        )

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
        logger.error(f"ERROR in /profile: {e}")
        traceback.print_exc()
        return JSONResponse(status_code=500, content={
            "error": str(e),
            "execution_time_sec": round(time.time() - start_time, 2)
        })

@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": time.time()}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app", 
        host="0.0.0.0", 
        port=8080, 
        workers=1,  # For memory efficiency
        loop="asyncio"
    )