# dlp_child_run_beam_dataflow.py
from __future__ import annotations
from datetime import datetime
import json
import logging
import os
import uuid as _uuid
from typing import Dict, Any

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.models.xcom_arg import XComArg

# ----------------------------
# Helpers
# ----------------------------
def _get_conf(context):
    dag_run = context.get("dag_run")
    if not dag_run:
        raise AirflowFailException("No dag_run context available")
    conf = dag_run.conf
    if not conf:
        raise AirflowFailException("dag_run.conf is required and cannot be empty")
    return conf


# ----------------------------
# validate_conf_fn  (STRICT - NO FALLBACKS)
# ----------------------------
def validate_conf_fn(**context):
    conf = _get_conf(context)

    required = [
        "project",
        "region",
        "temp_location",
        "staging_location",
        "dataflow_py_gcs",
        "tenant_config",
    ]
    missing = [r for r in required if not conf.get(r)]
    if missing:
        raise AirflowFailException(f"Missing required params in dag_run.conf: {missing}")

    if not conf["dataflow_py_gcs"].startswith("gs://"):
        raise AirflowFailException(
            "dataflow_py_gcs must be a valid GCS path pointing to beam_dlp_redact.py"
        )

    tenant_cfg = conf["tenant_config"]
    if not isinstance(tenant_cfg, dict):
        raise AirflowFailException("tenant_config must be a JSON object")

    try:
        tenant_config_json = json.dumps(tenant_cfg, separators=(",", ":"))
    except Exception as e:
        raise AirflowFailException(f"Failed to serialize tenant_config: {e}")

    logging.info("Validation succeeded for project=%s region=%s",
                 conf["project"], conf["region"])

    # Return both original conf (for py_file path) and serialized tenant_config_json
    return {"conf": conf, "tenant_config_json": tenant_config_json}


# ----------------------------
# prepare_pipeline_options_fn
# ----------------------------
def _sanitize_job_name(name: str):
    name = name.lower()
    name = "".join(c if c.isalnum() or c == "-" else "-" for c in name)
    if not name[0].isalpha():
        name = "a" + name
    if not name[-1].isalnum():
        name = name + "0"
    return name[:40]

# replace prepare_pipeline_options_fn body with this snippet
import base64

def prepare_pipeline_options_fn(**context):
    """
    Build pipeline_options and upload tenant config into staging_location/configs/<job_name>.json.
    The Beam job will read staging_location and job_name to download the config.
    """
    from google.cloud import storage

    ti = context["ti"]
    result = ti.xcom_pull(task_ids="validate_conf")
    if not result or "conf" not in result or "tenant_config_json" not in result:
        raise AirflowFailException("validate_conf did not return expected data (tenant_config_json missing)")

    conf = result["conf"]
    tenant_config_json = result["tenant_config_json"]

    # create job name (sanitize similar to earlier)
    raw_job_name = f"dlp-redact-{_uuid.uuid4().hex[:8]}"
    job_name = _sanitize_job_name(raw_job_name)

    staging = conf["staging_location"]
    if not staging or not staging.startswith("gs://"):
        raise AirflowFailException("staging_location must be a valid GCS path (gs://...)")

    # Build GCS object path under staging: staging/configs/<job_name>.json
    # If staging is gs://bucket/prefix, we will write to gs://bucket/<prefix>/configs/<job_name>.json
    staging_no_prefix = staging[len("gs://"):]
    bucket_name, _, prefix = staging_no_prefix.partition("/")
    prefix = prefix.rstrip("/")
    config_blob_path = f"{prefix}/configs/{job_name}.json" if prefix else f"configs/{job_name}.json"
    gcs_uri = f"gs://{bucket_name}/{config_blob_path}"

    # Upload config
    storage_client = storage.Client(project=conf["project"])
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(config_blob_path)
    try:
        blob.upload_from_string(tenant_config_json, content_type="application/json")
    except Exception as e:
        raise AirflowFailException(f"Failed to upload tenant config to GCS {gcs_uri}: {e}")

    logging.info("Uploaded tenant_config to GCS path: %s", gcs_uri)

    # Build pipeline options dict — include job_name and staging_location (which worker will use)
    pipeline_options: Dict[str, Any] = {
        "project": conf["project"],
        "region": conf["region"],
        "runner": "DataflowRunner",
        "job_name": job_name,
        "staging_location": conf["staging_location"],
        "temp_location": conf["temp_location"],
        # do NOT include raw JSON or long flags; worker will compute GCS path from staging + job_name
    }

    # convenience copy of tables (optional)
    pipeline_options["src_table"] = conf["tenant_config"].get("src_table")
    pipeline_options["out_table"] = conf["tenant_config"].get("out_table")

    logging.info("Prepared pipeline_options keys: %s", list(pipeline_options.keys()))
    return pipeline_options


# ----------------------------
# DAG definition
# ----------------------------
with DAG(
    dag_id="dlp_child_run_beam_dataflow",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dlp", "dataflow"],
) as dag:

    validate_conf = PythonOperator(
        task_id="validate_conf",
        python_callable=validate_conf_fn,
        provide_context=True,
    )

    prepare_pipeline_options = PythonOperator(
        task_id="prepare_pipeline_options",
        python_callable=prepare_pipeline_options_fn,
        provide_context=True,
    )

    # ----
    # Beam operator as a real DAG task.
    # - pipeline_options is passed as XComArg (the dict returned by prepare_pipeline_options)
    # - py_file is obtained from validate_conf XCom (dag_run.conf -> conf['dataflow_py_gcs'])
    # ----
    py_file_template = "{{ ti.xcom_pull(task_ids='validate_conf')['conf']['dataflow_py_gcs'] }}"

    beam_task = BeamRunPythonPipelineOperator(
        task_id="run_beam_dataflow",
        py_file=py_file_template,  # templated; will resolve to the GCS path
        pipeline_options=XComArg(prepare_pipeline_options),  # passes the dict returned by prepare_pipeline_options
        runner="DataflowRunner",
        # do_xcom_push True will push the job_id / result into XCom (optional)
        do_xcom_push=True,
    )

    # dependencies
    validate_conf >> prepare_pipeline_options >> beam_task