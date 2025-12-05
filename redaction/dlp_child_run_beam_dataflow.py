# dlp_child_run_beam_dataflow.py
from __future__ import annotations
from datetime import datetime
import json
import logging
import os
import re
import uuid as _uuid
from typing import Dict, Any

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}

DAG_ID = "dlp_child_run_beam_dataflow"
PROJECT_DEFAULT = os.environ.get("GCP_PROJECT")  # fallback


def _sanitize_job_name(name: str) -> str:
    # Dataflow name rules: lowercase letters, numbers, dashes; start with letter; end with letter/number.
    if not name:
        name = f"dlp-redact-{_uuid.uuid4().hex[:8]}"
    name = name.lower()
    name = re.sub(r"[^a-z0-9-]", "-", name)
    if not re.match(r"^[a-z]", name):
        name = "a" + name
    name = re.sub(r"[^a-z0-9]$", "0", name)
    return name[:40]


with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dlp", "dataflow", "beam"],
) as dag:
    def _get_conf(context) -> Dict[str, Any]:
        dr = context.get("dag_run")
        conf = {}
        if dr and dr.conf:
            conf = dict(dr.conf)
        conf.setdefault("project", conf.get("project") or PROJECT_DEFAULT)
        return conf

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
            raise AirflowFailException("dataflow_py_gcs must be a GCS path (gs://...) pointing to the Beam Python file")
        # Validate tenant_config keys
        tenant_cfg = conf["tenant_config"]
        if not isinstance(tenant_cfg, dict):
            raise AirflowFailException("tenant_config must be a JSON object")
        if not tenant_cfg.get("src_table") or not tenant_cfg.get("out_table"):
            raise AirflowFailException("tenant_config must contain 'src_table' and 'out_table'")
        # Serialize tenant_config tightly for pipeline option
        try:
            tenant_config_json = json.dumps(tenant_cfg, separators=(",", ":"))
        except Exception as e:
            raise AirflowFailException(f"Failed to serialize tenant_config: {e}")
        logging.info("Validation succeeded for project=%s region=%s", conf["project"], conf["region"])
        return {"conf": conf, "tenant_config_json": tenant_config_json}

    validate_conf = PythonOperator(
        task_id="validate_conf",
        python_callable=validate_conf_fn,
        provide_context=True,
    )

    def prepare_pipeline_options_fn(**context):
        ti = context["ti"]
        result = ti.xcom_pull(task_ids="validate_conf")
        if not result or "conf" not in result or "tenant_config_json" not in result:
            raise AirflowFailException("validate_conf did not return expected data")
        conf = result["conf"]
        tenant_config_json = result["tenant_config_json"]

        raw_job_name = f"dlp-redact-{_uuid.uuid4().hex[:8]}"
        job_name = _sanitize_job_name(raw_job_name)

        pipeline_options: Dict[str, Any] = {
            "project": conf["project"],
            "region": conf["region"],
            "runner": "DataflowRunner",
            "job_name": job_name,
            "staging_location": conf["staging_location"],
            "temp_location": conf["temp_location"],
            "tenant_config_json": tenant_config_json,
        }
        if conf.get("src_table"):
            pipeline_options["src_table"] = conf["src_table"]
        if conf.get("out_table"):
            pipeline_options["out_table"] = conf["out_table"]

        logging.info("Prepared pipeline_options keys: %s", list(pipeline_options.keys()))
        return pipeline_options

    prepare_pipeline_options = PythonOperator(
        task_id="prepare_pipeline_options",
        python_callable=prepare_pipeline_options_fn,
        provide_context=True,
    )

    def run_beam_runtime_wrapper(**context):
        ti = context["ti"]
        pipeline_options = ti.xcom_pull(task_ids="prepare_pipeline_options")
        if not isinstance(pipeline_options, dict):
            raise AirflowFailException("pipeline_options from XCom is not a dict; aborting")

        if "job_name" not in pipeline_options or not pipeline_options["job_name"]:
            pipeline_options["job_name"] = _sanitize_job_name(f"dlp-redact-{_uuid.uuid4().hex[:8]}")
        else:
            pipeline_options["job_name"] = _sanitize_job_name(str(pipeline_options["job_name"]))

        dr = context.get("dag_run")
        if not dr or not dr.conf or not dr.conf.get("dataflow_py_gcs"):
            raise AirflowFailException("dataflow_py_gcs not found in dag_run.conf")
        py_file = dr.conf.get("dataflow_py_gcs")

        logging.info("Launching Beam/ Dataflow job: job_name=%s py_file=%s", pipeline_options["job_name"], py_file)

        beam_op = BeamRunPythonPipelineOperator(
            task_id="run_beam_dataflow_inner",
            py_file=py_file,
            pipeline_options=pipeline_options,
            runner="DataflowRunner",
            do_xcom_push=True,
        )

        # Execute operator in-process (submit job)
        return beam_op.execute(context)

    run_beam_runtime = PythonOperator(
        task_id="run_beam_runtime_wrapper",
        python_callable=run_beam_runtime_wrapper,
        provide_context=True,
    )

    def finalize_fn(**context):
        ti = context["ti"]
        result = ti.xcom_pull(task_ids="run_beam_runtime_wrapper")
        logging.info("Beam/Dataflow job finished - result: %s", result)
        return result

    validate_conf >> prepare_pipeline_options >> run_beam_runtime >> PythonOperator(
        task_id="finalize",
        python_callable=finalize_fn,
        provide_context=True,
    )