# dlp_parent_trigger.py
from __future__ import annotations
import json
from datetime import datetime

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

DEFAULT_ARGS = {"owner": "airflow", "start_date": datetime(2025, 1, 1)}

with DAG("dlp_parent_trigger", schedule_interval=None, default_args=DEFAULT_ARGS, catchup=False) as dag:

    tenant_config = {
        "src_table": "custom-plating-475002-j7.dlp_audit.simple_customer_data",
        "out_table": "custom-plating-475002-j7.dlp_audit.simple_customer_data_beam",
        "mapping": [
           {
              "column": "email",
              "infoTypes": ["EMAIL_ADDRESS"],
              "redaction_method": "REPLACE_WITH",
              "redaction_params": {"replace_with": "[EMAIL]"}
            },
            {
                "column": "credit_card",
                "infoTypes": ["CREDIT_CARD_NUMBER"],
                "redaction_method": {"type": "MASK_LAST_N"},
                "redaction_params": {"n": 4}
            },
            {
            "column": "ssn",
            "infoTypes": ["US_SOCIAL_SECURITY_NUMBER"],
            "redaction_method": "FULL_REDACT"
            }

        ],
        "custom_info_types": []
    }

    conf = {
        "project": "custom-plating-475002-j7",
        "region": "us-central1",
        "staging_location": "gs://ravi_temp/staging",
        "temp_location": "gs://ravi_temp/temp",
        "dataflow_py_gcs": "gs://us-central1-my-composer-rav-f986408c-bucket/dags/beam_dlp_redact.py",
        "tenant_config": tenant_config
    }

    trigger = TriggerDagRunOperator(
        task_id="trigger_child_dag",
        trigger_dag_id="dlp_child_run_beam_dataflow",
        conf=conf,
        wait_for_completion=False
    )