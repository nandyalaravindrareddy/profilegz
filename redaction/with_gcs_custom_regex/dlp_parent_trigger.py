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
                "redaction_method": "MASK_LAST_N",
                "redaction_params": {"n": 4}
            },
            {
                "column": "comments",
                "info_type_rules": [
                    {
                        "infoTypes": ["US_SOCIAL_SECURITY_NUMBER"],
                        "redaction_method": "REPLACE_WITH",
                        "redaction_params": {"replace_with": "**ssn by dlp**"}
                    },
                    {
                        "infoTypes": ["CREDIT_CARD_NUMBER"],
                        "redaction_method": "MASK_LAST_N",
                        "redaction_params": {"n": 4},
                    },
                    {
                        "infoTypes": ["CUSTOM_PASSWORD"],  # Now using custom info type
                        "redaction_method": "REPLACE_WITH",
                        "redaction_params": {"replace_with": "**custom password by dlp**"}
                    },
                    {
                        "infoTypes": ["CUSTOM_SAR"],  # Now using custom info type
                        "redaction_method": "REPLACE_WITH",
                        "redaction_params": {"replace_with": "SAR_REDACTED"}
                    },
                ],
            },
        ],
        "custom_info_types": [
            {
                "info_type": {"name": "CUSTOM_PASSWORD"},
                "regex": {
                    "pattern": r"(?i)(password|passwd|pwd)[\\s]*[:=][\\s]*[^\\s]+",
                    "group_indexes": [0]
                },
                "likelihood": "POSSIBLE"
            },
            {
                "info_type": {"name": "CUSTOM_SAR"},
                "regex": {
                    "pattern": r"(?i)\b(SAR|S\.A\.R\.|Suspicious Activity Report)\b",
                    "group_indexes": [0]
                },
                "likelihood": "POSSIBLE"
            }
        ]
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