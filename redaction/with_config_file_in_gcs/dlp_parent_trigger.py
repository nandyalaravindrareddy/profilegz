# dlp_parent_trigger.py
from __future__ import annotations

from datetime import datetime
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# ------------------------------------------------------------------------------------------
# Parent DAG
#
# You trigger this DAG manually or from another process.
# It forwards the tenant config + dataflow params into the CHILD DAG.
# ------------------------------------------------------------------------------------------

with DAG(
    dag_id="dlp_parent_trigger",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dlp", "trigger"],
) as dag:

    TriggerDagRunOperator(
        task_id="trigger_child_dag",
        trigger_dag_id="dlp_child_run_beam_dataflow",
        reset_dag_run=True,
        wait_for_completion=False,

        # DAG RUN CONFIG PASSED TO CHILD DAG
        conf={
            # -------------------------------------
            # DATAFLOW REQUIRED OPTIONS
            # -------------------------------------
            "project": "custom-plating-475002-j7",
            "region": "us-central1",
            "temp_location": "gs://ravi_temp/temp",
            "staging_location": "gs://ravi_temp/staging",

            # Beam Python file (MUST be in GCS)
            "dataflow_py_gcs":
                "gs://us-central1-my-composer-rav-f986408c-bucket/dags/beam_dlp_redact.py",

            # -------------------------------------
            # TENANT CONFIG (REQUIRED)
            # -------------------------------------
            "tenant_config": {
                "src_table": "custom-plating-475002-j7.dlp_audit.simple_customer_data",
                "out_table": "custom-plating-475002-j7.dlp_audit.simple_customer_data_beam",

                "mapping": [
                    {
                        "column": "email",
                        "info_type_rules": [
                            {
                                "infoTypes": ["EMAIL_ADDRESS"],
                                "rule": {"type": "REPLACE", "replace_with": "[EMAIL]"},
                            }
                        ],
                    },
                    {
                        "column": "credit_card",
                        "info_type_rules": [
                            {
                                "infoTypes": ["CREDIT_CARD_NUMBER"],
                                "rule": {"type": "REPLACE", "replace_with": "[CREDIT_CARD]"},
                            }
                        ],
                    },
                ],

                "custom_info_types": [],
            },
        },
    )