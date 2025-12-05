# beam_dlp_redact.py
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from google.cloud import dlp_v2
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
import uuid
import json
import logging
import time
from typing import Dict, List, Any

logging.basicConfig(level=logging.INFO)

# =====================================================================
# IMPORTANT:
#   TENANT CONFIG WILL BE PASSED FROM AIRFLOW AS tenant_config_json
#   No fallback used anywhere. If missing → pipeline FAILS IMMEDIATELY.
# =====================================================================

tenant_config: Dict[str, Any] = {}
COLUMN_RULES: Dict[str, List[Dict[str, Any]]] = {}


# -------------------------------------------------------------
# Helpers
# -------------------------------------------------------------
def to_dlp_cell_value(val):
    if val is None:
        return ""
    if isinstance(val, (dict, list)):
        return json.dumps(val)
    return str(val)


def cast_back(orig, redacted):
    """Cast DLP output back to original datatype."""
    if orig is None:
        return redacted
    if isinstance(orig, bool):
        return redacted.lower() == "true"
    if isinstance(orig, int):
        return int(redacted)
    if isinstance(orig, float):
        return float(redacted)
    if isinstance(orig, (dict, list)):
        return json.loads(redacted)
    return redacted


# -------------------------------------------------------------
# BQ table preparation — fail-fast and STRICT
# -------------------------------------------------------------
def parse_table_spec(spec: str):
    if ":" in spec:
        project, rest = spec.split(":", 1)
    else:
        parts = spec.split(".")
        if len(parts) != 3:
            raise RuntimeError(f"Invalid table spec: {spec}")
        return parts[0], parts[1], parts[2]

    if "." not in rest:
        raise RuntimeError(f"Invalid table spec: {spec}")

    dataset, table = rest.split(".", 1)
    return project, dataset, table


def ensure_bq_table_exists(project: str, src_spec: str, out_spec: str):
    bq = bigquery.Client(project=project)

    src_proj, src_ds, src_tbl = parse_table_spec(src_spec)
    out_proj, out_ds, out_tbl = parse_table_spec(out_spec)

    src_table_id = f"{src_proj}.{src_ds}.{src_tbl}"
    out_table_id = f"{out_proj}.{out_ds}.{out_tbl}"

    try:
        bq.get_table(out_table_id)
        logging.info("Target table already exists: %s", out_table_id)
        return out_table_id
    except NotFound:
        logging.info("Target table does not exist. Creating: %s", out_table_id)

    try:
        src_table = bq.get_table(src_table_id)
    except NotFound:
        raise RuntimeError(f"Source table not found: {src_table_id}")

    # ensure dataset exists
    try:
        bq.get_dataset(f"{out_proj}.{out_ds}")
    except NotFound:
        logging.info("Dataset %s.%s missing. Creating...", out_proj, out_ds)
        bq.create_dataset(bigquery.Dataset(f"{out_proj}.{out_ds}"))

    # clone structure
    new_table = bigquery.Table(out_table_id, schema=src_table.schema)

    if getattr(src_table, "time_partitioning", None):
        new_table.time_partitioning = src_table.time_partitioning

    if getattr(src_table, "clustering_fields", None):
        new_table.clustering_fields = src_table.clustering_fields

    bq.create_table(new_table)
    logging.info("Created target table: %s", out_table_id)
    return out_table_id


# -------------------------------------------------------------
# BEAM Dofns
# -------------------------------------------------------------
class AddUuidAndDlpPayload(beam.DoFn):
    def process(self, row):
        row_uuid = str(uuid.uuid4())
        yield beam.pvalue.TaggedOutput("original", (row_uuid, row))

        dlp_row = {"uuid": row_uuid}
        for col in COLUMN_RULES.keys():
            parts = col.split(".")
            val = row
            for p in parts:
                if not isinstance(val, dict) or p not in val:
                    val = None
                    break
                val = val[p]
            dlp_row[col] = to_dlp_cell_value(val)

        yield beam.pvalue.TaggedOutput("dlp_payload", dlp_row)


class BatchToDlpAndDeidentify(beam.DoFn):
    def __init__(self, project):
        self.project = project

    def start_bundle(self):
        self.client = dlp_v2.DlpServiceClient()

    def process(self, batch):

        if not batch:
            return

        headers = ["uuid"] + list(COLUMN_RULES.keys())

        logging.info("DLP DoFn: sending batch=%d; headers=%s", len(batch), headers)

        # Build DLP table
        table = {
            "headers": [{"name": h} for h in headers],
            "rows": [
                {
                    "values": [{"string_value": str(row.get(h, ""))} for h in headers]
                }
                for row in batch
            ],
        }

        # Collect infoTypes
        info_types = []
        for col_rules in COLUMN_RULES.values():
            for rule_def in col_rules:
                for it in rule_def["infoTypes"]:
                    info_types.append({"name": it})

        inspect_config = {"info_types": info_types}

        # Build transformations
        transformations = []
        transform_debug = []

        for col, rules in COLUMN_RULES.items():
            for rule_def in rules:
                rule = rule_def["rule"]
                type_ = rule["type"].upper()

                for info_type in rule_def["infoTypes"]:
                    if type_ == "REPLACE":
                        txt = rule["replace_with"]
                        transformations.append({
                            "info_types": [{"name": info_type}],
                            "primitive_transformation": {
                                "replace_config": {
                                    "new_value": {"string_value": txt}
                                }
                            },
                        })
                        transform_debug.append((col, info_type, "REPLACE", txt))

        logging.info("Transformations applied: %s", transform_debug)

        deidentify_config = {
            "info_type_transformations": {"transformations": transformations}
        }

        parent = f"projects/{self.project}"

        request = {
            "parent": parent,
            "deidentify_config": deidentify_config,
            "inspect_config": inspect_config,
            "item": {"table": table},
        }

        logging.info("Calling DLP.deidentify_content ...")
        resp = self.client.deidentify_content(request=request)

        # parse response table
        out_rows = []
        resp_table = resp.item.table

        for r in resp_table.rows:
            vals = [v.string_value for v in r.values]
            out_rows.append(vals)

        # preview logs
        for i, r in enumerate(out_rows[:5]):
            logging.info("DLP response row[%d]=%s", i, dict(zip(headers, r)))

        # yield
        for row_vals in out_rows:
            d = dict(zip(headers, row_vals))
            uuid_ = d["uuid"]
            del d["uuid"]
            yield uuid_, d


class MergeOriginalAndDeid(beam.DoFn):
    def process(self, kv):
        uuid_, grouped = kv
        original = grouped["original"][0]   # guaranteed
        deid_map = grouped["deid"][0]

        for col in COLUMN_RULES:
            parts = col.split(".")
            parent = original

            for p in parts[:-1]:
                parent = parent[p]

            leaf = parts[-1]
            orig_val = parent.get(leaf)

            parent[leaf] = cast_back(orig_val, deid_map[col])

        yield original


def run(argv=None):
    """
    Beam pipeline entrypoint.

    Behavior:
      - Expects Dataflow runner options to include job_name and staging_location.
      - Attempts to load tenant config from: gs://<staging_prefix>/configs/<job_name>.json
      - If not found, falls back to tenant_config_b64 or tenant_config_json (strict).
      - Fails fast on any error.
    """
    import base64
    try:
        from google.cloud import storage
    except Exception as e:
        # Worker must have google-cloud-storage installed to fetch config from staging.
        raise RuntimeError(
            "google-cloud-storage import failed on worker. "
            "Add 'google-cloud-storage' to Dataflow worker requirements or use a worker image that includes it. "
            f"Original error: {e}"
        )

    global tenant_config, COLUMN_RULES

    # create options and gcloud options early
    options = PipelineOptions(argv)
    gcloud_options = options.view_as(GoogleCloudOptions)

    # Ensure project default if not set
    gcloud_options.project = (gcloud_options.project or 'custom-plating-475002-j7')

    # fail fast for critical options
    if not gcloud_options.job_name:
        raise RuntimeError("Pipeline option 'job_name' must be provided")
    if not gcloud_options.temp_location:
        raise RuntimeError("Pipeline option 'temp_location' must be provided")

    options.view_as(StandardOptions).runner = 'DataflowRunner'

    # get all options (this contains any extras)
    p_opts = options.get_all_options()

    # Attempt #1: Load config from staging_location/configs/<job_name>.json
    staging = gcloud_options.staging_location or p_opts.get("staging_location")
    job_name = gcloud_options.job_name or p_opts.get("job_name")
    tenant_config = None

    if staging and staging.startswith("gs://"):
        # Parse bucket and prefix
        staging_no_prefix = staging[len("gs://"):]
        bucket_name, _, prefix = staging_no_prefix.partition("/")
        prefix = prefix.rstrip("/")
        blob_path = f"{prefix}/configs/{job_name}.json" if prefix else f"configs/{job_name}.json"
        gcs_uri = f"gs://{bucket_name}/{blob_path}"
        logging.info("Looking for tenant config at staging path: %s", gcs_uri)
        try:
            storage_client = storage.Client(project=gcloud_options.project)
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_path)
            if blob.exists():
                content = blob.download_as_text()
                tenant_config = json.loads(content)
                logging.info("Loaded tenant_config from %s", gcs_uri)
            else:
                logging.info("No tenant config found at %s", gcs_uri)
        except Exception as e:
            # fail fast if we cannot access staging GCS
            raise RuntimeError(f"Failed to read tenant_config from GCS {gcs_uri}: {e}")

    # Attempt #2: Back-compat inline base64 / json
    if tenant_config is None:
        tcfg_b64 = p_opts.get("tenant_config_b64")
        tcfg_json_raw = p_opts.get("tenant_config_json")
        if tcfg_b64:
            logging.info("Found tenant_config_b64 pipeline option; decoding base64.")
            try:
                decoded = base64.b64decode(tcfg_b64.encode("ascii")).decode("utf-8")
                tenant_config = json.loads(decoded)
                logging.info("Loaded tenant_config from tenant_config_b64")
            except Exception as e:
                raise RuntimeError(f"Failed to base64-decode tenant_config_b64: {e}")
        elif tcfg_json_raw:
            logging.info("Found tenant_config_json pipeline option; parsing JSON.")
            try:
                tenant_config = json.loads(tcfg_json_raw) if isinstance(tcfg_json_raw, str) else tcfg_json_raw
                logging.info("Loaded tenant_config from tenant_config_json")
            except Exception as e:
                raise RuntimeError(f"Failed to parse tenant_config_json: {e}")

    # Final check - fail fast if still missing
    if tenant_config is None:
        raise RuntimeError(
            "Missing tenant configuration. Provide staging_location/configs/<job_name>.json "
            "or pipeline option tenant_config_b64/tenant_config_json."
        )

    # Validate mapping
    if "mapping" not in tenant_config or not isinstance(tenant_config["mapping"], list) or len(tenant_config["mapping"]) == 0:
        raise RuntimeError("tenant_config.mapping must be a non-empty list")

    # Build COLUMN_RULES
    COLUMN_RULES = {}
    for entry in tenant_config["mapping"]:
        col = entry.get("column")
        rules = entry.get("info_type_rules")
        if not col or not isinstance(rules, list) or len(rules) == 0:
            raise RuntimeError(f"Invalid mapping entry: {entry}")
        COLUMN_RULES[col] = rules

    logging.info("Runtime tenant_config applied; scanning columns: %s", list(COLUMN_RULES.keys()))

    # Validate tables
    submitter_project = gcloud_options.project
    src_table_spec = tenant_config.get("src_table")
    out_table_spec = tenant_config.get("out_table")
    if not src_table_spec or not out_table_spec:
        raise RuntimeError("tenant_config must contain 'src_table' and 'out_table'")

    canonical_out_table = ensure_bq_table_exists(submitter_project, src_table_spec, out_table_spec)
    logging.info("Canonical out table: %s", canonical_out_table)

    # --- continue the pipeline as before ---
    with beam.Pipeline(options=options) as p:
        rows = p | "ReadBQ" >> beam.io.ReadFromBigQuery(table=tenant_config['src_table'], method='DIRECT_READ')
        multi = (rows
                 | "AddUuidAndDlp" >> beam.ParDo(AddUuidAndDlpPayload()).with_outputs('original','dlp_payload'))
        originals = multi.original
        dlp_payloads = multi.dlp_payload
        key_originals = originals | "KeyOriginals" >> beam.Map(lambda t: (t[0], t[1]))
        batched = (dlp_payloads
                   | "Batch" >> beam.BatchElements(min_batch_size=10, max_batch_size=500)
                   | "CallDLP" >> beam.ParDo(BatchToDlpAndDeidentify(gcloud_options.project))
                  )
        keyed_deid = batched | "KeyDeid" >> beam.Map(lambda t: (t[0], t[1]))
        merged = ({
            'original': key_originals,
            'deid': keyed_deid
        }
                  | "CoGroup" >> beam.CoGroupByKey()
                  | "Merge" >> beam.ParDo(MergeOriginalAndDeid())
                 )

        # BigQuery sink - use canonical_out_table and require gcloud_options.temp_location
        from apache_beam.io.gcp.bigquery import WriteToBigQuery

        custom_gcs_temp_location = gcloud_options.temp_location.rstrip('/') + '/bq-temp'
        bq_client = bigquery.Client(project=submitter_project)
        bq_table_obj = bq_client.get_table(canonical_out_table)  # fail if missing
        schema_fields = [{"name": f.name, "type": f.field_type, "mode": f.mode} for f in bq_table_obj.schema]
        schema_json = {"fields": schema_fields}

        logging.info("Writing to BigQuery table %s via FILE_LOADS using temp %s", canonical_out_table, custom_gcs_temp_location)
        merged | "WriteBQ" >> WriteToBigQuery(
            table=canonical_out_table,
            schema=schema_json,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            custom_gcs_temp_location=custom_gcs_temp_location,
            method=WriteToBigQuery.Method.FILE_LOADS,
        )


if __name__ == "__main__":
    run()