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
from typing import Dict, List

logging.basicConfig(level=logging.INFO)

# ---------- Tenant config (example) ----------
tenant_config = {
    "src_table": "custom-plating-475002-j7.dlp_audit.simple_customer_data",
    "out_table": "custom-plating-475002-j7.dlp_audit.simple_customer_data_beam",
    "mapping": [
        {
            "column": "email",
            "info_type_rules": [
                {
                    "infoTypes": ["EMAIL_ADDRESS"],
                    "rule": {"type": "REPLACE", "replace_with": "[EMAIL]"}
                }
            ]
        }
    ],
    "custom_info_types": []
}

# Map of column -> list of rules
if "mapping" not in tenant_config or not isinstance(tenant_config["mapping"], list):
    raise RuntimeError("tenant_config.mapping must be a list")
COLUMN_RULES = {m["column"]: m["info_type_rules"] for m in tenant_config["mapping"]}

# ---------- Utilities ----------
def to_dlp_cell_value(val):
    if val is None:
        return ""
    if isinstance(val, (dict, list)):
        return json.dumps(val, default=str)
    return str(val)

def cast_back(orig_value, deid_str):
    if orig_value is None:
        return deid_str
    if isinstance(orig_value, bool):
        return deid_str.lower() in ("true", "1")
    if isinstance(orig_value, int):
        return int(deid_str)
    if isinstance(orig_value, float):
        return float(deid_str)
    if isinstance(orig_value, (dict, list)):
        return json.loads(deid_str)
    return deid_str

# --- Helper: ensure target table exists (fail-fast, no fallbacks) ---
def parse_table_spec(table_spec: str):
    if ":" in table_spec:
        project, rest = table_spec.split(":", 1)
    else:
        parts = table_spec.split(".")
        if len(parts) == 3:
            project, dataset, table = parts
            return project, dataset, table
        raise RuntimeError(f"Invalid table spec: {table_spec}")
    if "." not in rest:
        raise RuntimeError(f"Invalid table spec: {table_spec}")
    dataset, table = rest.split(".", 1)
    return project, dataset, table

def ensure_bq_table_exists(submitter_project: str, src_table_spec: str, out_table_spec: str, max_wait_sec=30):
    logging.info("ensure_bq_table_exists: submitter_project=%s src=%s out=%s",
                 submitter_project, src_table_spec, out_table_spec)
    bq = bigquery.Client(project=submitter_project)

    src_proj, src_ds, src_table = parse_table_spec(src_table_spec)
    out_proj, out_ds, out_table = parse_table_spec(out_table_spec)
    src_proj = src_proj or submitter_project
    out_proj = out_proj or submitter_project
    src_table_id = f"{src_proj}.{src_ds}.{src_table}"
    out_table_id = f"{out_proj}.{out_ds}.{out_table}"

    # fail fast if out table exists -> return canonical id
    try:
        bq.get_table(out_table_id)
        logging.info("Target table already exists: %s", out_table_id)
        return out_table_id
    except NotFound:
        logging.info("Target table does not exist and will be created: %s", out_table_id)

    # fetch source schema (fail if missing)
    try:
        src_bq_table = bq.get_table(src_table_id)
    except NotFound:
        raise RuntimeError(f"Source table not found: {src_table_id}. Cannot create target table.")

    # ensure dataset exists (fail if missing and creation fails)
    dataset_ref = bigquery.DatasetReference(out_proj, out_ds)
    try:
        bq.get_dataset(dataset_ref)
    except NotFound:
        logging.info("Dataset %s in project %s not found - creating dataset", out_ds, out_proj)
        dataset = bigquery.Dataset(dataset_ref)
        created_dataset = bq.create_dataset(dataset)  # may raise
        logging.info("Created dataset: %s", created_dataset.reference)

    # create table with schema, copy partition/clustering if any
    table_ref = f"{out_proj}.{out_ds}.{out_table}"
    table = bigquery.Table(table_ref, schema=src_bq_table.schema)
    if getattr(src_bq_table, 'time_partitioning', None):
        table.time_partitioning = src_bq_table.time_partitioning
    if getattr(src_bq_table, 'clustering_fields', None):
        table.clustering_fields = src_bq_table.clustering_fields

    created = bq.create_table(table)  # will raise if permission denied
    logging.info("Created BigQuery table %s (fields=%d)", created.full_table_id, len(created.schema))

    # verify creation quickly and fail if not found
    waited = 0
    while waited < max_wait_sec:
        try:
            bq.get_table(created.full_table_id)
            logging.info("Verified table exists: %s", created.full_table_id)
            return created.full_table_id
        except NotFound:
            time.sleep(1)
            waited += 1
    raise RuntimeError(f"Unable to verify creation of table {created.full_table_id} after {max_wait_sec}s")

# ---------- Beam DoFns (core logic unchanged) ----------
class AddUuidAndDlpPayload(beam.DoFn):
    def process(self, row: Dict):
        row_uuid = str(uuid.uuid4())
        logging.debug("Assign UUID %s for row", row_uuid)
        yield beam.pvalue.TaggedOutput('original', (row_uuid, row))
        dlp_row = {'uuid': row_uuid}
        for col in COLUMN_RULES.keys():
            parts = col.split('.')
            val = row
            for p in parts:
                if not isinstance(val, dict) or p not in val:
                    val = None
                    break
                val = val[p]
            dlp_row[col] = to_dlp_cell_value(val)
        yield beam.pvalue.TaggedOutput('dlp_payload', dlp_row)

class BatchToDlpAndDeidentify(beam.DoFn):
    def __init__(self, project):
        self.project = project

    def start_bundle(self):
        self.client = dlp_v2.DlpServiceClient()

    def process(self, batch_rows):
        if not batch_rows:
            return

        headers = ["uuid"] + list(COLUMN_RULES.keys())
        table = {"headers": [{"name": h} for h in headers], "rows": []}
        for r in batch_rows:
            vals = []
            for h in headers:
                v = r.get(h)
                if v is None:
                    sval = ""
                else:
                    sval = str(v)
                vals.append({"string_value": sval})
            table["rows"].append({"values": vals})

        # inspect config
        info_type_names = set()
        for col_rules in COLUMN_RULES.values():
            for rr in col_rules:
                for it in rr.get("infoTypes", []):
                    info_type_names.add(it)
        if not info_type_names:
            raise RuntimeError("No infoTypes configured for inspection; aborting DLP call")

        inspect_config = {"info_types": [{"name": n} for n in sorted(info_type_names)]}

        # custom info types - require correct format
        custom_info_types = []
        for cit in tenant_config.get("custom_info_types", []):
            name = cit.get("name")
            regex = cit.get("regex")
            if not name or not regex:
                raise RuntimeError(f"Invalid custom_info_type entry: {cit}")
            custom_info_types.append({"info_type": {"name": name}, "regex": {"pattern": regex}})
        if custom_info_types:
            inspect_config["custom_info_types"] = custom_info_types

        # build transformations (fail-fast on invalid rules)
        info_type_transformations = []
        for col, rules in COLUMN_RULES.items():
            for rule in rules:
                rinfo = rule.get("rule", {})
                rtype = rinfo.get("type", "REPLACE").upper()
                for itype in rule.get("infoTypes", []):
                    if rtype == "REPLACE":
                        replacement = rinfo.get("replace_with")
                        if replacement is None:
                            raise RuntimeError(f"REPLACE rule missing 'replace_with' for infoType {itype} on column {col}")
                        transform = {
                            "info_types": [{"name": itype}],
                            "primitive_transformation": {
                                "replace_config": {"new_value": {"string_value": replacement}}
                            }
                        }
                    elif rtype in ("MASK_LAST_N", "MASK_LAST", "MASK"):
                        n = rinfo.get("n")
                        if n is None:
                            raise RuntimeError(f"MASK rule missing 'n' for infoType {itype} on column {col}")
                        transform = {
                            "info_types": [{"name": itype}],
                            "primitive_transformation": {
                                "character_mask_config": {"number_to_mask": int(n), "reverse_order": True}
                            }
                        }
                    elif rtype == "FPE":
                        wrapped_key = rinfo.get("wrapped_key")
                        kms_key_name = rinfo.get("kms_key_name")
                        if not wrapped_key and not kms_key_name:
                            raise RuntimeError(f"FPE rule requires crypto key info for infoType {itype} on column {col}")
                        crypto_config = {}
                        if wrapped_key:
                            crypto_config["crypto_key"] = {"kms_wrapped": {"wrapped_key": wrapped_key}}
                        elif kms_key_name:
                            crypto_config["crypto_key"] = {"kms": {"name": kms_key_name}}
                        surrogate = rinfo.get("surrogate_info_type", {"name": "TOKEN"})
                        transform = {
                            "info_types": [{"name": itype}],
                            "primitive_transformation": {
                                "crypto_deterministic_config": {
                                    **crypto_config,
                                    "surrogate_info_type": surrogate
                                }
                            }
                        }
                    else:
                        raise RuntimeError(f"Unknown rule type '{rtype}' for infoType {itype} on column {col}")
                    info_type_transformations.append(transform)

        if not info_type_transformations:
            raise RuntimeError("No info_type_transformations were constructed; aborting")

        deidentify_config = {"info_type_transformations": {"transformations": info_type_transformations}}

        # build request (snake_case)
        parent = f"projects/{self.project}"
        request = {
            "parent": parent,
            "deidentify_config": deidentify_config,
            "inspect_config": inspect_config,
            "item": {"table": table},
        }

        # call DLP - fail on any exception
        logging.info("Calling DLP deidentify_content for %d rows; parent=%s", len(batch_rows), parent)
        resp = self.client.deidentify_content(request=request)  # will raise on error
        logging.info("DLP call completed; parsing response")

        # parse response - require response.table exists
        resp_table = getattr(getattr(resp, "item", None), "table", None)
        if resp_table is None:
            raise RuntimeError(f"DLP response has no table. Full response: {resp}")

        rows_out = []
        for r in resp_table.rows:
            vals = []
            for v in r.values:
                sval = getattr(v, "string_value", None)
                if sval is None:
                    # explicit fail: unexpected Value shape
                    raise RuntimeError("DLP response value missing 'string_value' field - incompatible client version")
                vals.append(sval)
            rows_out.append(vals)

        # Map rows back to uuid and yield
        for row_vals in rows_out:
            row_map = dict(zip(headers, row_vals))
            uuid_val = row_map.get("uuid")
            if uuid_val is None:
                raise RuntimeError("DLP response row missing uuid value")
            deid_map = {col: row_map.get(col) for col in COLUMN_RULES.keys()}
            yield (uuid_val, deid_map)

class MergeOriginalAndDeid(beam.DoFn):
    def process(self, key_and_values):
        uuid_key, grouped = key_and_values
        originals = grouped.get('original', [])
        deids = grouped.get('deid', [])
        if not originals:
            raise RuntimeError(f"No original row found for uuid {uuid_key}")
        orig = originals[0]
        deid_map = deids[0] if deids else {}
        for col in COLUMN_RULES.keys():
            parts = col.split('.')
            parent = orig
            for p in parts[:-1]:
                if p not in parent or not isinstance(parent, dict):
                    raise RuntimeError(f"Missing nested path {'.'.join(parts[:-1])} in original row for uuid {uuid_key}")
                parent = parent[p]
            last = parts[-1]
            original_cell_value = parent.get(last)
            if col in deid_map:
                new_val_casted = cast_back(original_cell_value, deid_map[col])
                parent[last] = new_val_casted
            else:
                # if deid not present, explicitly set empty string (fail-fast policy: treat as error)
                raise RuntimeError(f"Deidentified value missing for column {col} for uuid {uuid_key}")
        yield orig

# ---------- Pipeline ----------
def run(argv=None):
    options = PipelineOptions(argv)
    gcloud_options = options.view_as(GoogleCloudOptions)
    gcloud_options.project = (gcloud_options.project or 'custom-plating-475002-j7')
    # ensure job_name and temp_location provided by caller; fail if not
    if not gcloud_options.job_name:
        raise RuntimeError("Pipeline option 'job_name' must be provided")
    if not gcloud_options.temp_location:
        raise RuntimeError("Pipeline option 'temp_location' must be provided")
    options.view_as(StandardOptions).runner = 'DataflowRunner'

    submitter_project = gcloud_options.project
    if not submitter_project:
        raise RuntimeError("Unable to determine submitter project")

    src_table_spec = tenant_config.get("src_table")
    out_table_spec = tenant_config.get("out_table")
    if not src_table_spec or not out_table_spec:
        raise RuntimeError("tenant_config must contain 'src_table' and 'out_table'")

    # Ensure target table exists (fail fast on any error)
    canonical_out_table = ensure_bq_table_exists(submitter_project, src_table_spec, out_table_spec)
    logging.info("Canonical out table: %s", canonical_out_table)

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

if __name__ == '__main__':
    run()
