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

tenant_config: Dict[str, Any] = {}
COLUMN_RULES: Dict[str, List[Dict[str, Any]]] = {}

# ---------- Utilities ----------
def to_dlp_cell_value(val):
    if val is None:
        return ""
    if isinstance(val, (dict, list)):
        return json.dumps(val)
    return str(val)

def cast_back(orig, redacted):
    """Cast DLP output back to original datatype (used only when safe)."""
    if orig is None:
        return redacted
    if isinstance(orig, bool):
        return redacted.lower() in ("true", "1")
    if isinstance(orig, int):
        return int(redacted)
    if isinstance(orig, float):
        return float(redacted)
    if isinstance(orig, (dict, list)):
        return json.loads(redacted)
    return redacted

# ---------- Schema helper functions ----------
def _find_schemafield_by_name(fields: List[bigquery.SchemaField], name: str):
    for f in fields:
        if f.name == name:
            return f
    return None

def _schemafield_to_dict(f: bigquery.SchemaField) -> Dict[str, Any]:
    d = {"name": f.name, "type": f.field_type, "mode": f.mode}
    if f.field_type.upper() == "RECORD" and getattr(f, "fields", None):
        d["fields"] = [_schemafield_to_dict(sub) for sub in f.fields]
    return d

def _adjust_schema_fields_for_redaction(schema_fields: List[bigquery.SchemaField], column_rules: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    """
    Return a list of dicts (BQ schema) where any field affected by column_rules
    is converted to type STRING. Supports nested dot notation for columns like 'addr.street'.
    """
    # Convert original schema into mutable nested representation
    def clone_and_modify(fields: List[bigquery.SchemaField]) -> List[Dict[str, Any]]:
        out = []
        for f in fields:
            fd = {"name": f.name, "type": f.field_type, "mode": f.mode}
            if f.field_type.upper() == "RECORD" and getattr(f, "fields", None):
                fd["fields"] = clone_and_modify(f.fields)
            out.append(fd)
        return out

    mutable = clone_and_modify(schema_fields)

    # Helper to set a nested field's type to STRING
    def set_nested_to_string(mutable_fields: List[Dict[str, Any]], parts: List[str]):
        name = parts[0]
        # find field dict
        target = None
        for mf in mutable_fields:
            if mf["name"] == name:
                target = mf
                break
        if target is None:
            # field not found in schema: ignore (we will still allow process to run; DLP will get string back)
            return
        if len(parts) == 1:
            # set this field's type to STRING (for redacted columns)
            target["type"] = "STRING"
            # if this field was a RECORD, remove nested fields because a STRING replaces the struct
            if "fields" in target:
                target.pop("fields", None)
            return
        # descend for nested
        if target.get("type", "").upper() != "RECORD":
            # schema doesn't match expectation; set leaf to STRING anyway
            # create nested RECORD? Instead set target to RECORD with nested path replaced by string
            # Simpler: set the leaf path under this target as STRING by creating a synthetic nested structure.
            # But to avoid schema mismatch, if parent isn't RECORD, set the parent to STRING (safe fallback).
            target["type"] = "STRING"
            target.pop("fields", None)
            return
        if "fields" not in target:
            # no nested schema available, nothing to modify
            return
        set_nested_to_string(target["fields"], parts[1:])

    # For every configured column, navigate dot path and set leaf to STRING
    for col in column_rules.keys():
        parts = col.split(".")
        set_nested_to_string(mutable, parts)

    return mutable

# ---------- Helper: ensure target table exists (fail-fast, no fallbacks) ----------
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

def ensure_bq_table_exists(submitter_project: str, src_table_spec: str, out_table_spec: str, column_rules: Dict[str, List[Dict[str, Any]]], max_wait_sec=30):
    """
    Ensure output table exists. If not, create it using source schema but with
    redacted columns set to STRING (per column_rules). Returns canonical_out_table id.
    """
    logging.info("ensure_bq_table_exists: submitter_project=%s src=%s out=%s",
                 submitter_project, src_table_spec, out_table_spec)
    bq = bigquery.Client(project=submitter_project)

    src_proj, src_ds, src_table = parse_table_spec(src_table_spec)
    out_proj, out_ds, out_table = parse_table_spec(out_table_spec)
    src_proj = src_proj or submitter_project
    out_proj = out_proj or submitter_project
    src_table_id = f"{src_proj}.{src_ds}.{src_table}"
    out_table_id = f"{out_proj}.{out_ds}.{out_table}"

    try:
        bq.get_table(out_table_id)
        logging.info("Target table already exists: %s", out_table_id)
        return out_table_id
    except NotFound:
        logging.info("Target table does not exist and will be created: %s", out_table_id)

    try:
        src_bq_table = bq.get_table(src_table_id)
    except NotFound:
        raise RuntimeError(f"Source table not found: {src_table_id}. Cannot create target table.")

    # build adjusted schema: redacted columns => STRING
    adjusted_schema = _adjust_schema_fields_for_redaction(src_bq_table.schema, column_rules)

    # create dataset if missing
    dataset_ref = bigquery.DatasetReference(out_proj, out_ds)
    try:
        bq.get_dataset(dataset_ref)
    except NotFound:
        logging.info("Dataset %s in project %s not found - creating dataset", out_ds, out_proj)
        dataset = bigquery.Dataset(dataset_ref)
        created_dataset = bq.create_dataset(dataset)
        logging.info("Created dataset: %s", created_dataset.reference)

    # create table object with adjusted schema
    def dicts_to_schemafields(dict_fields: List[Dict[str, Any]]):
        out = []
        for fd in dict_fields:
            if fd.get("type", "").upper() == "RECORD":
                sub = dicts_to_schemafields(fd.get("fields", []))
                out.append(bigquery.SchemaField(fd["name"], "RECORD", mode=fd.get("mode", "NULLABLE"), fields=sub))
            else:
                out.append(bigquery.SchemaField(fd["name"], fd.get("type", "STRING"), mode=fd.get("mode", "NULLABLE")))
        return out

    schema_sf = dicts_to_schemafields(adjusted_schema)
    table = bigquery.Table(out_table_id, schema=schema_sf)

    # preserve partitioning/clustering if present
    if getattr(src_bq_table, 'time_partitioning', None):
        table.time_partitioning = src_bq_table.time_partitioning
    if getattr(src_bq_table, 'clustering_fields', None):
        table.clustering_fields = src_bq_table.clustering_fields

    created = bq.create_table(table)
    logging.info("Created BigQuery table %s (fields=%d)", out_table_id, len(created.schema))

    # verify creation using the canonical dot-form out_table_id (avoid created.full_table_id colon-format)
    waited = 0
    while waited < max_wait_sec:
        try:
            bq.get_table(out_table_id)
            logging.info("Verified table exists: %s", out_table_id)
            return out_table_id
        except NotFound:
            time.sleep(1)
            waited += 1
    raise RuntimeError(f"Unable to verify creation of table {out_table_id} after {max_wait_sec}s")

# ---------- Beam DoFns (core logic unchanged except using runtime COLUMN_RULES) ----------
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

    def process(self, batch):
        if not batch:
            return

        headers = ["uuid"] + list(COLUMN_RULES.keys())

        logging.info("DLP DoFn: sending batch of %d rows; headers=%s", len(batch), headers)

        table = {"headers": [{"name": h} for h in headers], "rows": []}
        for r in batch:
            row_vals = []
            for h in headers:
                v = r.get(h, "")
                s = "" if v is None else str(v)
                row_vals.append({"string_value": s})
            table["rows"].append({"values": row_vals})

        info_type_names = set()
        for col_rules in COLUMN_RULES.values():
            for rr in col_rules:
                for it in rr.get("infoTypes", []):
                    info_type_names.add(it)
        if not info_type_names:
            raise RuntimeError("No infoTypes configured for inspection; aborting DLP call")
        inspect_config = {"info_types": [{"name": n} for n in sorted(info_type_names)]}

        info_type_transformations = []
        transform_summary = []
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
                        transform_summary.append((col, itype, "REPLACE", replacement))
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
                        transform_summary.append((col, itype, "MASK", n))
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
                        transform_summary.append((col, itype, "FPE", "<crypto>"))
                    else:
                        raise RuntimeError(f"Unknown rule type '{rtype}' for infoType {itype} on column {col}")
                    info_type_transformations.append(transform)

        deidentify_config = {"info_type_transformations": {"transformations": info_type_transformations}}

        parent = f"projects/{self.project}"
        request = {
            "parent": parent,
            "deidentify_config": deidentify_config,
            "inspect_config": inspect_config,
            "item": {"table": table},
        }

        logging.info("Calling DLP.deidentify_content parent=%s rows=%d", parent, len(table["rows"]))
        logging.info("request=%s",request)
        resp = self.client.deidentify_content(request=request)
        logging.info("resp=%s",resp)
        logging.info("DLP call completed; parsing response")

        resp_table = getattr(getattr(resp, "item", None), "table", None)
        if resp_table is None:
            raise RuntimeError(f"DLP response missing table. Full response: {resp}")

        rows_out = []
        for r in resp_table.rows:
            vals = []
            for v in r.values:
                sval = getattr(v, "string_value", None)
                if sval is None:
                    raise RuntimeError("DLP response value missing 'string_value' field - incompatible client version")
                vals.append(sval)
            rows_out.append(vals)

        for i, rv in enumerate(rows_out[:5]):
            logging.info("DLP response row[%d] mapping=%s", i, dict(zip(headers, rv)))

        for row_vals in rows_out:
            row_map = dict(zip(headers, row_vals))
            uuid_val = row_map.get("uuid")
            if uuid_val is None:
                raise RuntimeError("DLP response row missing uuid value")
            deid_map = {col: row_map.get(col) for col in COLUMN_RULES.keys()}
            logging.debug("DLP yield uuid=%s deid_map=%s", uuid_val, deid_map)
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
        # For each configured column we set the deid string value directly (output schema will be STRING)
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
                # IMPORTANT: Since this column is redacted, set the value as the de-identified string.
                # Output schema for redacted columns will be STRING (handled when creating target table).
                new_val = deid_map[col]
                parent[last] = new_val
            else:
                raise RuntimeError(f"Deidentified value missing for column {col} for uuid {uuid_key}")
        yield orig

# ---------- Pipeline ----------
def run(argv=None):

    global tenant_config, COLUMN_RULES

    options = PipelineOptions(argv)
    gcloud_opts = options.view_as(GoogleCloudOptions)

    # Fail-fast required parameters
    if not gcloud_opts.job_name:
        raise RuntimeError("Missing pipeline option: job_name")
    if not gcloud_opts.temp_location:
        raise RuntimeError("Missing pipeline option: temp_location")

    options.view_as(StandardOptions).runner = "DataflowRunner"

    # Read tenant_config_json strictly (support b64/gcs/json as previously implemented)
    p_opts = options.get_all_options()
    # Attempt to load tenant_config from staging/configs/<job_name>.json (preferred), then b64/json (fallback)
    tenant_config_loaded = None

    # Try staging file
    staging = gcloud_opts.staging_location or p_opts.get("staging_location")
    job_name = gcloud_opts.job_name or p_opts.get("job_name")
    if staging and staging.startswith("gs://"):
        try:
            from google.cloud import storage
            staging_no_prefix = staging[len("gs://"):]
            bucket_name, _, prefix = staging_no_prefix.partition("/")
            prefix = prefix.rstrip("/")
            blob_path = f"{prefix}/configs/{job_name}.json" if prefix else f"configs/{job_name}.json"
            storage_client = storage.Client(project=gcloud_opts.project)
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_path)
            if blob.exists():
                content = blob.download_as_text()
                tenant_config_loaded = json.loads(content)
                logging.info("Loaded tenant_config from staging: gs://%s/%s", bucket_name, blob_path)
            else:
                logging.info("No tenant config found at staging gs://%s/%s", bucket_name, blob_path)
        except Exception as e:
            raise RuntimeError(f"Failed to read tenant_config from staging: {e}")

    # fallback to b64/json options
    if tenant_config_loaded is None:
        tcfg_b64 = p_opts.get("tenant_config_b64")
        tcfg_json_raw = p_opts.get("tenant_config_json")
        if tcfg_b64:
            import base64
            try:
                decoded = base64.b64decode(tcfg_b64.encode("ascii")).decode("utf-8")
                tenant_config_loaded = json.loads(decoded)
                logging.info("Loaded tenant_config from tenant_config_b64")
            except Exception as e:
                raise RuntimeError(f"Failed to decode tenant_config_b64: {e}")
        elif tcfg_json_raw:
            try:
                tenant_config_loaded = json.loads(tcfg_json_raw) if isinstance(tcfg_json_raw, str) else tcfg_json_raw
                logging.info("Loaded tenant_config from tenant_config_json")
            except Exception as e:
                raise RuntimeError(f"Failed to parse tenant_config_json: {e}")

    if tenant_config_loaded is None:
        raise RuntimeError("Missing required pipeline option 'tenant_config' (provide staging file or tenant_config_b64/tenant_config_json)")

    tenant_config = tenant_config_loaded

    if "mapping" not in tenant_config or not isinstance(tenant_config["mapping"], list):
        raise RuntimeError("tenant_config.mapping must be a non-empty list")

    COLUMN_RULES = {}
    for m in tenant_config["mapping"]:
        col = m.get("column")
        rules = m.get("info_type_rules")
        if not col or not isinstance(rules, list):
            raise RuntimeError(f"Invalid mapping entry: {m}")
        COLUMN_RULES[col] = rules

    logging.info("Columns to scan: %s", list(COLUMN_RULES.keys()))

    submitter_project = gcloud_opts.project
    src_table = tenant_config["src_table"]
    out_table = tenant_config["out_table"]

    canonical_out_table = ensure_bq_table_exists(submitter_project, src_table, out_table, COLUMN_RULES)
    logging.info("Canonical out table: %s", canonical_out_table)

    with beam.Pipeline(options=options) as p:

        rows = p | "ReadBQ" >> beam.io.ReadFromBigQuery(
            table=src_table, method="DIRECT_READ"
        )

        tagged = (
            rows
            | "AddUUID" >> beam.ParDo(AddUuidAndDlpPayload()).with_outputs(
                "original", "dlp_payload"
            )
        )

        originals = tagged.original
        dlp_payloads = tagged.dlp_payload

        keyed_originals = originals | "KeyOriginals" >> beam.Map(lambda t: (t[0], t[1]))

        dlp_results = (
            dlp_payloads
            | "Batch" >> beam.BatchElements(min_batch_size=10, max_batch_size=500)
            | "CallDLP" >> beam.ParDo(BatchToDlpAndDeidentify(submitter_project))
        )

        keyed_deid = dlp_results | "KeyDeid" >> beam.Map(lambda t: (t[0], t[1]))

        merged = (
            {"original": keyed_originals, "deid": keyed_deid}
            | "Group" >> beam.CoGroupByKey()
            | "Merge" >> beam.ParDo(MergeOriginalAndDeid())
        )

        # Write to BQ using FILE_LOADS
        temp_gcs = (gcloud_opts.temp_location or "").rstrip("/") + "/bqtemp"

        bq = bigquery.Client(project=submitter_project)
        # Fetch the canonical_out_table (should exist, created above with adjusted schema)
        out_table_obj = bq.get_table(canonical_out_table)
        schema = out_table_obj.schema

        from apache_beam.io.gcp.bigquery import WriteToBigQuery

        # convert SchemaField list to JSON schema for WriteToBigQuery
        schema_json = {"fields": [{"name": f.name, "type": f.field_type, "mode": f.mode} for f in schema]}

        logging.info("Writing to BigQuery table %s via FILE_LOADS using temp %s", canonical_out_table, temp_gcs)
        merged | "WriteBQ" >> WriteToBigQuery(
            table=canonical_out_table,
            schema=schema_json,
            write_disposition="WRITE_TRUNCATE",
            create_disposition="CREATE_IF_NEEDED",
            custom_gcs_temp_location=temp_gcs,
            method=WriteToBigQuery.Method.FILE_LOADS,
        )

if __name__ == "__main__":
    run()