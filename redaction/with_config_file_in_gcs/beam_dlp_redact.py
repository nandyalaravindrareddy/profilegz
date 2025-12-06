# beam_dlp_redact.py
"""
Updated Beam Dataflow job (single-file) — minimal change set to:
 - Strictly load tenant_config from staging/configs/<job_name>.json (fail-fast).
 - Accept tenant mapping where each entry supplies:
     {
       "column": "col_name" or "a.b.c",
       "infoTypes": ["EMAIL_ADDRESS", "CREDIT_CARD_NUMBER"],   # optional if redaction_method applies irrespective
       "redaction_method": "MASK_LAST_N" | "FULL_REDACT" | "REPLACE_WITH" | ...,
       "redaction_params": { "n": 4, "replace_with": "[X]" }   # optional
     }
 - Translate high-level redaction_method -> DLP primitiveTransformation automatically.
 - Preserve other working flow: uuid, send only mapped columns to DLP, merge by uuid, write BQ.
 - Ensure mapped columns become STRING in output table.
 - Log DLP request/response for debugging.
"""
from __future__ import annotations
import json
import logging
import time
import uuid
from typing import Any, Dict, List, Optional

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions

from google.cloud import dlp_v2
from google.cloud import bigquery
from google.cloud import storage
from google.api_core.exceptions import NotFound
from google.protobuf.json_format import MessageToDict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("beam_dlp_redact")

# -------------------------
# Helper functions for DLP proto field names
# -------------------------
def _to_info_type_dict(info_type_spec):
    """Convert infoType specification to DLP InfoType dict with correct field names."""
    if isinstance(info_type_spec, dict):
        name = info_type_spec.get("name")
        if name is None:
            raise ValueError(f"InfoType dict must have a 'name' key: {info_type_spec}")
        return {"name": name}
    else:
        return {"name": str(info_type_spec)}

# -------------------------
# Map high-level redaction_method -> DLP primitiveTransformation payload
# -------------------------
def _replace_config_value(val: str) -> Dict[str, Any]:
    return {"newValue": {"stringValue": val}}

def _primitive_for_method(method: Optional[Any], params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Convert a high-level redaction method into a DLP primitiveTransformation dict.

    Supports:
      - method as STRING: "FULL_REDACT", "REPLACE_WITH", "NULLIFY",
                          "MASK_LAST_N", "MASK_FIRST_N", "MASK",
                          "FPE", "CRYPTO", "TOKENIZE"
      - method as DICT (old 'rule' style): {"type": "...", ...}

    Returns a dict suitable to be used as:
      primitive_transformation = <returned dict>
    where the keys inside match DLP v2 proto fields:
      - replace_with_info_type_config
      - replace_config.new_value.string_value / null_value
      - character_mask_config.masking_character / number_to_mask / reverse_order
      - crypto_deterministic_config.crypto_key / surrogate_info_type
    """

    if params is None:
        params = {}

    # ---- Backward compatibility: method may be a rule dict ----
    if isinstance(method, dict):
        rule_dict = dict(method)
        method_str = (
            rule_dict.pop("type", None)
            or rule_dict.pop("rule", None)
            or rule_dict.pop("op", None)
        )
        if not method_str:
            raise ValueError(f"rule dict must contain 'type' (got {method})")
        merged_params = dict(rule_dict)
        merged_params.update(params)
        params = merged_params
        method = method_str

    if method is None or not isinstance(method, str):
        raise ValueError(f"redaction method must be a string or dict, got {type(method)}: {method}")

    m = method.upper()

    # -------- FULL REDACT ----------
    if m in ("FULL_REDACT", "REDACT"):
        # Replace entire finding with its infoType name
        return {"replace_with_info_type_config": {}}

    # -------- REPLACE WITH STRING ----------
    if m == "REPLACE_WITH":
        repl = params.get("replace_with", "[REDACTED]")
        return {
            "replace_config": {
                "new_value": {
                    "string_value": repl
                }
            }
        }

    # -------- NULLIFY ----------
    if m == "NULLIFY":
        return {
            "replace_config": {
                "new_value": {
                    "null_value": None
                }
            }
        }

    # -------- MASK_LAST_N ----------
    # Masks the LAST N characters of the finding.
    if m == "MASK_LAST_N":
        n = int(params.get("n", 4))
        return {
            "character_mask_config": {
                "number_to_mask": n,
                "reverse_order": True
            }
        }

    # -------- MASK_FIRST_N ----------
    # Masks the FIRST N characters of the finding.
    if m == "MASK_FIRST_N":
        n = int(params.get("n", 4))
        return {
            "character_mask_config": {
                "number_to_mask": n,
                "reverse_order": False
            }
        }

    # -------- MASK (generic) ----------
    # Uses 'number_to_mask' param directly (no preserve_* fields – DLP proto doesn't support them).
    if m == "MASK":
        num = int(params.get("number_to_mask", params.get("n", 4)))
        mc = params.get("masking_char", "*")
        cfg: Dict[str, Any] = {
            "masking_character": mc,
            "number_to_mask": num,
        }
        # Optional direction
        if "reverse_order" in params:
            cfg["reverse_order"] = bool(params["reverse_order"])
        return {"character_mask_config": cfg}

    # -------- FPE / CRYPTO / TOKENIZE (deterministic crypto) ----------
    if m in ("FPE", "CRYPTO", "TOKENIZE"):
        kms_key_name = params.get("kms_key_name")
        wrapped_key_b64 = params.get("wrapped_key_b64")

        if not kms_key_name and not wrapped_key_b64:
            raise ValueError(
                "FPE/CRYPTO requires 'kms_key_name' or 'wrapped_key_b64' in redaction_params"
            )

        crypto_key: Dict[str, Any] = {}
        # If we have a raw wrapped_key, use kms_wrapped.wrapped_key.
        # If not, we at least pass crypto_key_name.
        if wrapped_key_b64:
            crypto_key["kms_wrapped"] = {
                "wrapped_key": wrapped_key_b64,
                "crypto_key_name": kms_key_name,
            }
        else:
            crypto_key["kms_wrapped"] = {
                "crypto_key_name": kms_key_name,
            }

        surrogate_name = (
            params.get("surrogate_name")
            or params.get("surrogate_info_type_name")
            or "TOKEN"
        )
        surrogate = {"info_type": {"name": surrogate_name}}

        return {
            "crypto_deterministic_config": {
                "crypto_key": crypto_key,
                "surrogate_info_type": surrogate,
            }
        }

    # -------- DEFAULT (fallback) ----------
    # If unknown method, default to FULL_REDACT semantics.
    return {"replace_with_info_type_config": {}}


# -------------------------
# DLP/inspect builders
# -------------------------
def _make_field(name: str) -> Dict[str, Any]:
    return {"name": name}

def build_inspect_config_from_mapping(
    mapping: Dict[str, Any],
    include_quote: bool = False,
    include_fields: Optional[List[str]] = None,
    extra_custom_info_types: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """
    Build InspectConfig with correct DLP v2 fields.

    IMPORTANT:
      - We ONLY treat tenant_config['custom_info_types'] (extra_custom_info_types)
        as actual CustomInfoType objects.
      - We NEVER treat dicts from mapping/info_type_rules as CustomInfoType.
      - InspectConfig proto has:
          * info_types
          * custom_info_types
          * include_quote
        but NOT 'include_fields'.

    mapping (COLUMN_RULES) examples:

      "email": {
        "infoTypes": ["EMAIL_ADDRESS"],
        ...
      }

      "comments": {
        "info_type_rules": [
          {
            "infoTypes": ["US_SOCIAL_SECURITY_NUMBER"],
            ...
          },
          {
            "infoTypes": ["CREDIT_CARD_NUMBER"],
            ...
          }
        ]
      }

    extra_custom_info_types is expected to already be in proper DLP
    CustomInfoType shape, e.g.:

      {
        "info_type": { "name": "CUSTOM_PASSWORD" },
        "regex": { "pattern": "(?i)password\\s*[:=]\\s*\\S+" }
      }
    """

    builtin: set[str] = set()

    # 1) Collect ALL builtin infoTypes from mapping (strings only)
    for _, entry in mapping.items():
        # Column-level infoTypes
        its_col = entry.get("infoTypes") or []
        for it in its_col:
            if isinstance(it, str):
                builtin.add(it)
            # if it's a dict, we assume it's an infoType reference object
            # (like {"name": "EMAIL_ADDRESS"}), not a CustomInfoType definition,
            # and we DO NOT treat it as custom_info_type.

        # Per-infoType rules for this column
        for rule in entry.get("info_type_rules") or []:
            its_rule = rule.get("infoTypes") or []
            for it in its_rule:
                if isinstance(it, str):
                    builtin.add(it)
                # again, if it's a dict, we assume {"name": "..."} and ignore it
                # for custom_info_types.

    # 2) Build the InspectConfig dict in canonical snake_case
    cfg: Dict[str, Any] = {"include_quote": include_quote}

    if builtin:
        cfg["info_types"] = [{"name": n} for n in sorted(builtin)]

    # 3) Only use extra_custom_info_types (tenant_config["custom_info_types"])
    #    as CustomInfoType objects. We do NOT infer any CustomInfoType from mapping.
    if extra_custom_info_types:
        cfg["custom_info_types"] = extra_custom_info_types

    # 4) DO NOT set 'include_fields' here – not part of InspectConfig.
    #    Column-level filtering is handled by which headers/values we pass
    #    in the DLP Table (item.table).

    return cfg


def build_field_transformations_from_mapping(mapping: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Build recordTransformations.fieldTransformations using COLUMN_RULES.

    mapping (COLUMN_RULES) shape examples:

      # Simple column-level rule
      "email": {
        "infoTypes": ["EMAIL_ADDRESS"],
        "redaction_method": "REPLACE_WITH",
        "redaction_params": {"replace_with": "[EMAIL]"}
      }

      # Column with multiple infoTypes and different rules (comments)
      "comments": {
        "info_type_rules": [
          {
            "infoTypes": ["US_SOCIAL_SECURITY_NUMBER"],
            "redaction_method": "FULL_REDACT"
          },
          {
            "infoTypes": ["CREDIT_CARD_NUMBER"],
            "redaction_method": "MASK_LAST_N",
            "redaction_params": {"n": 4}
          },
          {
            "infoTypes": ["CUSTOM_PASSWORD"],
            "redaction_method": "FULL_REDACT"
          }
        ]
      }
    """
    fts: List[Dict[str, Any]] = []

    for col, entry in mapping.items():
        ft: Dict[str, Any] = {"fields": [{"name": col}]}

        # ---- Case 1: per-infoType rules on this column ----
        info_type_rules = entry.get("info_type_rules") or []
        if info_type_rules:
            transformations: List[Dict[str, Any]] = []

            for rule in info_type_rules:
                rule_info_types = rule.get("infoTypes") or []
                redaction_method = rule.get("redaction_method")
                redaction_params = rule.get("redaction_params") or {}

                prim = None
                if redaction_method is not None:
                    prim = _primitive_for_method(redaction_method, redaction_params)
                else:
                    # If no method given, default to FULL_REDACT for that infoType
                    prim = {"replace_with_info_type_config": {}}

                if not rule_info_types:
                    raise ValueError(
                        f"info_type_rule for column '{col}' must specify 'infoTypes'"
                    )

                for it in rule_info_types:
                    it_dict = _to_info_type_dict(it)
                    transformations.append(
                        {
                            "info_types": [it_dict],
                            "primitive_transformation": prim,
                        }
                    )

            if not transformations:
                raise ValueError(
                    f"Column '{col}' has info_type_rules but produced no transformations"
                )

            ft["info_type_transformations"] = {"transformations": transformations}
            fts.append(ft)
            continue  # done with this column

        # ---- Case 2: simple column-level rule (existing logic) ----
        info_types = entry.get("infoTypes") or []
        redaction_method = entry.get("redaction_method")
        redaction_params = entry.get("redaction_params") or {}

        prim = None
        if redaction_method is not None:
            prim = _primitive_for_method(redaction_method, redaction_params)

        if info_types and prim is not None:
            # Per-infoType transforms for this field (same method for all infoTypes)
            transformations = []
            for it in info_types:
                it_dict = _to_info_type_dict(it)
                transformations.append(
                    {
                        "info_types": [it_dict],
                        "primitive_transformation": prim,
                    }
                )
            ft["info_type_transformations"] = {"transformations": transformations}

        elif prim is not None:
            # Field-level primitive transformation (no per-infoType split)
            ft["primitive_transformation"] = prim

        else:
            # No explicit method; if infoTypes exist, default to FULL_REDACT
            if info_types:
                transformations = []
                for it in info_types:
                    it_dict = _to_info_type_dict(it)
                    transformations.append(
                        {
                            "info_types": [it_dict],
                            "primitive_transformation": {
                                "replace_with_info_type_config": {}
                            },
                        }
                    )
                ft["info_type_transformations"] = {"transformations": transformations}
            else:
                raise ValueError(
                    f"Mapping for column '{col}' must provide either 'info_type_rules' "
                    f"or ('infoTypes' and 'redaction_method')"
                )

        fts.append(ft)

    return fts

# -------------------------
# Schema helpers (mapped columns -> STRING)
# -------------------------
def parse_table_spec(table_spec: str):
    if ":" in table_spec and "." in table_spec and table_spec.find(":") < table_spec.find("."):
        proj, rest = table_spec.split(":", 1)
        ds, tbl = rest.split(".", 1)
        return proj, ds, tbl
    parts = table_spec.split(".")
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    if len(parts) == 2:
        return None, parts[0], parts[1]
    raise ValueError(f"Invalid table spec: {table_spec}")

def _adjust_schema_fields_for_redaction(schema_fields: List[bigquery.SchemaField], column_rules: Dict[str, Any]) -> List[Dict[str, Any]]:
    def clone(fields):
        out = []
        for f in fields:
            fd = {"name": f.name, "type": f.field_type, "mode": f.mode}
            if f.field_type.upper() == "RECORD" and getattr(f, "fields", None):
                fd["fields"] = clone(f.fields)
            out.append(fd)
        return out

    mutable = clone(schema_fields)

    def set_nested_to_string(fields_list, parts):
        name = parts[0]
        tgt = None
        for f in fields_list:
            if f["name"] == name:
                tgt = f
                break
        if tgt is None:
            return
        if len(parts) == 1:
            tgt["type"] = "STRING"
            tgt.pop("fields", None)
            return
        if tgt.get("type", "").upper() != "RECORD":
            tgt["type"] = "STRING"
            tgt.pop("fields", None)
            return
        set_nested_to_string(tgt.get("fields", []), parts[1:])

    for col in column_rules.keys():
        parts = col.split(".")
        set_nested_to_string(mutable, parts)
    return mutable

def dicts_to_schemafields(dict_fields: List[Dict[str, Any]]):
    out = []
    for fd in dict_fields:
        ftype = fd.get("type", "STRING").upper()
        mode = fd.get("mode", "NULLABLE")
        if ftype == "RECORD":
            subs = dicts_to_schemafields(fd.get("fields", []))
            out.append(bigquery.SchemaField(fd["name"], "RECORD", mode=mode, fields=subs))
        else:
            out.append(bigquery.SchemaField(fd["name"], ftype, mode=mode))
    return out

def ensure_bq_table_exists(submitter_project: str, src_table_spec: str, out_table_spec: str, column_rules: Dict[str, Any], max_wait_sec=30):
    client = bigquery.Client(project=submitter_project)

    src_proj, src_ds, src_table = parse_table_spec(src_table_spec)
    out_proj, out_ds, out_table = parse_table_spec(out_table_spec)
    src_proj = src_proj or submitter_project
    out_proj = out_proj or submitter_project
    src_id = f"{src_proj}.{src_ds}.{src_table}"
    out_id = f"{out_proj}.{out_ds}.{out_table}"

    try:
        client.get_table(out_id)
        logger.info("Target table exists: %s", out_id)
        return out_id
    except NotFound:
        logger.info("Target table will be created: %s", out_id)

    try:
        src_table_obj = client.get_table(src_id)
    except NotFound:
        raise RuntimeError(f"Source table not found: {src_id}")

    adjusted = _adjust_schema_fields_for_redaction(src_table_obj.schema, column_rules)

    ds_ref = bigquery.DatasetReference(out_proj, out_ds)
    try:
        client.get_dataset(ds_ref)
    except NotFound:
        logger.info("Dataset %s not found in project %s - creating", out_ds, out_proj)
        dataset = bigquery.Dataset(ds_ref)
        client.create_dataset(dataset)
        logger.info("Created dataset %s", ds_ref)

    schema_sf = dicts_to_schemafields(adjusted)
    table = bigquery.Table(out_id, schema=schema_sf)
    if getattr(src_table_obj, "time_partitioning", None):
        table.time_partitioning = src_table_obj.time_partitioning
    if getattr(src_table_obj, "clustering_fields", None):
        table.clustering_fields = src_table_obj.clustering_fields

    created = client.create_table(table)
    logger.info("Created BigQuery table %s (fields=%d)", out_id, len(created.schema))

    waited = 0
    while waited < max_wait_sec:
        try:
            client.get_table(out_id)
            logger.info("Verified table exists: %s", out_id)
            return out_id
        except NotFound:
            time.sleep(1)
            waited += 1
    raise RuntimeError(f"Unable to verify creation of table {out_id} after {max_wait_sec}s")

# -------------------------
# Beam transforms / DoFns
# -------------------------
def to_dlp_cell_value(val):
    if val is None:
        return ""
    if isinstance(val, (dict, list)):
        return json.dumps(val)
    return str(val)

class AddUuidAndDlpPayload(beam.DoFn):
    def process(self, row: Dict):
        row_uuid = str(uuid.uuid4())
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
    def __init__(self, project: str, debug_log: bool = True):
        self.project = project
        self.debug_log = debug_log

    def start_bundle(self):
        self.client = dlp_v2.DlpServiceClient()

    def process(self, batch: List[Dict[str, Any]]):
        if not batch:
            return

        headers = ["uuid"] + list(COLUMN_RULES.keys())
        logging.info(
            "DLP DoFn: sending batch of %d rows; headers=%s",
            len(batch),
            headers,
        )

        # ---- Build DLP Table (correct Value fields) ----
        table = {"headers": [{"name": h} for h in headers], "rows": []}
        for r in batch:
            vals = []
            for h in headers:
                v = r.get(h, "")
                vals.append({"string_value": "" if v is None else str(v)})
            table["rows"].append({"values": vals})

        # ---- Build inspect_config & deidentify_config ----
        inspect_cfg = build_inspect_config_from_mapping(
            COLUMN_RULES,
            include_quote=False,
            include_fields=headers[1:],  # ignored inside but kept for signature
        )
        field_transforms = build_field_transformations_from_mapping(COLUMN_RULES)

        deidentify_cfg: Dict[str, Any] = {}
        if field_transforms:
            deidentify_cfg["record_transformations"] = {
                "field_transformations": field_transforms
            }

        parent = f"projects/{self.project}"
        item = {"table": table}

        request = {
            "parent": parent,
            "inspect_config": inspect_cfg,
            "deidentify_config": deidentify_cfg,
            "item": item,
        }

        # ---- Log full DLP request ----
        if self.debug_log:
            try:
                logging.info(
                    "DLP REQUEST BEGIN\n%s\nDLP REQUEST END",
                    json.dumps(request, indent=2, sort_keys=True),
                )
            except Exception:
                logging.info("DLP REQUEST (repr) %s", repr(request))

        # ---- Call DLP ----
        resp = self.client.deidentify_content(request=request)

        # ---- Log full response ----
        if self.debug_log:
            try:
                from google.protobuf.json_format import MessageToDict

                resp_dict = MessageToDict(
                    resp._pb if hasattr(resp, "_pb") else resp,
                    preserving_proto_field_name=True,
                )
                logging.info(
                    "DLP RESPONSE BEGIN\n%s\nDLP RESPONSE END",
                    json.dumps(resp_dict, indent=2, sort_keys=True),
                )
            except Exception:
                logging.info("DLP RESPONSE (repr) %s", repr(resp))

        # ---- Parse returned table ----
        resp_table = getattr(getattr(resp, "item", None), "table", None)
        if resp_table is None:
            raise RuntimeError(f"DLP response missing table. Full response: {resp!r}")

        rows_out: List[List[str]] = []
        for r in resp_table.rows:
            row_vals: List[str] = []
            for v in r.values:
                sval = getattr(v, "string_value", "") or ""
                row_vals.append(sval)
            rows_out.append(row_vals)

        for rv in rows_out:
            mapping = dict(zip(headers, rv))
            uuidv = mapping.get("uuid")
            deid_map = {col: mapping.get(col) for col in COLUMN_RULES.keys()}
            yield (uuidv, deid_map)


class MergeOriginalAndDeid(beam.DoFn):
    def process(self, key_and_values):
        uuid_key, grouped = key_and_values
        originals = grouped.get("original", [])
        deids = grouped.get("deid", [])
        if not originals:
            raise RuntimeError(f"No original row found for uuid {uuid_key}")
        orig = originals[0]
        deid_map = deids[0] if deids else {}
        for col in COLUMN_RULES.keys():
            parts = col.split(".")
            parent = orig
            for p in parts[:-1]:
                if p not in parent or not isinstance(parent, dict):
                    raise RuntimeError(f"Missing nested path {'.'.join(parts[:-1])} in original row for uuid {uuid_key}")
                parent = parent[p]
            last = parts[-1]
            if col in deid_map:
                parent[last] = deid_map[col]
            else:
                raise RuntimeError(f"Deidentified value missing for column {col} for uuid {uuid_key}")
        yield orig

# -------------------------
# Main run() - strict staging-only config loading
# -------------------------
COLUMN_RULES: Dict[str, Any] = {}
tenant_config: Dict[str, Any] = {}

def run(argv=None):
    global COLUMN_RULES, tenant_config

    options = PipelineOptions(argv)
    gcloud_opts = options.view_as(GoogleCloudOptions)
    options.view_as(StandardOptions).runner = "DataflowRunner"
    p_opts = options.get_all_options()

    job_name = gcloud_opts.job_name or p_opts.get("job_name")
    staging = gcloud_opts.staging_location or p_opts.get("staging_location")
    temp_loc = gcloud_opts.temp_location or p_opts.get("temp_location")
    if not job_name:
        raise RuntimeError("Pipeline option 'job_name' is required")
    if not staging:
        raise RuntimeError("Pipeline option 'staging_location' is required")
    if not temp_loc:
        raise RuntimeError("Pipeline option 'temp_location' is required")
    if not gcloud_opts.project:
        raise RuntimeError("Pipeline option 'project' is required")

    # STRICT: load tenant_config only from staging_location/configs/<job_name>.json
    if not staging.startswith("gs://"):
        raise RuntimeError("staging_location must be a gs:// path and tenant_config must be uploaded there")

    s_no = staging[len("gs://"):]
    bucket_name, _, prefix = s_no.partition("/")
    prefix = prefix.rstrip("/")
    blob_path = f"{prefix}/configs/{job_name}.json" if prefix else f"configs/{job_name}.json"

    try:
        storage_client = storage.Client(project=gcloud_opts.project)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        if not blob.exists():
            raise RuntimeError(f"Missing tenant_config in staging. Expected gs://{bucket_name}/{blob_path}. Upload before running.")
        content = blob.download_as_text()
        tenant_config_loaded = json.loads(content)
        logger.info("Loaded tenant_config from staging: gs://%s/%s", bucket_name, blob_path)
    except Exception as e:
        raise RuntimeError(f"Failed to load tenant_config from staging gs://{bucket_name}/{blob_path}: {e}")

    tenant_config = tenant_config_loaded

    # Validate tenant_config
    if "mapping" not in tenant_config or not isinstance(tenant_config["mapping"], list) or len(tenant_config["mapping"]) == 0:
        raise RuntimeError("tenant_config.mapping must be a non-empty list")

    # Normalize COLUMN_RULES to dict: { "col": {"infoTypes": [...], "redaction_method": "...", "redaction_params": {...}} }
    COLUMN_RULES = {}
    for m in tenant_config["mapping"]:
        col = m.get("column")
        if not col:
            raise RuntimeError("Each mapping entry must include 'column'")
        
        # Use the mapping directly - preserve the structure
        COLUMN_RULES[col] = {
            "infoTypes": m.get("infoTypes") or [],
            "info_type_rules": m.get("info_type_rules") or [],
            "redaction_method": m.get("redaction_method"),
            "redaction_params": m.get("redaction_params") or {}
        }

    logger.info("Columns to scan: %s", list(COLUMN_RULES.keys()))

    submitter_project = gcloud_opts.project
    src_table = tenant_config.get("src_table")
    out_table = tenant_config.get("out_table")
    if not src_table or not out_table:
        raise RuntimeError("tenant_config must include src_table and out_table")

    canonical_out = ensure_bq_table_exists(submitter_project, src_table, out_table, COLUMN_RULES)
    logger.info("Canonical out table: %s", canonical_out)

    # Build pipeline
    with beam.Pipeline(options=options) as p:
        rows = p | "ReadBQ" >> beam.io.ReadFromBigQuery(table=src_table, method="DIRECT_READ")
        tagged = rows | "AddUUID" >> beam.ParDo(AddUuidAndDlpPayload()).with_outputs("original", "dlp_payload")
        originals = tagged.original
        dlp_payloads = tagged.dlp_payload

        keyed_originals = originals | "KeyOriginals" >> beam.Map(lambda t: (t[0], t[1]))

        dlp_results = (
            dlp_payloads
            | "BatchElements" >> beam.BatchElements(min_batch_size=10, max_batch_size=500)
            | "CallDLP" >> beam.ParDo(BatchToDlpAndDeidentify(submitter_project, debug_log=True))
        )

        keyed_deid = dlp_results | "KeyDeid" >> beam.Map(lambda t: (t[0], t[1]))

        merged = (
            {"original": keyed_originals, "deid": keyed_deid}
            | "CoGroup" >> beam.CoGroupByKey()
            | "Merge" >> beam.ParDo(MergeOriginalAndDeid())
        )

        temp_gcs = temp_loc.rstrip("/") + "/bqtemp"
        bq_client = bigquery.Client(project=submitter_project)
        out_table_obj = bq_client.get_table(canonical_out)
        schema = out_table_obj.schema
        schema_json = {"fields": [{"name": f.name, "type": f.field_type, "mode": f.mode} for f in schema]}

        from apache_beam.io.gcp.bigquery import WriteToBigQuery
        merged | "WriteBQ" >> WriteToBigQuery(
            table=canonical_out,
            schema=schema_json,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            method=WriteToBigQuery.Method.FILE_LOADS,
            custom_gcs_temp_location=temp_gcs,
        )

if __name__ == "__main__":
    run()