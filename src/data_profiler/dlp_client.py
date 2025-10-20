from google.cloud import dlp_v2
from google.api_core import exceptions as gcp_exceptions
from collections import defaultdict
import math, time, traceback

# small curated high-value builtins (keeps request < 150)
HIGH_PRIORITY_INFO_TYPES = [
    "EMAIL_ADDRESS", "PHONE_NUMBER", "PERSON_NAME", "CREDIT_CARD_NUMBER", "US_SOCIAL_SECURITY_NUMBER",
    "IP_ADDRESS", "DATE", "IBAN_CODE", "BANK_ACCOUNT_NUMBER", "ACCOUNT_NUMBER",
    "USERNAME", "US_PASSPORT", "US_DRIVERS_LICENSE_NUMBER", "AGE", "GENDER",
    "IN_PAN", "IN_AADHAAR",  # note: some region issues; we'll handle errors gracefully
]

CUSTOM_INFO_TYPES = [
    # custom regex examples
    {
        "info_type": {"name": "CUSTOM_PAN_REGEX"},
        "regex": {"pattern": r"[A-Z]{5}\d{4}[A-Z]"}
    },
    {
        "info_type": {"name": "CUSTOM_TXN"},
        "regex": {"pattern": r"TXN\d{6,}"}
    },
]

def list_region_info_types(client, project_id, region="global"):
    # Try to list info types for a region. If fails, return None and rely on curated list.
    try:
        parent = f"projects/{project_id}/locations/{region}"
        response = client.list_info_types(request={"parent": parent})
        types = [it.name for it in response.info_types]
        return types
    except Exception as e:
        return None


def _dlp_table_from_col(col_name, series):
    headers = [{"name": col_name}]
    rows = []
    for v in series.fillna("").astype(str).tolist():
        rows.append({"values": [{"string_value": v}]})
    return {"table": {"headers": headers, "rows": rows}}


def inspect_table_dlp(project_id: str, df, region="global", max_info_types=140):
    """
    Inspect each dataframe column using DLP.
    - Use curated high-priority info types, plus a few custom regex detectors.
    - If the total info types exceed DLP limits, batch them.
    """
    print("🚀 Starting DLP inspection in region:", region)
    client = dlp_v2.DlpServiceClient()
    parent = f"projects/{project_id}"

    # Try list region info types (best-effort)
    try:
        available = list_region_info_types(client, project_id, region=region)
        if available and len(available) > 0:
            # choose intersection of HIGH_PRIORITY and available
            info_types = [it for it in HIGH_PRIORITY_INFO_TYPES if it in available]
            if len(info_types) < len(HIGH_PRIORITY_INFO_TYPES):
                # extend with some available types up to max_info_types
                for it in available:
                    if it not in info_types and len(info_types) < max_info_types:
                        info_types.append(it)
        else:
            # fallback to curated list (some may error but we'll catch)
            info_types = HIGH_PRIORITY_INFO_TYPES.copy()
    except Exception:
        info_types = HIGH_PRIORITY_INFO_TYPES.copy()

    # ensure under the allowed limit (DLP max ~150)
    if len(info_types) > max_info_types:
        info_types = info_types[:max_info_types]

    # prepare summary structure
    summary = {col: {"info_types": [], "samples": []} for col in df.columns}

    # Inspect column-by-column to map findings to column names (less error-prone)
    for col in df.columns:
        try:
            item = _dlp_table_from_col(col, df[col].dropna().astype(str))
            # If large number of info types, we can call inspect_content once (info_types limited) —
            # but DLP requires info_types list objects
            inspect_config = {
                "include_quote": True,
                "min_likelihood": dlp_v2.Likelihood.POSSIBLE,
                "limits": {"max_findings_per_item": 100},
                "info_types": [{"name": n} for n in info_types],
                "custom_info_types": CUSTOM_INFO_TYPES
            }
            response = client.inspect_content(request={"parent": parent, "inspect_config": inspect_config, "item": item})
            findings = getattr(response.result, "findings", []) or []
            for f in findings:
                it = f.info_type.name if f.info_type else "UNKNOWN"
                quote = f.quote if hasattr(f, "quote") else ""
                summary[col]["info_types"].append(it)
                if quote:
                    summary[col]["samples"].append(quote)
        except gcp_exceptions.GoogleAPICallError as e:
            # Handle specific API errors (invalid info_type names, region limitations, quota)
            print(f"❌ Error inspecting column {col}: {e}")
            traceback.print_exc()
            # fallback: attempt a small set of common detectors
            try:
                fallback_info = [{"name": "EMAIL_ADDRESS"}, {"name": "PHONE_NUMBER"}, {"name": "PERSON_NAME"}]
                inspect_config = {"include_quote": True, "info_types": fallback_info, "limits": {"max_findings_per_item": 50}}
                response = client.inspect_content(request={"parent": parent, "inspect_config": inspect_config, "item": item})
                findings = getattr(response.result, "findings", []) or []
                for f in findings:
                    it = f.info_type.name if f.info_type else "UNKNOWN"
                    quote = f.quote if hasattr(f, "quote") else ""
                    summary[col]["info_types"].append(it)
                    if quote:
                        summary[col]["samples"].append(quote)
            except Exception:
                continue
        except Exception as e:
            print(f"Unexpected DLP error for {col}: {e}")
            traceback.print_exc()
    print("✅ DLP inspection completed.")
    return summary
