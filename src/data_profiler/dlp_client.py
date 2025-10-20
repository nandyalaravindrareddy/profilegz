import math
from google.cloud import dlp_v2
from collections import defaultdict

INFO_TYPE_CACHE = {}

def safe_get_valid_info_types(dlp_client, region="global"):
    """Return list of infoTypes valid in this region (cached)."""
    if region in INFO_TYPE_CACHE:
        return INFO_TYPE_CACHE[region]

    parent = f"locations/{region}"
    try:
        response = dlp_client.list_info_types(parent=parent)
        valid_types = [t.name for t in response.info_types if hasattr(t, "name")]
        print(f"✅ Found {len(valid_types)} built-in infoTypes in region '{region}'.")
        INFO_TYPE_CACHE[region] = valid_types
        return valid_types
    except Exception as e:
        print(f"⚠️ Warning: Unable to list info types for region {region}: {e}")
        return []


def inspect_table_dlp(df,project_id):
    """
    Perform DLP inspection on a DataFrame and return findings per column.
    Auto-handles batching, invalid detectors, and region-specific retries.
    """
    print(f"🚀 Starting DLP inspection in region: global")
    dlp_client = dlp_v2.DlpServiceClient()

    # ✅ Get global + regional infoTypes
    global_types = safe_get_valid_info_types(dlp_client, "global")
    asia_types = safe_get_valid_info_types(dlp_client, "asia-south1")

    combined_info_types = list(dict.fromkeys(global_types + asia_types))
    print(f"🧩 Total detectors in use: {len(combined_info_types)} built-in + 5 custom")

    if len(combined_info_types) > 150:
        total_batches = math.ceil(len(combined_info_types) / 150)
        print(f"⚙️ Using {total_batches} batches to stay under the 150-infoType limit.")
    else:
        total_batches = 1

    summary = defaultdict(lambda: {"info_types": [], "samples": []})

    for col in df.columns:
        print(f"🧩 Inspecting column: {col}")
        text_data = "\n".join(df[col].dropna().astype(str).tolist())
        if not text_data.strip():
            print(f"⚠️ Skipping column '{col}' (empty or null).")
            continue

        for batch_idx in range(total_batches):
            start = batch_idx * 150
            end = start + 150
            batch_info_types = combined_info_types[start:end]

            inspect_config = {
                "include_quote": True,
                "min_likelihood": dlp_v2.Likelihood.POSSIBLE,
                "limits": {"max_findings_per_item": 50},
                "info_types": [{"name": it} for it in batch_info_types],
            }

            try:
                response = dlp_client.inspect_content(
                    request={
                        "parent": f"projects/{project_id}/locations/global",
                        "inspect_config": inspect_config,
                        "item": {"value": text_data},
                    }
                )

                findings = getattr(response.result, "findings", [])
                if findings:
                    for f in findings:
                        summary[col]["info_types"].append(f.info_type.name)
                        if f.quote:
                            summary[col]["samples"].append(f.quote)
                    print(f"   ✅ Batch {batch_idx+1}/{total_batches} → {len(findings)} findings so far...")

            except Exception as e:
                err_msg = str(e)
                if "Invalid built-in info type name" in err_msg:
                    print(f"⚠️ Retrying column '{col}' in asia-south1 region (regional fallback)...")
                    try:
                        regional_types = [it for it in batch_info_types if it.startswith("IN_")]
                        if not regional_types:
                            continue

                        inspect_config["info_types"] = [{"name": it} for it in regional_types]
                        response = dlp_client.inspect_content(
                            request={
                                "parent": f"projects/{project_id}/locations/asia-south1",
                                "inspect_config": inspect_config,
                                "item": {"value": text_data},
                            }
                        )

                        findings = getattr(response.result, "findings", [])
                        if findings:
                            for f in findings:
                                summary[col]["info_types"].append(f.info_type.name)
                                if f.quote:
                                    summary[col]["samples"].append(f.quote)
                            print(f"   ✅ Regional retry success → {len(findings)} regional findings.")
                    except Exception as inner:
                        print(f"❌ Regional retry failed for '{col}': {inner}")
                else:
                    print(f"❌ Error inspecting column {col}: {e}")

    print(f"✅ DLP inspection completed successfully.")
    return dict(summary)
