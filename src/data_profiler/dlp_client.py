import json
from google.cloud import dlp_v2
import pandas as pd

def inspect_table_dlp(df: pd.DataFrame, project_id: str, location: str = "global"):
    """
    Dynamically fetches DLP infoTypes, limits to 140 max (DLP service limit = 150),
    merges with custom regex detectors for Indian banking & identity data.
    """
    print(f"🚀 Starting DLP inspection in region: {location}")
    dlp_client = dlp_v2.DlpServiceClient(client_options={"api_endpoint": "dlp.googleapis.com"})
    parent = f"projects/{project_id}/locations/{location}"

    # === STEP 1: Fetch available infoTypes ===
    try:
        info_types_response = dlp_client.list_info_types(request={"parent": f"locations/{location}"})
        all_info_types = [{"name": it.name} for it in info_types_response.info_types]
        print(f"✅ Found {len(all_info_types)} built-in infoTypes in region '{location}'.")
    except Exception as e:
        print(f"⚠️ Could not list infoTypes for region '{location}': {e}")
        all_info_types = []

    # === STEP 2: Keep only the top 140 to stay within API limits ===
    info_types = all_info_types[:140]
    print(f"⚙️ Using {len(info_types)} built-in detectors (limit = 150).")

    # === STEP 3: Add domain-specific custom detectors ===
    custom_info_types = [
        {"info_type": {"name": "IN_PAN"}, "regex": {"pattern": r"[A-Z]{5}[0-9]{4}[A-Z]{1}"}},
        {"info_type": {"name": "IN_AADHAAR"}, "regex": {"pattern": r"\b\d{4}\s\d{4}\s\d{4}\b"}},
        {"info_type": {"name": "IN_IFSC"}, "regex": {"pattern": r"[A-Z]{4}0[A-Z0-9]{6}"}},
        {"info_type": {"name": "IN_UPI"}, "regex": {"pattern": r"[a-zA-Z0-9.\-_]{2,256}@[a-zA-Z]{2,64}"}},
        {"info_type": {"name": "BANK_ACCOUNT"}, "regex": {"pattern": r"\b\d{9,18}\b"}}
    ]

    print(f"🧩 Total detectors in use: {len(info_types)} built-in + {len(custom_info_types)} custom")

    inspect_config = {
        "include_quote": True,
        "min_likelihood": dlp_v2.Likelihood.POSSIBLE,
        "limits": {"max_findings_per_item": 50},
        "info_types": info_types,
        "custom_info_types": custom_info_types,
    }

    summary = {col: {"info_types": [], "samples": []} for col in df.columns}

    # === STEP 4: Inspect column by column ===
    for col in df.columns:
        print(f"🧩 Inspecting column: {col}")
        col_values = df[col].dropna().astype(str).tolist()
        if not col_values:
            continue

        try:
            item = {"item": {"value": "\n".join(col_values[:200])}}
            response = dlp_client.inspect_content(
                request={"parent": parent, "inspect_config": inspect_config, **item}
            )
            findings = getattr(response.result, "findings", [])
            for f in findings:
                info_type = f.info_type.name if f.info_type else "UNKNOWN"
                summary[col]["info_types"].append(info_type)
                if f.quote:
                    summary[col]["samples"].append(f.quote)
        except Exception as e:
            print(f"❌ Error inspecting column {col}: {e}")

        summary[col]["info_types"] = list(set(summary[col]["info_types"]))
        summary[col]["samples"] = list(set(summary[col]["samples"]))

    print("✅ DLP inspection completed successfully.")
    return summary