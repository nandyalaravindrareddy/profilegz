import math, time, re
import pandas as pd, numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter
from .dlp_client import inspect_table_dlp
from .vertex_client import call_llm_for_columns  # wrapper that supports Vertex or OpenAI
import numbers, datetime

# Local deterministic helpers
def shannon_entropy(values):
    from collections import Counter
    cnt = Counter(values)
    total = sum(cnt.values()) or 1
    p = [v/total for v in cnt.values()]
    return -sum(x*math.log2(x) for x in p if x>0)

def infer_dtype_and_stats(series: pd.Series):
    n = len(series)
    non_null = series.dropna().astype(str)
    null_pct = round(1 - (len(non_null)/n) if n else 1.0, 3)
    stats = {"null_pct": null_pct}
    dtype = "string"

    # numeric detection
    num = pd.to_numeric(series.dropna(), errors="coerce")
    if len(num) and num.notna().mean() > 0.6:
        dtype = "int" if (num.dropna() % 1 == 0).all() else "float"
        stats.update({"min": float(num.min()), "max": float(num.max()), "mean": float(num.mean()), "std": float(num.std())})
    else:
        # date detection (sample up to 50 values)
        try:
            sample = non_null.sample(min(50, len(non_null))) if len(non_null) else non_null
            dt = pd.to_datetime(sample, errors="coerce")
            if dt.notna().mean() > 0.6:
                dtype = "date"
                full = pd.to_datetime(non_null, errors="coerce")
                stats.update({"min_date": str(full.min()), "max_date": str(full.max())})
            elif set(non_null.str.lower()) <= {"true","false","0","1","yes","no"}:
                dtype = "boolean"
            else:
                dtype = "string"
        except Exception:
            dtype = "string"

    stats["entropy"] = round(shannon_entropy(non_null.tolist()), 3)
    stats["distinct_pct"] = round((series.dropna().nunique() / len(series.dropna())) if len(series.dropna()) else 0, 3)

    # cardinality bucket
    if stats["distinct_pct"] == 1:
        stats["cardinality"] = "high"
    elif stats["distinct_pct"] >= 0.2:
        stats["cardinality"] = "medium"
    else:
        stats["cardinality"] = "low"

    return {"dtype": dtype, "stats": stats}


def local_rules(series: pd.Series):
    import pandas as pd
    non_null = series.dropna()
    rules = []
    n = len(series)
    # completeness
    completeness = round(1 - ((n - len(non_null)) / n) if n else 0, 3)
    rules.append({"rule": "Must not be null", "confidence": completeness})

    # uniqueness / duplicates
    unique_ratio = round((len(non_null.unique()) / len(non_null)) if len(non_null) else 0, 3)
    rules.append({"rule": "Should be unique", "confidence": unique_ratio})

    # duplicates sample
    dup_counts = non_null.value_counts()
    dups = dup_counts[dup_counts > 1].head(5).to_dict()
    if dups:
        rules.append({"rule": "Contains duplicates (top examples)", "confidence": 0.9, "examples": dups})

    # numeric specifics
    if pd.api.types.is_numeric_dtype(non_null):
        rules.append({"rule": "Numeric range validation", "confidence": 0.95})
        if (non_null < 0).any():
            rules.append({"rule": "Contains negative values", "confidence": 0.8})
    else:
        # string heuristics
        avg_len = int(non_null.astype(str).map(len).mean()) if len(non_null) else 0
        rules.append({"rule": f"Average length ≈ {avg_len} chars", "confidence": 0.8})
        if any("@" in str(x) for x in non_null):
            rules.append({"rule": "Contains email-like patterns", "confidence": 0.98})
        # PAN like pattern (India) heuristic
        if non_null.astype(str).str.match(r"^[A-Z]{5}\d{4}[A-Z]$").mean() > 0.5:
            rules.append({"rule": "Matches PAN format (India)", "confidence": 0.9})

    # date check
    try:
        sample = non_null.sample(min(20, len(non_null)))
        dt = pd.to_datetime(sample, errors="coerce")
        if dt.notna().mean() > 0.5:
            full = pd.to_datetime(non_null, errors="coerce")
            rules.append({"rule": f"Date range {full.min().date()} -> {full.max().date()}", "confidence": 0.95})
    except Exception:
        pass

    return rules


def run_profiling(df: pd.DataFrame, project_id: str, parallel: bool = True, max_workers: int = 8):
    """
    Main orchestrator:
     - run lightweight local profiling in parallel (columns)
     - run DLP (batched, with region/global fallback)
     - optionally call LLM for textual enrichment
    """
    start = time.time()
    columns = list(df.columns)
    results = {}

    # parallel column profiling
    if parallel:
        workers = min(max_workers, max(1, len(columns)))
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = {ex.submit(infer_dtype_and_stats, df[c]): c for c in columns}
            for fut in as_completed(futures):
                col = futures[fut]
                try:
                    prof = fut.result()
                except Exception as e:
                    prof = {"dtype": "string", "stats": {"null_pct": 1.0}}
                    print("Profiling error for", col, e)
                rules = local_rules(df[col])
                results[col] = {
                    "inferred_dtype": prof["dtype"],
                    "stats": prof["stats"],
                    "rules": rules,
                    "classification": None
                }
    else:
        for c in columns:
            prof = infer_dtype_and_stats(df[c])
            results[c] = {
                "inferred_dtype": prof["dtype"],
                "stats": prof["stats"],
                "rules": local_rules(df[c]),
                "classification": None
            }

    # DLP inspection (single shot but batched internally to respect infoType limits)
    try:
        dlp_summary = inspect_table_dlp(project_id, df)
    except Exception as e:
        print("DLP failed:", e)
        dlp_summary = {c: {"info_types": [], "samples": []} for c in columns}

    # Merge DLP results (give DLP priority)
    for col, meta in dlp_summary.items():
        if col not in results:
            # safety: if DLP returned a column not present, skip
            continue
        results[col]["dlp_info_types"] = meta.get("info_types", [])
        results[col]["dlp_samples"] = meta.get("samples", [])
        # choose top DLP label if any
        if meta.get("info_types"):
            top = Counter(meta["info_types"]).most_common(1)[0][0]
            results[col]["classification"] = top

    # LLM enrichment for ambiguous or string columns (small sample) — parallel
    text_cols = [c for c, v in results.items() if v["inferred_dtype"] == "string" and not results[c].get("classification")]
    if text_cols and len(df) <= 1000:  # avoid huge LLM cost / latency
        samples = {c: df[c].dropna().astype(str).head(20).tolist() for c in text_cols}
        try:
            llm_outputs = call_llm_for_columns(samples)  # returns dict col->parsed json
            for c, out in llm_outputs.items():
                results[c]["llm_output"] = out
                if isinstance(out, dict) and out.get("classification"):
                    results[c]["classification"] = out["classification"]
                # append LLM rules (if any)
                for rule in out.get("profiling_rules", []) if isinstance(out, dict) else []:
                    results[c]["rules"].append({**rule, "source": "llm"})
        except Exception as e:
            print("LLM enrichment skipped:", e)

    # compute overall_confidence metric (avg rule confidence)
    for c, meta in results.items():
        confs = [r.get("confidence", 0.5) for r in meta.get("rules", []) if isinstance(r, dict)]
        meta["overall_confidence"] = round(float(sum(confs)/len(confs)) if confs else 0.5, 3)

    total = round(time.time() - start, 2)
    print(f"Profiling finished in {total}s")
    return results
