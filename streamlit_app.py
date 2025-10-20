import streamlit as st
import requests
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="AI Data Profiler Dashboard", layout="wide")
st.title("🧠 AI + DLP Data Profiler")
st.caption("Google DLP + Gemini/OpenAI -> profiling rules, classifications, and visual dashboard")

BACKEND = st.sidebar.text_input("Backend URL", "http://127.0.0.1:8080/profile")
gcs_path = st.sidebar.text_input("GCS path", "gs://sample_data_dataprofiling/customer_sample_global.csv")
sample_rows = st.sidebar.number_input("Sample rows", min_value=1, max_value=5000, value=100)
parallel = st.sidebar.checkbox("Run in parallel", value=True)
run = st.sidebar.button("Run Profiling")

if "result" not in st.session_state:
    st.session_state["result"] = None

if run:
    with st.spinner("Running profiler..."):
        try:
            resp = requests.post(BACKEND, data={"gcs_path": gcs_path, "sample_rows": sample_rows, "parallel": str(parallel)})
            if resp.status_code == 200:
                st.session_state["result"] = resp.json()
                st.success("Profiling finished ✅")
            else:
                st.error(f"Backend error: {resp.text}")
        except Exception as e:
            st.error(f"Failed to call backend: {e}")

res = st.session_state["result"]
if res and "result_table" in res:
    rt = res["result_table"]
    # Summary metrics
    st.markdown("## Summary")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Columns", res["columns_profiled"])
    c2.metric("Rows", res["rows_profiled"])
    c3.metric("Exec time (s)", res["execution_time_sec"])
    c4.metric("Project", res.get("project", "unknown"))

    # Data type distribution
    dtype_count = {}
    for col, meta in rt.items():
        dtype = meta.get("inferred_dtype", "unknown")
        dtype_count[dtype] = dtype_count.get(dtype, 0) + 1
    df_dtype = pd.DataFrame(list(dtype_count.items()), columns=["dtype", "count"])
    fig = px.pie(df_dtype, names="dtype", values="count", title="Data Type Distribution")
    st.plotly_chart(fig, use_container_width=True)

    # Top sensitive columns by DLP + confidence
    sens_scores = []
    for col, meta in rt.items():
        dtypes = meta.get("dlp_info_types", [])
        score = meta.get("overall_confidence", 0.0)
        sens_scores.append({"col": col, "dlp_hits": len(dtypes), "confidence": score, "dlp_types": ", ".join(list(set(dtypes)))})
    sens_df = pd.DataFrame(sens_scores).sort_values(["dlp_hits", "confidence"], ascending=False)
    st.markdown("### 🔐 Top sensitive columns (by hits)")
    if not sens_df.empty:
        st.dataframe(sens_df.head(10), use_container_width=True)
    else:
        st.info("No sensitive columns detected by DLP")

    # Column details
    st.markdown("## Column Details")
    cols = list(rt.keys())
    sel = st.selectbox("Select column", cols)
    if sel:
        md = rt[sel]
        st.subheader(f"Column: {sel}")
        st.write("**Classification:**", md.get("classification") or "N/A")
        st.write("**Inferred Type:**", md.get("inferred_dtype", "N/A"))
        st.markdown("**Business Insights**")
        st.write(md.get("stats", {}))
        st.markdown("**Profiling Rules**")
        for r in md.get("rules", []):
            st.write("-", r.get("rule"), f"(confidence: {r.get('confidence', 'N/A')})")
            if r.get("examples"):
                st.write("  examples:", r.get("examples"))
        st.markdown("**DLP findings**")
        if md.get("dlp_info_types"):
            st.success(", ".join(md.get("dlp_info_types")))
        else:
            st.info("No DLP findings")
        if md.get("dlp_samples"):
            with st.expander("DLP matched samples"):
                st.write(md.get("dlp_samples"))
        if md.get("llm_output"):
            with st.expander("LLM output"):
                st.write(md.get("llm_output"))

else:
    st.info("Enter parameters and click Run Profiling")
