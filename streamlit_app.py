import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import json
from collections import Counter
import vertexai
from vertexai.preview.generative_models import GenerativeModel
import os

# =====================
# 🎯 Page Configuration
# =====================
st.set_page_config(page_title="AI Data Profiler Dashboard", layout="wide")

st.title("🧠 AI + DLP Data Profiling Dashboard")
st.caption("Automated data discovery, classification, and profiling powered by Google Cloud DLP & Gemini AI")

PROJECT_ID = os.getenv('GCP_PROJECT', 'custom-plating-475002-j7')
VERTEX_LOCATION = os.getenv('VERTEX_LOCATION', 'us-central1')
VERTEX_MODEL = os.getenv('VERTEX_MODEL', 'gemini-2.5-flash')
BACKEND_URL =  os.getenv('BACKEND_URL', "http://127.0.0.1:8080/profile")

# Initialize Vertex AI
vertexai.init(project=PROJECT_ID, location=VERTEX_LOCATION)
chat_model = GenerativeModel("gemini-2.5-flash")

# Sidebar configuration
st.sidebar.header("⚙️ Configuration")
gcs_path = st.sidebar.text_input(
    "Enter GCS path (gs://...)", 
    "gs://sample_data_dataprofiling/customer_sample_global.csv"
)
sample_rows = st.sidebar.number_input("Sample rows", min_value=1, max_value=1000, value=100)
run_btn = st.sidebar.button("🚀 Run Profiling")

# Maintain session state
if "profiling_result" not in st.session_state:
    st.session_state["profiling_result"] = None
if "chat_history" not in st.session_state:
    st.session_state["chat_history"] = []

# --------------------------
# 📊 Helper Functions
# --------------------------
def interpret_stats(stats):
    insights = []
    if not stats:
        return ["No statistics available."]
    if stats.get("null_pct", 0) == 0:
        insights.append("✅ No missing values")
    else:
        insights.append(f"⚠️ {round(stats['null_pct'] * 100, 2)}% missing values")
    if stats.get("distinct_pct") == 1:
        insights.append("🔢 All values are unique")
    elif stats.get("distinct_pct") is not None:
        insights.append(f"🔢 {round(stats['distinct_pct'] * 100, 1)}% unique values")
    if "min_date" in stats and "max_date" in stats:
        insights.append(f"📅 Date range: {stats['min_date']} → {stats['max_date']}")
    if "mean" in stats:
        insights.append(f"📈 Average value: {round(stats['mean'], 2)}")
    return insights

# --------------------------
# 🚀 Trigger Profiling
# --------------------------
if run_btn:
    with st.spinner("Profiling data... please wait ⏳"):
        try:
            resp = requests.post(BACKEND_URL, data={"gcs_path": gcs_path, "sample_rows": sample_rows}, timeout=300)
            if resp.status_code == 200:
                st.session_state["profiling_result"] = resp.json()
                st.success("✅ Profiling completed successfully!")
            else:
                st.error(f"❌ Backend Error: {resp.text}")
        except Exception as e:
            st.error(f"Error connecting to backend: {e}")

result = st.session_state["profiling_result"]

# --------------------------
# 📈 Dashboard Section
# --------------------------
if result and "result_table" in result:
    st.markdown("## 📊 Dataset Summary")

    cols = st.columns(4)
    cols[0].metric("📁 Columns Profiled", result["columns_profiled"])
    cols[1].metric("🧾 Rows Profiled", result["rows_profiled"])
    cols[2].metric("⏱️ Execution Time (sec)", result["execution_time_sec"])
    cols[3].metric("🧠 Project", result.get("project", "unknown"))

    df_summary = []
    dlp_hits = []
    type_counts = Counter()

    for col, data in result["result_table"].items():
        dtype = data.get("inferred_dtype", "unknown")
        type_counts[dtype] += 1
        dlp_types = list(set(data.get("dlp_info_types", [])))
        classification = data.get("classification", "N/A")
        confidence = 0
        for rule in data.get("rules", []):
            if rule.get("rule", "").lower().startswith("must not be null"):
                confidence = max(confidence, rule.get("confidence", 0))
        df_summary.append({
            "Column": col,
            "Type": dtype,
            "Classification": classification,
            "DLP_Detected": ", ".join(dlp_types) or "None",
            "Confidence": confidence
        })
        if dlp_types:
            for info in dlp_types:
                dlp_hits.append(info)

    df_summary = pd.DataFrame(df_summary)

    # --- Data Type Distribution ---
    st.markdown("### 📈 Data Type Distribution")
    type_df = pd.DataFrame(type_counts.items(), columns=["DataType", "Count"])
    fig = px.pie(type_df, names="DataType", values="Count", title="Data Type Composition")
    fig.update_traces(textposition="inside", textinfo="percent+label")
    st.plotly_chart(fig, use_container_width=True)

    # --- Top 5 Sensitive Columns ---
    st.markdown("### 🔍 Top 5 Sensitive Columns (by DLP Confidence)")
    sensitive_df = df_summary[df_summary["DLP_Detected"] != "None"].nlargest(5, "Confidence")
    if not sensitive_df.empty:
        fig2 = px.bar(sensitive_df, x="Column", y="Confidence", color="DLP_Detected", text_auto=True)
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("✅ No sensitive columns detected by DLP.")

    # --- Summary Table ---
    st.markdown("### 🧩 Column Classification Summary")
    st.dataframe(df_summary, use_container_width=True, height=250)

    st.markdown("---")
    st.subheader("📘 Column-Level Analysis")

    col_names = list(result["result_table"].keys())
    selected_col = st.selectbox("Select a column for detailed profiling", col_names)

    if selected_col:
        col_data = result["result_table"][selected_col]

        st.markdown(f"### 🔍 Column: `{selected_col}`")
        c1, c2 = st.columns(2)
        c1.metric("📘 Data Type", col_data.get("inferred_dtype", "unknown"))
        c2.metric("🏷️ Classification", col_data.get("classification", "Not detected"))

        st.markdown("#### 💼 Business Insights")
        for i in interpret_stats(col_data.get("stats", {})):
            st.markdown(f"- {i}")

        st.markdown("#### 🧩 Profiling Rules")
        for rule in col_data.get("rules", []):
            st.markdown(f"- {rule.get('rule')} (Confidence: {rule.get('confidence')})")

        st.markdown("#### 🔐 DLP Findings")
        dlp_types = col_data.get("dlp_info_types", [])
        if dlp_types:
            st.success(", ".join(list(set(dlp_types))))
        else:
            st.info("No DLP findings for this column.")

        llm_output = col_data.get("llm_output", {})
        if isinstance(llm_output, dict) and llm_output.get("raw_output"):
            with st.expander("🤖 AI Interpretation"):
                st.markdown(llm_output["raw_output"].replace("\n", "  \n"))

else:
    st.info("👈 Enter a GCS path and click **Run Profiling** to begin.")

# --------------------------
# 💬 Chatbot Assistant
# --------------------------
st.markdown("---")
st.markdown("## 💬 AI Data Assistant")
st.caption("Ask questions about your profiled dataset — powered by Vertex AI Gemini.")

user_input = st.chat_input("Ask a question (e.g., 'Do I have sensitive data in my dataset?')")
if user_input:
    profiling_result = st.session_state.get("profiling_result", {})
    try:
        summary_context = json.dumps(profiling_result.get("result_table", {}))[:6000]
        prompt = f"""
        You are a helpful data assistant. Here is a summary of profiled dataset columns:
        {summary_context}

        Question: {user_input}
        Respond with short, clear insights in plain English.
        """

        response = chat_model.generate_content(prompt)
        ai_answer = response.text.strip() if hasattr(response, "text") else "Sorry, no response."

        st.session_state["chat_history"].append(("user", user_input))
        st.session_state["chat_history"].append(("assistant", ai_answer))

    except Exception as e:
        st.session_state["chat_history"].append(("assistant", f"Error: {e}"))

# Display chat history
for role, text in st.session_state["chat_history"]:
    with st.chat_message(role):
        st.markdown(text)
