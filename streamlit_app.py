import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import json
import os
import vertexai
from vertexai.preview.generative_models import GenerativeModel

st.set_page_config(page_title="AI Data Profiler Dashboard", layout="wide")

st.title("🧠 AI + DLP Data Profiling Dashboard")
st.caption("Automated data discovery, classification, and profiling powered by Google Cloud DLP & Gemini AI")

BACKEND_URL = st.sidebar.text_input("Backend URL", "http://127.0.0.1:8080/profile")
gcs_path = st.sidebar.text_input("GCS path", "gs://sample_data_dataprofiling/customer_sample_global.csv")
sample_rows = st.sidebar.number_input("Sample rows", min_value=1, max_value=1000, value=100)
run_parallel = st.sidebar.checkbox("Run in parallel", value=True)
run_btn = st.sidebar.button("🚀 Run Profiling")

# Maintain session states
if "profiling_result" not in st.session_state:
    st.session_state["profiling_result"] = None
if "chat_history" not in st.session_state:
    st.session_state["chat_history"] = []

# Run profiling when button clicked
if run_btn:
    with st.spinner("Profiling data... please wait ⏳"):
        try:
            resp = requests.post(BACKEND_URL, data={"gcs_path": gcs_path, "sample_rows": sample_rows})
            if resp.status_code == 200:
                st.session_state["profiling_result"] = resp.json()
                st.success("✅ Profiling completed successfully!")
                st.rerun()  # Force refresh so chatbot appears instantly
            else:
                st.error(f"❌ Backend Error: {resp.text}")
        except Exception as e:
            st.error(f"Error connecting to backend: {e}")

result = st.session_state["profiling_result"]

# Helper: interpret stats into natural insights
def interpret_stats(stats):
    insights = []
    if not stats:
        return ["No statistics available."]
    if stats.get("null_pct", 0) == 0:
        insights.append("✅ No missing values")
    else:
        insights.append(f"⚠️ {round(stats['null_pct'] * 100, 2)}% missing values")
    if stats.get("distinct_pct") == 1:
        insights.append("🔢 All values unique")
    elif stats.get("distinct_pct") is not None:
        insights.append(f"🔢 {round(stats['distinct_pct'] * 100, 1)}% unique values")
    if "min_date" in stats and "max_date" in stats:
        insights.append(f"📅 Date range: {stats['min_date']} → {stats['max_date']}")
    if "mean" in stats:
        insights.append(f"📈 Average value: {round(stats['mean'], 2)}")
    return insights


# ======================================================
#  DASHBOARD: Summary + Visualizations + Column Analysis
# ======================================================
if result and "result_table" in result:
    st.markdown("## 📊 Dataset Summary")

    cols = st.columns(4)
    cols[0].metric("📁 Columns Profiled", result["columns_profiled"])
    cols[1].metric("🧾 Rows Profiled", result["rows_profiled"])
    cols[2].metric("⏱️ Execution Time (sec)", result["execution_time_sec"])
    cols[3].metric("🧠 Project", result.get("project", "unknown"))

    df_summary = []
    dlp_hits = []
    for col, data in result["result_table"].items():
        df_summary.append({
            "Column": col,
            "Type": data.get("inferred_dtype", "unknown"),
            "Classification": data.get("classification", "N/A"),
            "DLP_Detected": ", ".join(data.get("dlp_info_types", [])) or "None"
        })
        if data.get("dlp_info_types"):
            for info in data["dlp_info_types"]:
                dlp_hits.append(info)

    df_summary = pd.DataFrame(df_summary)

    # ───────────────────────────────────────────────
    # 🥧 Data Type Distribution (Fixed for Deprecation)
    # ───────────────────────────────────────────────
    st.markdown("### 🥧 Data Type Distribution")

    dtype_counts = {}
    for col, data in result["result_table"].items():
        dtype = data.get("inferred_dtype", "unknown")
        dtype_counts[dtype] = dtype_counts.get(dtype, 0) + 1

    if dtype_counts:
        dtype_df = pd.DataFrame(
            list(dtype_counts.items()), columns=["Data Type", "Count"]
        ).sort_values("Count", ascending=False)

        fig_dtype = px.pie(
            dtype_df,
            names="Data Type",
            values="Count",
            color="Data Type",
            color_discrete_sequence=px.colors.qualitative.Vivid,
            hole=0.3,
            title="Distribution of Detected Data Types",
        )

        fig_dtype.update_traces(textinfo="percent+label", pull=[0.03] * len(dtype_df))
        fig_dtype.update_layout(
            height=420,
            margin=dict(l=30, r=30, t=60, b=30),
            title_font=dict(size=16, family="Arial", color="#333"),
        )

        # ✅ Use modern config style (no warnings)
        st.plotly_chart(
            fig_dtype,
            config={
                "displayModeBar": False,
                "responsive": True,
            },
            width="stretch",
        )
    else:
        st.info("No data type information available for this dataset.")


    # ───────────────────────────────────────────────
    # 🔐 Sensitive Data Classification Overview (with Category Pie + Filtered Bar)
    # ───────────────────────────────────────────────
    st.markdown("### 🔐 Sensitive Data Classification Overview")

    if dlp_hits:
        # ✅ Normalize InfoType names (remove "(xN)" suffix)
        import re
        cleaned_hits = [re.sub(r"\(x?\d+\)", "", h, flags=re.IGNORECASE).strip() for h in dlp_hits]


        # ✅ Categorize InfoTypes into groups
        category_map = {
            "PERSON": "Personal",
            "EMAIL": "Personal",
            "PHONE": "Personal",
            "LOCATION": "Location",
            "ADDRESS": "Location",
            "CITY": "Location",
            "STATE": "Location",
            "COUNTRY": "Location",
            "DATE": "Personal",
            "GOVERNMENT": "Government",
            "TAX": "Financial",
            "BANK": "Financial",
            "ACCOUNT": "Financial",
            "IBAN": "Financial",
            "CREDIT": "Financial",
            "FINANCE": "Financial",
            "GENERIC": "General",
            "CUSTOM": "Custom",
        }

        def classify_category(info_type):
            for key, val in category_map.items():
                if key in info_type.upper():
                    return val
            return "Other"

        # ✅ Build DLP DataFrame
        dlp_df = pd.DataFrame({"InfoType": cleaned_hits})
        dlp_df["Category"] = dlp_df["InfoType"].apply(classify_category)

        # ✅ Frequency summary by Category
        category_summary = dlp_df["Category"].value_counts().reset_index()
        category_summary.columns = ["Category", "Count"]
        category_summary = category_summary.sort_values("Count", ascending=False)

        # ✅ Add Pie Chart for category distribution
        pie_fig = px.pie(
            category_summary,
            names="Category",
            values="Count",
            color="Category",
            color_discrete_sequence=px.colors.qualitative.Vivid,
            hole=0.4,
            title="Detected Sensitive Data Categories",
        )

        pie_fig.update_traces(textinfo="percent+label", pull=[0.05] * len(category_summary))
        pie_fig.update_layout(
            showlegend=True,
            height=400,
            title_font=dict(size=16, family="Arial", color="#333"),
            margin=dict(l=50, r=50, t=50, b=30),
        )

        st.plotly_chart(pie_fig, config={"displayModeBar": False}, use_container_width=True)

        # ✅ Aggregate InfoTypes with Category
        freq_df = dlp_df.groupby(["Category", "InfoType"]).size().reset_index(name="Count")

        # ✅ Add category selector for horizontal bar
        st.markdown("#### 📊 Explore InfoTypes by Category")
        selected_category = st.selectbox(
            "Select DLP InfoType Category:",
            sorted(dlp_df["Category"].unique()),
            index=0,
        )

        # ✅ Filter data by category
        filtered_df = freq_df[freq_df["Category"] == selected_category].sort_values("Count", ascending=True)
        filtered_df = filtered_df.head(10)

        # ✅ Create horizontal bar chart
        bar_fig = px.bar(
            filtered_df,
            y="InfoType",
            x="Count",
            orientation="h",
            color="Category",
            title=f"Top 10 Sensitive InfoTypes — {selected_category} Category",
            text="Count",
            color_discrete_sequence=px.colors.qualitative.Pastel,
            height=450,
        )

        bar_fig.update_traces(
            texttemplate="%{text}",
            textposition="outside",
            marker_line_width=0.8,
        )

        bar_fig.update_layout(
            xaxis_title="Detected Occurrences",
            yaxis_title="InfoType",
            yaxis=dict(autorange="reversed"),
            showlegend=False,
            title_font=dict(size=16, family="Arial", color="#333"),
            margin=dict(l=100, r=40, t=60, b=40),
            plot_bgcolor="rgba(0,0,0,0)",
        )

        st.plotly_chart(bar_fig, config={"displayModeBar": False}, use_container_width=True)


    st.markdown("### 🧩 Column Classification Summary")
    st.dataframe(df_summary, use_container_width=True, height=250)

    st.divider()
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
        insights = interpret_stats(col_data.get("stats", {}))
        for i in insights:
            st.markdown(f"- {i}")

        st.markdown("#### 🧩 Profiling Rules")
        rules = col_data.get("rules", [])
        if rules:
            for rule in rules:
                st.markdown(f"- {rule.get('rule')} (confidence: {rule.get('confidence')})")
        else:
            st.info("No profiling rules available.")

        st.markdown("#### 🔐 DLP Findings")
        dlp_types = col_data.get("dlp_info_types", [])
        if dlp_types:
            unique_dlp = list(set(dlp_types))
            st.success(", ".join(unique_dlp))
        else:
            st.info("No DLP findings for this column.")

        if col_data.get("dlp_matched_samples"):
            with st.expander("📋 DLP matched samples"):
                st.json(col_data["dlp_matched_samples"])

else:
    st.info("👈 Enter a GCS path and click **Run Profiling** to begin.")

# ======================================================
# 🤖 CHATBOT SECTION — Gemini 2.5 Flash Integration
# ======================================================
st.markdown("---")
st.subheader("💬 Ask Questions About Your Profiled Dataset")

if not st.session_state.get("profiling_result"):
    st.info("Please run profiling first before chatting.")
else:
    profiling_result = st.session_state["profiling_result"]
    project_id = profiling_result.get("project", os.getenv("GCP_PROJECT", "custom-plating-475002-j7"))

    with st.expander("🤖 Chat with Gemini 2.5 Flash", expanded=True):
        # Display chat history
        for role, msg in st.session_state.chat_history:
            if role == "user":
                st.chat_message("user").write(msg)
            else:
                st.chat_message("assistant").write(msg)

        user_input = st.text_input("Ask your question:", key="user_query_input")
        send_btn = st.button("Send", key="send_query_btn")

        if send_btn and user_input.strip():
            try:
                vertexai.init(project=project_id, location="us-central1")
                model = GenerativeModel("gemini-2.5-flash")

                # Create rich context prompt
                context = (
                    "You are an expert data profiling and compliance assistant. "
                    "Use the profiling JSON below to answer accurately about columns, data quality, "
                    "and sensitive information detected.\n\n"
                    f"Profiling Results:\n{json.dumps(profiling_result)[:15000]}"
                )
                prompt = f"{context}\n\nUser Question: {user_input}"

                response = model.generate_content(prompt)
                if response.candidates and response.candidates[0].content.parts:
                    answer = response.candidates[0].content.parts[0].text
                else:
                    answer = "⚠️ Gemini returned no answer."

                # Update session history
                st.session_state.chat_history.append(("user", user_input))
                st.session_state.chat_history.append(("assistant", answer))

                # Display conversation
                st.chat_message("user").write(user_input)
                st.chat_message("assistant").write(answer)

            except Exception as e:
                st.error(f"Error during chat: {str(e)}")
