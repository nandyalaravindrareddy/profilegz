import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import json
import os
import vertexai
from vertexai.preview.generative_models import GenerativeModel
import asyncio
import time
from typing import Optional
import re

st.set_page_config(page_title="AI Data Profiler Dashboard", layout="wide")

st.title("🧠 AI + DLP Data Profiling Dashboard")
st.caption("Automated data discovery, classification, and profiling powered by Google Cloud DLP & Gemini AI")

# Configuration with caching
@st.cache_data(ttl=3600)
def get_default_config():
    return {
        "backend_url": "http://127.0.0.1:8080/profile",
        "sample_gcs_path": "gs://sample_data_dataprofiling/customer_sample_global.csv",
        "default_sample_rows": 100
    }

config = get_default_config()

# Sidebar with optimized layout
with st.sidebar:
    st.header("⚙️ Configuration")
    
    BACKEND_URL = st.text_input("Backend URL", config["backend_url"])
    gcs_path = st.text_input("GCS path", config["sample_gcs_path"])
    sample_rows = st.number_input("Sample rows", min_value=1, max_value=500, value=100)
    run_parallel = st.checkbox("Run in parallel", value=True)
    run_btn = st.button("🚀 Run Profiling", type="primary")
    
    # Quick actions
    st.header("🎯 Quick Actions")
    if st.button("🔄 Clear Cache"):
        st.cache_data.clear()
        st.success("Cache cleared!")

# Session state optimization
if "profiling_result" not in st.session_state:
    st.session_state.profiling_result = None
if "chat_history" not in st.session_state:
    st.session_state.chat_history = []
if "last_request" not in st.session_state:
    st.session_state.last_request = None

# Debounced profiling request
def should_process_request():
    """Prevent rapid repeated requests"""
    if st.session_state.last_request is None:
        return True
    return time.time() - st.session_state.last_request > 5

if run_btn and should_process_request():
    st.session_state.last_request = time.time()
    
    with st.spinner("🚀 Profiling data... This may take a few moments"):
        try:
            resp = requests.post(
                BACKEND_URL, 
                data={
                    "gcs_path": gcs_path, 
                    "sample_rows": sample_rows,
                    "parallel": str(run_parallel).lower()
                },
                timeout=120
            )
            
            if resp.status_code == 200:
                st.session_state.profiling_result = resp.json()
                st.success("✅ Profiling completed successfully!")
            else:
                st.error(f"❌ Backend Error ({resp.status_code}): {resp.text}")
                
        except requests.exceptions.Timeout:
            st.error("⏰ Request timeout - try with fewer sample rows")
        except requests.exceptions.ConnectionError:
            st.error("🔌 Cannot connect to backend - check if server is running")
        except Exception as e:
            st.error(f"❌ Error: {str(e)}")

result = st.session_state.profiling_result

# Optimized helper functions
@st.cache_data
def interpret_stats(stats):
    """Cached stats interpretation"""
    insights = []
    if not stats:
        return ["No statistics available."]
    
    if stats.get("null_pct", 0) == 0:
        insights.append("✅ No missing values")
    else:
        insights.append(f"⚠️ {stats['null_pct'] * 100:.1f}% missing values")
    
    if stats.get("distinct_pct") == 1:
        insights.append("🔢 All values unique")
    elif stats.get("distinct_pct") is not None:
        insights.append(f"🔢 {stats['distinct_pct'] * 100:.1f}% unique values")
    
    if "min_date" in stats and "max_date" in stats:
        insights.append(f"📅 Date range: {stats['min_date']} → {stats['max_date']}")
    
    if "mean" in stats:
        insights.append(f"📈 Average value: {stats['mean']:.2f}")
    
    return insights

@st.cache_data
def create_summary_dataframe(result_table):
    """Efficient dataframe creation"""
    df_summary = []
    for col, data in result_table.items():
        if col == "_dataset_insights":  # Skip dataset insights in main table
            continue
        df_summary.append({
            "Column": col,
            "Type": data.get("inferred_dtype", "unknown"),
            "Classification": data.get("classification", "N/A"),
            "Primary Category": data.get("primary_category", "N/A"),
            "Data Sensitivity": data.get("data_sensitivity", "N/A"),
            "DLP Findings": ", ".join(data.get("dlp_info_types", [])) or "None",
            "Confidence": f"{data.get('overall_confidence', 0.5)*100:.1f}%"
        })
    return pd.DataFrame(df_summary)

# Main dashboard
if result and "result_table" in result:
    st.markdown("## 📊 Dataset Summary")
    
    # Optimized metrics with caching
    cols = st.columns(4)
    cols[0].metric("📁 Columns", result["columns_profiled"])
    cols[1].metric("🧾 Rows", result["rows_profiled"])
    cols[2].metric("⏱️ Time (sec)", result["execution_time_sec"])
    cols[3].metric("🧠 Project", result.get("project", "unknown"))
    
    # NEW: AI-Powered Executive Summary
    if "_dataset_insights" in result["result_table"]:
        insights = result["result_table"]["_dataset_insights"]
        
        st.markdown("### 🧠 AI-Powered Insights")
        
        col1, col2 = st.columns(2)
        
        with col1:
            if "executive_summary" in insights:
                st.markdown("#### 📋 Executive Summary")
                st.info(insights["executive_summary"])
            
            if "key_risks" in insights:
                st.markdown("#### ⚠️ Key Risks")
                for risk in insights["key_risks"][:3]:
                    st.markdown(f"- {risk}")
        
        with col2:
            if "recommended_actions" in insights:
                st.markdown("#### 🎯 Recommended Actions")
                for action in insights["recommended_actions"][:3]:
                    st.markdown(f"- {action}")
            
            # Quality and risk scores
            cols_insights = st.columns(2)
            if "data_quality_score" in insights:
                cols_insights[0].metric("📈 Data Quality Score", f"{insights['data_quality_score']*100:.0f}%")
            if "privacy_risk_level" in insights:
                risk_level = insights['privacy_risk_level']
                color = {"low": "🟢", "medium": "🟡", "high": "🔴"}.get(risk_level, "⚪")
                cols_insights[1].metric("🛡️ Privacy Risk", f"{color} {risk_level.title()}")
    
    # Create summary dataframe
    df_summary = create_summary_dataframe(result["result_table"])
    
    # Data type distribution with error handling
    st.markdown("### 🥧 Data Type Distribution")
    
    dtype_counts = {}
    for col, data in result["result_table"].items():
        if col == "_dataset_insights":  # Skip dataset insights
            continue
        dtype = data.get("inferred_dtype", "unknown")
        dtype_counts[dtype] = dtype_counts.get(dtype, 0) + 1
    
    if dtype_counts:
        dtype_df = pd.DataFrame(
            list(dtype_counts.items()), 
            columns=["Data Type", "Count"]
        ).sort_values("Count", ascending=False)
        
        fig_dtype = px.pie(
            dtype_df,
            names="Data Type",
            values="Count",
            color="Data Type",
            color_discrete_sequence=px.colors.qualitative.Vivid,
            hole=0.3,
        )
        
        fig_dtype.update_traces(textinfo="percent+label")
        fig_dtype.update_layout(
            height=400,
            margin=dict(l=30, r=30, t=30, b=30),
            showlegend=True
        )
        
        st.plotly_chart(fig_dtype, use_container_width=True)
    else:
        st.info("No data type information available")
    
    # ───────────────────────────────────────────────
    # 🔐 Sensitive Data Classification Overview
    # ───────────────────────────────────────────────
    st.markdown("### 🔐 Sensitive Data Classification Overview")

    # Collect all DLP findings with their categories
    dlp_findings = []
    for col, data in result["result_table"].items():
        if col == "_dataset_insights":  # Skip dataset insights
            continue
        categories = data.get("categories", [])
        info_types = data.get("dlp_info_types", [])
        
        for info_type in info_types:
            # Extract clean info type name (remove count)
            clean_type = re.sub(r"\s*\(x?\d+\)", "", info_type, flags=re.IGNORECASE).strip()
            if clean_type and categories:
                primary_category = data.get("primary_category", "Unknown")
                dlp_findings.append({
                    "InfoType": clean_type,
                    "Category": primary_category,
                    "Column": col
                })

    if dlp_findings:
        dlp_df = pd.DataFrame(dlp_findings)
        
        # Category distribution
        category_summary = dlp_df["Category"].value_counts().reset_index()
        category_summary.columns = ["Category", "Count"]
        category_summary = category_summary.sort_values("Count", ascending=False)

        # Pie Chart for category distribution
        if not category_summary.empty:
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

        # InfoType breakdown by category
        st.markdown("#### 📊 Detailed InfoType Breakdown")
        
        # Create frequency table
        freq_df = dlp_df.groupby(["Category", "InfoType"]).size().reset_index(name="Count")
        
        # Category selector
        available_categories = sorted(dlp_df["Category"].unique())
        selected_category = st.selectbox(
            "Select Category to Explore:",
            available_categories,
            index=0,
        )

        # Filter and display
        filtered_df = freq_df[freq_df["Category"] == selected_category].sort_values("Count", ascending=True)
        
        if not filtered_df.empty:
            bar_fig = px.bar(
                filtered_df,
                y="InfoType",
                x="Count",
                orientation="h",
                color="InfoType",
                title=f"InfoTypes in '{selected_category}' Category",
                text="Count",
                color_discrete_sequence=px.colors.qualitative.Pastel,
                height=400,
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
        else:
            st.info(f"No InfoTypes found in '{selected_category}' category")

        # Show columns with their primary categories
        st.markdown("#### 🗂️ Column-Level Classification")
        category_data = []
        for col, data in result["result_table"].items():
            if col == "_dataset_insights":  # Skip dataset insights
                continue
            if data.get("dlp_info_types"):
                category_data.append({
                    "Column": col,
                    "Primary Category": data.get("primary_category", "Unknown"),
                    "InfoTypes": ", ".join(data.get("dlp_info_types", [])),
                    "Data Type": data.get("inferred_dtype", "unknown"),
                    "Sensitivity": data.get("data_sensitivity", "N/A")
                })
        
        if category_data:
            category_df = pd.DataFrame(category_data)
            st.dataframe(category_df, use_container_width=True)
    else:
        st.success("🎉 No sensitive data detected by DLP!")

    st.markdown("### 🧩 Column Classification Summary")
    st.dataframe(df_summary, use_container_width=True, height=250)

    st.divider()
    st.subheader("📘 Column-Level Analysis")

    # Filter out dataset insights from column selection
    col_names = [col for col in result["result_table"].keys() if col != "_dataset_insights"]
    selected_col = st.selectbox("Select a column for detailed profiling", col_names)

    if selected_col:
        col_data = result["result_table"][selected_col]

        st.markdown(f"### 🔍 Column: `{selected_col}`")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("📘 Data Type", col_data.get("inferred_dtype", "unknown"))
        c2.metric("🏷️ Classification", col_data.get("classification", "Not detected"))
        c3.metric("📂 Primary Category", col_data.get("primary_category", "N/A"))
        c4.metric("🔒 Sensitivity", col_data.get("data_sensitivity", "N/A").title())

        st.markdown("#### 💼 Business Insights")
        insights = interpret_stats(col_data.get("stats", {}))
        for i in insights:
            st.markdown(f"- {i}")

        # NEW: AI-Powered Analysis
        st.markdown("#### 🤖 AI-Powered Analysis")
        
        ai_classification = col_data.get("ai_classification", {})
        
        if ai_classification and not isinstance(ai_classification, str):
            col1, col2, col3 = st.columns(3)
            
            with col1:
                business_class = ai_classification.get("business_classification", "Not classified")
                st.metric("🏢 Business Role", business_class)
            
            with col2:
                sensitivity = ai_classification.get("data_sensitivity", "low").title()
                st.metric("🔒 Sensitivity", sensitivity)
            
            with col3:
                privacy_risk = ai_classification.get("privacy_risk", "low").title()
                st.metric("🛡️ Privacy Risk", privacy_risk)
            
            # Governance policies
            if "suggested_governance_policies" in ai_classification:
                with st.expander("📋 Suggested Governance Policies"):
                    for policy in ai_classification["suggested_governance_policies"]:
                        st.markdown(f"- {policy}")
            
            # Compliance considerations
            if "compliance_considerations" in ai_classification and ai_classification["compliance_considerations"] != ["none"]:
                with st.expander("⚖️ Compliance Considerations"):
                    for compliance in ai_classification["compliance_considerations"]:
                        st.markdown(f"- {compliance}")
        
        # AI-Enhanced Quality Rules
        ai_rules = col_data.get("ai_enhanced_rules", [])
        if ai_rules:
            st.markdown("#### 🎯 AI-Generated Quality Rules")
            for rule in ai_rules[:5]:  # Show top 5 rules
                confidence = rule.get('confidence', 0)
                rule_type = rule.get('type', 'general')
                confidence_color = "green" if confidence > 0.8 else "orange" if confidence > 0.6 else "red"
                
                st.markdown(
                    f"- **{rule['rule']}** "
                    f"<span style='color: {confidence_color}; font-weight: bold;'>({confidence*100:.0f}% confidence)</span> "
                    f"`{rule_type}`",
                    unsafe_allow_html=True
                )

        st.markdown("#### 🧩 Profiling Rules")
        rules = col_data.get("rules", [])
        if rules:
            for rule in rules:
                confidence = rule.get('confidence', 0.5)
                st.markdown(f"- {rule.get('rule')} (confidence: {confidence:.1%})")
        else:
            st.info("No profiling rules available.")

        st.markdown("#### 🔐 DLP Findings")
        dlp_types = col_data.get("dlp_info_types", [])
        if dlp_types:
            unique_dlp = list(set(dlp_types))
            st.success(", ".join(unique_dlp))
        else:
            st.info("No DLP findings for this column.")

        if col_data.get("dlp_samples"):
            with st.expander("📋 DLP matched samples"):
                st.json(col_data["dlp_samples"])

else:
    st.info("👈 Enter a GCS path and click **Run Profiling** to begin.")

# Optimized chatbot section
st.markdown("---")
st.subheader("💬 Chat About Your Data")

if not st.session_state.profiling_result:
    st.info("👆 Please run profiling first before chatting.")
else:
    profiling_result = st.session_state.profiling_result
    
    with st.expander("🤖 Chat with Gemini 2.5 Flash", expanded=True):
        # Chat management buttons
        col1, col2, col3 = st.columns([2, 1, 1])
        with col2:
            if st.button("🗑️ Clear Chat", key="clear_chat", use_container_width=True):
                st.session_state.chat_history = []
                st.rerun()
        with col3:
            if st.button("🔄 Reset All", key="reset_all", use_container_width=True):
                st.session_state.chat_history = []
                st.session_state.profiling_result = None
                st.session_state.current_dataset = None
                st.rerun()
        
        # Display chat history
        if st.session_state.chat_history:
            st.markdown("**💭 Conversation History:**")
            for role, msg in st.session_state.chat_history[-8:]:  # Show last 8 messages
                if role == "user":
                    st.chat_message("user").write(f"**You:** {msg}")
                else:
                    st.chat_message("assistant").write(f"**AI:** {msg}")
            st.markdown("---")
        else:
            st.info("💡 Ask questions about your profiled data. Example: 'Which columns contain sensitive information?'")
        
        # Chat input
        user_input = st.text_area(
            "Your question:", 
            key="chat_input",
            placeholder="E.g., What sensitive data was found? Which columns have quality issues?",
            height=80
        )
        
        col1, col2 = st.columns([4, 1])
        with col2:
            send_btn = st.button("🚀 Send", key="send_chat", use_container_width=True)
        
        if send_btn and user_input.strip():
            try:
                # Initialize Vertex AI
                project_id = profiling_result.get("project", os.getenv("GCP_PROJECT"))
                vertexai.init(project=project_id, location="us-central1")
                model = GenerativeModel("gemini-2.5-flash")
                
                # Enhanced context from current profiling results
                sensitive_columns = [
                    col for col, data in profiling_result.get('result_table', {}).items() 
                    if col != "_dataset_insights" and data.get('dlp_info_types')
                ]
                
                data_quality_issues = [
                    col for col, data in profiling_result.get('result_table', {}).items()
                    if col != "_dataset_insights" and data.get('stats', {}).get('null_pct', 0) > 0.1
                ]
                
                # Get AI insights if available
                ai_insights = profiling_result.get('result_table', {}).get('_dataset_insights', {})
                
                context = f"""
                You are a data profiling expert analyzing this dataset:
                
                - Project: {profiling_result.get('project', 'Unknown')}
                - Dataset: {profiling_result.get('rows_profiled', 0)} rows, {profiling_result.get('columns_profiled', 0)} columns
                - Sensitive columns found: {len(sensitive_columns)} ({', '.join(sensitive_columns[:5])}{'...' if len(sensitive_columns) > 5 else ''})
                - Data quality issues: {len(data_quality_issues)} columns with >10% nulls
                - Execution time: {profiling_result.get('execution_time_sec', 0)} seconds
                - AI Insights: {ai_insights.get('executive_summary', 'No AI insights available')}
                - Key Risks: {', '.join(ai_insights.get('key_risks', ['No risks identified']))}
                - Recommended Actions: {', '.join(ai_insights.get('recommended_actions', ['No specific recommendations']))}
                
                Provide specific, actionable insights based on the actual profiling results and AI analysis.
                """
                
                prompt = f"{context}\n\nQuestion: {user_input}"
                
                with st.spinner("🔍 Analyzing your data..."):
                    response = model.generate_content(prompt)
                    if response.candidates and response.candidates[0].content.parts:
                        answer = response.candidates[0].content.parts[0].text
                    else:
                        answer = "I couldn't generate a response. Please try rephrasing your question."
                
                # Update chat history
                st.session_state.chat_history.append(("user", user_input))
                st.session_state.chat_history.append(("assistant", answer))
                
                # Clear input and refresh
                st.rerun()
                
            except Exception as e:
                st.error(f"💥 Chat error: {str(e)}")