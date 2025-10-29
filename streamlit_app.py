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
from datetime import datetime
import base64
from fpdf import FPDF

st.set_page_config(page_title="AI Data Profiler Dashboard", layout="wide")

st.title("🧠 AI-Powered Data Profiling Dashboard")
st.caption("Automated data discovery and classification powered by Google Cloud DLP & Gemini AI")

# Configuration with caching
@st.cache_data(ttl=3600)
def get_default_config():
    return {
        "backend_url": "http://127.0.0.1:8080/profile",
        "sample_gcs_path": "gs://sample_data_dataprofiling/customer_sample_global.csv",
        "default_sample_rows": 100
    }

config = get_default_config()

# PDF Generation Class
class DataProfilingPDF(FPDF):
    def header(self):
        self.set_font('Arial', 'B', 16)
        self.cell(0, 10, 'AI Data Profiling Report', 0, 1, 'C')
        self.set_font('Arial', 'I', 10)
        self.cell(0, 10, f'Generated on: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}', 0, 1, 'C')
        self.ln(5)
    
    def footer(self):
        self.set_y(-15)
        self.set_font('Arial', 'I', 8)
        self.cell(0, 10, f'Page {self.page_no()}', 0, 0, 'C')
    
    def chapter_title(self, title):
        self.set_font('Arial', 'B', 14)
        self.set_fill_color(200, 220, 255)
        self.cell(0, 10, title, 0, 1, 'L', 1)
        self.ln(4)
    
    def chapter_body(self, body):
        self.set_font('Arial', '', 10)
        clean_body = self.clean_text(body)
        self.multi_cell(0, 8, clean_body)
        self.ln()
    
    def clean_text(self, text):
        """Clean text to remove problematic Unicode characters"""
        if not isinstance(text, str):
            text = str(text)
        
        # Replace common problematic Unicode characters
        replacements = {
            '•': '-',
            '→': '->',
            '≈': '~',
            '✅': '[OK]',
            '⚠️': '[WARNING]',
            '🔢': '[NUMERIC]',
            '📅': '[DATE]',
            '📈': '[TREND]',
            '🧠': '[AI]',
            '📊': '[CHART]',
            '🔐': '[SECURE]',
            '🎯': '[TARGET]',
            '🛡️': '[SHIELD]',
            '📋': '[LIST]',
            '⚖️': '[BALANCE]',
            '📁': '[FOLDER]',
            '🧾': '[RECEIPT]',
            '⏱️': '[TIMER]',
            '🥧': '[PIE]',
            '🗂️': '[CATEGORY]',
            '🧩': '[PUZZLE]',
            '📘': '[BOOK]',
            '🔍': '[SEARCH]',
            '💼': '[BUSINESS]',
            '🤖': '[ROBOT]',
            '🏢': '[BUILDING]',
            '🔒': '[LOCK]',
            '🎉': '[CELEBRATE]',
            '❌': '[ERROR]',
            '⏰': '[CLOCK]',
            '🔌': '[PLUG]',
            '🔄': '[REFRESH]',
            '📄': '[DOCUMENT]',
            '🚀': '[ROCKET]',
            '💬': '[CHAT]',
            '🗑️': '[TRASH]',
            '💡': '[IDEA]',
            '💥': '[EXPLOSION]',
            '🟢': '[GREEN]',
            '🟡': '[YELLOW]',
            '🔴': '[RED]',
            '⚪': '[WHITE]'
        }
        
        for char, replacement in replacements.items():
            text = text.replace(char, replacement)
        
        # Remove any other non-ASCII characters
        text = text.encode('ascii', 'ignore').decode('ascii')
        return text
    
    def add_table(self, df, col_widths=None):
        self.set_font('Arial', 'B', 9)
        
        # Calculate column widths if not provided
        if col_widths is None:
            col_widths = [40] * len(df.columns)
            if sum(col_widths) > 190:
                col_widths = [190 / len(df.columns)] * len(df.columns)
        
        # Header
        for i, column in enumerate(df.columns):
            clean_column = self.clean_text(column)
            self.cell(col_widths[i], 10, clean_column, 1, 0, 'C')
        self.ln()
        
        # Data
        self.set_font('Arial', '', 8)
        for _, row in df.iterrows():
            for i, value in enumerate(row):
                display_value = self.clean_text(str(value))
                if len(display_value) > 30:
                    display_value = display_value[:27] + "..."
                self.cell(col_widths[i], 8, display_value, 1, 0, 'L')
            self.ln()
        self.ln(5)

def generate_pdf_report(profiling_result):
    """Generate comprehensive PDF report from profiling results"""
    pdf = DataProfilingPDF()
    pdf.add_page()
    
    # Cover Page
    pdf.set_font('Arial', 'B', 20)
    pdf.cell(0, 40, 'AI Data Profiling Report', 0, 1, 'C')
    pdf.ln(20)
    
    # Basic Information
    pdf.set_font('Arial', '', 12)
    pdf.cell(0, 8, f"Project: {profiling_result.get('project', 'Unknown')}", 0, 1)
    pdf.cell(0, 8, f"Rows Profiled: {profiling_result.get('rows_profiled', 0)}", 0, 1)
    pdf.cell(0, 8, f"Columns Profiled: {profiling_result.get('columns_profiled', 0)}", 0, 1)
    pdf.cell(0, 8, f"Execution Time: {profiling_result.get('execution_time_sec', 0)} seconds", 0, 1)
    pdf.ln(10)
    
    # Dataset Overview
    pdf.chapter_title("1. Dataset Overview")
    
    # Create summary dataframe
    result_table = profiling_result.get('result_table', {})
    summary_data = []
    
    for col, data in result_table.items():
        if col == "_dataset_insights":
            continue
        summary_data.append({
            "Column": col,
            "Data Type": data.get("inferred_dtype", "unknown"),
            "Classification": data.get("classification", "N/A"),
            "Sensitivity": data.get("data_sensitivity", "N/A"),
            "DLP Findings": len(data.get("dlp_info_types", [])),
            "Null %": f"{data.get('stats', {}).get('null_pct', 0)*100:.1f}%"
        })
    
    if summary_data:
        df_summary = pd.DataFrame(summary_data)
        pdf.add_table(df_summary, [35, 25, 40, 25, 25, 20])
    
    # Numeric Data Analysis
    pdf.add_page()
    pdf.chapter_title("2. Numeric Data Analysis")
    
    numeric_cols = []
    for col, data in result_table.items():
        if col == "_dataset_insights":
            continue
        if data.get("inferred_dtype") in ["int", "float"]:
            stats = data.get("stats", {})
            numeric_cols.append({
                "Column": col,
                "Min": f"{stats.get('min', 'N/A')}",
                "Max": f"{stats.get('max', 'N/A')}",
                "Mean": f"{stats.get('mean', 'N/A')}",
                "Median": f"{stats.get('median', 'N/A')}",
                "Std Dev": f"{stats.get('std', 'N/A')}"
            })
    
    if numeric_cols:
        df_numeric = pd.DataFrame(numeric_cols)
        pdf.add_table(df_numeric, [30, 15, 15, 15, 15, 15])
    else:
        pdf.chapter_body("No numeric columns detected.")
    
    # Sensitive Data Analysis
    pdf.add_page()
    pdf.chapter_title("3. Sensitive Data Analysis")
    
    sensitive_cols = []
    for col, data in result_table.items():
        if col == "_dataset_insights":
            continue
        if data.get("dlp_info_types"):
            sensitive_cols.append({
                "Column": col,
                "Info Types": ", ".join(data.get("dlp_info_types", [])),
                "Category": data.get("classification", "Unknown"),
                "Risk Level": data.get("data_sensitivity", "Unknown").title()
            })
    
    if sensitive_cols:
        df_sensitive = pd.DataFrame(sensitive_cols)
        pdf.add_table(df_sensitive, [35, 60, 40, 25])
    else:
        pdf.chapter_body("No sensitive data detected by DLP.")
    
    # Data Quality Assessment
    pdf.add_page()
    pdf.chapter_title("4. Data Quality Assessment")
    
    quality_issues = []
    for col, data in result_table.items():
        if col == "_dataset_insights":
            continue
        stats = data.get("stats", {})
        if stats.get("null_pct", 0) > 0.1:
            quality_issues.append({
                "Column": col,
                "Null %": f"{stats.get('null_pct', 0)*100:.1f}%",
                "Distinct %": f"{stats.get('distinct_pct', 0)*100:.1f}%",
                "Data Type": data.get("inferred_dtype", "unknown")
            })
    
    if quality_issues:
        df_quality = pd.DataFrame(quality_issues)
        pdf.add_table(df_quality, [40, 20, 20, 20])
    else:
        pdf.chapter_body("No significant data quality issues detected.")
    
    # AI Insights
    pdf.add_page()
    pdf.chapter_title("5. AI-Powered Insights")
    
    insights = result_table.get('_dataset_insights', {})
    if insights:
        pdf.set_font('Arial', 'B', 11)
        pdf.cell(0, 8, "Executive Summary:", 0, 1)
        pdf.set_font('Arial', '', 10)
        pdf.multi_cell(0, 8, insights.get('executive_summary', 'No summary available'))
        pdf.ln(5)
        
        if insights.get('key_risks'):
            pdf.set_font('Arial', 'B', 11)
            pdf.cell(0, 8, "Key Risks:", 0, 1)
            pdf.set_font('Arial', '', 10)
            for risk in insights.get('key_risks', []):
                pdf.cell(10, 8, "-", 0, 0)
                pdf.multi_cell(0, 8, risk)
            pdf.ln(5)
        
        if insights.get('recommended_actions'):
            pdf.set_font('Arial', 'B', 11)
            pdf.cell(0, 8, "Recommended Actions:", 0, 1)
            pdf.set_font('Arial', '', 10)
            for action in insights.get('recommended_actions', []):
                pdf.cell(10, 8, "-", 0, 0)
                pdf.multi_cell(0, 8, action)
    else:
        pdf.chapter_body("No AI insights available.")
    
    return pdf

def create_download_link(pdf_output, filename):
    """Create a download link for the PDF"""
    b64 = base64.b64encode(pdf_output).decode()
    href = f'<a href="data:application/octet-stream;base64,{b64}" download="{filename}" style="background-color: #4CAF50; color: white; padding: 10px 20px; text-align: center; text-decoration: none; display: inline-block; border-radius: 5px;">📥 Download PDF Report</a>'
    return href

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
    
    # PDF Export Section
    st.header("📄 Export Report")
    
    # Check if we have profiling results
    has_results = st.session_state.get('profiling_result') is not None
    
    if not has_results:
        st.info("Run profiling first to generate PDF report")
    else:
        if st.button("📊 Generate PDF Report", type="secondary", use_container_width=True):
            with st.spinner("Generating PDF report..."):
                try:
                    pdf = generate_pdf_report(st.session_state.profiling_result)
                    pdf_output = pdf.output(dest='S').encode('latin1', 'replace')
                    
                    # Create download link
                    st.markdown(create_download_link(
                        pdf_output, 
                        f"data_profiling_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
                    ), unsafe_allow_html=True)
                    st.success("PDF report generated successfully!")
                    
                except Exception as e:
                    st.error(f"PDF generation failed: {str(e)}")

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
    
    with st.spinner("🚀 Profiling data with GenAI... This may take a few moments"):
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
                st.success("✅ AI-Powered Profiling completed successfully!")
            else:
                st.error(f"❌ Backend Error ({resp.status_code}): {resp.text}")
                
        except requests.exceptions.Timeout:
            st.error("⏰ Request timeout - try with fewer sample rows")
        except requests.exceptions.ConnectionError:
            st.error("🔌 Cannot connect to backend - check if server is running")
        except Exception as e:
            st.error(f"❌ Error: {str(e)}")

result = st.session_state.profiling_result

# Enhanced helper functions with numeric statistics
@st.cache_data
def interpret_stats(stats):
    """Cached stats interpretation with enhanced numeric data display"""
    insights = []
    if not stats:
        return ["No statistics available."]
    
    # Null analysis
    if stats.get("null_pct", 0) == 0:
        insights.append("✅ No missing values")
    else:
        insights.append(f"⚠️ {stats['null_pct'] * 100:.1f}% missing values")
    
    # Distinct values
    if stats.get("distinct_pct") == 1:
        insights.append("🔢 All values unique")
    elif stats.get("distinct_pct") is not None:
        insights.append(f"🔢 {stats['distinct_pct'] * 100:.1f}% unique values")
    
    # Numeric statistics display
    if "min" in stats and "max" in stats:
        insights.append(f"📊 Range: {stats['min']:.2f} → {stats['max']:.2f}")
    if "mean" in stats:
        insights.append(f"📈 Average: {stats['mean']:.2f}")
    if "median" in stats:
        insights.append(f"📊 Median: {stats['median']:.2f}")
    if "std" in stats:
        insights.append(f"📐 Std Dev: {stats['std']:.2f}")
    
    # Additional numeric insights
    if "zeros_count" in stats and stats["zeros_count"] > 0:
        insights.append(f"🔘 Contains {stats['zeros_count']} zero values")
    if "negatives_count" in stats and stats["negatives_count"] > 0:
        insights.append(f"🔻 Contains {stats['negatives_count']} negative values")
    
    # Date insights
    if "min_date" in stats and "max_date" in stats:
        insights.append(f"📅 Date range: {stats['min_date']} → {stats['max_date']}")
    
    return insights

@st.cache_data
def create_summary_dataframe(result_table):
    """Efficient dataframe creation with simplified classification"""
    df_summary = []
    for col, data in result_table.items():
        if col == "_dataset_insights":
            continue
        
        # Use only the main classification
        classification = data.get("classification", "N/A")
        
        df_summary.append({
            "Column": col,
            "Type": data.get("inferred_dtype", "unknown"),
            "Classification": classification,
            "Sensitivity": data.get("data_sensitivity", "N/A").title(),
            "DLP Findings": ", ".join(data.get("dlp_info_types", [])) or "None",
            "Confidence": f"{data.get('overall_confidence', 0.5)*100:.1f}%"
        })
    return pd.DataFrame(df_summary)

# Main dashboard
if result and "result_table" in result:
    # PDF Export Button in Main Area
    st.markdown("---")
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        if st.button("📄 Export Full Report as PDF", type="primary", use_container_width=True):
            with st.spinner("Generating comprehensive PDF report..."):
                try:
                    pdf = generate_pdf_report(result)
                    pdf_output = pdf.output(dest='S').encode('latin1', 'replace')
                    
                    # Create download link
                    st.markdown(create_download_link(
                        pdf_output, 
                        f"data_profiling_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
                    ), unsafe_allow_html=True)
                    st.success("✅ PDF report generated successfully!")
                    st.balloons()
                    
                except Exception as e:
                    st.error(f"❌ PDF generation failed: {str(e)}")
    
    st.markdown("## 🧠 AI-Powered Dataset Summary")
    
    # Enhanced metrics with AI insights
    cols = st.columns(4)
    cols[0].metric("📁 Columns", result["columns_profiled"])
    cols[1].metric("🧾 Rows", result["rows_profiled"])
    cols[2].metric("⏱️ Time (sec)", result["execution_time_sec"])
    cols[3].metric("🤖 AI Model", "Gemini 1.5 Flash")
    
    # AI-Powered Executive Summary
    if "_dataset_insights" in result["result_table"]:
        insights = result["result_table"]["_dataset_insights"]
        
        st.markdown("### 💡 AI-Generated Insights")
        
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
                cols_insights[0].metric("📈 Data Quality", f"{insights['data_quality_score']*100:.0f}%")
            if "privacy_risk_level" in insights:
                risk_level = insights['privacy_risk_level']
                color = {"low": "🟢", "medium": "🟡", "high": "🔴"}.get(risk_level, "⚪")
                cols_insights[1].metric("🛡️ Privacy Risk", f"{color} {risk_level.title()}")
    
    # Create enhanced summary dataframe
    df_summary = create_summary_dataframe(result["result_table"])
    
    # Enhanced Data Type Distribution
    st.markdown("### 🥧 Data Type & Classification Distribution")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Data Type Distribution
        dtype_counts = {}
        for col, data in result["result_table"].items():
            if col == "_dataset_insights":
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
                title="Data Type Distribution"
            )
            
            fig_dtype.update_traces(textinfo="percent+label")
            fig_dtype.update_layout(
                height=400,
                margin=dict(l=30, r=30, t=50, b=30),
                showlegend=True
            )
            
            st.plotly_chart(fig_dtype, use_container_width=True)
        else:
            st.info("No data type information available")
    
    with col2:
        # AI Classification Distribution
        classification_counts = {}
        for col, data in result["result_table"].items():
            if col == "_dataset_insights":
                continue
            classification = data.get("classification", "Unknown")
            classification_counts[classification] = classification_counts.get(classification, 0) + 1
        
        if classification_counts:
            class_df = pd.DataFrame(
                list(classification_counts.items()), 
                columns=["Classification", "Count"]
            ).sort_values("Count", ascending=False).head(8)
            
            fig_class = px.pie(
                class_df,
                names="Classification",
                values="Count", 
                color="Classification",
                color_discrete_sequence=px.colors.qualitative.Pastel,
                hole=0.3,
                title="AI Classification Distribution"
            )
            
            fig_class.update_traces(textinfo="percent+label")
            fig_class.update_layout(
                height=400,
                margin=dict(l=30, r=30, t=50, b=30),
                showlegend=True
            )
            
            st.plotly_chart(fig_class, use_container_width=True)
        else:
            st.info("No AI classifications available")
    
    # Enhanced Classification Summary Table
    st.markdown("### 📊 Classification Summary")
    st.dataframe(df_summary, use_container_width=True, height=300)

    # Sensitive Data Classification Overview
    st.markdown("### 🔐 Sensitive Data Classification Overview")

    # Collect all DLP findings with their classifications
    dlp_findings = []
    for col, data in result["result_table"].items():
        if col == "_dataset_insights":
            continue
        classification = data.get("classification", "Unknown")
        info_types = data.get("dlp_info_types", [])
        
        for info_type in info_types:
            # Extract clean info type name (remove count)
            clean_type = re.sub(r"\s*\(x?\d+\)", "", info_type, flags=re.IGNORECASE).strip()
            if clean_type:
                dlp_findings.append({
                    "InfoType": clean_type,
                    "Classification": classification,
                    "Column": col
                })

    if dlp_findings:
        dlp_df = pd.DataFrame(dlp_findings)
        
        # Classification distribution
        classification_summary = dlp_df["Classification"].value_counts().reset_index()
        classification_summary.columns = ["Classification", "Count"]
        classification_summary = classification_summary.sort_values("Count", ascending=False)

        # Pie Chart for classification distribution
        if not classification_summary.empty:
            pie_fig = px.pie(
                classification_summary,
                names="Classification",
                values="Count",
                color="Classification",
                color_discrete_sequence=px.colors.qualitative.Vivid,
                hole=0.4,
                title="Detected Data Classifications",
            )

            pie_fig.update_traces(textinfo="percent+label", pull=[0.05] * len(classification_summary))
            pie_fig.update_layout(
                showlegend=True,
                height=400,
                title_font=dict(size=16, family="Arial", color="#333"),
                margin=dict(l=50, r=50, t=50, b=30),
            )

            st.plotly_chart(pie_fig, config={"displayModeBar": False}, use_container_width=True)

        # InfoType breakdown by classification
        st.markdown("#### 📊 Detailed InfoType Breakdown")
        
        # Create frequency table
        freq_df = dlp_df.groupby(["Classification", "InfoType"]).size().reset_index(name="Count")
        
        # Classification selector
        available_classifications = sorted(dlp_df["Classification"].unique())
        selected_classification = st.selectbox(
            "Select Classification to Explore:",
            available_classifications,
            index=0,
        )

        # Filter and display
        filtered_df = freq_df[freq_df["Classification"] == selected_classification].sort_values("Count", ascending=True)
        
        if not filtered_df.empty:
            bar_fig = px.bar(
                filtered_df,
                y="InfoType",
                x="Count",
                orientation="h",
                color="InfoType",
                title=f"InfoTypes in '{selected_classification}' Classification",
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
            st.info(f"No InfoTypes found in '{selected_classification}' classification")

        # Show columns with their classifications
        st.markdown("#### 🗂️ Column-Level Classification")
        classification_data = []
        for col, data in result["result_table"].items():
            if col == "_dataset_insights":
                continue
            if data.get("dlp_info_types"):
                classification_data.append({
                    "Column": col,
                    "Classification": data.get("classification", "Unknown"),
                    "InfoTypes": ", ".join(data.get("dlp_info_types", [])),
                    "Data Type": data.get("inferred_dtype", "unknown"),
                    "Sensitivity": data.get("data_sensitivity", "N/A").title()
                })
        
        if classification_data:
            classification_df = pd.DataFrame(classification_data)
            st.dataframe(classification_df, use_container_width=True)
    else:
        st.success("🎉 No sensitive data detected by DLP!")

    st.divider()
    st.subheader("🔍 Column-Level Analysis")

    # Filter out dataset insights from column selection
    col_names = [col for col in result["result_table"].keys() if col != "_dataset_insights"]
    
    # FIX: Only show column selection if there are columns
    if col_names:
        selected_col = st.selectbox("Select a column for detailed profiling", col_names)

        if selected_col:
            col_data = result["result_table"][selected_col]

            st.markdown(f"### 🔍 Column: `{selected_col}`")
            
            # SIMPLIFIED metrics
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Data Type", col_data.get("inferred_dtype", "unknown"))
            c2.metric("Classification", col_data.get("classification", "N/A"))
            c3.metric("Sensitivity", col_data.get("data_sensitivity", "N/A").title())
            c4.metric("Confidence", f"{col_data.get('overall_confidence', 0.5)*100:.1f}%")

            # Show DLP findings clearly
            dlp_types = col_data.get("dlp_info_types", [])
            if dlp_types:
                st.markdown("#### 🔐 DLP Findings")
                for dlp_type in dlp_types:
                    st.success(f"• {dlp_type}")

            # Enhanced numeric metrics for numeric columns
            if col_data.get("inferred_dtype") in ["int", "float"]:
                stats = col_data.get("stats", {})
                if "min" in stats and "max" in stats:
                    st.markdown("#### 📊 Numeric Statistics")
                    num_cols = st.columns(4)
                    num_cols[0].metric("Min", f"{stats['min']:.2f}")
                    num_cols[1].metric("Max", f"{stats['max']:.2f}")
                    num_cols[2].metric("Mean", f"{stats.get('mean', 0):.2f}")
                    num_cols[3].metric("Median", f"{stats.get('median', 0):.2f}")

            st.markdown("#### 💼 Business Insights")
            insights = interpret_stats(col_data.get("stats", {}))
            for i in insights:
                st.markdown(f"- {i}")

            # AI-Enhanced Quality Rules
            ai_rules = col_data.get("ai_enhanced_rules", [])
            if ai_rules:
                st.markdown("#### 🎯 AI-Generated Quality Rules")
                for rule in ai_rules[:5]:
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

            if col_data.get("dlp_samples"):
                with st.expander("📋 DLP matched samples"):
                    st.json(col_data["dlp_samples"])
    else:
        st.warning("No columns available for analysis")

else:
    st.info("👈 Enter a GCS path and click **Run Profiling** to begin AI-powered data analysis.")

# Chatbot section
st.markdown("---")
st.subheader("💬 Chat About Your Data")

if not st.session_state.profiling_result:
    st.info("👆 Please run profiling first before chatting.")
else:
    profiling_result = st.session_state.profiling_result
    
    with st.expander("🤖 Chat with Gemini AI", expanded=True):
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
            for role, msg in st.session_state.chat_history[-8:]:
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
            placeholder="E.g., What percentage of columns are numeric? Which columns have quality issues?",
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
                model = GenerativeModel("gemini-1.5-flash-001")
                
                # Build context from profiling results
                result_table = profiling_result.get('result_table', {})
                
                # Build dataset statistics
                total_columns = profiling_result.get('columns_profiled', 0)
                data_type_counts = {}
                sensitive_columns = []
                quality_issues = []
                
                for col, data in result_table.items():
                    if col == "_dataset_insights":
                        continue
                    
                    # Count data types
                    dtype = data.get("inferred_dtype", "unknown")
                    data_type_counts[dtype] = data_type_counts.get(dtype, 0) + 1
                    
                    # Track sensitive columns
                    if data.get("dlp_info_types"):
                        sensitive_columns.append(col)
                    
                    # Track quality issues
                    if data.get("stats", {}).get("null_pct", 0) > 0.1:
                        quality_issues.append(col)
                
                # Get AI insights
                ai_insights = result_table.get('_dataset_insights', {})
                
                # Build context
                context = f"""
                You are a data profiling expert analyzing this dataset.

                DATASET OVERVIEW:
                - Total Columns: {total_columns}
                - Total Rows: {profiling_result.get('rows_profiled', 0)}
                - Data Types: {data_type_counts}
                - Sensitive Columns: {len(sensitive_columns)}
                - Quality Issues: {len(quality_issues)} columns with >10% nulls

                AI INSIGHTS:
                - Executive Summary: {ai_insights.get('executive_summary', 'No summary available')}
                - Data Quality Score: {ai_insights.get('data_quality_score', 'Not available')}

                Question: {user_input}

                Provide a detailed answer based on the dataset profiling results:
                """
                
                with st.spinner("🔍 Analyzing your data..."):
                    response = model.generate_content(context)
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
                st.info("💡 Make sure Vertex AI is properly configured and you have the necessary permissions.")