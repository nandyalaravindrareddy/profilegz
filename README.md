# profilegz
# 🧠 AI + DLP Data Profiler Dashboard (GCP + Gemini 2.5 Flash)

[![Python](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.18+-red.svg)](https://streamlit.io/)
[![Google Cloud](https://img.shields.io/badge/Google_Cloud-DLP_&_Vertex_AI-yellow.svg)](https://cloud.google.com)
[![Docker](https://img.shields.io/badge/Docker-ready-2496ED.svg)](https://www.docker.com/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

> 🚀 Intelligent Data Profiling, Sensitive Data Classification & AI-Powered Exploration
> Built with **FastAPI**, **Streamlit**, **Google Cloud DLP**, and **Gemini 2.5 Flash (Vertex AI)**
> **GitHub Repository:** [nandyalaravindrareddy/profilegz](https://github.com/nandyalaravindrareddy/profilegz.git)

---

## 🔖 Overview

This project provides an end-to-end **Data Profiling & Privacy Classification Dashboard** that reads data from **Google Cloud Storage (GCS)**, identifies **sensitive information** using **Cloud DLP**, generates **profiling summaries**, and enables users to **chat with their dataset** using **Vertex AI Gemini 2.5 Flash**.

It’s a production-ready, cloud-native solution designed for **data governance**, **privacy compliance**, and **analytics teams**.

---

## 🧬 Features

### 🔍 Automated Data Profiling

* Reads CSVs directly from GCS.
* Detects null %, unique %, min/max dates, and inferred data types.
* Generates intuitive rules such as:

  * “Must not be null”
  * “Should be unique”
  * “Average length ≈ N chars”
* Identifies duplicates and skewness patterns.

### 🛡️ Sensitive Data Detection (Google Cloud DLP)

* Uses 200+ built-in **DLP infoTypes** (global mode).
* Supports **custom detectors** (e.g., PAN, IFSC, SSN).
* Provides **confidence levels (LIKELY, VERY_LIKELY)**.
* Handles DLP API limits via intelligent batching.
* Returns both detected **infoTypes** and **confidence** for each column.

### 💬 Gemini 2.5 Flash Chatbot (Vertex AI)

* Ask natural questions like:

  * “Which fields contain PII?”
  * “What’s the most sensitive column?”
  * “Which columns are numeric or unique?”
* Context-aware with recent profiling results.
* Fully integrated inside Streamlit UI.

### 📊 Interactive Dashboard (Streamlit)

* **Dataset Summary Metrics**

  * Columns, Rows, Runtime, Project
* **Data Type Distribution Pie Chart**
* **Sensitive Data Classification Overview**

  * Horizontal bar chart for top DLP infoTypes
  * Dropdown filter by classification
* **Column Classification Summary**

  * Inferred type, Classification, DLP InfoType, Confidence
* **Column-Level Insights**

  * Profiling rules, stats, and business interpretations
* **Gemini Chatbot**

  * Ask your dataset anything

---

## 🧱 Architecture

```
             ┌─────────────────────────────────┐
             │        Streamlit UI         │
             │ (Dashboard + Chatbot Front) │
             └─────────────────────────────────┘
                          │
                          ▼
             ┌─────────────────────────────────┐
             │          FastAPI            │
             │ (Backend: main.py)          │
             └─────────────────────────────────┘
                          │
                          ▼
         ┌──────────────────────────────────────────────────┐
         │        Profiling Engine             │
         │ (profiler.py → stats & rules)       │
         ├──────────────────────────────────────────────────└┐
         │       DLP Client (dlp_client.py)    │
         │ → GCP DLP inspectContent + batching │
         ├──────────────────────────────────────────────────└┐
         │    Vertex Client (vertex_client.py) │
         │ → Gemini 2.5 Flash (LLM + Chatbot)  │
         └──────────────────────────────────────────────────┘
```

---

## ⚙️ Setup Instructions

### 1️⃣ Clone the repository

```bash
git clone https://github.com/nandyalaravindrareddy/profilegz.git
cd profilegz
```

### 2️⃣ Create a virtual environment

```bash
python3 -m venv venv
source venv/bin/activate
```

### 3️⃣ Install dependencies

```bash
pip install -r requirements.txt
```

### 4️⃣ Configure environment variables

Create a file named `.env` in the project root:

```bash
OPENAI_ORG_ID=<org-f6BDGM5EkA4OufIsBEUsVeKM>
OPENAI_PROJECT_ID=<proj_JQluUsBz5xIIwALlypD38IVU>
OPENAI_API_KEY=<OPENAI_API_KEY>
PINECONE_API_KEY=<PINECONE_API_KEY>
PINECONE_ENV=us-east-1-aws
PINECONE_INDEX_NAME=pdf-chat
PROJECT_ID=custom-plating-475002-j7
LOCATION=us-central1
VERTEX_MODEL=gemini-2.5-flash
GCP_PROJECT=custom-plating-475002-j7
GOOGLE_APPLICATION_CREDENTIALS=<service_account_key_path>
USE_LLM=true
USE_VERTEX=true
BACKEND_URL=http://127.0.0.1:8080
```

---

## ▶️ Run Locally

### Start the FastAPI backend:

```bash
python main.py
```

### Start the Streamlit dashboard:

```bash
streamlit run streamlit_app.py
```

Then open the URL:

```
http://localhost:8501
```

---

## 🐳 Run with Docker

### Build the image

```bash
docker build -t gcp-data-profiler .
```

### Run the container

```bash
docker run -p 8080:8080 -p 8501:8501 gcp-data-profiler
```

---

## 📊 Example Workflow

| Step | Action                                                                       | Output                                            |
| ---- | ---------------------------------------------------------------------------- | ------------------------------------------------- |
| 1️⃣  | Enter GCS path (`gs://sample_data_dataprofiling/customer_sample_global.csv`) | Reads data                                        |
| 2️⃣  | Click “🚀 Run Profiling”                                                     | Backend computes stats                            |
| 3️⃣  | View “Sensitive Data Overview”                                               | DLP detects `EMAIL_ADDRESS`, `PHONE_NUMBER`, etc. |
| 4️⃣  | Open “Chatbot” panel                                                         | Ask questions about results                       |

---

## 🧠 Example Chatbot Prompts

| Question                              | Expected Insight                                    |
| ------------------------------------- | --------------------------------------------------- |
| “Do I have any sensitive data?”       | Lists all DLP-detected columns                      |
| “What’s the most unique field?”       | Identifies columns with distinct_pct = 1            |
| “Which columns contain names or IDs?” | Uses DLP infoTypes like `PERSON_NAME`, `GENERIC_ID` |
| “Show me columns classified as EMAIL” | Filters and returns classification results          |

---

## 📂 Folder Structure

```
.
├── main.py                # FastAPI backend (DLP + profiling orchestrator)
├── streamlit_app.py       # Streamlit UI + Gemini Chatbot
├── requirements.txt       # Python dependencies
├── Dockerfile             # Container image definition
├── commands.txt           # Setup commands
├── src/
│   └── data_profiler/
│       ├── profiler.py    # Profiling logic & business rules
│       ├── dlp_client.py  # GCP DLP scanning integration
│       └── vertex_client.py # Gemini model integration
├── README.md
└── .env                   # Environment variables (ignored by Git)
```

---

## 🗮️ Data Visualization Highlights

| Visualization                   | Description                                     |
| ------------------------------- | ----------------------------------------------- |
| 🥧 **Data Type Distribution**   | Pie chart showing string/int/date proportion    |
| 📊 **Sensitive InfoType Chart** | Horizontal bar for top DLP infoTypes            |
| 📋 **Column Summary Table**     | Includes DLP confidence levels                  |
| 💬 **Gemini Chat Interface**    | Ask free-form questions about profiling results |

---

## 🧠 Technologies Used

| Category             | Tools                                    |
| -------------------- | ---------------------------------------- |
| **Frontend**         | Streamlit (interactive dashboard + chat) |
| **Backend**          | FastAPI                                  |
| **Cloud APIs**       | Google Cloud DLP, Vertex AI Gemini       |
| **Data**             | Google Cloud Storage (CSV datasets)      |
| **Visualization**    | Plotly Express                           |
| **AI Layer**         | Gemini 2.5 Flash                         |
| **Containerization** | Docker                                   |
| **Infra Ready For**  | Cloud Run / Vertex AI Workbench          |

---

## 🐿️ Future Enhancements

* Parallelized DLP calls using ThreadPoolExecutor
* Custom Gemini prompt templates for better rule generation
* Support for Parquet/Avro file formats
* Chat memory persistence using Pinecone
* Integration with BigQuery lineage tracking

---

## 🖼️ Sample Dashboard (Preview)

> 📸 Example: Profiling Output and Chatbot

![Dashboard Preview](docs/screenshot_dashboard.png)

---

## 💡 Summary

👍 Data Profiling + AI Assistant
🌐 GCP-Native: DLP + Vertex AI
🔄 Fully Interactive Streamlit Dashboard
🚀 Ready for Cloud Run or Local Demo

---

**Developed by:** [Ravi Nandyala](https://github.com/nandyalaravindrareddy)
📧 *Hackathon 2025 - Fintech Data Governance Track*

---

```text
© 2025 profilegz | MIT License
```
