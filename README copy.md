# AI Data Profiling Assistant (GCP demo)


## Overview
This demo profiles CSV datasets stored in GCS using Google DLP for PII detection and Vertex AI for enrichment (summary & rules). A Streamlit UI lets judges interactively explore results.

## Quickstart (local)
1. Set up service account and ADC:
   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS="/path/to/dataprofiling-saaccount.json"
   export GCP_PROJECT="custom-plating-475002-j7"
   export VERTEX_LOCATION="us-central1"
   export VERTEX_MODEL="text-bison@001"
   ```
2. Enable APIs:
   ```bash
   gcloud services enable aiplatform.googleapis.com dlp.googleapis.com storage.googleapis.com
   ```
3. Install and run locally:
   ```bash
   python -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   uvicorn main:app --reload --port 8080
   streamlit run streamlit_app.py
   ```
4. In Streamlit: set backend to http://127.0.0.1:8080/profile and GCS path to `gs://sample_data_dataprofiling/customer_sample.csv` then Run Profiling.

## Deploy to Cloud Run (suggested)
1. Build and push container to Google Container Registry or Artifact Registry.
2. Deploy Cloud Run service with the service account `dataprofiling-saaccount@custom-plating-475002-j7.iam.gserviceaccount.com` and allow unauthenticated if desired for demo.

## Notes
- Ensure the service account has roles: roles/dlp.user, roles/storage.objectViewer, roles/aiplatform.user (or broad roles/editor for POC).
- Vertex model availability depends on project access; you can fall back to local heuristics if Vertex/Model isn't available.
