import os, re, json
from tenacity import retry, stop_after_attempt, wait_exponential

USE_OPENAI = bool(os.getenv("OPENAI_API_KEY"))
LLM_MODEL = os.getenv("LLM_MODEL", "gpt-4o-mini")

if USE_OPENAI:
    from openai import OpenAI
    openai = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
else:
    # Vertex sdk (legacy genai SDK may be deprecated; keep a minimal wrapper)
    try:
        from vertexai import init
        from vertexai.preview.interfaces import TextGenerationModel  # adjust if SDK differs
        PROJECT_ID = os.getenv("GCP_PROJECT")
        LOCATION = os.getenv("LOCATION", "us-central1")
        init(project=PROJECT_ID, location=LOCATION)
        vertex_model = TextGenerationModel.from_pretrained("gemini-1.5")
    except Exception:
        vertex_model = None

@retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=6))
def _call_llm(prompt: str):
    if USE_OPENAI:
        resp = openai.chat.completions.create(
            model=LLM_MODEL,
            messages=[{"role":"system","content":"You are a data governance expert."},{"role":"user","content":prompt}],
            temperature=0.2,
            max_tokens=600
        )
        text = resp.choices[0].message.content.strip()
    else:
        if vertex_model is None:
            raise RuntimeError("No LLM available")
        response = vertex_model.predict(prompt, max_output_tokens=512)
        text = response.text.strip()
    text = re.sub(r"```(json)?", "", text).strip()
    try:
        return json.loads(text) if text.startswith("{") else {"raw_output": text}
    except Exception:
        return {"raw_output": text}


def call_llm_for_columns(samples_by_col: dict):
    """
    samples_by_col: {col: [value1, value2, ...]}
    returns dict: {col: parsed_json_or_raw}
    """
    prompts = {}
    for c, vals in samples_by_col.items():
        prompts[c] = (
            "You are an expert data quality analyst. "
            "Classify the following sample values, return JSON with keys: classification, profiling_rules (list of {rule,confidence}).\n"
            f"Sample values: {vals}\n"
            "Return pure JSON."
        )

    results = {}
    # Execute sequentially or in small concurrent pool to limit quota
    from concurrent.futures import ThreadPoolExecutor, as_completed
    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = {ex.submit(_call_llm, prompts[c]): c for c in prompts}
        for fut in as_completed(futures):
            col = futures[fut]
            try:
                results[col] = fut.result()
            except Exception as e:
                results[col] = {"error": str(e)}
    return results
