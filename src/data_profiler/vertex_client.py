import os, re
from tenacity import retry, stop_after_attempt, wait_exponential

@retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=1, min=1, max=6))
def call_vertex_generate(prompt: str, project: str, location: str = 'us-central1', model: str = 'text-bison@001'):
    """Call Vertex AI text generation (Generative AI) and return string output.
    Uses google-cloud-aiplatform (vertexai) SDK if available.
    """
    try:
        # Use the modern vertexai SDK if available
        from vertexai.preview import language_models as lm_preview
        from vertexai import init
        init(project=project, location=location)
        model_obj = lm_preview.TextGenerationModel.from_pretrained(model)
        response = model_obj.predict(prompt, max_output_tokens=800, temperature=0.2)
        return getattr(response, 'text', str(response))
    except Exception as e:
        # fallback: return error string
        return f'VERTEX_ERROR: {e}'
