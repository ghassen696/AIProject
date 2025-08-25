
# LLM_backend/ollama_client.py
import subprocess, json, re, tempfile, os

def _run_ollama(model: str, prompt: str) -> str:
    """
    Runs: ollama run <model> -p "<prompt>"
    Returns raw stdout (string).
    """
    # Use a temp file to avoid shell escaping issues for large prompts
    with tempfile.NamedTemporaryFile('w', delete=False) as f:
        f.write(prompt)
        path = f.name
    try:
        # NOTE: if your ollama requires JSON streaming, keep it simple and parse final text.
        result = subprocess.run(
            ["ollama", "run", model, "-f", path],
            capture_output=True, text=True, check=True
        )
        return result.stdout.strip()
    finally:
        try: os.remove(path)
        except: pass

def ask_json(model: str, prompt: str):
    """
    Ask Ollama and parse JSON from the reply.
    We extract the last JSON array/object found in the text.
    """
    raw = _run_ollama(model, prompt)

    # Try direct JSON
    try:
        return json.loads(raw)
    except Exception:
        pass

    # Fallback: extract the last JSON array/object from any text
    candidates = re.findall(r'(\{.*\}|\[.*\])', raw, flags=re.DOTALL)
    for text in reversed(candidates):
        try:
            return json.loads(text)
        except Exception:
            continue

    # Last resort
    return {"_raw": raw}
