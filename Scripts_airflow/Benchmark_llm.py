import time
from LLM.llama_classifier import classify_logs_with_llama  # your own module
from LLM.prompts import build_classification_prompt
from uti.ES_helper import load_sample_logs  # or just load from a file

# Models to test (must be pulled in Ollama first)
models = ["tinyllama", "gemma:2b", "mistral", "llama3:8b"]

# Load a small sample of employee logs (1-2 entries)
sample_logs = load_sample_logs(employee_id="G50047910-5JjP5", limit=2)

# Build prompt
prompt = build_classification_prompt(sample_logs)

results = []

for model in models:
    print(f"\n=== Testing model: {model} ===")
    start_time = time.time()
    try:
        output = classify_logs_with_llama(prompt, model=model)
        duration = time.time() - start_time

        results.append({
            "model": model,
            "time_sec": round(duration, 2),
            "output": output[:500]  # trim for preview
        })

        print(f"⏱ Time: {duration:.2f}s")
        print("✅ Output sample:", output[:300], "...\n")

    except Exception as e:
        print(f"❌ Failed on model {model}: {e}")
