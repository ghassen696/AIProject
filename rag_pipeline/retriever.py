"""
import chromadb
from sentence_transformers import SentenceTransformer
from .prompt_builder import build_prompt
import subprocess
import json

# Load embedding model
model = SentenceTransformer("all-MiniLM-L6-v2")

# Connect to Chroma
client = chromadb.PersistentClient(path="chroma_db")
collection = client.get_collection("html_chunks")

def query_llm(prompt: str) -> str:
    result = subprocess.run(
        ["ollama", "run", "gemma:2b"],
        input=prompt,
        capture_output=True,
        text=True
    )
    return result.stdout.strip()

def retrieve_and_answer(user_query: str, n_results=5) -> str:
    query_embedding = model.encode([user_query])

    results = collection.query(
        query_embeddings=query_embedding,
        n_results=n_results
    )
    context_chunks = results["documents"][0]

    # Build final prompt
    final_prompt = build_prompt(context_chunks, user_query)

    # Query Ollama LLM
    answer = query_llm(final_prompt)
    return answer
"""