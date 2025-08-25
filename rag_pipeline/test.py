import sys
import os
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from rag_pipeline.retriever import client, collection, model
from rag_pipeline.prompt_builder import build_prompt
import ollama

def ask_question(query: str):
    # Step 1: Embed the query
    query_embedding = model.encode([query])

    # Step 2: Retrieve top 5 relevant chunks from ChromaDB
    results = collection.query(query_embeddings=query_embedding, n_results=5)
    chunks = results["documents"][0]

    # Step 3: Print retrieved chunks for debugging
    print("🔹 Retrieved Chunks:")
    for i, chunk in enumerate(chunks):
        print(f"\n--- Chunk {i+1} ---\n{chunk}")

    # Step 4: Build the prompt using retrieved chunks + query
    prompt = build_prompt(chunks, query)

    # Step 5: Call Ollama LLM with prompt
    response = ollama.chat(
        model="gemma:2b",
        messages=[
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": prompt}
        ]
    )

    # Step 6: Print the LLM answer
    print("\n🤖 Answer:\n", response['message']['content'])

if __name__ == "__main__":
    # Test the function with your query
    ask_question("How to troubleshoot load balancer listener?")
