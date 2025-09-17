# retriever.py
from sentence_transformers import SentenceTransformer
from elasticsearch import Elasticsearch
from .prompt_builder import build_prompt
import subprocess

# ---------- Config ----------
ES_URL = "http://localhost:9200"
INDEX_NAME = "html_chunks"
EMBEDDING_MODEL = "sentence-transformers/all-mpnet-base-v2"
# ----------------------------

# Connect to Elasticsearch
es = Elasticsearch(ES_URL)

# Load embedding model
model = SentenceTransformer(EMBEDDING_MODEL)

def query_llm(prompt: str) -> str:
    result = subprocess.run(
        ["ollama", "run", "llama3.2:latest"],
        input=prompt,
        capture_output=True,
        text=True
    )
    return result.stdout.strip()

def retrieve_and_answer(user_query: str, n_results=5):
    # Encode the query into a vector
    query_vector = model.encode([user_query])[0].tolist()

    # Script_score search for cosine similarity
    response = es.search(
        index=INDEX_NAME,
        body={
            "size": n_results,
            "query": {
                "script_score": {
                    "query": {"match_all": {}},
                    "script": {
                        "source": "cosineSimilarity(params.query_vector, 'embedding') + 1.0",
                        "params": {"query_vector": query_vector}
                    }
                }
            },
            "_source": ["chunk_id", "title", "filepath", "content"]
        }
    )

    context_chunks = [hit["_source"]["content"] for hit in response["hits"]["hits"]]

    final_prompt = build_prompt(context_chunks, user_query)
    return query_llm(final_prompt)
