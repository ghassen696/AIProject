# rag_pipeline/chunker.py
import json
from transformers import AutoTokenizer

# ---------- Config ----------
EMBEDDING_MODEL = "sentence-transformers/all-mpnet-base-v2"  # must match embedder.py
CHUNK_SIZE = 512   # tokens per chunk
OVERLAP = 50       # tokens overlap
INPUT_JSON = "html_docs.json"
OUTPUT_JSON = "chunked_docs.json"
# ----------------------------

# Load tokenizer for token-based chunking
tokenizer = AutoTokenizer.from_pretrained(EMBEDDING_MODEL)

def chunk_text(text: str, chunk_size=CHUNK_SIZE, overlap=OVERLAP):
    """
    Split text into overlapping token chunks.
    """
    tokens = tokenizer.encode(text, add_special_tokens=False)
    chunks = []

    start = 0
    while start < len(tokens):
        end = start + chunk_size
        chunk_tokens = tokens[start:end]
        chunk_text = tokenizer.decode(chunk_tokens, skip_special_tokens=True).strip()
        chunks.append(chunk_text)
        start += chunk_size - overlap

    return chunks

def chunk_documents(docs, chunk_size=CHUNK_SIZE, overlap=OVERLAP):
    """
    Convert parsed docs into chunks with metadata.
    """
    all_chunks = []

    for doc in docs:
        content = doc["content"]
        title = doc.get("title", "No Title")
        filepath = doc["filepath"]

        chunks = chunk_text(content, chunk_size, overlap)

        for i, chunk in enumerate(chunks):
            all_chunks.append({
                "chunk_id": f"{filepath}#chunk_{i}",
                "content": chunk,
                "title": title,
                "filepath": filepath,
                "chunk_index": i,
                "total_chunks": len(chunks)
            })

    return all_chunks

if __name__ == "__main__":
    with open(INPUT_JSON, "r", encoding="utf-8") as f:
        docs = json.load(f)

    chunked_docs = chunk_documents(docs)

    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(chunked_docs, f, indent=2, ensure_ascii=False)

    print(f"✅ Saved {len(chunked_docs)} chunks (token-based).")
