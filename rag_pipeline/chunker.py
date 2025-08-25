# rag_pipeline/chunker.py
import json
def chunk_text(text, chunk_size=500, overlap=100):
    """
    Split a long string into overlapping chunks.
    """
    chunks = []
    start = 0
    while start < len(text):
        end = start + chunk_size
        chunk = text[start:end]
        chunks.append(chunk.strip())
        start += chunk_size - overlap
    return chunks

def chunk_documents(docs, chunk_size=500, overlap=100):
    """
    Take parsed docs and return a list of chunk dicts.
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
                "filepath": filepath
            })

    return all_chunks

with open("html_docs.json", "r", encoding="utf-8") as f:
    docs = json.load(f)

chunked_docs = chunk_documents(docs)

with open("chunked_docs.json", "w", encoding="utf-8") as f:
    json.dump(chunked_docs, f, indent=2, ensure_ascii=False)

print(f"✅ Saved {len(chunked_docs)} chunks.")