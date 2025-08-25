import json
import chromadb
from sentence_transformers import SentenceTransformer
from tqdm import tqdm

# ---------- Config ----------
CHROMA_DB_PATH = "chroma_db"
COLLECTION_NAME = "html_chunks"
CHUNKS_JSON_PATH = "chunked_docs.json"
BATCH_SIZE = 5000
# ----------------------------

# Load JSON chunks
with open(CHUNKS_JSON_PATH, "r", encoding="utf-8") as f:
    docs = json.load(f)

# Prepare texts, IDs, metadata
texts = [doc["content"] for doc in docs]
ids = [doc["chunk_id"] for doc in docs]
metadatas = [{"title": doc["title"], "filepath": doc["filepath"]} for doc in docs]

# Initialize Chroma
client = chromadb.PersistentClient(path=CHROMA_DB_PATH)
try:
    collection = client.get_collection(name=COLLECTION_NAME)
    print(f"✅ Collection '{COLLECTION_NAME}' loaded.")
except:
    collection = client.create_collection(name=COLLECTION_NAME)
    print(f"🆕 Collection '{COLLECTION_NAME}' created.")

# Get existing chunk IDs (to skip duplicates)
existing_ids = set()
try:
    existing_docs = collection.get(ids=None, limit=1_000_000)
    existing_ids = set(existing_docs["ids"])
    print(f"📦 Found {len(existing_ids)} existing chunks.")
except Exception as e:
    print("⚠️ Could not load existing IDs (collection may be empty).")

# Filter new chunks
new_texts, new_ids, new_metadatas = [], [], []
for text, id_, meta in zip(texts, ids, metadatas):
    if id_ not in existing_ids:
        new_texts.append(text)
        new_ids.append(id_)
        new_metadatas.append(meta)

print(f"🔍 {len(new_ids)} new chunks to embed (skipped {len(texts) - len(new_ids)} existing).")

# Load embedding model
model = SentenceTransformer("all-MiniLM-L6-v2")

# Batch insert new embeddings
for i in tqdm(range(0, len(new_texts), BATCH_SIZE), desc="🔢 Embedding Batches"):
    batch_texts = new_texts[i:i+BATCH_SIZE]
    batch_ids = new_ids[i:i+BATCH_SIZE]
    batch_metadatas = new_metadatas[i:i+BATCH_SIZE]

    embeddings = model.encode(batch_texts, show_progress_bar=False, batch_size=32)

    collection.add(
        documents=batch_texts,
        embeddings=embeddings,
        metadatas=batch_metadatas,
        ids=batch_ids
    )

print("✅ All new chunks embedded and stored in ChromaDB.")
