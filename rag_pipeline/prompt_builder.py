# rag_pipeline/prompt_builder.py
def build_prompt(context_chunks: list[str], query: str) -> str:
    context = "\n\n".join(context_chunks)
    prompt = f"""
You are a helpful assistant. Use only the context below to answer the question.
If the answer is not in the context, say "I don’t know".

Context:
{context}

Question: {query}
Answer:
"""
    return prompt.strip()
