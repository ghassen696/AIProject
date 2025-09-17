from typing import List

def build_prompt(context_chunks: List[str], query: str) -> str:
    context = ""
    for i, chunk in enumerate(context_chunks, 1):
        context += f"[{i}] {chunk}\n\n"

    prompt = f"""
You are a helpful Huawei Cloud Assistant. Use only the context below to answer the question.
If the answer is not in the context, say "I don’t know".

Context:
{context}

Question: {query}
Answer (with references like [1], [2] if applicable):
"""
    return prompt.strip()
