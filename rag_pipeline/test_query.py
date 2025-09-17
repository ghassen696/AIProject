import sys
import os
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
os.environ["TOKENIZERS_PARALLELISM"] = "false"

from rag_pipeline.retriever import retrieve_and_answer

question = "Hey?"
answer = retrieve_and_answer(question)

print("Q:", question)

print("A:", answer)