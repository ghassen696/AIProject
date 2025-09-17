import json
import re
import subprocess
import asyncio
from datetime import datetime
from typing import List, Dict, Any
import pandas as pd
from elasticsearch import Elasticsearch

# Local imports (adjust if needed depending on your package structure)
from .elastic_utils import connect_elasticsearch, sanitize_for_es
from .preprocessing import normalize_date

# Initialize spaCy for keyword extraction
import re

from typing import List

def extract_keywords(text: str, top_n: int = 10) -> List[str]:

    """
    Simple keyword extraction using regex and stopword filtering.
    """
    text = text.lower()
    words = re.findall(r'\w+', text)
    stopwords = {"the", "a", "an", "and", "or", "in", "on", "at", "to", "for", "of", "with"}
    keywords = [w for w in words if w not in stopwords]
    return keywords[:top_n]


# Connect to Elasticsearch
es = connect_elasticsearch()

# -----------------------------
# Task Input Functions
# -----------------------------
def read_tasks_from_excel(file_path: str) -> List[str]:
    """
    Read tasks from Excel file with 'task_description' column.
    """
    try:
        df = pd.read_excel(file_path)
        return df["task_description"].dropna().tolist()
    except Exception as e:
        print(f"⚠️ Error reading Excel: {e}")
        return []

def read_tasks_from_list(task_list: List[str]) -> List[str]:
    """
    Process a list of task strings.
    """
    return [t.strip() for t in task_list if t.strip()]

# -----------------------------
# Task Parsing
# -----------------------------
def parse_task(task_str: str, employee_id: str) -> Dict[str, Any]:
    """
    Parse task string into structured dict, adding employee_id.
    Format: "task description; ddl: date; priority: high/medium/low; notes: ..."
    """
    description_match = re.match(r"^(.*?);\s*ddl:\s*([^;]+);\s*priority:\s*([^;]+);\s*notes:\s*(.*)$", task_str)
    if not description_match:
        raise ValueError(f"Invalid task format: {task_str}")
    description = description_match.group(1).strip()
    ddl_str = description_match.group(2).strip()
    priority = description_match.group(3).strip().lower()
    notes = description_match.group(4).strip()

    try:
        ddl = datetime.strptime(ddl_str, "%m/%d/%Y")
    except ValueError:
        raise ValueError(f"Invalid DDL format: {ddl_str}. Use m/d/Y")

    return {
        "employee_id": employee_id,
        "description": description,
        "ddl": ddl.isoformat(),
        "priority": priority,
        "notes": notes,
        "assigned_at": datetime.utcnow().isoformat()
    }

# -----------------------------
# Keyword Extraction
# -----------------------------
# -----------------------------
# Fetch Employee Activities
# -----------------------------
def keyword_overlap(task_keywords, chunk_summary):
    chunk_words = set(re.findall(r'\w+', chunk_summary.lower()))
    overlap = len(set(task_keywords) & chunk_words)
    return overlap / max(len(task_keywords), 1)

import numpy as np

# Example embedding function (replace with your actual model call)
def embed_text(text: str) -> np.ndarray:
    """
    Generate a vector embedding for a given text.
    Replace this with your preferred embedding model.
    """
    # Placeholder: random vector (for testing)
    return np.random.rand(512)

def cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
    return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b)))

def fetch_relevant_chunks(
    es,
    employee_id: str,
    start_date: str,
    end_date: str,
    task_description: str,
    top_n: int = 10,
    min_keyword_overlap: float = 0.1
) -> List[Dict[str, Any]]:
    """
    Fetch relevant chunks using hybrid approach:
    1. Keyword pre-filtering in Elasticsearch
    2. Semantic re-ranking using embeddings
    """
    start = normalize_date(start_date).strftime("%Y-%m-%d")
    end = normalize_date(end_date).strftime("%Y-%m-%d")
    keywords = extract_keywords(task_description)
    
    # ---------- Step 1: Keyword Pre-Filtering ----------
    query = {
        "bool": {
            "must": [
                {"term": {"employee_id.keyword": employee_id}},
                {"range": {"date": {"gte": start, "lte": end}}},
                {"multi_match": {"query": " ".join(keywords),
                                 "fields": ["chunk_summaries.summary"],
                                 "fuzziness": "AUTO"}}
            ]
        }
    }
    
    try:
        resp = es.search(
            index="employee_daily_reports",
            query=query,
            sort=[{"_score": {"order": "desc"}}],
            size=50  # fetch more for semantic re-ranking
        )
        
        chunks = []
        for hit in resp["hits"]["hits"]:
            for summary in hit["_source"].get("chunk_summaries", []):
                if summary.get("summary") and summary.get("start_time") and summary.get("end_time"):
                    duration = (datetime.fromisoformat(summary["end_time"].replace("Z", "+00:00")) -
                                datetime.fromisoformat(summary["start_time"].replace("Z", "+00:00"))).total_seconds() / 60
                    if 0 <= duration <= 180:  # filter unrealistic durations
                        summary["duration"] = duration
                        # Keyword overlap
                        summary["keyword_overlap"] = keyword_overlap(keywords, summary["summary"])
                        chunks.append(summary)
        
        # Apply minimal keyword overlap filter
        chunks = [c for c in chunks if c["keyword_overlap"] >= min_keyword_overlap]
        if not chunks:
            return []

        # ---------- Step 2: Semantic Re-Ranking ----------
        task_vector = embed_text(task_description)
        for c in chunks:
            chunk_vector = embed_text(c["summary"])
            c["semantic_score"] = cosine_similarity(task_vector, chunk_vector)
            # Optional: combine with keyword overlap
            c["final_score"] = 0.5 * c["keyword_overlap"] + 0.5 * c["semantic_score"]

        # Sort by final score
        chunks = sorted(chunks, key=lambda x: x["final_score"], reverse=True)
        return chunks[:top_n]
    
    except Exception as e:
        print(f"⚠️ Error fetching chunks (hybrid): {e}")
        return []

# -----------------------------
# LLM Matching
# -----------------------------
# -----------------------------
# LLM Matching
# -----------------------------
def create_task_chunk_matching_prompt(employee_id: str, task: Dict[str, Any], chunk: Dict[str, Any]) -> str:
    """
    Build LLM prompt to match a single task to a single chunk summary and duration.
    """
    duration = chunk.get("duration", 0)
    prompt = f"""
You are an expert at analyzing employee activity log summaries to match against an assigned task. Return ONLY valid JSON, with no additional text, code, or explanations. Use the exact format below.

Task: {task["description"]}
Priority: {task["priority"]}
Notes: {task["notes"]}
Chunk Summary: {chunk.get('summary', '')}
Chunk Duration: {duration:.1f} minutes

Instructions:
You must ONLY use the content in the Chunk Summary to generate reasoning. Do NOT invent details from the task description or external knowledge. Reference specific words or phrases in the Chunk Summary that justify the match.
2. Assign a match_score (0.0–1.0) based on relevance (1.0 = perfect match, 0.0 = no match).
3. If match_score > 0.5, set time_spent_minutes to the chunk duration; otherwise, set to 0.0.
4. Provide brief reasoning (1-2 sentences, max 200 characters) referencing specific terms in the chunk summary.

Return JSON:
{{
  "match_score": float,
  "time_spent_minutes": float,
  "reasoning": str
}}

"""
    return prompt


async def call_ollama_async(prompt: str, chunk_duration: float, max_retries: int = 3) -> Dict[str, Any]:
    """
    Call Ollama asynchronously with retry mechanism, handling non-JSON responses.
    """
    for attempt in range(max_retries):
        try:
            process = await asyncio.create_subprocess_exec(
                "ollama", "run", "llama3.2:latest", prompt,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=120)
            output = stdout.decode().strip()
            if output.startswith("{"):
                return json.loads(output)
            print(f"⚠️ Non-JSON output on attempt {attempt + 1}: {output[:200]}...")
            with open("debug_non_json_output.txt", "a") as f:
                f.write(f"\n--- Non-JSON Output (Attempt {attempt + 1}) ---\n{output}\n")
            return parse_non_json_output(output, chunk_duration)
        except (asyncio.TimeoutError, json.JSONDecodeError, subprocess.SubprocessError) as e:
            print(f"⚠️ Error on attempt {attempt + 1}: {e}")
            if attempt == max_retries - 1:
                return {"match_score": 0.0, "time_spent_minutes": 0.0, "reasoning": f"Failed after {max_retries} attempts"}
    return {"match_score": 0.0, "time_spent_minutes": 0.0, "reasoning": "Unexpected error"}

def parse_non_json_output(output: str, chunk_duration: float) -> Dict[str, Any]:
    """
    Parse non-JSON output from Ollama to extract match_score, time_spent_minutes, and reasoning.
    """
    match_score = 0.0
    reasoning = "Failed to parse non-JSON output"
    time_spent_minutes = 0.0

    # Extract match_score
    score_match = re.search(r"[Mm]atch [Ss]core:?\s*(\d*\.?\d+)", output, re.IGNORECASE)
    if score_match:
        match_score = float(score_match.group(1))
        if match_score > 0.5:
            time_spent_minutes = chunk_duration

    # Extract reasoning
    reasoning_match = re.search(r"[Rr]easoning:?\s*(.+?)(?:\n\n|$)", output, re.DOTALL | re.IGNORECASE)
    if reasoning_match:
        reasoning = reasoning_match.group(1).strip()[:200]
    else:
        sentences = [s.strip() for s in output.split(".") if s.strip()]
        reasoning = sentences[0][:200] if sentences else reasoning

    return {
        "match_score": min(match_score, 1.0),
        "time_spent_minutes": time_spent_minutes,
        "reasoning": reasoning
    }

# -----------------------------
# Main Matching Function
# -----------------------------
async def match_tasks_to_activities(es, employee_id: str, start_date: str, end_date: str, task_strings: List[str]) -> Dict[str, Any]:
    """
    Match tasks to employee activities, using chunk summaries and durations with pre-filtering.
    """
    # Parse tasks
    tasks = []
    for task_str in task_strings:
        try:
            task = parse_task(task_str, employee_id)
            tasks.append(task)
        except ValueError as e:
            print(f"⚠️ Invalid task: {task_str}. Error: {e}")
            continue

    if not tasks:
        return {"productivity_score": 0.0, "message": "No valid tasks provided", "matches": []}

    # Match tasks to chunks
    matches = []
    min_match_threshold = 0.7
    priority_weights = {"high": 3, "medium": 2, "low": 1}
    expected_time_per_task = 60  # Assume each task requires 60 minutes

    for task in tasks:
        # Fetch relevant chunks using keyword filtering
        chunks = fetch_relevant_chunks(es, employee_id, start_date, end_date, task["description"])
        if not chunks:
            matches.append({
                "task_description": task["description"],
                "time_spent_minutes": 0.0,
                "match_score": 0.0,
                "reasoning": "No relevant chunks found."
            })
            continue

        # Match task to chunks
        task_matches = []
        for chunk in chunks:
            overlap_score = keyword_overlap(extract_keywords(task["description"]), chunk["summary"])
            if overlap_score < 0.3:  # skip irrelevant chunks
                continue
            prompt = create_task_chunk_matching_prompt(employee_id, task, chunk)
            result = await call_ollama_async(prompt, chunk["duration"])
            if result["match_score"] > 0.5:
                task_matches.append(result)
            with open("task_chunk_matches.log", "a") as f:
                f.write(f"Task: {task['description']}, Chunk: {chunk['chunk_index']}, Score: {result['match_score']}, Reasoning: {result['reasoning']}\n")

        if task_matches:
# Ensure each match uses the chunk duration
            for i, match in enumerate(task_matches):
                # Override time_spent_minutes with actual chunk duration
                chunk_duration = chunks[i].get("duration", 0)
                match["time_spent_minutes"] = chunk_duration

            # Compute weighted sum
            total_time_spent = round(sum(match["time_spent_minutes"] * match["match_score"] for match in task_matches), 2)
            best_match = max(task_matches, key=lambda x: x["match_score"])
            if best_match["match_score"] >= min_match_threshold:
                matches.append({
                    "task_description": task["description"],
                    "time_spent_minutes": total_time_spent,
                    "match_score": best_match["match_score"],
                    "reasoning": best_match["reasoning"]
                })
            else:
                matches.append({
                    "task_description": task["description"],
                    "time_spent_minutes": 0.0,
                    "match_score": 0.0,
                    "reasoning": "No chunk summary sufficiently matches the task."
                })
        else:
            matches.append({
                "task_description": task["description"],
                "time_spent_minutes": 0.0,
                "match_score": 0.0,
                "reasoning": "No matching chunks found."
            })

    # Calculate productivity score
    total_weight = sum(priority_weights.get(task["priority"], 1) for task in tasks)
    matched_tasks = sum(1 for match in matches if match["match_score"] >= min_match_threshold)
    completion_rate = matched_tasks / len(tasks) if tasks else 0.0
    time_efficiency = sum(min(match["time_spent_minutes"] / expected_time_per_task, 1.0)
                         for match in matches) / len(matches) if matches else 0.0
    productivity_score = (sum(match["match_score"] * priority_weights.get(task["priority"], 1)
                             for task, match in zip(tasks, matches)) / total_weight * completion_rate * time_efficiency) if total_weight > 0 else 0.0

    return {
        "matches": matches,
        "productivity_score": round(productivity_score, 2),
        "completion_rate": round(completion_rate, 2),
        "time_efficiency": round(time_efficiency, 2)
    }

# -----------------------------
# Example Usage
# -----------------------------
if __name__ == "__main__":
    async def main():
        employee_id = "G50047910-5JjP5"
        start_date = "2025-08-28"
        end_date = "2025-08-28"

        # Example with list input
        task_strings = [
            "start working on the ia project; ddl: 9/2/2025; priority: high; notes: Include sales metrics and trends",
            "Conduct Team Meeting; ddl: 9/2/2025; priority: medium; notes: Discuss project updates"
        ]
        result = await match_tasks_to_activities(es, employee_id, start_date, end_date, task_strings)
        print(json.dumps(result, indent=2))

        # Example with Excel input
        # excel_file = "tasks.xlsx"
        # task_strings = read_tasks_from_excel(excel_file)
        # result = await match_tasks_to_activities(es, employee_id, start_date, end_date, task_strings)
        # print(json.dumps(result, indent=2))

    asyncio.run(main())