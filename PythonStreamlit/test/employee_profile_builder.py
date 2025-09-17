# LLM/task_matcher.py
"""
Employee Profile & Task Matching System
---------------------------------------
Features:
  ✅ Embeds daily reports and maintains employee profiles in Elasticsearch
  ✅ Recomputes profile embeddings with recency weighting
  ✅ Matches new tasks to employees (semantic similarity + productivity + feedback + workload)
  ✅ Provides LLM explanations for top candidates
  ✅ Stores admin feedback directly in profiles (acceptance stats + task history)
"""

import json
import math
import subprocess
from datetime import datetime
from typing import List, Dict

import numpy as np
from elasticsearch import Elasticsearch

from .summarization import MODEL_CONFIGS, robust_ollama_call
from .elastic_utils import fetch_employee_kpis

# ----------------------------
# SETTINGS
# ----------------------------
EMBED_METHOD = "mpnet"          # "mpnet" or "ollama"
MPNET_NAME = "sentence-transformers/all-mpnet-base-v2"
OLLAMA_EMBED_MODEL = "nomic-embed-text"
EMB_DIM = 768 if EMBED_METHOD == "mpnet" else 384

PROFILE_KEEP_LAST_N = 90
HALF_LIFE_DAYS = 14
ES_PROFILE_INDEX = "employee_profiles"

# ----------------------------
# EMBEDDING UTILS
# ----------------------------
try:
    from sentence_transformers import SentenceTransformer
except Exception:
    SentenceTransformer = None

import ollama


def _normalize(vec: List[float]) -> List[float]:
    arr = np.array(vec, dtype=float)
    n = np.linalg.norm(arr)
    return (arr / n).tolist() if n > 0 else arr.tolist()


def embed_with_mpnet(text: str, model_name: str = MPNET_NAME) -> List[float]:
    if SentenceTransformer is None:
        raise RuntimeError("sentence-transformers not installed. Run: pip install sentence-transformers")

    global _mpnet_model
    try:
        _mpnet_model
    except NameError:
        _mpnet_model = SentenceTransformer(model_name)

    vec = _mpnet_model.encode(text, normalize_embeddings=True)
    return vec.tolist()


def embed_with_ollama_subprocess(text: str, model: str = OLLAMA_EMBED_MODEL) -> List[float]:
    try:
        cmd = ["ollama", "embed", "-m", model, text]
        proc = subprocess.run(cmd, capture_output=True, text=True, check=True)
        data = json.loads(proc.stdout)
        return data.get("embedding", [])
    except Exception as e:
        print(f"ollama embed subprocess failed: {e}")
        return []


def embed_with_ollama_client(text: str, model: str = OLLAMA_EMBED_MODEL) -> List[float]:
    try:
        resp = ollama.embed(model=model, input=text)

        if isinstance(resp, dict) and "embedding" in resp:
            return resp["embedding"]
        if isinstance(resp, list) and resp and isinstance(resp[0], dict) and "embedding" in resp[0]:
            return resp[0]["embedding"]
        if isinstance(resp, list) and isinstance(resp[0], (float, int)):
            return resp
        return []
    except Exception as e:
        print(f"ollama client embed failed: {e}")
        return embed_with_ollama_subprocess(text, model)


def embed_text(text: str, method: str = EMBED_METHOD) -> List[float]:
    if not text:
        return [0.0] * EMB_DIM
    if method == "mpnet":
        return _normalize(embed_with_mpnet(text))
    elif method == "ollama":
        return _normalize(embed_with_ollama_client(text, OLLAMA_EMBED_MODEL))
    else:
        raise ValueError("Unknown embed method")

# ----------------------------
# PROFILE MANAGEMENT
# ----------------------------
def append_daily_report_to_profile(es: Elasticsearch, employee_id: str, date_str: str,
                                   embed_method: str = EMBED_METHOD,
                                   profile_index: str = ES_PROFILE_INDEX,
                                   keep_last_n: int = PROFILE_KEEP_LAST_N):
    """Embed daily report and upsert into employee profile (keeps last N)."""
    doc_id = f"{employee_id}-{date_str}"
    try:
        resp = es.get(index="employee_daily_reports", id=doc_id)
        report = resp["_source"]
    except Exception:
        q = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"employee_id.keyword": employee_id}},
                        {"term": {"date": date_str}}
                    ]
                }
            },
            "size": 1
        }
        r = es.search(index="employee_daily_reports", body=q)
        hits = r["hits"]["hits"]
        if not hits:
            print(f"No daily report found for {employee_id} {date_str}")
            return
        report = hits[0]["_source"]

    summary_text = report.get("daily_summary", "")
    emb = embed_text(summary_text, method=embed_method)

    update_script = {
        "script": {
            "source": """
                if (ctx._source.daily_reports == null) { ctx._source.daily_reports = []; }
                ctx._source.daily_reports.removeIf(r -> r.date == params.new_report.date);
                ctx._source.daily_reports.add(params.new_report);
                ctx._source.daily_reports.sort((a,b) -> a.date.compareTo(b.date));
                while (ctx._source.daily_reports.size() > params.keep_n) { ctx._source.daily_reports.remove(0); }
                ctx._source.last_update = params.last_update;
            """,
            "lang": "painless",
            "params": {
                "new_report": {
                    "date": date_str,
                    "summary": summary_text,
                    "embedded_summary": emb
                },
                "keep_n": keep_last_n,
                "last_update": datetime.utcnow().isoformat()
            }
        },
        "upsert": {
            "employee_id": employee_id,
            "daily_reports": [
                {"date": date_str, "summary": summary_text, "embedded_summary": emb}
            ],
            "profile_embedding": emb,
            "current_load": 0,
            "feedback_stats": {"accepted_count": 0, "rejected_count": 0, "acceptance_rate": 0.5},
            "last_update": datetime.utcnow().isoformat()
        }
    }

    es.update(index=profile_index, id=employee_id, body=update_script)
    recompute_and_update_profile_embedding(es, employee_id, profile_index)


def recompute_and_update_profile_embedding(es: Elasticsearch, employee_id: str,
                                           profile_index: str = ES_PROFILE_INDEX,
                                           half_life_days: int = HALF_LIFE_DAYS):
    """Recompute profile embedding as weighted average of reports."""
    try:
        resp = es.get(index=profile_index, id=employee_id)
        profile = resp["_source"]
    except Exception as e:
        print(f"No profile for {employee_id}: {e}")
        return

    reports = profile.get("daily_reports", [])
    if not reports:
        print("No daily reports to build embedding")
        return

    today = datetime.utcnow().date()
    total, wsum = None, 0.0

    for r in reports:
        try:
            d = datetime.fromisoformat(r.get("date")).date()
        except Exception:
            d = datetime.strptime(r.get("date")[:10], "%Y-%m-%d").date()
        days_old = (today - d).days
        weight = 0.5 ** (days_old / half_life_days)

        emb = np.array(r.get("embedded_summary", [0.0] * EMB_DIM), dtype=float)
        total = emb * weight if total is None else total + emb * weight
        wsum += weight

    if total is None or wsum == 0:
        print("Empty embeddings while recomputing")
        return

    avg = total / wsum
    profile_emb = _normalize(avg.tolist())

    es.update(index=profile_index, id=employee_id,
              body={"doc": {"profile_embedding": profile_emb, "last_update": datetime.utcnow().isoformat()}})
    print(f"✅ Updated profile embedding for {employee_id}")

# ----------------------------
# TASK MATCHING
# ----------------------------
def es_vector_search(es: Elasticsearch, query_vector: List[float],
                     index: str = ES_PROFILE_INDEX, k: int = 10):
    body = {
        "size": k,
        "query": {
            "script_score": {
                "query": {"match_all": {}},
                "script": {
                    "source": "cosineSimilarity(params.query_vector, 'profile_embedding') + 1.0",
                    "params": {"query_vector": query_vector}
                }
            }
        },
        "_source": ["employee_id", "skills", "productivity_metrics", "feedback_stats", "current_load", "last_update", "weekly_summary"]
    }
    return es.search(index=index, body=body)["hits"]["hits"]

def compute_productivity_from_kpi(kpi):
    if not kpi:
        return 0.0
    active_pct = kpi.get("active_pct", 0)
    keystrokes = kpi.get("total_keystrokes", 0)
    idle_pct = kpi.get("idle_pct", 0)
    pause_pct = kpi.get("pause_pct", 0)
    
    # Example formula: weighted combination
    score = (active_pct * 0.6 + (keystrokes / max(1, 10000)) * 0.4) * (1 - (idle_pct + pause_pct)/100)
    return min(max(score, 0.0), 1.0)  # normalize to [0,1]

def compose_final_score(src, semantic_similarity, recency_boost: float = 0.0,kpi_score: float = 0.0):
    feedback_stats = src.get("feedback_stats", {})
    acceptance_rate = float(feedback_stats.get("acceptance_rate", 0.5))

    load_penalty = float(src.get("current_load", 0)) * 0.05

    α, β, γ, δ, ε = 0.65, 0.15, 0.10, 0.05, 0.05
    return (
        α * semantic_similarity +
        β * kpi_score +       
        γ * acceptance_rate +
        δ * recency_boost -
        load_penalty
    )


def generate_llm_explanation(task: str, employee_summary: str) -> str:
    prompt = f"""
You are an expert task allocator. Based on the following employee profile summary, explain why this employee is a good or bad fit for the task.

Task: {task}
Employee Profile Summary: {employee_summary}

Respond in 2-3 sentences, concise and professional.
"""
    config = MODEL_CONFIGS.get("daily_report", {})
    try:
        return robust_ollama_call(prompt, **config).strip()
    except Exception as e:
        return f"LLM error: {e}"


def match_task_to_employees(es: Elasticsearch, task_text: str, top_k: int = 10, explain_top_n: int = 3):
    today_str = datetime.utcnow().date().isoformat()
    task_emb = embed_text(task_text)
    hits = es_vector_search(es, task_emb, k=top_k)

    candidates = []
    for h in hits:
        src = h["_source"]
        semantic_score = h["_score"] - 1.0

        # 1️⃣ Fetch KPI for the employee today
        kpi = fetch_employee_kpis(es, employee_id=src["employee_id"], date=today_str)
        kpi_score = compute_productivity_from_kpi(kpi)

        # 2️⃣ Compute final score
        final_score = compose_final_score(src, semantic_score, kpi_score=kpi_score)

        candidate = {
            "employee_id": src["employee_id"],
            "skills": src.get("skills", []),
            "semantic_score": semantic_score,
            "final_score": final_score,
            "kpi_score": kpi_score,                     # new: include for inspection
            "productivity_metrics": src.get("productivity_metrics", {}),
            "feedback_stats": src.get("feedback_stats", {}),
            "current_load": src.get("current_load", 0),
            "weekly_summary": src.get("weekly_summary", "")
        }

        candidates.append(candidate)

    candidates = sorted(candidates, key=lambda x: x["final_score"], reverse=True)

    # Add LLM explanations to top N
    for c in candidates[:explain_top_n]:
        c["llm_explanation"] = generate_llm_explanation(task_text, c.get("weekly_summary", ""))

    return candidates


# ----------------------------
# ADMIN FEEDBACK
# ----------------------------
def store_admin_feedback(es: Elasticsearch, employee_id: str, task_id: str,
                         task_text: str, prediction_score: float, feedback: str):
    """
    Store admin feedback in employee profile.
    If accepted, reinforce profile embedding with task embedding.
    feedback ∈ {"accepted", "rejected"}
    """
    # 1️⃣ Fetch profile
    profile = es.get(index=ES_PROFILE_INDEX, id=employee_id)["_source"]

    # 2️⃣ Store feedback in task history
    feedback_entry = {
        "task_id": task_id,
        "task_text": task_text,
        "prediction_score": prediction_score,
        "admin_feedback": feedback,
        "assigned_at": datetime.utcnow().isoformat()
    }

    history = profile.get("task_history", [])
    history.append(feedback_entry)

    # 3️⃣ Update feedback stats
    stats = profile.get("feedback_stats", {"accepted_count": 0, "rejected_count": 0})
    if feedback == "accepted":
        stats["accepted_count"] += 1
    else:
        stats["rejected_count"] += 1

    stats["acceptance_rate"] = stats["accepted_count"] / max(1, (stats["accepted_count"] + stats["rejected_count"]))

    # 4️⃣ Reinforce profile embedding if task accepted
    if feedback == "accepted":
        task_emb = embed_text(task_text)
        # Add task as a pseudo daily report
        es.update(index=ES_PROFILE_INDEX, id=employee_id,
                  body={"script": {
                      "source": """
                          if (ctx._source.daily_reports == null) { ctx._source.daily_reports = []; }
                          ctx._source.daily_reports.add(params.rpt);
                      """,
                      "params": {"rpt": {
                          "date": datetime.utcnow().isoformat(),
                          "summary": task_text,
                          "embedded_summary": task_emb
                      }}
                  }})
        # Recompute profile embedding
        recompute_and_update_profile_embedding(es, employee_id)

    # 5️⃣ Update profile with feedback history and stats
    es.update(index=ES_PROFILE_INDEX, id=employee_id,
              body={"doc": {"task_history": history, "feedback_stats": stats}})

    print(f"✅ Feedback stored for {employee_id} (task {task_id}, {feedback})")


# ----------------------------
# CLIENT INIT
# ----------------------------
es = Elasticsearch("http://localhost:9200")
