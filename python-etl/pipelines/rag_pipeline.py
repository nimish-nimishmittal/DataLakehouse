# python-etl/pipelines/rag_pipeline.py
"""
RAG Pipeline — Retrieval-Augmented Generation

Sits on top of the existing vector search system.
Flow:
  1. Embed user query  (nomic-embed-text via ollama container)
  2. Retrieve top-k rows from row_embeddings  (reuses existing search())
  3. Build structured context string from retrieved rows
  4. Stream response from gemma3:270m via ollama_rag container

Three modes:
  qa         → answer a question grounded in the retrieved data
  summarize  → summarize retrieved rows, identify patterns & anomalies
  predict    → trend analysis and predictions from the data

Multi-turn memory:
  Pass conversation_history as list of {"role": "user"|"assistant", "content": "..."}
  The system prompt is always first; history is replayed; new user message
  gets context injected.  History messages keep their original content —
  context is only injected into the latest user turn.
"""

import os
import json
import logging
import requests
from typing import Generator, Optional

logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────
_OLLAMA_RAG_HOST   = os.getenv("OLLAMA_RAG_HOST",  "http://ollama_rag:11434")
_RAG_MODEL         = os.getenv("OLLAMA_RAG_MODEL",  "gemma3:270m")
_MAX_CONTEXT_ROWS  = int(os.getenv("RAG_MAX_CONTEXT_ROWS",  "15"))
_MAX_CONTEXT_CHARS = int(os.getenv("RAG_MAX_CONTEXT_CHARS", "12000"))


# ── System prompt templates ───────────────────────────────────────────────
_SYSTEM_PROMPTS = {
    "qa": (
        "You are an expert data analyst assistant for a data lakehouse system.\n"
        "You are given relevant rows retrieved from the lakehouse database via semantic search.\n"
        "Answer the user's question accurately and concisely based ONLY on the provided data.\n\n"
        "Rules:\n"
        "- Cite which table and row your answer comes from (e.g. 'From table sales_report, row 12: ...')\n"
        "- If the data doesn't contain enough information, say so clearly — do not guess\n"
        "- Be specific with numbers, dates, and values\n"
        "- If multiple rows are relevant, synthesize them into a coherent answer\n"
        "- Format numbers clearly (use commas for thousands)"
    ),
    "summarize": (
        "You are an expert data analyst assistant for a data lakehouse system.\n"
        "You are given data rows retrieved from the lakehouse database.\n"
        "Produce a clear, structured summary of the data.\n\n"
        "Rules:\n"
        "- Identify key patterns, trends, and notable values\n"
        "- Group related information logically\n"
        "- Highlight anomalies or outliers\n"
        "- Mention the table(s) the data comes from\n"
        "- Use bullet points or sections for clarity\n"
        "- Include specific numbers and values\n"
        "- End with 2-3 concise key takeaways"
    ),
    "predict": (
        "You are an expert data analyst assistant for a data lakehouse system.\n"
        "You are given data rows retrieved from the lakehouse database.\n"
        "Analyze trends and make data-driven predictions or forecasts.\n\n"
        "Rules:\n"
        "- Base ALL predictions strictly on patterns visible in the provided data\n"
        "- State the observed trend before each prediction\n"
        "- Quantify predictions where possible (e.g. 'based on 15% monthly growth...')\n"
        "- Distinguish between high-confidence and speculative predictions\n"
        "- Note any data gaps that affect prediction quality\n"
        "- Cite specific rows/values that support your analysis\n"
        "- End with actionable recommendations"
    ),
}


# ── Context builder ───────────────────────────────────────────────────────

def _format_row(row: dict, rank: int) -> str:
    """Format a single retrieved row as readable text for the LLM."""
    table  = row.get("table_name", "unknown").replace("data_", "").replace("_", " ")
    row_id = row.get("row_id", "?")
    sim    = row.get("similarity", 0)
    data   = row.get("row_data") or {}

    lines = [f"[Row {rank} | Table: {table} | ID: {row_id} | Relevance: {sim:.0%}]"]
    for col, val in data.items():
        if val is not None and str(val).strip():
            lines.append(f"  {col}: {val}")
    return "\n".join(lines)


def build_context(retrieved_rows: list) -> str:
    """Convert retrieved rows into a context string. Truncates at _MAX_CONTEXT_CHARS."""
    if not retrieved_rows:
        return "No relevant data found in the lakehouse for this query."

    rows  = retrieved_rows[:_MAX_CONTEXT_ROWS]
    parts = ["=== RETRIEVED DATA FROM LAKEHOUSE ===\n"]
    total = len(parts[0])

    for i, row in enumerate(rows, 1):
        chunk = _format_row(row, i) + "\n\n"
        if total + len(chunk) > _MAX_CONTEXT_CHARS:
            parts.append(f"[... {len(retrieved_rows) - i + 1} more rows truncated to fit context ...]\n")
            break
        parts.append(chunk)
        total += len(chunk)

    parts.append("=== END OF RETRIEVED DATA ===")
    return "".join(parts)


# ── Message builder ───────────────────────────────────────────────────────

def build_messages(
    query: str,
    mode: str,
    context: str,
    conversation_history: list,
) -> list:
    """
    Build the messages array for Ollama /api/chat.

    Single-shot :  [system, user_with_context]
    Multi-turn  :  [system, ...history_msgs, user_with_context]

    Context is only injected into the current user message.
    History is replayed as-is (context was already consumed then).
    """
    system_prompt = _SYSTEM_PROMPTS.get(mode, _SYSTEM_PROMPTS["qa"])

    if mode == "summarize":
        user_content = (
            f"{context}\n\nPlease summarize this data."
            if not (query and query.strip())
            else f"{context}\n\nFocus your summary on: {query}"
        )
    else:
        user_content = f"{context}\n\nQuestion: {query}"

    messages = [{"role": "system", "content": system_prompt}]

    for msg in conversation_history:
        role    = msg.get("role", "user")
        content = msg.get("content", "")
        if role in ("user", "assistant") and content:
            messages.append({"role": role, "content": content})

    messages.append({"role": "user", "content": user_content})
    return messages


# ── Health / model helpers ────────────────────────────────────────────────

def get_rag_status() -> dict:
    """Check ollama_rag container health and list loaded models."""
    try:
        r = requests.get(f"{_OLLAMA_RAG_HOST}/api/tags", timeout=5)
        if r.status_code == 200:
            models       = [m["name"] for m in r.json().get("models", [])]
            model_loaded = any(_RAG_MODEL in m for m in models)
            return {
                "status":           "ok",
                "host":             _OLLAMA_RAG_HOST,
                "model":            _RAG_MODEL,
                "model_loaded":     model_loaded,
                "available_models": models,
            }
    except Exception as e:
        logger.warning(f"ollama_rag health check failed: {e}")
    return {
        "status":           "unavailable",
        "host":             _OLLAMA_RAG_HOST,
        "model":            _RAG_MODEL,
        "model_loaded":     False,
        "available_models": [],
    }


def get_available_models() -> list:
    try:
        r = requests.get(f"{_OLLAMA_RAG_HOST}/api/tags", timeout=5)
        if r.status_code == 200:
            return [m["name"] for m in r.json().get("models", [])]
    except Exception as e:
        logger.warning(f"Failed fetching models: {e}")
    return []


# ── Core streaming generator ──────────────────────────────────────────────

def stream_rag_response(
    query: str,
    mode: str = "qa",
    top_k: int = 10,
    table_filter=None,
    conversation_history: Optional[list] = None,
    model: Optional[str] = None,
) -> Generator[str, None, None]:
    """
    Full RAG pipeline — yields SSE-formatted string chunks.

    Chunk types:
      "data: [SOURCES]<json_array>\\n\\n"   — emitted first; retrieved row metadata
      "data: <token>\\n\\n"                 — streamed LLM token
      "data: [DONE]\\n\\n"                  — stream complete
      "data: [ERROR]<message>\\n\\n"        — error

    Flask usage:
        from flask import Response, stream_with_context
        return Response(
            stream_with_context(stream_rag_response(...)),
            mimetype="text/event-stream",
            headers={"X-Accel-Buffering": "no"},
        )
    """
    history   = conversation_history or []
    use_model = model or _RAG_MODEL

    # ── Step 1: Retrieve ─────────────────────────────────────────────────
    try:
        from pipelines.embedding_pipeline import search as vector_search
        retrieved = vector_search(query=query, top_k=top_k, table_filter=table_filter)
    except Exception as e:
        logger.exception("RAG retrieval step failed")
        yield f"data: [ERROR]Retrieval failed: {e}\n\n"
        return

    # ── Step 2: Emit source cards BEFORE the answer starts ───────────────
    sources = [
        {
            "table_name": r.get("table_name"),
            "row_id":     r.get("row_id"),
            "similarity": round(float(r.get("similarity", 0)), 4),
            "row_data":   r.get("row_data"),
            "source":     r.get("source"),
        }
        for r in retrieved
    ]
    yield f"data: [SOURCES]{json.dumps(sources)}\n\n"

    # ── Step 3: Build prompt ─────────────────────────────────────────────
    context  = build_context(retrieved)
    messages = build_messages(query, mode, context, history)

    # ── Step 4: Stream from gemma3:270m ──────────────────────────────────────
    try:
        payload = {
            "model":    use_model,
            "messages": messages,
            "stream":   True,
            "options": {
                "temperature": 0.3,    # grounded / factual
                "top_p":       0.9,
                "num_ctx":     16384,  # safe for gemma3:270m
            },
        }

        with requests.post(
            f"{_OLLAMA_RAG_HOST}/api/chat",
            json=payload,
            stream=True,
            timeout=300,
        ) as resp:
            if resp.status_code != 200:
                yield f"data: [ERROR]LLM returned HTTP {resp.status_code}: {resp.text[:200]}\n\n"
                return

            for line in resp.iter_lines():
                if not line:
                    continue
                try:
                    chunk = json.loads(line)
                    token = chunk.get("message", {}).get("content", "")
                    if token:
                        # Replace literal newlines so SSE frame stays intact
                        safe = token.replace("\n", "\\n")
                        yield f"data: {safe}\n\n"
                    if chunk.get("done"):
                        break
                except json.JSONDecodeError:
                    continue

    except requests.exceptions.ConnectionError:
        yield (
            f"data: [ERROR]Cannot connect to ollama_rag at {_OLLAMA_RAG_HOST}. "
            "Make sure the container is running and gemma3:270m is pulled.\n\n"
        )
        return
    except requests.exceptions.Timeout:
        yield (
            "data: [ERROR]ollama_rag timed out. "
            "The model may still be loading — try again in a moment.\n\n"
        )
        return
    except Exception as e:
        logger.exception("RAG generation step failed")
        yield f"data: [ERROR]Generation failed: {e}\n\n"
        return

    yield "data: [DONE]\n\n"