# python-etl/rag_api.py
"""
RAG API — Flask Blueprint

Endpoints:

  POST /rag/query   (streaming SSE)
    Body: {
      "query":                "...",
      "mode":                 "qa" | "summarize" | "predict",   default: "qa"
      "top_k":                10,                               default: 10
      "table_filter":         null | "table" | ["t1","t2"],     default: null
      "conversation_history": [{"role":"user","content":"..."}], default: []
      "model":                "gemma3:270m"                         default: env var
    }
    Returns: text/event-stream  (SSE)
      data: [SOURCES]<json_array>    ← retrieved rows, arrives first
      data: <token>                  ← streamed LLM tokens
      data: [DONE]                   ← stream finished
      data: [ERROR]<message>         ← something went wrong

  GET /rag/status
    Returns health of ollama_rag container + whether gemma3:270m is loaded

  GET /rag/models
    Returns list of models available on ollama_rag

Mount in uploader_app.py:
    from rag_api import rag_bp
    app.register_blueprint(rag_bp)
"""

import logging
from flask import Blueprint, request, jsonify, Response, stream_with_context
from flask_jwt_extended import jwt_required

logger = logging.getLogger(__name__)

rag_bp = Blueprint("rag", __name__)

_VALID_MODES = {"qa", "summarize", "predict"}


@rag_bp.route("/rag/query", methods=["POST"])
@jwt_required()
def rag_query():
    """
    POST /rag/query

    Streams a RAG response via Server-Sent Events.
    The frontend reads this with fetch() + ReadableStream.

    First SSE message is always [SOURCES] with the retrieved context rows.
    Subsequent messages are streamed LLM tokens.
    Final message is [DONE] or [ERROR].
    """
    body = request.get_json(silent=True) or {}

    query = (body.get("query") or "").strip()
    if not query:
        return jsonify({"error": "query is required"}), 400

    mode = body.get("mode", "qa")
    if mode not in _VALID_MODES:
        return jsonify({"error": f"mode must be one of: {', '.join(_VALID_MODES)}"}), 400

    top_k = min(int(body.get("top_k", 10)), 50)
    model = body.get("model") or None

    # Normalise table_filter — same logic as search_api.py
    raw_table = body.get("table_filter")
    if isinstance(raw_table, list):
        table_filter = [t for t in raw_table if t and str(t).strip()] or None
    elif isinstance(raw_table, str) and raw_table.strip():
        table_filter = raw_table.strip()
    else:
        table_filter = None

    # conversation_history: list of {role, content}
    history = body.get("conversation_history") or []
    if not isinstance(history, list):
        history = []

    try:
        from pipelines.rag_pipeline import stream_rag_response

        def generate():
            yield from stream_rag_response(
                query=query,
                mode=mode,
                top_k=top_k,
                table_filter=table_filter,
                conversation_history=history,
                model=model,
            )

        return Response(
            stream_with_context(generate()),
            mimetype="text/event-stream",
            headers={
                "Cache-Control":    "no-cache",
                "X-Accel-Buffering": "no",   # Disable nginx proxy buffering
                "Connection":       "keep-alive",
            },
        )

    except Exception as e:
        logger.exception("RAG query endpoint error")
        return jsonify({"error": str(e)}), 500


@rag_bp.route("/rag/status", methods=["GET"])
@jwt_required()
def rag_status():
    """
    GET /rag/status

    Returns:
      {
        "status":           "ok" | "unavailable",
        "host":             "http://ollama_rag:11434",
        "model":            "gemma3:270m",
        "model_loaded":     true | false,
        "available_models": ["gemma3:270m", ...]
      }
    """
    try:
        from pipelines.rag_pipeline import get_rag_status
        return jsonify(get_rag_status()), 200
    except Exception as e:
        logger.exception("Failed getting RAG status")
        return jsonify({"error": str(e)}), 500


@rag_bp.route("/rag/models", methods=["GET"])
@jwt_required()
def rag_models():
    """
    GET /rag/models

    Returns list of models available on the ollama_rag container.
    Useful for letting users switch between models in the UI.
    """
    try:
        from pipelines.rag_pipeline import get_available_models
        models = get_available_models()
        return jsonify({"models": models, "total": len(models)}), 200
    except Exception as e:
        logger.exception("Failed listing RAG models")
        return jsonify({"error": str(e)}), 500