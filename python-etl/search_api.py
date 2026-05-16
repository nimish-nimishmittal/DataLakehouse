# python-etl/search_api.py
"""
Search API — Flask Blueprint

Exposes two endpoints on top of the vector search system:

  POST /search
    Body: { "query": "...", "top_k": 10, "table": "optional_table_name" }
    Returns: ranked list of matching rows with similarity scores

  GET /search/tables
    Returns: list of all tables that have embeddings, with row counts

Both endpoints require a valid JWT (same auth as /upload).

Mount in uploader_app.py:
    from search_api import search_bp
    app.register_blueprint(search_bp)
"""

import os
import logging
import psycopg2
import psycopg2.extras
from flask import Blueprint, request, jsonify
from flask_jwt_extended import jwt_required

logger = logging.getLogger(__name__)

search_bp = Blueprint("search", __name__)


def _get_pg_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        database=os.getenv("POSTGRES_DB", "lakehouse_db"),
        user=os.getenv("POSTGRES_USER", "lakehouse_user"),
        password=os.getenv("POSTGRES_PASSWORD", "lakehouse_pass"),
    )


@search_bp.route("/search", methods=["POST"])
@jwt_required()
def semantic_search():
    """
    POST /search

    Request body (JSON):
        query   : required — natural language question or phrase
        top_k   : optional — number of results (default 10, max 50)
        table   : optional — restrict to one table name

    Response:
        {
            "query": "...",
            "results": [
                {
                    "table_name": "data_doc1_sales_report_table_1",
                    "row_id": "42",
                    "similarity": 0.91,
                    "row_data": { "region": "North", "sales": "45000", ... },
                    "row_text": "table: data doc1 sales report | region: North | ..."
                },
                ...
            ],
            "total": 10
        }

    Example queries:
        "employees in the engineering department"
        "Q3 revenue for North America"
        "software license renewals expiring soon"
        "customers with high churn risk"
    """
    body = request.get_json(silent=True) or {}
    query = body.get("query", "").strip()
    top_k = min(int(body.get("top_k", 10)), 50)

    # table_filter accepts:
    #   null / missing → all tables
    #   "table_name"   → single table (string)
    #   ["t1", "t2"]   → multiple tables (list) ← new
    raw_table = body.get("table")
    if isinstance(raw_table, list):
        table_filter = [t for t in raw_table if t and t.strip()] or None
    elif isinstance(raw_table, str) and raw_table.strip():
        table_filter = raw_table.strip()
    else:
        table_filter = None

    if not query:
        return jsonify({"error": "query is required"}), 400

    try:
        from pipelines.embedding_pipeline import search as vector_search

        results = vector_search(
            query=query,
            top_k=top_k,
            table_filter=table_filter,
        )

        return jsonify({
            "query": query,
            "results": [
                {
                    "table_name": r["table_name"],
                    "row_id": r["row_id"],
                    "similarity": round(float(r["similarity"]), 4),
                    "row_data": r["row_data"],
                    "row_text": r["row_text"],
                    "source": r.get("source"),
                }
                for r in results
            ],
            "total": len(results),
            "table_filter": table_filter,
        }), 200

    except Exception as e:
        logger.exception("Search failed")
        return jsonify({"error": str(e)}), 500


@search_bp.route("/search/tables", methods=["GET"])
@jwt_required()
def list_embedded_tables():
    """
    GET /search/tables

    Returns all tables that have at least one embedding,
    along with their row counts and last update time.

    Useful for building a search UI that shows which tables
    are available for querying.
    """
    pg_conn = None
    try:
        pg_conn = _get_pg_conn()
        cursor = pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute(
            """
            SELECT
                table_name,
                COUNT(*)                                        AS embedded_rows,
                COUNT(*) FILTER (WHERE embedding_768 IS NOT NULL) AS rows_768,
                COUNT(*) FILTER (WHERE embedding_384 IS NOT NULL) AS rows_384,
                COUNT(*) FILTER (WHERE embedding_768 IS NOT NULL
                                   AND embedding_384 IS NOT NULL) AS rows_both,
                MAX(updated_at)                                 AS last_updated
            FROM row_embeddings
            GROUP BY table_name
            ORDER BY table_name
            """
        )
        tables = [dict(r) for r in cursor.fetchall()]
        cursor.close()
        return jsonify({"tables": tables, "total": len(tables)}), 200
    except Exception as e:
        logger.exception("Failed listing tables")
        return jsonify({"error": str(e)}), 500
    finally:
        if pg_conn:
            pg_conn.close()


@search_bp.route("/search/status", methods=["GET"])
@jwt_required()
def queue_status():
    """
    GET /search/status

    Returns the current state of the embedding queue.
    Useful for monitoring — shows how many rows are waiting to be embedded.
    """
    pg_conn = None
    try:
        pg_conn = _get_pg_conn()
        cursor = pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute(
            """
            SELECT status, COUNT(*) AS count
            FROM embedding_queue
            GROUP BY status
            ORDER BY status
            """
        )
        stats = {r["status"]: r["count"] for r in cursor.fetchall()}
        cursor.close()

        cursor2 = pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor2.execute("""
            SELECT
                COUNT(*)                                            AS total_rows,
                COUNT(*) FILTER (WHERE embedding_768 IS NOT NULL)   AS rows_with_768,
                COUNT(*) FILTER (WHERE embedding_384 IS NOT NULL)   AS rows_with_384,
                COUNT(*) FILTER (WHERE embedding_768 IS NOT NULL
                                   AND embedding_384 IS NOT NULL)   AS rows_with_both,
                COUNT(DISTINCT table_name)                          AS tables_indexed
            FROM row_embeddings
        """)
        emb_stats = dict(cursor2.fetchone())
        cursor2.close()

        return jsonify({
            "queue":      stats,
            "embeddings": emb_stats,
        }), 200
    except Exception as e:
        logger.exception("Failed getting queue status")
        return jsonify({"error": str(e)}), 500
    finally:
        if pg_conn:
            pg_conn.close()