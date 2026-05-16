# dags/embedding_sync_dag.py
"""
Airflow DAG: embedding_sync_dag

Schedule: every 5 minutes.

Tasks:
  1. backfill_existing_data  — one-time: queues all existing rows that
                               don't yet have an embedding. Safe to run
                               repeatedly (skips already-embedded rows).
  2. cleanup_embeddings      — removes stale embeddings from row_embeddings:
                               Phase 1: orphaned tables (DROP TABLE detected).
                               Phase 2: TRUNCATE / bulk DELETE detected via
                               NOT EXISTS join against source table.
  3. attach_new_triggers     — detects new data_* tables and attaches
                               the embedding trigger automatically.
  4. process_queue           — reads embedding_queue, calls Ollama
                               (or MiniLM fallback), upserts into
                               row_embeddings.
  5. log_stats               — prints a summary to Airflow logs.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "lakehouse",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    dag_id="embedding_sync_dag",
    description="Sync pgvector embeddings — Ollama (nomic-embed-text) primary",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["search", "embeddings", "pgvector", "ollama"],
    default_args=default_args,
)

# ── Shared DB connection helper ──────────────────────────────

def _pg_conn():
    import os, psycopg2
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        database=os.getenv("POSTGRES_DB", "lakehouse_db"),
        user=os.getenv("POSTGRES_USER", "lakehouse_user"),
        password=os.getenv("POSTGRES_PASSWORD", "lakehouse_pass"),
    )


def _import_pipeline():
    import sys
    sys.path.insert(0, "/opt/python-etl")
    from pipelines import embedding_pipeline
    return embedding_pipeline


# ── Task 1: backfill ─────────────────────────────────────────

def _backfill_existing_data(**context):
    """
    Queue all existing rows that don't yet have an embedding.
    Runs every DAG cycle but skips already-embedded rows,
    so it's cheap once backfill is complete (no new rows to queue).
    """
    ep = _import_pipeline()
    conn = _pg_conn()
    conn.autocommit = False
    try:
        n = ep.backfill_all_tables(conn, skip_already_embedded=True)
        context["ti"].xcom_push(key="backfill_queued", value=n)
        return n
    finally:
        conn.close()


# ── Task 2: cleanup orphaned + truncated embeddings ──────────

def _cleanup_embeddings(**context):
    """
    Two-phase cleanup — runs every DAG cycle before process_queue:

    Phase 1 — orphan cleanup (fast):
      Removes all row_embeddings for tables that were DROPped.
      DROP TABLE silently removes the trigger so no DELETE events
      ever reach embedding_queue.  This is the only way to catch it.

    Phase 2 — truncate/bulk-delete cleanup (per-table NOT EXISTS join):
      Catches TRUNCATE TABLE and massive bulk DELETEs that bypassed
      the row-level trigger.  Only runs on tables that still exist.
      Skipped entirely if Phase 1 already found and removed everything.
    """
    ep   = _import_pipeline()
    conn = _pg_conn()
    conn.autocommit = False
    try:
        orphan_result   = ep.cleanup_orphaned_embeddings(conn)
        truncate_result = ep.cleanup_truncated_tables(conn)

        combined = {
            "orphaned_tables":          orphan_result["orphaned_tables"],
            "orphan_embeddings_removed": orphan_result["removed_embeddings"],
            "truncate_embeddings_removed": truncate_result["removed_embeddings"],
            "truncate_tables_checked":   truncate_result["checked_tables"],
            "total_removed": (
                orphan_result["removed_embeddings"]
                + truncate_result["removed_embeddings"]
            ),
        }
        context["ti"].xcom_push(key="cleanup_result", value=combined)
        return combined
    finally:
        conn.close()


# ── Task 3: attach triggers ───────────────────────────────────

def _attach_triggers(**context):
    ep = _import_pipeline()
    conn = _pg_conn()
    conn.autocommit = False
    try:
        n = ep.attach_triggers_for_new_tables(conn)
        context["ti"].xcom_push(key="new_triggers", value=n)
        return n
    finally:
        conn.close()


# ── Task 4: process queue ─────────────────────────────────────

def _process_queue(**context):
    ep = _import_pipeline()
    result = ep.process_embedding_queue(
        batch_size=200,
        max_batches=50,       # max 10,000 rows per run; rest processed next cycle
    )
    context["ti"].xcom_push(key="result", value=result)
    return result


# ── Task 5: log stats ─────────────────────────────────────────

def _log_stats(**context):
    import logging
    log = logging.getLogger(__name__)
    ti = context["ti"]

    backfill_queued = ti.xcom_pull(task_ids="backfill_existing_data", key="backfill_queued") or 0
    new_triggers    = ti.xcom_pull(task_ids="attach_triggers",        key="new_triggers")    or 0
    result          = ti.xcom_pull(task_ids="process_queue",          key="result")          or {}
    cleanup         = ti.xcom_pull(task_ids="cleanup_embeddings",     key="cleanup_result")  or {}

    log.info("=" * 55)
    log.info("  Embedding Sync Summary")
    log.info(f"  Rows backfill-queued    : {backfill_queued}")
    log.info(f"  New tables watched      : {new_triggers}")
    log.info(f"  Rows embedded           : {result.get('processed', 0)}")
    log.info(f"  Embed errors            : {result.get('errors', 0)}")
    log.info(f"  Orphaned tables removed : {len(cleanup.get('orphaned_tables', []))}")
    log.info(f"  Stale embeddings purged : {cleanup.get('total_removed', 0)}")
    if cleanup.get("orphaned_tables"):
        log.warning(f"  Dropped tables cleaned  : {cleanup['orphaned_tables']}")
    log.info("=" * 55)


# ── DAG wiring ────────────────────────────────────────────────

backfill_task = PythonOperator(
    task_id="backfill_existing_data",
    python_callable=_backfill_existing_data,
    dag=dag,
)

cleanup_task = PythonOperator(
    task_id="cleanup_embeddings",
    python_callable=_cleanup_embeddings,
    dag=dag,
)

attach_task = PythonOperator(
    task_id="attach_triggers",
    python_callable=_attach_triggers,
    dag=dag,
)

process_task = PythonOperator(
    task_id="process_queue",
    python_callable=_process_queue,
    dag=dag,
)

stats_task = PythonOperator(
    task_id="log_stats",
    python_callable=_log_stats,
    dag=dag,
)

# backfill, cleanup, and attach_triggers all run in parallel first,
# then process_queue runs (clean slate — no stale embeddings),
# then log_stats summarises the full cycle.
#
#   backfill_existing_data ──┐
#   cleanup_embeddings     ──┼──► process_queue ──► log_stats
#   attach_triggers        ──┘
[backfill_task, cleanup_task, attach_task] >> process_task >> stats_task