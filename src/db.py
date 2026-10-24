import os
import sqlite3
from sqlalchemy import create_engine
import logging

logger = logging.getLogger(__name__)

DB_PATH = os.getenv("SQLITE_DB_PATH", "fintech_db.db")
DATABASE_URL = f"sqlite:///{DB_PATH}"

def get_sqlalchemy_engine():
    return create_engine(DATABASE_URL)

def get_db_connection():
    return sqlite3.connect(DB_PATH)

def log_pipeline_run(payload: dict):
    conn = get_db_connection()
    cursor = conn.cursor()
    query = """
    INSERT INTO pipeline_run_metadata
    (dag_run_id, execution_date, extracted_rows, validated_rows, rejected_rows, inserted_rows, prediction_count, status)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(dag_run_id) DO UPDATE SET
        extracted_rows = excluded.extracted_rows,
        validated_rows = excluded.validated_rows,
        rejected_rows = excluded.rejected_rows,
        inserted_rows = excluded.inserted_rows,
        prediction_count = excluded.prediction_count,
        status = excluded.status,
        updated_at = CURRENT_TIMESTAMP;
    """
    cursor.execute(query, (
        str(payload['dag_run_id']),
        str(payload['execution_date']),
        payload['extracted_rows'],
        payload['validated_rows'],
        payload['rejected_rows'],
        payload['inserted_rows'],
        payload['prediction_count'],
        payload['status']
    ))
    conn.commit()
    cursor.close()
    conn.close()
