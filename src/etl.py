import pandas as pd
import numpy as np
import logging
from datetime import datetime, timezone
from src.db import get_sqlalchemy_engine, get_db_connection

logger = logging.getLogger(__name__)

def extract_batch(start_time: datetime, end_time: datetime) -> tuple[pd.DataFrame, dict]:
    engine = get_sqlalchemy_engine()
    query = """
    SELECT transaction_id, user_id, amount, merchant_category, transaction_timestamp, created_at
    FROM raw_transactions
    """
    try:
        raw_df = pd.read_sql(query, con=engine)
    except Exception as e:
        logger.warning(f"Raw transaction query empty or failed: {e}")
        raw_df = pd.DataFrame()
    metrics = {"extracted_records": len(raw_df)}
    return raw_df, metrics

def transform_and_validate(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    if df.empty:
        return pd.DataFrame(), {"valid_rows": 0, "rejected_rows": 0}
    
    initial_count = len(df)
    df_clean = df.drop_duplicates(subset=['transaction_id']).copy()
    df_clean = df_clean[df_clean['amount'] > 0]
    df_clean['merchant_category'] = df_clean['merchant_category'].fillna('UNKNOWN')
    
    df_clean['transaction_timestamp'] = pd.to_datetime(df_clean['transaction_timestamp'])
    df_clean['hour_of_day'] = df_clean['transaction_timestamp'].dt.hour
    df_clean['day_of_week'] = df_clean['transaction_timestamp'].dt.day_of_week
    df_clean['amount_log'] = np.log1p(df_clean['amount'])
    
    valid_count = len(df_clean)
    rejected_count = initial_count - valid_count
    
    metrics = {
        "valid_rows": valid_count,
        "rejected_rows": rejected_count
    }
    return df_clean, metrics

def load_processed_data(df: pd.DataFrame) -> dict:
    if df.empty:
        return {"inserted_rows": 0}
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Convert Pandas Timestamp to ISO formatted string for SQLite compatibility
    df_to_load = df.copy()
    df_to_load['transaction_timestamp'] = df_to_load['transaction_timestamp'].astype(str)
    
    records = df_to_load[['transaction_id', 'user_id', 'amount', 'merchant_category', 
                         'transaction_timestamp', 'hour_of_day', 'day_of_week', 'amount_log']].values.tolist()
    
    query = """
    INSERT INTO processed_transactions
    (transaction_id, user_id, amount, merchant_category, transaction_timestamp, hour_of_day, day_of_week, amount_log)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(transaction_id) DO UPDATE SET
        amount = excluded.amount,
        merchant_category = excluded.merchant_category,
        transaction_timestamp = excluded.transaction_timestamp,
        updated_at = CURRENT_TIMESTAMP;
    """
    cursor.executemany(query, records)
    conn.commit()
    cursor.close()
    conn.close()
    return {"inserted_rows": len(df)}
