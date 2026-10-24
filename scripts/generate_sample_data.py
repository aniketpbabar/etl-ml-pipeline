import sqlite3
import uuid
import random
from datetime import datetime, timedelta, timezone

DB_PATH = "fintech_db.db"

def generate_data(num_records=100):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    categories = ['RETAIL', 'TRAVEL', 'ENTERTAINMENT', 'UTILITIES', None]
    records = []
    now = datetime.now(timezone.utc)
    for _ in range(num_records):
        txn_id = str(uuid.uuid4())
        user_id = f"USR_{random.randint(1000, 9999)}"
        amount = round(random.uniform(-10.0, 500.0), 2)
        category = random.choice(categories)
        timestamp = (now - timedelta(minutes=random.randint(0, 30))).isoformat()
        records.append((txn_id, user_id, amount, category, timestamp))
        
    query = """
    INSERT INTO raw_transactions (transaction_id, user_id, amount, merchant_category, transaction_timestamp)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(transaction_id) DO NOTHING;
    """
    cursor.executemany(query, records)
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Successfully generated {num_records} sample records in raw_transactions.")

if __name__ == "__main__":
    generate_data()
