import os
import joblib
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from sklearn.ensemble import RandomForestClassifier
from src.db import get_db_connection
import logging

logger = logging.getLogger(__name__)
MODEL_PATH = os.getenv("MODEL_PATH", "models/fraud_model_v1.pkl")

def train_and_save_mock_model(save_path: str = MODEL_PATH):
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    np.random.seed(42)
    n_samples = 1000
    amounts = np.random.exponential(scale=50, size=n_samples) + 1.0
    hours = np.random.randint(0, 24, size=n_samples)
    days = np.random.randint(0, 7, size=n_samples)
    amount_log = np.log1p(amounts)
    
    fraud_prob = (amounts > 200).astype(int) * 0.4 + ((hours < 5) | (hours > 23)).astype(int) * 0.3
    is_fraud = (np.random.rand(n_samples) < fraud_prob).astype(int)
    
    X = pd.DataFrame({'amount': amounts, 'hour_of_day': hours, 'day_of_week': days, 'amount_log': amount_log})
    y = is_fraud
    
    model = RandomForestClassifier(n_estimators=50, random_state=42)
    model.fit(X, y)
    joblib.dump(model, save_path)
    logger.info(f"Model saved successfully to {save_path}")

def run_batch_predictions(df: pd.DataFrame, model_version: str = "v1.0.0") -> dict:
    if df.empty:
        return {"prediction_count": 0}
    
    if not os.path.exists(MODEL_PATH):
        logger.info("Model artifact not found. Training baseline model...")
        train_and_save_mock_model(MODEL_PATH)
        
    model = joblib.load(MODEL_PATH)
    
    # Reset index to match sequential NumPy predictions array length
    df_pred = df.reset_index(drop=True)
    features = df_pred[['amount', 'hour_of_day', 'day_of_week', 'amount_log']]
    
    predictions = model.predict(features)
    probabilities = model.predict_proba(features)[:, 1]
    
    now_str = datetime.now(timezone.utc).isoformat()
    
    records = []
    for txn_id, pred, prob in zip(df_pred['transaction_id'], predictions, probabilities):
        records.append((
            str(txn_id),
            model_version,
            int(pred),
            float(prob),
            now_str
        ))
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    query = """
    INSERT INTO transaction_predictions
    (transaction_id, model_version, prediction_label, fraud_probability, predicted_at)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(transaction_id, model_version) DO UPDATE SET
        prediction_label = excluded.prediction_label,
        fraud_probability = excluded.fraud_probability,
        predicted_at = excluded.predicted_at;
    """
    cursor.executemany(query, records)
    conn.commit()
    cursor.close()
    conn.close()
    return {"prediction_count": len(records)}
