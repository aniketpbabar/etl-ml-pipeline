# Financial Transaction Incremental ETL & ML Prediction Pipeline

An enterprise-ready, incremental data engineering pipeline that ingests financial transaction data, performs automated schema validation and data-quality cleaning, runs a versioned Machine Learning model (Random Forest Classifier) for batch fraud detection, and loads records into target analytical database tables using idempotent upserts.

---

## 🏗️ Architecture & Data Flow

```
+---------------------+       +---------------------------+       +---------------------------+
|  Raw Transactions   |  ---> |  Validation & Cleaning    |  ---> |   Processed DB Tables     |
| (raw_transactions)  |       |  (Pandas / Feature Eng.)  |       |  (processed_transactions) |
+---------------------+       +---------------------------+       +---------------------------+
                                                                                |
                                                                                v
+---------------------+       +---------------------------+       +---------------------------+
| Audit & Run Logs    |  <--- |   ML Batch Predictions    |  <--- |    Random Forest Model    |
| (pipeline_metadata) |       | (transaction_predictions) |       |   (fraud_model_v1.pkl)    |
+---------------------+       +---------------------------+       +---------------------------+
```

---

## ⚡ Key Features

* **Incremental Extraction:** Extracts data within watermarked time execution windows.
* **Data Quality & Validation:** Deduplicates records, filters invalid/negative amounts, handles missing categories, and computes engineered features (`hour_of_day`, `day_of_week`, `amount_log`).
* **Idempotent Storage:** Built using primary keys and `ON CONFLICT DO UPDATE` upsert logic to ensure re-running the same batch never duplicates records or prediction entries.
* **ML Integration:** Integrates a Scikit-Learn Random Forest Classifier to store prediction labels (`0 = Legitimate`, `1 = Fraud`), fraud probabilities, and model versioning metadata.
* **Operational Monitoring:** Captures metrics for every run (extracted count, validated count, rejected count, loaded count, prediction count, and execution status) inside `pipeline_run_metadata`.
* **Automated Testing:** Covered with unit tests (`pytest`) verifying deduplication and validation logic.

---

## 🛠️ Technology Choices & Justification

| Layer | Technology | Justification |
| :--- | :--- | :--- |
| **Workflow Orchestrator** | Apache Airflow | Provides robust scheduling (`*/10 * * * *`), task retries, dependency management, and execution logging. |
| **Data Processing** | Pandas / NumPy | Chosen over PySpark as 10-minute transaction batch volumes (<500,000 records) fit easily into memory, delivering lower processing latency with minimal infrastructure overhead. |
| **Database** | PostgreSQL / SQLite | Relational engine providing ACID guarantees, primary keys, and unique constraints required for idempotent upserts. |
| **Machine Learning** | Scikit-Learn | Lightweight Random Forest classifier for tabular fraud classification, serialized via `joblib`. |
| **Testing** | Pytest | Unit testing framework for data transformation validation. |

---

## 🗄️ Database Schema & Tables

The database schema consists of four core tables:

1. **`raw_transactions`**: Ingestion staging table for incoming event batches.
2. **`processed_transactions`**: Cleaned, validated transactions with engineered features.
3. **`transaction_predictions`**: Inferred batch outputs linked by `transaction_id` and `model_version` (`UNIQUE(transaction_id, model_version)` constraint).
4. **`pipeline_run_metadata`**: Execution tracking and audit logging.

---

## 🚀 Setup & Execution Guide

### 1. Prerequisites
* Python 3.10+
* Git

### 2. Environment Setup
Clone the repository and install dependencies:
```bash
git clone https://github.com/aniketpbabar/etl-ml-pipeline.git
cd etl-ml-pipeline
pip install pandas numpy scikit-learn pytest sqlalchemy joblib
```

### 3. Run Unit Tests
Execute the test suite to verify data validation and deduplication logic:
```bash
pytest
```

### 4. Seed Mock Data
Generate synthetic transaction data:
```bash
python3 scripts/generate_sample_data.py
```

### 5. Execute Pipeline End-to-End
Run the ETL and ML pipeline process:
```bash
python3 -c "
from datetime import datetime, timedelta, timezone
from src.etl import extract_batch, transform_and_validate, load_processed_data
from src.ml import run_batch_predictions

start = datetime.now(timezone.utc) - timedelta(hours=1)
end = datetime.now(timezone.utc) + timedelta(hours=1)

raw_df, _ = extract_batch(start, end)
clean_df, transform_metrics = transform_and_validate(raw_df)
load_metrics = load_processed_data(clean_df)
pred_metrics = run_batch_predictions(clean_df, model_version='v1.0.0')

print('Run Metrics:', {
    'extracted': len(raw_df),
    'transformed': transform_metrics,
    'loaded': load_metrics,
    'predictions': pred_metrics
})
"
```

---

## 📊 Analytical Verification Queries

```sql
-- Query 1: Join Processed Transactions with Fraud Predictions
SELECT
    pt.transaction_id,
    pt.user_id,
    pt.amount,
    pt.merchant_category,
    tp.prediction_label AS is_fraud_predicted,
    tp.fraud_probability,
    tp.model_version
FROM processed_transactions pt
JOIN transaction_predictions tp ON pt.transaction_id = tp.transaction_id
ORDER BY tp.fraud_probability DESC
LIMIT 10;

-- Query 2: Pipeline Execution Audit Summary
SELECT
    dag_run_id,
    execution_date,
    extracted_rows,
    validated_rows,
    rejected_rows,
    prediction_count,
    status
FROM pipeline_run_metadata
ORDER BY execution_date DESC;
```