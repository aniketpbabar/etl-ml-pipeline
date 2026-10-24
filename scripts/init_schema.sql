CREATE TABLE IF NOT EXISTS raw_transactions (
    transaction_id VARCHAR(64) PRIMARY KEY,
    user_id VARCHAR(64) NOT NULL,
    amount NUMERIC(12, 2) NOT NULL,
    merchant_category VARCHAR(64),
    transaction_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS processed_transactions (
    transaction_id VARCHAR(64) PRIMARY KEY,
    user_id VARCHAR(64) NOT NULL,
    amount NUMERIC(12, 2) NOT NULL,
    merchant_category VARCHAR(64) NOT NULL,
    transaction_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    hour_of_day INT NOT NULL,
    day_of_week INT NOT NULL,
    amount_log NUMERIC(10, 4) NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS transaction_predictions (
    prediction_id BIGSERIAL PRIMARY KEY,
    transaction_id VARCHAR(64) NOT NULL,
    model_version VARCHAR(32) NOT NULL,
    prediction_label INT NOT NULL,
    fraud_probability NUMERIC(6, 4) NOT NULL,
    predicted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_processed_transaction FOREIGN KEY (transaction_id) REFERENCES processed_transactions(transaction_id) ON DELETE CASCADE,
    CONSTRAINT unique_txn_model UNIQUE (transaction_id, model_version)
);

CREATE TABLE IF NOT EXISTS pipeline_run_metadata (
    run_id SERIAL PRIMARY KEY,
    dag_run_id VARCHAR(128) UNIQUE NOT NULL,
    execution_date TIMESTAMP WITH TIME ZONE NOT NULL,
    extracted_rows INT NOT NULL DEFAULT 0,
    validated_rows INT NOT NULL DEFAULT 0,
    rejected_rows INT NOT NULL DEFAULT 0,
    inserted_rows INT NOT NULL DEFAULT 0,
    prediction_count INT NOT NULL DEFAULT 0,
    status VARCHAR(32) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_raw_txn_ts ON raw_transactions(transaction_timestamp);
CREATE INDEX IF NOT EXISTS idx_proc_txn_ts ON processed_transactions(transaction_timestamp);
CREATE INDEX IF NOT EXISTS idx_predictions_txn ON transaction_predictions(transaction_id);
