from datetime import datetime, timedelta
import os
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator

from src.etl import extract_batch, transform_and_validate, load_processed_data
from src.ml import run_batch_predictions
from src.db import log_pipeline_run

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=8),
}

with DAG(
    dag_id='financial_transaction_etl_ml_v1',
    default_args=DEFAULT_ARGS,
    description='Incremental ETL and ML prediction pipeline for transaction fraud detection',
    schedule_interval='*/10 * * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['production', 'etl', 'ml', 'fintech'],
) as dag:

    def task_extract(**context):
        data_interval_start = context['data_interval_start']
        data_interval_end = context['data_interval_end']
        raw_df, metrics = extract_batch(data_interval_start, data_interval_end)
        return {
            'extract_count': len(raw_df),
            'raw_records_json': raw_df.to_json(orient='records', date_format='iso'),
            'metrics': metrics
        }

    def task_transform(**context):
        ti = context['ti']
        extract_res = ti.xcom_pull(task_ids='extract_raw_data')
        import pandas as pd
        raw_df = pd.read_json(extract_res['raw_records_json'])
        clean_df, transform_metrics = transform_and_validate(raw_df)
        return {
            'clean_records_json': clean_df.to_json(orient='records', date_format='iso'),
            'transform_metrics': transform_metrics
        }

    def task_load(**context):
        ti = context['ti']
        transform_res = ti.xcom_pull(task_ids='transform_and_validate_data')
        import pandas as pd
        clean_df = pd.read_json(transform_res['clean_records_json'])
        load_metrics = load_processed_data(clean_df)
        return load_metrics

    def task_predict(**context):
        ti = context['ti']
        transform_res = ti.xcom_pull(task_ids='transform_and_validate_data')
        import pandas as pd
        clean_df = pd.read_json(transform_res['clean_records_json'])
        model_version = os.getenv("MODEL_VERSION", "v1.0.0")
        prediction_metrics = run_batch_predictions(clean_df, model_version=model_version)
        return prediction_metrics

    def task_audit_log(**context):
        ti = context['ti']
        extract_res = ti.xcom_pull(task_ids='extract_raw_data') or {}
        transform_res = ti.xcom_pull(task_ids='transform_and_validate_data') or {}
        load_res = ti.xcom_pull(task_ids='load_data') or {}
        predict_res = ti.xcom_pull(task_ids='predict_fraud') or {}
        
        run_id = context['run_id']
        execution_date = context['logical_date']
        
        audit_payload = {
            'dag_run_id': run_id,
            'execution_date': execution_date,
            'extracted_rows': extract_res.get('extract_count', 0),
            'validated_rows': transform_res.get('transform_metrics', {}).get('valid_rows', 0),
            'rejected_rows': transform_res.get('transform_metrics', {}).get('rejected_rows', 0),
            'inserted_rows': load_res.get('inserted_rows', 0),
            'prediction_count': predict_res.get('prediction_count', 0),
            'status': 'SUCCESS'
        }
        log_pipeline_run(audit_payload)
        logger.info(f"Audit log successfully saved for run {run_id}")

    extract_op = PythonOperator(task_id='extract_raw_data', python_callable=task_extract)
    transform_op = PythonOperator(task_id='transform_and_validate_data', python_callable=task_transform)
    load_op = PythonOperator(task_id='load_data', python_callable=task_load)
    predict_op = PythonOperator(task_id='predict_fraud', python_callable=task_predict)
    audit_op = PythonOperator(task_id='audit_pipeline_run', python_callable=task_audit_log)

    extract_op >> transform_op >> load_op >> predict_op >> audit_op
