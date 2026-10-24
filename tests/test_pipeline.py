import pytest
import pandas as pd
from src.etl import transform_and_validate

def test_transformation_deduplication_and_validation():
    sample_data = pd.DataFrame([
        {'transaction_id': 'TX1', 'user_id': 'U1', 'amount': 100.0, 'merchant_category': 'RETAIL', 'transaction_timestamp': '2026-01-01 10:00:00'},
        {'transaction_id': 'TX1', 'user_id': 'U1', 'amount': 100.0, 'merchant_category': 'RETAIL', 'transaction_timestamp': '2026-01-01 10:00:00'},
        {'transaction_id': 'TX2', 'user_id': 'U2', 'amount': -50.0, 'merchant_category': 'TRAVEL', 'transaction_timestamp': '2026-01-01 10:05:00'},
        {'transaction_id': 'TX3', 'user_id': 'U3', 'amount': 250.0, 'merchant_category': None, 'transaction_timestamp': '2026-01-01 10:10:00'},
    ])
    
    clean_df, metrics = transform_and_validate(sample_data)
    
    assert metrics['valid_rows'] == 2
    assert metrics['rejected_rows'] == 2
    assert 'TX1' in clean_df['transaction_id'].values
    assert 'TX2' not in clean_df['transaction_id'].values
    assert clean_df.loc[clean_df['transaction_id'] == 'TX3', 'merchant_category'].values[0] == 'UNKNOWN'
