"""Pytest fixtures and configuration for test suite."""

import pytest
import pandas as pd
from datetime import datetime, timedelta
import io
import gzip


@pytest.fixture
def sample_cur_data():
    """Generate sample CUR data for testing."""
    dates = pd.date_range(start='2024-01-01', end='2024-01-31', freq='D')

    data = []
    for date in dates:
        # Generate sample records
        for service in ['AmazonEC2', 'AmazonS3', 'AmazonRDS', 'AWSLambda']:
            for account in ['123456789012', '210987654321']:
                data.append({
                    'line_item_usage_start_date': date,
                    'line_item_usage_account_id': account,
                    'line_item_product_code': service,
                    'line_item_unblended_cost': 100.50 + (hash(f"{date}{service}{account}") % 100),
                    'line_item_usage_type': f'{service}:BoxUsage',
                    'line_item_operation': 'RunInstances',
                    'product_region': 'us-east-1',
                    'line_item_resource_id': f'i-{hash(f"{date}{service}{account}") % 1000000:06d}'
                })

    return pd.DataFrame(data)


@pytest.fixture
def sample_cur_csv_content(sample_cur_data):
    """Generate CSV content from sample data."""
    return sample_cur_data.to_csv(index=False).encode('utf-8')


@pytest.fixture
def sample_cur_csv_gz_content(sample_cur_csv_content):
    """Generate gzipped CSV content."""
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode='wb') as gz:
        gz.write(sample_cur_csv_content)
    return buffer.getvalue()


@pytest.fixture
def mock_s3_objects():
    """Generate mock S3 object list."""
    return [
        {
            'Key': 'cur-reports/test-cur/20240101-20240201/test-cur-00001.csv.gz',
            'LastModified': datetime(2024, 1, 15),
            'Size': 1024
        },
        {
            'Key': 'cur-reports/test-cur/20240101-20240201/test-cur-00002.csv.gz',
            'LastModified': datetime(2024, 1, 16),
            'Size': 1024
        },
        {
            'Key': 'cur-reports/test-cur/20240201-20240301/test-cur-00001.csv.gz',
            'LastModified': datetime(2024, 2, 15),
            'Size': 1024
        }
    ]


@pytest.fixture
def sample_aggregated_data():
    """Generate sample aggregated data for testing processor outputs."""
    return {
        'cost_by_service': pd.DataFrame({
            'service': ['AmazonEC2', 'AmazonS3', 'AmazonRDS'],
            'total_cost': [1500.00, 800.00, 600.00]
        }),
        'cost_by_account': pd.DataFrame({
            'account_id': ['123456789012', '210987654321'],
            'total_cost': [2000.00, 900.00]
        }),
        'daily_trend': pd.DataFrame({
            'date': pd.date_range(start='2024-01-01', periods=10),
            'total_cost': [100.0, 110.0, 105.0, 120.0, 115.0, 130.0, 125.0, 140.0, 135.0, 150.0],
            '7_day_ma': [100.0, 105.0, 105.0, 108.75, 110.0, 113.33, 115.71, 120.71, 124.29, 130.0],
            '30_day_ma': [100.0, 105.0, 105.0, 108.75, 110.0, 113.33, 115.71, 120.71, 124.29, 130.0]
        })
    }


@pytest.fixture
def temp_output_dir(tmp_path):
    """Create a temporary output directory for tests."""
    output_dir = tmp_path / "test_reports"
    output_dir.mkdir()
    return output_dir


@pytest.fixture
def mock_env_vars(monkeypatch):
    """Set mock environment variables for testing."""
    monkeypatch.setenv('CUR_BUCKET', 'test-bucket')
    monkeypatch.setenv('CUR_PREFIX', 'cur-reports/test-cur')
    monkeypatch.setenv('AWS_REGION', 'us-east-1')
    monkeypatch.setenv('AWS_PROFILE', 'test-profile')
    monkeypatch.setenv('OUTPUT_DIR', 'test-reports')
    monkeypatch.setenv('TOP_N', '10')
