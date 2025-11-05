"""Pytest fixtures and configuration for test suite."""

import pytest
import pandas as pd
from datetime import datetime, timedelta
import io
import gzip


@pytest.fixture
def sample_cur_data():
    """Generate sample CUR data with distinct patterns for each service."""
    import random
    import math
    random.seed(42)  # For reproducibility

    dates = pd.date_range(start='2024-01-01', end='2024-01-31', freq='D')
    regions = ['us-east-1', 'us-west-2', 'eu-west-1', 'ap-southeast-1', 'ap-northeast-1']
    accounts = ['123456789012', '210987654321']

    data = []

    for i, date in enumerate(dates):
        day_of_month = i
        is_weekend = date.weekday() >= 5

        for account in accounts:
            # Account multiplier: prod is 2x dev
            account_mult = 2.0 if account == '123456789012' else 1.0

            # EC2: High base, steady upward growth (migration to cloud)
            ec2_base = 2000 + (day_of_month * 50)  # Strong growth
            ec2_cost = ec2_base * account_mult
            ec2_cost *= 0.85 if is_weekend else 1.0  # Less compute on weekends
            ec2_cost *= random.uniform(0.95, 1.05)  # Small variation

            # RDS: Medium-high, with weekly spike pattern (batch processing)
            rds_base = 1500
            # Spike every 7 days (weekly batch job)
            rds_spike = 1.8 if day_of_month % 7 == 3 else 1.0
            rds_cost = rds_base * account_mult * rds_spike
            rds_cost *= random.uniform(0.9, 1.1)

            # S3: Low and very stable (storage doesn't change much)
            s3_base = 400 + (day_of_month * 5)  # Slow growth
            s3_cost = s3_base * account_mult
            s3_cost *= random.uniform(0.98, 1.02)  # Very stable

            # Lambda: Very spiky event-driven pattern
            lambda_base = 300
            # Random spikes to simulate event-driven workloads
            if day_of_month % 5 == 0 or day_of_month % 11 == 0:
                lambda_spike = random.uniform(3.0, 5.0)  # Big spikes
            else:
                lambda_spike = random.uniform(0.5, 1.5)
            lambda_cost = lambda_base * account_mult * lambda_spike

            # CloudFront: Cyclical weekly pattern (web traffic)
            cloudfront_base = 800
            # Weekly cycle: low on weekends, peak mid-week
            day_of_week = date.weekday()
            if day_of_week in [0, 6]:  # Mon, Sun
                traffic_mult = 0.6
            elif day_of_week in [2, 3]:  # Wed, Thu peak
                traffic_mult = 1.4
            else:
                traffic_mult = 1.0
            cloudfront_cost = cloudfront_base * account_mult * traffic_mult
            cloudfront_cost *= random.uniform(0.9, 1.1)

            # DynamoDB: Step function growth (capacity increases)
            dynamo_base = 600
            # Capacity scaling events every ~10 days
            capacity_level = 1.0 + (day_of_month // 10) * 0.5
            dynamo_cost = dynamo_base * account_mult * capacity_level
            dynamo_cost *= random.uniform(0.95, 1.05)

            # Create records for each service
            services_data = [
                ('AmazonEC2', ec2_cost, 'us-east-1'),
                ('AmazonRDS', rds_cost, 'us-east-1'),
                ('AmazonS3', s3_cost, random.choice(regions)),
                ('AWSLambda', lambda_cost, random.choice(regions)),
                ('AmazonCloudFront', cloudfront_cost, random.choice(regions[:3])),
                ('AmazonDynamoDB', dynamo_cost, 'us-east-1'),
            ]

            for service, cost, region in services_data:
                data.append({
                    'line_item_usage_start_date': date,
                    'line_item_usage_account_id': account,
                    'line_item_product_code': service,
                    'line_item_unblended_cost': round(max(10.0, cost), 2),
                    'line_item_usage_type': f'{service}:Usage',
                    'line_item_operation': 'RunInstances' if service == 'AmazonEC2' else 'StandardStorage',
                    'product_region': region,
                    'line_item_resource_id': f'{service[6:9].lower()}-{hash(f"{date}{service}{account}") % 1000000:06d}'
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
