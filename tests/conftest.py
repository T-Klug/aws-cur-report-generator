"""Pytest fixtures and configuration for test suite."""

import gzip
import io
from datetime import datetime

import pandas as pd
import pytest


@pytest.fixture
def sample_cur_data():
    """Generate 6 months of monthly CUR data with dramatically distinct patterns per account."""
    import random

    random.seed(42)  # For reproducibility

    # 6 months of daily data (Jan - Jun 2024)
    dates = pd.date_range(start="2024-01-01", end="2024-06-30", freq="D")
    regions = ["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1", "ap-northeast-1"]

    data = []

    for i, date in enumerate(dates):
        day_num = i
        month_num = date.month - 1  # 0-5 for Jan-Jun
        is_weekend = date.weekday() >= 5
        day_of_week = date.weekday()

        # ========================================
        # PRODUCTION ACCOUNT (123456789012)
        # Large enterprise workload with high, steady costs
        # ========================================

        # EC2: Massive, steadily growing (cloud migration in progress)
        ec2_prod_base = 8000 + (month_num * 1500)  # Big growth each month
        ec2_prod = ec2_prod_base * (0.90 if is_weekend else 1.0)
        ec2_prod *= random.uniform(0.95, 1.05)

        # RDS: Very high, stable production databases
        rds_prod_base = 6500 + (month_num * 200)  # Gradual growth
        rds_prod = rds_prod_base * random.uniform(0.98, 1.02)  # Very stable

        # S3: Large data lake, growing steadily
        s3_prod_base = 2800 + (day_num * 8)  # Continuous data accumulation
        s3_prod = s3_prod_base * random.uniform(0.99, 1.01)

        # Lambda: Heavy serverless usage, consistent
        lambda_prod_base = 1200
        lambda_prod = lambda_prod_base * random.uniform(0.8, 1.3)

        # CloudFront: High traffic production website
        cf_prod_base = 3500
        # Business hours pattern (higher during weekdays)
        cf_prod = cf_prod_base * (1.3 if day_of_week < 5 else 0.6)
        cf_prod *= random.uniform(0.9, 1.1)

        # DynamoDB: Large-scale production NoSQL
        dynamo_prod_base = 2200 + (month_num * 300)  # Scaling up
        dynamo_prod = dynamo_prod_base * random.uniform(0.95, 1.05)

        # ========================================
        # DEVELOPMENT ACCOUNT (210987654321)
        # Smaller, highly variable with test spikes
        # ========================================

        # EC2: Low baseline, huge spikes during load testing
        ec2_dev_base = 800
        # Load testing days (every ~14 days)
        if day_num % 14 in [0, 1, 2]:  # 3-day load test
            ec2_dev = ec2_dev_base * random.uniform(5.0, 8.0)  # Massive spike
        else:
            ec2_dev = ec2_dev_base * random.uniform(0.3, 0.7)  # Very low baseline

        # RDS: Medium, spiky (database testing)
        rds_dev_base = 1200
        # Testing spikes on specific days
        if day_num % 10 == 5:  # Every 10 days
            rds_dev = rds_dev_base * random.uniform(2.5, 3.5)
        else:
            rds_dev = rds_dev_base * random.uniform(0.5, 1.0)

        # S3: Small, very stable
        s3_dev_base = 350 + (day_num * 1)  # Slow growth
        s3_dev = s3_dev_base * random.uniform(0.95, 1.05)

        # Lambda: EXTREMELY spiky (integration testing)
        lambda_dev_base = 150
        # Random massive spikes
        if day_num % 7 == 3 or day_num % 11 == 7:
            lambda_dev = lambda_dev_base * random.uniform(8.0, 15.0)  # Huge spikes
        elif is_weekend:
            lambda_dev = lambda_dev_base * random.uniform(0.1, 0.3)  # Nearly zero
        else:
            lambda_dev = lambda_dev_base * random.uniform(0.5, 2.0)

        # CloudFront: Low traffic, testing only
        cf_dev_base = 400
        cf_dev = cf_dev_base * random.uniform(0.3, 1.5)
        cf_dev *= 0.2 if is_weekend else 1.0  # Off on weekends

        # DynamoDB: Small, declining (moving to RDS)
        dynamo_dev_base = 600 - (month_num * 80)  # Decreasing over time
        dynamo_dev = max(100, dynamo_dev_base * random.uniform(0.8, 1.2))

        # Add all records
        services = [
            # Production account
            ("123456789012", "AmazonEC2", ec2_prod, "us-east-1"),
            ("123456789012", "AmazonRDS", rds_prod, "us-east-1"),
            ("123456789012", "AmazonS3", s3_prod, random.choice(regions)),
            ("123456789012", "AWSLambda", lambda_prod, random.choice(regions)),
            ("123456789012", "AmazonCloudFront", cf_prod, "us-east-1"),
            ("123456789012", "AmazonDynamoDB", dynamo_prod, "us-east-1"),
            # Dev account
            ("210987654321", "AmazonEC2", ec2_dev, "us-west-2"),
            ("210987654321", "AmazonRDS", rds_dev, "us-west-2"),
            ("210987654321", "AmazonS3", s3_dev, random.choice(regions)),
            ("210987654321", "AWSLambda", lambda_dev, random.choice(regions)),
            ("210987654321", "AmazonCloudFront", cf_dev, "us-west-2"),
            ("210987654321", "AmazonDynamoDB", dynamo_dev, "us-west-2"),
        ]

        for account, service, cost, region in services:
            data.append(
                {
                    "line_item_usage_start_date": date,
                    "line_item_usage_account_id": account,
                    "line_item_product_code": service,
                    "line_item_unblended_cost": round(max(10.0, cost), 2),
                    "line_item_usage_type": f"{service}:Usage",
                    "line_item_operation": (
                        "RunInstances" if service == "AmazonEC2" else "StandardStorage"
                    ),
                    "product_region": region,
                    "line_item_resource_id": f'{service[6:9].lower()}-{hash(f"{date}{service}{account}") % 1000000:06d}',
                }
            )

    return pd.DataFrame(data)


@pytest.fixture
def sample_cur_csv_content(sample_cur_data):
    """Generate CSV content from sample data."""
    return sample_cur_data.to_csv(index=False).encode("utf-8")


@pytest.fixture
def sample_cur_csv_gz_content(sample_cur_csv_content):
    """Generate gzipped CSV content."""
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb") as gz:
        gz.write(sample_cur_csv_content)
    return buffer.getvalue()


@pytest.fixture
def mock_s3_objects():
    """Generate mock S3 object list for 6 months of CUR exports."""
    return [
        # January 2024 - available at end of January
        {
            "Key": "cur-reports/test-cur/20240101-20240201/test-cur-00001.csv.gz",
            "LastModified": datetime(2024, 1, 31),
            "Size": 2048,
        },
        # February 2024 - available at end of February
        {
            "Key": "cur-reports/test-cur/20240201-20240301/test-cur-00001.csv.gz",
            "LastModified": datetime(2024, 2, 29),
            "Size": 2048,
        },
        # March 2024 - available at end of March
        {
            "Key": "cur-reports/test-cur/20240301-20240401/test-cur-00001.csv.gz",
            "LastModified": datetime(2024, 3, 31),
            "Size": 2048,
        },
        # April 2024 - available at end of April
        {
            "Key": "cur-reports/test-cur/20240401-20240501/test-cur-00001.csv.gz",
            "LastModified": datetime(2024, 4, 30),
            "Size": 2048,
        },
        # May 2024 - available at end of May
        {
            "Key": "cur-reports/test-cur/20240501-20240601/test-cur-00001.csv.gz",
            "LastModified": datetime(2024, 5, 31),
            "Size": 2048,
        },
        # June 2024 - available at end of June
        {
            "Key": "cur-reports/test-cur/20240601-20240701/test-cur-00001.csv.gz",
            "LastModified": datetime(2024, 6, 30),
            "Size": 2048,
        },
    ]


@pytest.fixture
def sample_aggregated_data():
    """Generate sample aggregated data for testing processor outputs."""
    return {
        "cost_by_service": pd.DataFrame(
            {
                "service": ["AmazonEC2", "AmazonS3", "AmazonRDS"],
                "total_cost": [1500.00, 800.00, 600.00],
            }
        ),
        "cost_by_account": pd.DataFrame(
            {"account_id": ["123456789012", "210987654321"], "total_cost": [2000.00, 900.00]}
        ),
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
    monkeypatch.setenv("CUR_BUCKET", "test-bucket")
    monkeypatch.setenv("CUR_PREFIX", "cur-reports/test-cur")
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("AWS_PROFILE", "test-profile")
    monkeypatch.setenv("OUTPUT_DIR", "test-reports")
    monkeypatch.setenv("TOP_N", "10")
