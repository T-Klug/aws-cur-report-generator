"""Pytest fixtures and configuration for test suite."""

import gzip
import io
from datetime import datetime

import pandas as pd
import polars as pl
import pytest


@pytest.fixture
def sample_cur_data():
    """Generate 6 months of realistic CUR data with multiple accounts and comprehensive service coverage."""
    import random

    random.seed(42)  # For reproducibility

    # 6 months of monthly data (Jan - Jun 2024)
    dates = pd.date_range(start="2024-01-01", end="2024-06-01", freq="MS")
    regions = [
        "us-east-1",
        "us-west-2",
        "eu-west-1",
        "eu-central-1",
        "ap-southeast-1",
        "ap-northeast-1",
        "ca-central-1",
    ]

    data = []

    for i, date in enumerate(dates):
        month_num = i  # 0-5 for Jan-Jun

        # ========================================
        # PRODUCTION ACCOUNT (111111111111)
        # Enterprise workload with high, steady costs
        # ========================================
        prod_growth = 1 + (month_num * 0.15)  # 15% growth per month

        services_prod = {
            "AmazonEC2": {
                "base": 350000 * prod_growth,
                "pattern": lambda: random.uniform(0.97, 1.03),
            },
            "AmazonRDS": {
                "base": 255000 * prod_growth,
                "pattern": lambda: random.uniform(0.98, 1.02),  # Very stable
            },
            "AmazonS3": {
                "base": 125000 + (month_num * 8000),  # Continuous growth
                "pattern": lambda: random.uniform(0.99, 1.01),
            },
            "AmazonEKS": {
                "base": 204000 * prod_growth,
                "pattern": lambda: random.uniform(0.96, 1.04),
            },
            "AWSLambda": {
                "base": 84000,
                "pattern": lambda: random.uniform(0.90, 1.10),
            },
            "AmazonCloudFront": {
                "base": 165000,
                "pattern": lambda: random.uniform(0.92, 1.08),
            },
            "AmazonDynamoDB": {
                "base": 96000 * prod_growth,
                "pattern": lambda: random.uniform(0.95, 1.05),
            },
            "AmazonElastiCache": {
                "base": 72000 * prod_growth,
                "pattern": lambda: random.uniform(0.97, 1.03),
            },
            "AmazonRedshift": {
                "base": 216000 * prod_growth,
                "pattern": lambda: random.uniform(0.98, 1.02),
            },
            "AmazonRoute53": {
                "base": 25500,
                "pattern": lambda: random.uniform(0.95, 1.05),
            },
            "AmazonCloudWatch": {
                "base": 54000,
                "pattern": lambda: random.uniform(0.92, 1.08),
            },
            "AmazonECS": {
                "base": 135000 * prod_growth,
                "pattern": lambda: random.uniform(0.94, 1.06),
            },
        }

        # ========================================
        # STAGING ACCOUNT (222222222222)
        # Medium-sized with moderate growth
        # ========================================
        staging_growth = 1 + (month_num * 0.08)

        services_staging = {
            "AmazonEC2": {
                "base": 135000 * staging_growth,
                "pattern": lambda: random.uniform(0.85, 1.15),
            },
            "AmazonRDS": {
                "base": 96000 * staging_growth,
                "pattern": lambda: random.uniform(0.90, 1.10),
            },
            "AmazonS3": {
                "base": 54000 + (month_num * 3000),
                "pattern": lambda: random.uniform(0.95, 1.05),
            },
            "AmazonEKS": {
                "base": 84000 * staging_growth,
                "pattern": lambda: random.uniform(0.88, 1.12),
            },
            "AWSLambda": {
                "base": 27000,
                "pattern": lambda: random.uniform(0.80, 1.20),
            },
            "AmazonCloudFront": {
                "base": 36000,
                "pattern": lambda: random.uniform(0.85, 1.15),
            },
            "AmazonDynamoDB": {
                "base": 42000 * staging_growth,
                "pattern": lambda: random.uniform(0.92, 1.08),
            },
            "AmazonElastiCache": {
                "base": 24000 * staging_growth,
                "pattern": lambda: random.uniform(0.95, 1.05),
            },
        }

        # ========================================
        # DEVELOPMENT ACCOUNT (333333333333)
        # Small, variable usage with occasional load testing
        # ========================================
        is_load_test_month = month_num % 2 == 0  # Load tests in Jan, Mar, May

        services_dev = {
            "AmazonEC2": {
                "base": 45000,
                "pattern": lambda: (
                    random.uniform(1.5, 2.5) if is_load_test_month else random.uniform(0.6, 1.2)
                ),
            },
            "AmazonRDS": {
                "base": 54000,
                "pattern": lambda: random.uniform(0.7, 1.3),
            },
            "AmazonS3": {
                "base": 18000 + (month_num * 1500),
                "pattern": lambda: random.uniform(0.90, 1.10),
            },
            "AmazonEKS": {
                "base": 36000,
                "pattern": lambda: (
                    random.uniform(1.8, 2.8) if is_load_test_month else random.uniform(0.5, 1.0)
                ),
            },
            "AWSLambda": {
                "base": 9000,
                "pattern": lambda: (
                    random.uniform(2.0, 4.0) if is_load_test_month else random.uniform(0.5, 1.5)
                ),
            },
            "AmazonCloudFront": {
                "base": 15000,
                "pattern": lambda: random.uniform(0.6, 1.4),
            },
            "AmazonDynamoDB": {
                "base": max(27000 - (month_num * 3000), 5000),  # Migrating away
                "pattern": lambda: random.uniform(0.8, 1.2),
            },
        }

        # ========================================
        # SANDBOX ACCOUNT (444444444444)
        # Very small, experimental workloads
        # ========================================
        services_sandbox = {
            "AmazonEC2": {
                "base": 12000,
                "pattern": lambda: random.uniform(0.3, 2.0),  # Highly variable
            },
            "AmazonRDS": {
                "base": 10500,
                "pattern": lambda: random.uniform(0.5, 1.5),
            },
            "AmazonS3": {
                "base": 5400 + (month_num * 300),
                "pattern": lambda: random.uniform(0.9, 1.1),
            },
            "AWSLambda": {
                "base": 3600,
                "pattern": lambda: random.uniform(0.2, 2.5),
            },
            "AmazonDynamoDB": {
                "base": 6000,
                "pattern": lambda: random.uniform(0.5, 2.0),
            },
        }

        # ========================================
        # SECURITY ACCOUNT (555555555555)
        # Security & compliance tooling, steady usage
        # ========================================
        services_security = {
            "AmazonEC2": {
                "base": 84000,
                "pattern": lambda: random.uniform(0.98, 1.02),  # Very steady
            },
            "AmazonS3": {
                "base": 105000 + (month_num * 5000),  # Log storage, continuous growth
                "pattern": lambda: random.uniform(0.99, 1.01),
            },
            "AmazonCloudWatch": {
                "base": 66000,
                "pattern": lambda: random.uniform(0.96, 1.04),
            },
            "AWSLambda": {
                "base": 45000,
                "pattern": lambda: random.uniform(0.92, 1.08),
            },
            "AmazonGuardDuty": {
                "base": 54000,
                "pattern": lambda: random.uniform(0.97, 1.03),
            },
            "AWSSecurityHub": {
                "base": 27000,
                "pattern": lambda: random.uniform(0.98, 1.02),
            },
            "AWSCloudTrail": {
                "base": 19500,
                "pattern": lambda: random.uniform(0.96, 1.04),
            },
        }

        # Combine all account services
        all_account_services = [
            ("111111111111", services_prod, "us-east-1"),
            ("222222222222", services_staging, "us-west-2"),
            ("333333333333", services_dev, "eu-west-1"),
            ("444444444444", services_sandbox, "us-east-1"),
            ("555555555555", services_security, "us-east-1"),
        ]

        # Generate records for all accounts and services
        for account_id, services, primary_region in all_account_services:
            for service_name, service_config in services.items():
                base_cost = service_config["base"]
                cost = base_cost * service_config["pattern"]()

                # Randomly distribute some services across regions
                if service_name in ["AmazonS3", "AWSLambda", "AmazonCloudFront"]:
                    region = random.choice(regions)
                else:
                    region = primary_region

                data.append(
                    {
                        "line_item_usage_start_date": date,
                        "line_item_usage_account_id": account_id,
                        "line_item_product_code": service_name,
                        "line_item_unblended_cost": round(max(10.0, cost), 2),
                        "line_item_usage_type": f"{region}:{service_name}:Usage",
                        "line_item_operation": (
                            "RunInstances"
                            if service_name in ["AmazonEC2", "AmazonEKS", "AmazonECS"]
                            else "StandardStorage"
                        ),
                        "product_region": region,
                        "line_item_resource_id": f'{service_name[6:9].lower()}-{hash(f"{date}{service_name}{account_id}") % 1000000:06d}',
                    }
                )

    return pl.DataFrame(data)


@pytest.fixture
def sample_cur_csv_content(sample_cur_data):
    """Generate CSV content from sample data."""
    return sample_cur_data.to_pandas().to_csv(index=False).encode("utf-8")


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
