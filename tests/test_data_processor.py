"""Tests for data processor module."""

import os
import sys

import pandas as pd
import polars as pl

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from data_processor import CURDataProcessor


class TestCURDataProcessor:
    """Test cases for CURDataProcessor class."""

    def test_initialization(self, sample_cur_data):
        """Test processor initialization."""
        processor = CURDataProcessor(sample_cur_data)

        assert processor.df is not None
        assert len(processor.df) > 0
        assert processor.normalized_columns is not None

    def test_normalize_column_names(self, sample_cur_data):
        """Test column name normalization."""
        processor = CURDataProcessor(sample_cur_data)

        assert "cost" in processor.normalized_columns
        assert "usage_date" in processor.normalized_columns
        assert "account_id" in processor.normalized_columns
        assert "service" in processor.normalized_columns

        # Check that mappings are correct
        assert processor.normalized_columns["cost"] == "line_item_unblended_cost"
        assert processor.normalized_columns["usage_date"] == "line_item_usage_start_date"

    def test_prepare_data(self, sample_cur_data):
        """Test data preparation and cleaning."""
        processor = CURDataProcessor(sample_cur_data)
        prepared_df = processor.prepare_data()

        assert len(prepared_df) > 0
        assert "cost" in prepared_df.columns
        assert "usage_date" in prepared_df.columns
        assert "year_month" in prepared_df.columns
        assert "date" in prepared_df.columns

        # Check that costs are numeric
        assert prepared_df["cost"].dtype == pl.Float64

        # Note: We allow negative costs (discounts) and zero costs
        # This sample data only has positive costs, but real CUR data includes negatives

    def test_prepare_data_keeps_all_costs(self):
        """Test that all costs including zero and negative are kept.

        Negative costs represent discounts (SavingsPlanNegation, EdpDiscount, etc.)
        and must be included for accurate total cost calculation.
        """
        df = pl.DataFrame(
            {
                "line_item_usage_start_date": ["2024-01-01"] * 5,
                "line_item_usage_account_id": ["123456789012"] * 5,
                "line_item_product_code": ["AmazonEC2"] * 5,
                "line_item_unblended_cost": [100.0, 0.0, -50.0, 200.0, 0.5],
                "line_item_usage_type": ["BoxUsage"] * 5,
                "line_item_operation": ["RunInstances"] * 5,
                "product_region": ["us-east-1"] * 5,
            }
        )

        processor = CURDataProcessor(df)
        prepared_df = processor.prepare_data()

        # All 5 rows should remain (including zero and negative costs)
        assert len(prepared_df) == 5

        # Verify the total cost includes discounts
        total = prepared_df["cost"].sum()
        assert total == 250.5  # 100 + 0 + (-50) + 200 + 0.5

    def test_get_total_cost(self, sample_cur_data):
        """Test total cost calculation."""
        processor = CURDataProcessor(sample_cur_data)
        total_cost = processor.get_total_cost()

        assert isinstance(total_cost, float)
        assert total_cost > 0

    def test_get_cost_by_service(self, sample_cur_data):
        """Test cost aggregation by service."""
        processor = CURDataProcessor(sample_cur_data)
        cost_by_service = processor.get_cost_by_service()

        assert isinstance(cost_by_service, pd.DataFrame)
        assert "service" in cost_by_service.columns
        assert "total_cost" in cost_by_service.columns
        assert len(cost_by_service) > 0

        # Check sorted descending
        assert cost_by_service["total_cost"].is_monotonic_decreasing

    def test_get_cost_by_service_top_n(self, sample_cur_data):
        """Test cost by service with top_n limit."""
        processor = CURDataProcessor(sample_cur_data)
        cost_by_service = processor.get_cost_by_service(top_n=2)

        assert len(cost_by_service) == 2

    def test_get_cost_by_account(self, sample_cur_data):
        """Test cost aggregation by account."""
        processor = CURDataProcessor(sample_cur_data)
        cost_by_account = processor.get_cost_by_account()

        assert isinstance(cost_by_account, pd.DataFrame)
        assert "account_id" in cost_by_account.columns
        assert "total_cost" in cost_by_account.columns
        assert len(cost_by_account) > 0

    def test_get_cost_by_account_and_service(self, sample_cur_data):
        """Test cost breakdown by account and service."""
        processor = CURDataProcessor(sample_cur_data)
        breakdown = processor.get_cost_by_account_and_service(top_accounts=2, top_services=2)

        assert isinstance(breakdown, pd.DataFrame)
        assert "account_id" in breakdown.columns
        assert "service" in breakdown.columns
        assert "total_cost" in breakdown.columns
        assert len(breakdown) > 0

    def test_get_cost_trend_by_service(self, sample_cur_data):
        """Test monthly cost trends over time for services."""
        processor = CURDataProcessor(sample_cur_data)
        service_trend = processor.get_cost_trend_by_service(top_services=2)

        assert isinstance(service_trend, pd.DataFrame)
        assert "month" in service_trend.columns
        assert "service" in service_trend.columns
        assert "total_cost" in service_trend.columns
        assert len(service_trend) > 0

    def test_get_cost_trend_by_account(self, sample_cur_data):
        """Test monthly cost trends over time for accounts."""
        processor = CURDataProcessor(sample_cur_data)
        account_trend = processor.get_cost_trend_by_account(top_accounts=2)

        assert isinstance(account_trend, pd.DataFrame)
        assert "month" in account_trend.columns
        assert "account_id" in account_trend.columns
        assert "total_cost" in account_trend.columns
        assert len(account_trend) > 0

    def test_get_monthly_summary(self, sample_cur_data):
        """Test monthly cost summary."""
        processor = CURDataProcessor(sample_cur_data)
        monthly_summary = processor.get_monthly_summary()

        assert isinstance(monthly_summary, pd.DataFrame)
        assert "month" in monthly_summary.columns
        assert "total_cost" in monthly_summary.columns
        assert "avg_record_cost" in monthly_summary.columns
        assert "num_records" in monthly_summary.columns

    def test_detect_cost_anomalies(self):
        """Test cost anomaly detection by service and month."""
        # Create data spanning 6 months with a clear anomaly in month 3 for EC2
        dates = []
        services = []
        costs = []

        # Normal months for EC2 (months 1,2,4,5,6): 5000 total each
        # Anomaly month (month 3): 50000 total (10x higher, clearly above z-score threshold)
        for month in range(1, 7):
            for day in range(1, 6):  # 5 days per month
                # Use ISO 8601 format with timezone like real CUR data
                dates.append(f"2024-{month:02d}-{day:02d}T00:00:00Z")
                services.append("AmazonEC2")
                # Month 3 has very high costs (10x normal)
                costs.append(1000.0 if month != 3 else 10000.0)

        # Add S3 with consistent costs (no anomalies)
        for month in range(1, 7):
            for day in range(1, 6):
                dates.append(f"2024-{month:02d}-{day:02d}T00:00:00Z")
                services.append("AmazonS3")
                costs.append(500.0)

        df = pl.DataFrame(
            {
                "line_item_usage_start_date": dates,
                "line_item_usage_account_id": ["123456789012"] * len(dates),
                "line_item_product_code": services,
                "line_item_unblended_cost": costs,
                "line_item_usage_type": ["Usage"] * len(dates),
                "line_item_operation": ["Operation"] * len(dates),
                "product_region": ["us-east-1"] * len(dates),
            }
        )

        processor = CURDataProcessor(df)
        anomalies = processor.detect_cost_anomalies(threshold_std=2.0)

        assert isinstance(anomalies, pd.DataFrame)
        assert len(anomalies) > 0  # Should detect the EC2 anomaly in March
        assert "month" in anomalies.columns
        assert "service" in anomalies.columns
        assert "z_score" in anomalies.columns
        assert "pct_change" in anomalies.columns
        assert "mean_cost" in anomalies.columns

    def test_detect_cost_anomalies_none(self):
        """Test anomaly detection with consistent data."""
        # Create data spanning 6 months with consistent costs (no anomalies)
        dates = []
        services = []
        costs = []

        # Consistent costs for EC2 across all months
        for month in range(1, 7):
            for day in range(1, 6):
                dates.append(f"2024-{month:02d}-{day:02d}")
                services.append("AmazonEC2")
                costs.append(1000.0)

        df = pl.DataFrame(
            {
                "line_item_usage_start_date": dates,
                "line_item_usage_account_id": ["123456789012"] * len(dates),
                "line_item_product_code": services,
                "line_item_unblended_cost": costs,
                "line_item_usage_type": ["BoxUsage"] * len(dates),
                "line_item_operation": ["RunInstances"] * len(dates),
                "product_region": ["us-east-1"] * len(dates),
            }
        )

        processor = CURDataProcessor(df)
        anomalies = processor.detect_cost_anomalies(threshold_std=2.0)

        # Should find no anomalies (std is 0, so filtered out)
        assert len(anomalies) == 0

    def test_get_cost_by_region(self, sample_cur_data):
        """Test cost aggregation by region."""
        processor = CURDataProcessor(sample_cur_data)
        cost_by_region = processor.get_cost_by_region()

        assert isinstance(cost_by_region, pd.DataFrame)
        assert "region" in cost_by_region.columns
        assert "total_cost" in cost_by_region.columns

    def test_get_summary_statistics(self, sample_cur_data):
        """Test summary statistics generation."""
        processor = CURDataProcessor(sample_cur_data)
        summary = processor.get_summary_statistics()

        assert isinstance(summary, dict)
        assert "total_cost" in summary
        assert "num_accounts" in summary
        assert "num_services" in summary
        assert "date_range_start" in summary
        assert "date_range_end" in summary
        assert "total_records" in summary

        # Check types
        assert isinstance(summary["total_cost"], float)
        assert isinstance(summary["num_accounts"], int)
        assert isinstance(summary["num_services"], int)

    def test_alternative_column_format(self):
        """Test handling of alternative CUR column format."""
        df = pl.DataFrame(
            {
                "lineItem/UsageStartDate": pd.date_range("2024-01-01", periods=10),
                "lineItem/UsageAccountId": ["123456789012"] * 10,
                "lineItem/ProductCode": ["AmazonEC2"] * 10,
                "lineItem/UnblendedCost": [100.0] * 10,
                "lineItem/UsageType": ["BoxUsage"] * 10,
                "lineItem/Operation": ["RunInstances"] * 10,
                "product/region": ["us-east-1"] * 10,
            }
        )

        processor = CURDataProcessor(df)
        prepared_df = processor.prepare_data()

        # Should still work with alternative format
        assert len(prepared_df) > 0
        assert "cost" in prepared_df.columns
        assert "usage_date" in prepared_df.columns

    def test_empty_dataframe(self):
        """Test handling of empty DataFrame."""
        df = pl.DataFrame()
        processor = CURDataProcessor(df)

        # Should not crash
        assert processor.df.is_empty()
