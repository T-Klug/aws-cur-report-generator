"""Tests for data processor module."""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

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

        assert 'cost' in processor.normalized_columns
        assert 'usage_date' in processor.normalized_columns
        assert 'account_id' in processor.normalized_columns
        assert 'service' in processor.normalized_columns

        # Check that mappings are correct
        assert processor.normalized_columns['cost'] == 'line_item_unblended_cost'
        assert processor.normalized_columns['usage_date'] == 'line_item_usage_start_date'

    def test_prepare_data(self, sample_cur_data):
        """Test data preparation and cleaning."""
        processor = CURDataProcessor(sample_cur_data)
        prepared_df = processor.prepare_data()

        assert len(prepared_df) > 0
        assert 'cost' in prepared_df.columns
        assert 'usage_date' in prepared_df.columns
        assert 'year_month' in prepared_df.columns
        assert 'date' in prepared_df.columns

        # Check that costs are numeric
        assert pd.api.types.is_numeric_dtype(prepared_df['cost'])

        # Check that no costs are negative
        assert (prepared_df['cost'] > 0).all()

    def test_prepare_data_removes_zero_costs(self):
        """Test that zero and negative costs are removed."""
        df = pd.DataFrame({
            'line_item_usage_start_date': ['2024-01-01'] * 5,
            'line_item_usage_account_id': ['123456789012'] * 5,
            'line_item_product_code': ['AmazonEC2'] * 5,
            'line_item_unblended_cost': [100.0, 0.0, -50.0, 200.0, 0.5],
            'line_item_usage_type': ['BoxUsage'] * 5,
            'line_item_operation': ['RunInstances'] * 5,
            'product_region': ['us-east-1'] * 5,
        })

        processor = CURDataProcessor(df)
        prepared_df = processor.prepare_data()

        # Only 3 rows should remain (positive costs)
        assert len(prepared_df) == 3

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
        assert 'service' in cost_by_service.columns
        assert 'total_cost' in cost_by_service.columns
        assert len(cost_by_service) > 0

        # Check sorted descending
        assert cost_by_service['total_cost'].is_monotonic_decreasing

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
        assert 'account_id' in cost_by_account.columns
        assert 'total_cost' in cost_by_account.columns
        assert len(cost_by_account) > 0

    def test_get_cost_by_account_and_service(self, sample_cur_data):
        """Test cost breakdown by account and service."""
        processor = CURDataProcessor(sample_cur_data)
        breakdown = processor.get_cost_by_account_and_service(
            top_accounts=2,
            top_services=2
        )

        assert isinstance(breakdown, pd.DataFrame)
        assert 'account_id' in breakdown.columns
        assert 'service' in breakdown.columns
        assert 'total_cost' in breakdown.columns
        assert len(breakdown) > 0

    def test_get_daily_cost_trend(self, sample_cur_data):
        """Test daily cost trend calculation."""
        processor = CURDataProcessor(sample_cur_data)
        daily_trend = processor.get_daily_cost_trend()

        assert isinstance(daily_trend, pd.DataFrame)
        assert 'date' in daily_trend.columns
        assert 'total_cost' in daily_trend.columns
        assert '7_day_ma' in daily_trend.columns
        assert '30_day_ma' in daily_trend.columns

        # Check sorted by date
        assert daily_trend['date'].is_monotonic_increasing

    def test_get_cost_trend_by_service(self, sample_cur_data):
        """Test cost trends over time for services."""
        processor = CURDataProcessor(sample_cur_data)
        service_trend = processor.get_cost_trend_by_service(top_services=2)

        assert isinstance(service_trend, pd.DataFrame)
        assert 'date' in service_trend.columns
        assert 'service' in service_trend.columns
        assert 'total_cost' in service_trend.columns
        assert len(service_trend) > 0

    def test_get_cost_trend_by_account(self, sample_cur_data):
        """Test cost trends over time for accounts."""
        processor = CURDataProcessor(sample_cur_data)
        account_trend = processor.get_cost_trend_by_account(top_accounts=2)

        assert isinstance(account_trend, pd.DataFrame)
        assert 'date' in account_trend.columns
        assert 'account_id' in account_trend.columns
        assert 'total_cost' in account_trend.columns
        assert len(account_trend) > 0

    def test_get_monthly_summary(self, sample_cur_data):
        """Test monthly cost summary."""
        processor = CURDataProcessor(sample_cur_data)
        monthly_summary = processor.get_monthly_summary()

        assert isinstance(monthly_summary, pd.DataFrame)
        assert 'month' in monthly_summary.columns
        assert 'total_cost' in monthly_summary.columns
        assert 'avg_daily_cost' in monthly_summary.columns
        assert 'num_records' in monthly_summary.columns

    def test_detect_cost_anomalies(self):
        """Test cost anomaly detection."""
        # Create data with a clear anomaly
        dates = pd.date_range(start='2024-01-01', periods=30)
        costs = [100.0] * 30
        costs[15] = 1000.0  # Anomaly

        df = pd.DataFrame({
            'line_item_usage_start_date': dates,
            'line_item_usage_account_id': ['123456789012'] * 30,
            'line_item_product_code': ['AmazonEC2'] * 30,
            'line_item_unblended_cost': costs,
            'line_item_usage_type': ['BoxUsage'] * 30,
            'line_item_operation': ['RunInstances'] * 30,
            'product_region': ['us-east-1'] * 30,
        })

        processor = CURDataProcessor(df)
        anomalies = processor.detect_cost_anomalies(threshold_std=2.0)

        assert isinstance(anomalies, pd.DataFrame)
        assert len(anomalies) > 0  # Should detect the anomaly
        assert 'z_score' in anomalies.columns

    def test_detect_cost_anomalies_none(self):
        """Test anomaly detection with consistent data."""
        # Create data with no anomalies
        dates = pd.date_range(start='2024-01-01', periods=30)
        costs = [100.0] * 30

        df = pd.DataFrame({
            'line_item_usage_start_date': dates,
            'line_item_usage_account_id': ['123456789012'] * 30,
            'line_item_product_code': ['AmazonEC2'] * 30,
            'line_item_unblended_cost': costs,
            'line_item_usage_type': ['BoxUsage'] * 30,
            'line_item_operation': ['RunInstances'] * 30,
            'product_region': ['us-east-1'] * 30,
        })

        processor = CURDataProcessor(df)
        anomalies = processor.detect_cost_anomalies(threshold_std=2.0)

        # Should find no anomalies
        assert len(anomalies) == 0

    def test_get_cost_by_region(self, sample_cur_data):
        """Test cost aggregation by region."""
        processor = CURDataProcessor(sample_cur_data)
        cost_by_region = processor.get_cost_by_region()

        assert isinstance(cost_by_region, pd.DataFrame)
        assert 'region' in cost_by_region.columns
        assert 'total_cost' in cost_by_region.columns

    def test_get_summary_statistics(self, sample_cur_data):
        """Test summary statistics generation."""
        processor = CURDataProcessor(sample_cur_data)
        summary = processor.get_summary_statistics()

        assert isinstance(summary, dict)
        assert 'total_cost' in summary
        assert 'average_daily_cost' in summary
        assert 'min_daily_cost' in summary
        assert 'max_daily_cost' in summary
        assert 'num_accounts' in summary
        assert 'num_services' in summary
        assert 'date_range_start' in summary
        assert 'date_range_end' in summary
        assert 'total_records' in summary

        # Check types
        assert isinstance(summary['total_cost'], float)
        assert isinstance(summary['num_accounts'], int)
        assert isinstance(summary['num_services'], int)

    def test_alternative_column_format(self):
        """Test handling of alternative CUR column format."""
        df = pd.DataFrame({
            'lineItem/UsageStartDate': pd.date_range('2024-01-01', periods=10),
            'lineItem/UsageAccountId': ['123456789012'] * 10,
            'lineItem/ProductCode': ['AmazonEC2'] * 10,
            'lineItem/UnblendedCost': [100.0] * 10,
            'lineItem/UsageType': ['BoxUsage'] * 10,
            'lineItem/Operation': ['RunInstances'] * 10,
            'product/region': ['us-east-1'] * 10,
        })

        processor = CURDataProcessor(df)
        prepared_df = processor.prepare_data()

        # Should still work with alternative format
        assert len(prepared_df) > 0
        assert 'cost' in prepared_df.columns
        assert 'usage_date' in prepared_df.columns

    def test_empty_dataframe(self):
        """Test handling of empty DataFrame."""
        df = pd.DataFrame()
        processor = CURDataProcessor(df)

        # Should not crash
        assert processor.df.empty
