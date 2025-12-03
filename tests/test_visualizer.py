"""Tests for visualizer module."""

import os
import sys
from pathlib import Path

import pandas as pd
from pyecharts.charts import Bar, HeatMap, Scatter
from pyecharts.globals import ThemeType

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from visualizer import CURVisualizer


class TestCURVisualizer:
    """Test cases for CURVisualizer class."""

    def test_initialization(self):
        """Test visualizer initialization."""
        visualizer = CURVisualizer()

        assert visualizer.theme == ThemeType.MACARONS
        assert visualizer.charts == []

    def test_initialization_custom_theme(self):
        """Test visualizer with custom theme."""
        visualizer = CURVisualizer(theme="shine")

        assert visualizer.theme == ThemeType.SHINE

    def test_create_service_trend_chart(self):
        """Test creation of service trend chart."""
        df = pd.DataFrame(
            {
                "month": ["2024-01", "2024-02", "2024-03", "2024-04", "2024-05"] * 2,
                "service": ["AmazonEC2"] * 5 + ["AmazonS3"] * 5,
                "total_cost": [100.0] * 5 + [50.0] * 5,
            }
        )

        visualizer = CURVisualizer()
        chart = visualizer.create_service_trend_chart(df)

        assert chart is not None
        assert isinstance(chart, Bar)

    def test_create_account_trend_chart(self):
        """Test creation of account trend chart."""
        df = pd.DataFrame(
            {
                "month": ["2024-01", "2024-02", "2024-03", "2024-04", "2024-05"] * 2,
                "account_id": ["123456789012"] * 5 + ["210987654321"] * 5,
                "total_cost": [100.0] * 5 + [50.0] * 5,
            }
        )

        visualizer = CURVisualizer()
        chart = visualizer.create_account_trend_chart(df)

        assert chart is not None
        assert isinstance(chart, Bar)

    def test_create_account_service_heatmap(self):
        """Test creation of account-service heatmap."""
        df = pd.DataFrame(
            {
                "account_id": ["123456789012", "123456789012", "210987654321"],
                "service": ["AmazonEC2", "AmazonS3", "AmazonEC2"],
                "total_cost": [1000.0, 500.0, 800.0],
            }
        )

        visualizer = CURVisualizer()
        chart = visualizer.create_account_service_heatmap(df)

        assert chart is not None
        assert isinstance(chart, HeatMap)

    def test_create_monthly_summary_chart(self):
        """Test creation of monthly summary chart."""
        df = pd.DataFrame(
            {"month": ["2024-01", "2024-02", "2024-03"], "total_cost": [10000.0, 12000.0, 11000.0]}
        )

        visualizer = CURVisualizer()
        chart = visualizer.create_monthly_summary_chart(df)

        assert chart is not None
        assert isinstance(chart, Bar)

    def test_create_anomaly_chart(self):
        """Test creation of anomaly chart."""
        df = pd.DataFrame(
            {
                "month": ["2024-01", "2024-02", "2024-03"],
                "service": ["AmazonEC2", "AmazonEC2", "AmazonS3"],
                "total_cost": [1000.0, 5000.0, 800.0],
                "mean_cost": [2000.0, 2000.0, 600.0],
                "z_score": [0.5, 3.5, 2.1],
                "pct_change": [-50.0, 150.0, 33.3],
            }
        )

        visualizer = CURVisualizer()
        chart = visualizer.create_anomaly_chart(df)

        assert chart is not None
        assert isinstance(chart, Scatter)

    def test_create_anomaly_chart_empty(self):
        """Test creation of anomaly chart with empty data."""
        df = pd.DataFrame()

        visualizer = CURVisualizer()
        chart = visualizer.create_anomaly_chart(df)

        assert chart is not None
        assert isinstance(chart, Scatter)

    def test_generate_html_report(self, temp_output_dir):
        """Test HTML report generation."""
        visualizer = CURVisualizer()

        # Create some charts using trend methods
        service_trend = pd.DataFrame(
            {
                "month": ["2024-01", "2024-02"] * 2,
                "service": ["AmazonEC2"] * 2 + ["AmazonS3"] * 2,
                "total_cost": [100.0, 120.0, 50.0, 60.0],
            }
        )
        visualizer.create_service_trend_chart(service_trend)

        monthly_summary = pd.DataFrame(
            {"month": ["2024-01", "2024-02"], "total_cost": [150.0, 180.0]}
        )
        visualizer.create_monthly_summary_chart(monthly_summary)

        summary_stats = {
            "total_cost": 10000.0,
            "num_accounts": 2,
            "num_services": 3,
            "date_range_start": "2024-01-01",
            "date_range_end": "2024-01-31",
            "total_records": 1000,
        }

        output_path = temp_output_dir / "test_report.html"
        result_path = visualizer.generate_html_report(
            str(output_path), summary_stats, title="Test Report"
        )

        assert result_path == str(output_path)
        assert Path(output_path).exists()

        # Read and verify HTML content
        with open(output_path, "r", encoding="utf-8") as f:
            html_content = f.read()

        assert "Test Report" in html_content
        assert "10,000.00" in html_content  # Total cost
        assert "Executive Summary" in html_content
        assert "echarts" in html_content.lower()

    def test_generate_html_report_no_charts(self, temp_output_dir):
        """Test HTML report generation with no charts."""
        visualizer = CURVisualizer()

        summary_stats = {
            "total_cost": 0.0,
            "num_accounts": 0,
            "num_services": 0,
            "date_range_start": "2024-01-01",
            "date_range_end": "2024-01-31",
            "total_records": 0,
        }

        output_path = temp_output_dir / "empty_report.html"
        visualizer.generate_html_report(str(output_path), summary_stats)

        assert Path(output_path).exists()

    def test_multiple_charts_accumulation(self):
        """Test that multiple charts are accumulated correctly."""
        visualizer = CURVisualizer()

        service_trend = pd.DataFrame(
            {
                "month": ["2024-01", "2024-02"] * 2,
                "service": ["AmazonEC2"] * 2 + ["AmazonS3"] * 2,
                "total_cost": [100.0, 120.0, 50.0, 60.0],
            }
        )
        visualizer.create_service_trend_chart(service_trend)

        account_trend = pd.DataFrame(
            {
                "month": ["2024-01", "2024-02"] * 2,
                "account_id": ["111111111111"] * 2 + ["222222222222"] * 2,
                "total_cost": [100.0, 120.0, 50.0, 60.0],
            }
        )
        visualizer.create_account_trend_chart(account_trend)

        assert len(visualizer.charts) == 2

        # Check all chart names are unique
        chart_names = [name for name, _ in visualizer.charts]
        assert len(chart_names) == len(set(chart_names))

    def test_chart_customization(self):
        """Test chart customization with different parameters."""
        visualizer = CURVisualizer(theme="dark")

        service_trend = pd.DataFrame(
            {
                "month": ["2024-01", "2024-02"] * 2,
                "service": ["AmazonEC2"] * 2 + ["AmazonS3"] * 2,
                "total_cost": [100.0, 120.0, 50.0, 60.0],
            }
        )
        chart = visualizer.create_service_trend_chart(service_trend, title="Custom Title")

        assert chart is not None
        assert isinstance(chart, Bar)
        # Chart has been created with custom parameters
        assert visualizer.theme == ThemeType.DARK

    def test_create_region_trend_chart(self):
        """Test creation of region trend chart."""
        df = pd.DataFrame(
            {
                "month": ["2024-01", "2024-02", "2024-03", "2024-04", "2024-05"] * 2,
                "region": ["us-east-1"] * 5 + ["us-west-2"] * 5,
                "total_cost": [1000.0, 1100.0, 1200.0, 1150.0, 1300.0]
                + [500.0, 550.0, 600.0, 580.0, 650.0],
            }
        )

        visualizer = CURVisualizer()
        chart = visualizer.create_region_trend_chart(df)

        assert chart is not None
        assert isinstance(chart, Bar)
        assert len(visualizer.charts) == 1
        assert visualizer.charts[0][0] == "region_trend"

    def test_create_region_trend_chart_empty(self):
        """Test region trend chart with empty data."""
        df = pd.DataFrame()

        visualizer = CURVisualizer()
        chart = visualizer.create_region_trend_chart(df)

        assert chart is not None
        assert isinstance(chart, Bar)

    def test_create_discounts_trend_chart(self):
        """Test creation of discounts trend chart."""
        df = pd.DataFrame(
            {
                "month": ["2024-01", "2024-02", "2024-03"] * 2,
                "discount_type": ["SavingsPlanNegation"] * 3 + ["EdpDiscount"] * 3,
                "total_discount": [5000.0, 5500.0, 6000.0] + [1000.0, 1100.0, 1200.0],
            }
        )

        visualizer = CURVisualizer()
        chart = visualizer.create_discounts_trend_chart(df)

        assert chart is not None
        assert isinstance(chart, Bar)
        assert len(visualizer.charts) == 1
        assert visualizer.charts[0][0] == "discounts_trend"

    def test_create_discounts_trend_chart_empty(self):
        """Test discounts trend chart with empty data."""
        df = pd.DataFrame()

        visualizer = CURVisualizer()
        chart = visualizer.create_discounts_trend_chart(df)

        assert chart is not None
        assert isinstance(chart, Bar)

    def test_create_discounts_by_service_trend_chart(self):
        """Test creation of discounts by service trend chart."""
        df = pd.DataFrame(
            {
                "month": ["2024-01", "2024-02", "2024-03"] * 2,
                "service": ["AmazonEC2"] * 3 + ["AmazonRDS"] * 3,
                "total_discount": [3000.0, 3200.0, 3500.0] + [1500.0, 1600.0, 1700.0],
            }
        )

        visualizer = CURVisualizer()
        chart = visualizer.create_discounts_by_service_trend_chart(df)

        assert chart is not None
        assert isinstance(chart, Bar)
        assert len(visualizer.charts) == 1
        assert visualizer.charts[0][0] == "discounts_by_service_trend"

    def test_create_discounts_by_service_trend_chart_empty(self):
        """Test discounts by service trend chart with empty data."""
        df = pd.DataFrame()

        visualizer = CURVisualizer()
        chart = visualizer.create_discounts_by_service_trend_chart(df)

        assert chart is not None
        assert isinstance(chart, Bar)

    def test_create_savings_plan_trend_chart(self):
        """Test creation of savings plan trend chart."""
        df = pd.DataFrame(
            {
                "month": ["2024-01", "2024-02", "2024-03", "2024-04", "2024-05"],
                "on_demand_equivalent": [10000.0, 11000.0, 12000.0, 11500.0, 13000.0],
                "savings": [3000.0, 3300.0, 3600.0, 3450.0, 3900.0],
                "savings_percentage": [30.0, 30.0, 30.0, 30.0, 30.0],
            }
        )

        visualizer = CURVisualizer()
        chart = visualizer.create_savings_plan_trend_chart(df)

        assert chart is not None
        assert isinstance(chart, Bar)
        assert len(visualizer.charts) == 1
        assert visualizer.charts[0][0] == "savings_plan_trend"

    def test_create_savings_plan_trend_chart_empty(self):
        """Test savings plan trend chart with empty data."""
        df = pd.DataFrame()

        visualizer = CURVisualizer()
        chart = visualizer.create_savings_plan_trend_chart(df)

        assert chart is not None
        assert isinstance(chart, Bar)
