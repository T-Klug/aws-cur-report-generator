"""Tests for visualizer module."""

import os
import sys
from pathlib import Path

import pandas as pd
from pyecharts.charts import Bar, HeatMap, Line, Pie, Scatter
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

    def test_create_cost_by_service_chart(self, sample_aggregated_data):
        """Test creation of cost by service chart."""
        visualizer = CURVisualizer()
        chart = visualizer.create_cost_by_service_chart(
            sample_aggregated_data["cost_by_service"], top_n=3
        )

        assert chart is not None
        assert isinstance(chart, Bar)
        assert len(visualizer.charts) == 1
        assert visualizer.charts[0][0] == "cost_by_service"

    def test_create_cost_by_account_chart(self, sample_aggregated_data):
        """Test creation of cost by account chart."""
        visualizer = CURVisualizer()
        chart = visualizer.create_cost_by_account_chart(
            sample_aggregated_data["cost_by_account"], top_n=2
        )

        assert chart is not None
        assert isinstance(chart, Bar)
        assert len(visualizer.charts) == 1

    def test_create_service_trend_chart(self):
        """Test creation of service trend chart."""
        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", periods=10).tolist() * 2,
                "service": ["AmazonEC2"] * 10 + ["AmazonS3"] * 10,
                "total_cost": [100.0] * 10 + [50.0] * 10,
            }
        )

        visualizer = CURVisualizer()
        chart = visualizer.create_service_trend_chart(df)

        assert chart is not None
        assert isinstance(chart, Line)

    def test_create_account_trend_chart(self):
        """Test creation of account trend chart."""
        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", periods=10).tolist() * 2,
                "account_id": ["123456789012"] * 10 + ["210987654321"] * 10,
                "total_cost": [100.0] * 10 + [50.0] * 10,
            }
        )

        visualizer = CURVisualizer()
        chart = visualizer.create_account_trend_chart(df)

        assert chart is not None
        assert isinstance(chart, Line)

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

    def test_create_cost_distribution_pie(self, sample_aggregated_data):
        """Test creation of pie chart."""
        visualizer = CURVisualizer()
        chart = visualizer.create_cost_distribution_pie(
            sample_aggregated_data["cost_by_service"], category="service", top_n=3
        )

        assert chart is not None
        assert isinstance(chart, Pie)

    def test_create_cost_distribution_pie_with_other(self):
        """Test pie chart with 'Other' category."""
        df = pd.DataFrame(
            {"service": [f"Service{i}" for i in range(15)], "total_cost": [100.0] * 15}
        )

        visualizer = CURVisualizer()
        chart = visualizer.create_cost_distribution_pie(df, category="service", top_n=5)

        assert chart is not None
        assert isinstance(chart, Pie)
        # The chart has been created with grouped data

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
                "date": pd.date_range("2024-01-01", periods=5),
                "total_cost": [100.0, 110.0, 500.0, 105.0, 108.0],
                "z_score": [0.1, 0.2, 3.5, 0.15, 0.18],
            }
        )

        visualizer = CURVisualizer()
        chart = visualizer.create_anomaly_chart(df)

        assert chart is not None
        assert isinstance(chart, Scatter)

    def test_create_region_chart(self):
        """Test creation of region chart."""
        df = pd.DataFrame(
            {
                "region": ["us-east-1", "us-west-2", "eu-west-1"],
                "total_cost": [1000.0, 800.0, 600.0],
            }
        )

        visualizer = CURVisualizer()
        chart = visualizer.create_region_chart(df, top_n=3)

        assert chart is not None
        assert isinstance(chart, Bar)

    def test_generate_html_report(self, sample_aggregated_data, temp_output_dir):
        """Test HTML report generation."""
        visualizer = CURVisualizer()

        # Create some charts
        visualizer.create_cost_by_service_chart(sample_aggregated_data["cost_by_service"])
        visualizer.create_cost_by_account_chart(sample_aggregated_data["cost_by_account"])

        summary_stats = {
            "total_cost": 10000.0,
            "average_daily_cost": 333.33,
            "num_accounts": 2,
            "num_services": 3,
            "date_range_start": "2024-01-01",
            "date_range_end": "2024-01-31",
            "max_daily_cost": 400.0,
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
            "average_daily_cost": 0.0,
            "num_accounts": 0,
            "num_services": 0,
            "date_range_start": "2024-01-01",
            "date_range_end": "2024-01-31",
            "max_daily_cost": 0.0,
            "total_records": 0,
        }

        output_path = temp_output_dir / "empty_report.html"
        visualizer.generate_html_report(str(output_path), summary_stats)

        assert Path(output_path).exists()

    def test_multiple_charts_accumulation(self, sample_aggregated_data):
        """Test that multiple charts are accumulated correctly."""
        visualizer = CURVisualizer()

        visualizer.create_cost_by_service_chart(sample_aggregated_data["cost_by_service"])
        visualizer.create_cost_by_account_chart(sample_aggregated_data["cost_by_account"])

        assert len(visualizer.charts) == 2

        # Check all chart names are unique
        chart_names = [name for name, _ in visualizer.charts]
        assert len(chart_names) == len(set(chart_names))

    def test_chart_customization(self, sample_aggregated_data):
        """Test chart customization with different parameters."""
        visualizer = CURVisualizer(theme="dark")

        chart = visualizer.create_cost_by_service_chart(
            sample_aggregated_data["cost_by_service"], top_n=5, title="Custom Title"
        )

        assert chart is not None
        assert isinstance(chart, Bar)
        # Chart has been created with custom parameters
        assert visualizer.theme == ThemeType.DARK
