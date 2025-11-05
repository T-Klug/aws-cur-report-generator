"""Tests for visualizer module."""

import os
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from visualizer import CURVisualizer


class TestCURVisualizer:
    """Test cases for CURVisualizer class."""

    def test_initialization(self):
        """Test visualizer initialization."""
        visualizer = CURVisualizer()

        assert visualizer.theme == 'plotly_white'
        assert visualizer.figures == []

    def test_initialization_custom_theme(self):
        """Test visualizer with custom theme."""
        visualizer = CURVisualizer(theme='plotly_dark')

        assert visualizer.theme == 'plotly_dark'

    def test_create_cost_by_service_chart(self, sample_aggregated_data):
        """Test creation of cost by service chart."""
        visualizer = CURVisualizer()
        fig = visualizer.create_cost_by_service_chart(
            sample_aggregated_data['cost_by_service'],
            top_n=3
        )

        assert fig is not None
        assert len(visualizer.figures) == 1
        assert visualizer.figures[0][0] == 'cost_by_service'

        # Check figure data
        assert len(fig.data) > 0
        assert fig.data[0].type == 'bar'

    def test_create_cost_by_account_chart(self, sample_aggregated_data):
        """Test creation of cost by account chart."""
        visualizer = CURVisualizer()
        fig = visualizer.create_cost_by_account_chart(
            sample_aggregated_data['cost_by_account'],
            top_n=2
        )

        assert fig is not None
        assert len(visualizer.figures) == 1

    def test_create_daily_trend_chart(self, sample_aggregated_data):
        """Test creation of daily trend chart."""
        visualizer = CURVisualizer()
        fig = visualizer.create_daily_trend_chart(
            sample_aggregated_data['daily_trend']
        )

        assert fig is not None
        assert len(fig.data) == 3  # Daily cost + 7-day MA + 30-day MA

    def test_create_service_trend_chart(self):
        """Test creation of service trend chart."""
        df = pd.DataFrame({
            'date': pd.date_range('2024-01-01', periods=10).tolist() * 2,
            'service': ['AmazonEC2'] * 10 + ['AmazonS3'] * 10,
            'total_cost': [100.0] * 10 + [50.0] * 10
        })

        visualizer = CURVisualizer()
        fig = visualizer.create_service_trend_chart(df)

        assert fig is not None
        assert len(fig.data) == 2  # One trace per service

    def test_create_account_trend_chart(self):
        """Test creation of account trend chart."""
        df = pd.DataFrame({
            'date': pd.date_range('2024-01-01', periods=10).tolist() * 2,
            'account_id': ['123456789012'] * 10 + ['210987654321'] * 10,
            'total_cost': [100.0] * 10 + [50.0] * 10
        })

        visualizer = CURVisualizer()
        fig = visualizer.create_account_trend_chart(df)

        assert fig is not None
        assert len(fig.data) == 2  # One trace per account

    def test_create_account_service_heatmap(self):
        """Test creation of account-service heatmap."""
        df = pd.DataFrame({
            'account_id': ['123456789012', '123456789012', '210987654321'],
            'service': ['AmazonEC2', 'AmazonS3', 'AmazonEC2'],
            'total_cost': [1000.0, 500.0, 800.0]
        })

        visualizer = CURVisualizer()
        fig = visualizer.create_account_service_heatmap(df)

        assert fig is not None
        assert fig.data[0].type == 'heatmap'

    def test_create_cost_distribution_pie(self, sample_aggregated_data):
        """Test creation of pie chart."""
        visualizer = CURVisualizer()
        fig = visualizer.create_cost_distribution_pie(
            sample_aggregated_data['cost_by_service'],
            category='service',
            top_n=3
        )

        assert fig is not None
        assert fig.data[0].type == 'pie'

    def test_create_cost_distribution_pie_with_other(self):
        """Test pie chart with 'Other' category."""
        df = pd.DataFrame({
            'service': [f'Service{i}' for i in range(15)],
            'total_cost': [100.0] * 15
        })

        visualizer = CURVisualizer()
        fig = visualizer.create_cost_distribution_pie(
            df,
            category='service',
            top_n=5
        )

        assert fig is not None
        # Should have top 5 + "Other" = 6 items
        assert len(fig.data[0].labels) == 6
        assert 'Other' in fig.data[0].labels

    def test_create_monthly_summary_chart(self):
        """Test creation of monthly summary chart."""
        df = pd.DataFrame({
            'month': ['2024-01', '2024-02', '2024-03'],
            'total_cost': [10000.0, 12000.0, 11000.0]
        })

        visualizer = CURVisualizer()
        fig = visualizer.create_monthly_summary_chart(df)

        assert fig is not None
        assert fig.data[0].type == 'bar'

    def test_create_anomaly_chart(self):
        """Test creation of anomaly chart."""
        df = pd.DataFrame({
            'date': pd.date_range('2024-01-01', periods=5),
            'total_cost': [100.0, 110.0, 500.0, 105.0, 108.0],
            'z_score': [0.1, 0.2, 3.5, 0.15, 0.18]
        })

        visualizer = CURVisualizer()
        fig = visualizer.create_anomaly_chart(df)

        assert fig is not None
        assert fig.data[0].type == 'scatter'

    def test_create_region_chart(self):
        """Test creation of region chart."""
        df = pd.DataFrame({
            'region': ['us-east-1', 'us-west-2', 'eu-west-1'],
            'total_cost': [1000.0, 800.0, 600.0]
        })

        visualizer = CURVisualizer()
        fig = visualizer.create_region_chart(df, top_n=3)

        assert fig is not None
        assert fig.data[0].type == 'bar'

    def test_generate_html_report(self, sample_aggregated_data, temp_output_dir):
        """Test HTML report generation."""
        visualizer = CURVisualizer()

        # Create some charts
        visualizer.create_cost_by_service_chart(
            sample_aggregated_data['cost_by_service']
        )
        visualizer.create_cost_by_account_chart(
            sample_aggregated_data['cost_by_account']
        )

        summary_stats = {
            'total_cost': 10000.0,
            'average_daily_cost': 333.33,
            'num_accounts': 2,
            'num_services': 3,
            'date_range_start': '2024-01-01',
            'date_range_end': '2024-01-31',
            'max_daily_cost': 400.0,
            'total_records': 1000
        }

        output_path = temp_output_dir / 'test_report.html'
        result_path = visualizer.generate_html_report(
            str(output_path),
            summary_stats,
            title='Test Report'
        )

        assert result_path == str(output_path)
        assert Path(output_path).exists()

        # Read and verify HTML content
        with open(output_path, 'r', encoding='utf-8') as f:
            html_content = f.read()

        assert 'Test Report' in html_content
        assert '10,000.00' in html_content  # Total cost
        assert 'Summary Statistics' in html_content
        assert 'plotly' in html_content.lower()

    def test_generate_html_report_no_charts(self, temp_output_dir):
        """Test HTML report generation with no charts."""
        visualizer = CURVisualizer()

        summary_stats = {
            'total_cost': 0.0,
            'average_daily_cost': 0.0,
            'num_accounts': 0,
            'num_services': 0,
            'date_range_start': '2024-01-01',
            'date_range_end': '2024-01-31',
            'max_daily_cost': 0.0,
            'total_records': 0
        }

        output_path = temp_output_dir / 'empty_report.html'
        visualizer.generate_html_report(
            str(output_path),
            summary_stats
        )

        assert Path(output_path).exists()

    def test_multiple_charts_accumulation(self, sample_aggregated_data):
        """Test that multiple charts are accumulated correctly."""
        visualizer = CURVisualizer()

        visualizer.create_cost_by_service_chart(
            sample_aggregated_data['cost_by_service']
        )
        visualizer.create_cost_by_account_chart(
            sample_aggregated_data['cost_by_account']
        )
        visualizer.create_daily_trend_chart(
            sample_aggregated_data['daily_trend']
        )

        assert len(visualizer.figures) == 3

        # Check all figure names are unique
        figure_names = [name for name, _ in visualizer.figures]
        assert len(figure_names) == len(set(figure_names))

    def test_chart_customization(self, sample_aggregated_data):
        """Test chart customization with different parameters."""
        visualizer = CURVisualizer(theme='plotly_dark')

        fig = visualizer.create_cost_by_service_chart(
            sample_aggregated_data['cost_by_service'],
            top_n=5,
            title='Custom Title'
        )

        assert fig.layout.title.text == 'Custom Title'
        # Plotly applies the template, just verify it's not None
        assert fig.layout.template is not None
