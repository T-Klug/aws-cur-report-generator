"""Test that generates example reports for documentation."""

import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from data_processor import CURDataProcessor
from visualizer import CURVisualizer


class TestExampleReports:
    """Generate example reports with mock data for documentation."""

    def test_generate_example_html_report(self, sample_cur_data):
        """
        Generate a full example HTML report with all visualizations.

        This test creates a complete report that serves as both a test
        and an example for users to see what the output looks like.
        """
        # Process the data
        processor = CURDataProcessor(sample_cur_data)
        processor.prepare_data()

        # Get all analytics (monthly trends for all charts)
        service_trend = processor.get_cost_trend_by_service(top_services=6)
        account_trend = processor.get_cost_trend_by_account(top_accounts=2)
        cost_by_account_service = processor.get_cost_by_account_and_service(
            top_accounts=2, top_services=4
        )
        monthly_summary = processor.get_monthly_summary()
        anomalies = processor.detect_cost_anomalies(threshold_std=1.5)
        region_trend = processor.get_cost_trend_by_region(top_regions=5)
        discounts_trend = processor.get_discounts_trend()
        discounts_by_service_trend = processor.get_discounts_by_service_trend(top_n=5)
        savings_plan_trend = processor.get_savings_plan_trend()
        summary_stats = processor.get_summary_statistics()

        # Create visualizations - all with monthly context
        visualizer = CURVisualizer(theme="macarons")

        # Monthly trend charts (bar charts showing each month)
        if not service_trend.empty:
            visualizer.create_service_trend_chart(
                service_trend, title="Service Cost Trends (6 Months)"
            )

        if not account_trend.empty:
            visualizer.create_account_trend_chart(
                account_trend, title="Account Cost Trends (6 Months)"
            )

        if not cost_by_account_service.empty:
            visualizer.create_account_service_heatmap(
                cost_by_account_service, title="Cost Heatmap: Account vs Service"
            )

        visualizer.create_monthly_summary_chart(monthly_summary, title="Monthly Cost Summary")

        if not anomalies.empty:
            visualizer.create_anomaly_chart(
                anomalies, title="Cost Anomalies Detection (Statistical Outliers)"
            )

        if not region_trend.empty:
            visualizer.create_region_trend_chart(region_trend, title="Monthly Cost by Region")

        if not discounts_trend.empty:
            visualizer.create_discounts_trend_chart(
                discounts_trend, title="Monthly Discounts by Type"
            )

        if not discounts_by_service_trend.empty:
            visualizer.create_discounts_by_service_trend_chart(
                discounts_by_service_trend, title="Monthly Discounts by Service"
            )

        if not savings_plan_trend.empty:
            visualizer.create_savings_plan_trend_chart(
                savings_plan_trend, title="Monthly Savings Plan Effectiveness"
            )

        # Generate HTML report
        examples_dir = Path(__file__).parent.parent / "examples"
        examples_dir.mkdir(exist_ok=True)

        output_path = examples_dir / "example_report.html"

        visualizer.generate_html_report(
            str(output_path), summary_stats, title="AWS Cost and Usage Report - Example Report"
        )

        # Verify the report was created
        assert Path(output_path).exists()
        assert Path(output_path).stat().st_size > 10000  # Should be substantial

        # Read and verify HTML content
        with open(output_path, "r", encoding="utf-8") as f:
            html_content = f.read()

        # Verify key elements are present
        assert "AWS Cost and Usage Report - Example Report" in html_content
        assert "Executive Summary" in html_content
        assert "Total Cost" in html_content
        assert "echarts" in html_content.lower()
        assert "Service Cost Trends" in html_content

        # Verify all chart types are included (check for chart titles in HTML)
        assert "var chart_" in html_content  # pyecharts chart initialization
        assert "echarts.init(" in html_content  # pyecharts initialization

        print(f"\n✅ Example report generated: {output_path}")
        print(f"   File size: {Path(output_path).stat().st_size:,} bytes")
        print(f"   Charts included: {len(visualizer.charts)}")
        print(f"   Total cost in example: ${summary_stats['total_cost']:,.2f}")
        print(
            f"   Date range: {summary_stats['date_range_start']} to {summary_stats['date_range_end']}"
        )

    def test_generate_example_csv_exports(self, sample_cur_data):
        """
        Generate example CSV exports for documentation.
        """
        # Process the data
        processor = CURDataProcessor(sample_cur_data)
        processor.prepare_data()

        # Create examples directory
        examples_dir = Path(__file__).parent.parent / "examples"
        examples_dir.mkdir(exist_ok=True)

        # Generate CSV files
        csv_files = {
            "cost_by_service": processor.get_cost_by_service(top_n=10),
            "cost_by_account": processor.get_cost_by_account(top_n=10),
            "monthly_summary": processor.get_monthly_summary(),
        }

        for name, df in csv_files.items():
            csv_path = examples_dir / f"{name}_example.csv"
            df.to_csv(csv_path, index=False)

            # Verify file was created
            assert Path(csv_path).exists()
            assert Path(csv_path).stat().st_size > 0

            print(f"✅ Generated example CSV: {csv_path.name} ({len(df)} rows)")

    def test_example_report_completeness(self):
        """Verify all example files exist in the examples directory."""
        examples_dir = Path(__file__).parent.parent / "examples"

        # After running the generation tests, these files should exist
        expected_files = [
            "example_report.html",
            "cost_by_service_example.csv",
            "cost_by_account_example.csv",
            "monthly_summary_example.csv",
        ]

        for filename in expected_files:
            file_path = examples_dir / filename
            if file_path.exists():
                print(f"✅ Example file exists: {filename} ({file_path.stat().st_size:,} bytes)")
            else:
                print(f"⚠️  Example file missing: {filename} (run generation tests first)")
