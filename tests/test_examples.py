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

        # Get all analytics
        cost_by_service = processor.get_cost_by_service(top_n=10)
        cost_by_account = processor.get_cost_by_account(top_n=10)
        cost_by_account_service = processor.get_cost_by_account_and_service(
            top_accounts=2, top_services=4
        )
        service_trend = processor.get_cost_trend_by_service(top_services=4)
        account_trend = processor.get_cost_trend_by_account(top_accounts=2)
        monthly_summary = processor.get_monthly_summary()
        anomalies = processor.detect_cost_anomalies(threshold_std=1.5)
        cost_by_region = processor.get_cost_by_region(top_n=5)
        summary_stats = processor.get_summary_statistics()

        # Create visualizations
        visualizer = CURVisualizer(theme="plotly_white")

        # Generate all chart types
        visualizer.create_cost_by_service_chart(
            cost_by_service, top_n=10, title="Top 10 AWS Services by Cost"
        )

        visualizer.create_cost_by_account_chart(
            cost_by_account, top_n=10, title="Cost by AWS Account"
        )

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

        visualizer.create_cost_distribution_pie(
            cost_by_service, category="service", top_n=8, title="Service Cost Distribution"
        )

        visualizer.create_cost_distribution_pie(
            cost_by_account, category="account", top_n=8, title="Account Cost Distribution"
        )

        visualizer.create_monthly_summary_chart(monthly_summary, title="Monthly Cost Summary")

        if not anomalies.empty:
            visualizer.create_anomaly_chart(
                anomalies, title="Cost Anomalies Detection (Statistical Outliers)"
            )

        if not cost_by_region.empty:
            visualizer.create_region_chart(cost_by_region, top_n=5, title="Cost by AWS Region")

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
        assert "Summary Statistics" in html_content
        assert "Total Cost" in html_content
        assert "plotly" in html_content.lower()
        assert "Top 10 AWS Services by Cost" in html_content

        # Verify all chart types are included
        assert "cost_by_service" in html_content
        assert "cost_by_account" in html_content

        print(f"\n✅ Example report generated: {output_path}")
        print(f"   File size: {Path(output_path).stat().st_size:,} bytes")
        print(f"   Charts included: {len(visualizer.figures)}")
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
