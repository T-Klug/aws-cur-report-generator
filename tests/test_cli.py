"""Tests for CLI interface."""

import os
import sys
from unittest.mock import Mock, patch

import pandas as pd
import pytest
from click.testing import CliRunner

# Add the parent directory to the path to import the main module
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))


class TestCLI:
    """Test cases for CLI interface."""

    @pytest.fixture
    def runner(self):
        """Create a CLI test runner."""
        return CliRunner()

    @pytest.fixture
    def mock_dependencies(self, sample_cur_data, sample_aggregated_data):
        """Mock all external dependencies for CLI tests."""
        with patch('cur_report_generator.load_dotenv'), \
             patch('cur_report_generator.CURReader') as mock_reader, \
             patch('cur_report_generator.CURDataProcessor') as mock_processor, \
             patch('cur_report_generator.CURVisualizer') as mock_visualizer:

            # Setup mock reader
            mock_reader_instance = Mock()
            mock_reader_instance.load_cur_data.return_value = sample_cur_data
            mock_reader.return_value = mock_reader_instance

            # Setup mock processor
            mock_processor_instance = Mock()
            mock_processor_instance.prepare_data.return_value = sample_cur_data
            mock_processor_instance.get_total_cost.return_value = 10000.0
            mock_processor_instance.get_cost_by_service.return_value = sample_aggregated_data['cost_by_service']
            mock_processor_instance.get_cost_by_account.return_value = sample_aggregated_data['cost_by_account']
            mock_processor_instance.get_cost_by_account_and_service.return_value = pd.DataFrame()
            mock_processor_instance.get_cost_trend_by_service.return_value = pd.DataFrame()
            mock_processor_instance.get_cost_trend_by_account.return_value = pd.DataFrame()
            mock_processor_instance.get_monthly_summary.return_value = pd.DataFrame({
                'month': ['2024-01'],
                'total_cost': [10000.0],
                'avg_daily_cost': [333.33],
                'num_records': [1000]
            })
            mock_processor_instance.detect_cost_anomalies.return_value = pd.DataFrame()
            mock_processor_instance.get_cost_by_region.return_value = pd.DataFrame()
            mock_processor_instance.get_summary_statistics.return_value = {
                'total_cost': 10000.0,
                'average_daily_cost': 333.33,
                'min_daily_cost': 300.0,
                'max_daily_cost': 400.0,
                'num_accounts': 2,
                'num_services': 4,
                'date_range_start': '2024-01-01',
                'date_range_end': '2024-01-31',
                'total_records': 1000
            }
            mock_processor.return_value = mock_processor_instance

            # Setup mock visualizer
            mock_visualizer_instance = Mock()
            mock_visualizer_instance.generate_html_report.return_value = 'test_report.html'
            mock_visualizer.return_value = mock_visualizer_instance

            yield {
                'reader': mock_reader,
                'processor': mock_processor,
                'visualizer': mock_visualizer
            }

    def test_help_command(self, runner):
        """Test --help flag."""
        from cur_report_generator import generate_report

        result = runner.invoke(generate_report, ['--help'])

        assert result.exit_code == 0
        assert 'Generate comprehensive AWS Cost and Usage Reports' in result.output

    def test_basic_execution(self, runner, mock_env_vars, mock_dependencies, tmp_path):
        """Test basic CLI execution with mocked dependencies."""
        from cur_report_generator import generate_report

        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(generate_report, ['--output-dir', 'test_reports'])

            # With mocked dependencies, should complete successfully
            assert result.exit_code == 0

    def test_custom_date_range(self, runner, mock_env_vars, mock_dependencies, tmp_path):
        """Test CLI with custom date range."""
        from cur_report_generator import generate_report

        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(generate_report, [
                '--start-date', '2024-01-01',
                '--end-date', '2024-01-31',
                '--output-dir', 'test_reports'
            ])

            assert result.exit_code == 0

    def test_sample_files_option(self, runner, mock_env_vars, mock_dependencies, tmp_path):
        """Test --sample-files option."""
        from cur_report_generator import generate_report

        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(generate_report, [
                '--sample-files', '5',
                '--output-dir', 'test_reports'
            ])

            assert result.exit_code == 0

    def test_csv_generation(self, runner, mock_env_vars, mock_dependencies, tmp_path):
        """Test CSV generation option."""
        from cur_report_generator import generate_report

        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(generate_report, [
                '--generate-csv',
                '--output-dir', 'test_reports'
            ])

            assert result.exit_code == 0

    def test_top_n_option(self, runner, mock_env_vars, mock_dependencies, tmp_path):
        """Test --top-n option."""
        from cur_report_generator import generate_report

        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(generate_report, [
                '--top-n', '20',
                '--output-dir', 'test_reports'
            ])

            assert result.exit_code == 0

    def test_invalid_date_format(self, runner, mock_env_vars):
        """Test error handling for invalid date format."""
        from cur_report_generator import generate_report

        result = runner.invoke(generate_report, [
            '--start-date', 'invalid-date'
        ])

        assert result.exit_code == 1
        assert 'Invalid date format' in result.output

    def test_missing_env_vars(self, runner):
        """Test error when required env vars are missing."""
        from cur_report_generator import generate_report

        # Don't set env vars - should fail validation
        result = runner.invoke(generate_report, [])

        assert result.exit_code == 1
        assert 'Missing required environment variables' in result.output

    def test_no_html_option(self, runner, mock_env_vars, mock_dependencies, tmp_path):
        """Test --no-html option."""
        from cur_report_generator import generate_report

        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(generate_report, [
                '--no-html',
                '--output-dir', 'test_reports'
            ])

            assert result.exit_code == 0

    def test_debug_mode(self, runner, mock_env_vars, mock_dependencies, tmp_path):
        """Test --debug flag."""
        from cur_report_generator import generate_report

        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(generate_report, [
                '--debug',
                '--output-dir', 'test_reports'
            ])

            assert result.exit_code == 0

    def test_empty_data_handling(self, runner, mock_env_vars, tmp_path):
        """Test handling when no CUR data is found."""
        from cur_report_generator import generate_report

        with patch('cur_report_generator.load_dotenv'), \
             patch('cur_report_generator.CURReader') as mock_reader:

            # Mock reader returns empty DataFrame
            mock_reader_instance = Mock()
            mock_reader_instance.load_cur_data.return_value = pd.DataFrame()
            mock_reader.return_value = mock_reader_instance

            with runner.isolated_filesystem(temp_dir=tmp_path):
                result = runner.invoke(generate_report, [
                    '--output-dir', 'test_reports'
                ])

                assert result.exit_code == 1
                assert 'No CUR data found' in result.output

    def test_output_directory_creation(self, runner, mock_env_vars, mock_dependencies, tmp_path):
        """Test that output directory is created if it doesn't exist."""
        from cur_report_generator import generate_report

        with runner.isolated_filesystem(temp_dir=tmp_path):
            output_dir = 'nested/test/reports'
            result = runner.invoke(generate_report, [
                '--output-dir', output_dir
            ])

            assert result.exit_code == 0

    def test_banner_display(self, runner, mock_env_vars, mock_dependencies, tmp_path):
        """Test that banner is displayed."""
        from cur_report_generator import generate_report

        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(generate_report, [
                '--output-dir', 'test_reports'
            ])

            assert 'AWS Cost and Usage Report Generator' in result.output

    def test_summary_output(self, runner, mock_env_vars, mock_dependencies, tmp_path):
        """Test that summary is displayed in output."""
        from cur_report_generator import generate_report

        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(generate_report, [
                '--output-dir', 'test_reports'
            ])

            assert result.exit_code == 0
            assert 'Report Summary' in result.output
            assert 'Total Cost' in result.output


class TestValidateEnvVars:
    """Test environment variable validation."""

    def test_validate_env_vars_success(self, mock_env_vars):
        """Test validation with all required vars present."""
        from cur_report_generator import validate_env_vars

        # Should not raise
        validate_env_vars()

    def test_validate_env_vars_missing(self):
        """Test validation with missing vars."""
        from cur_report_generator import validate_env_vars

        with pytest.raises(SystemExit):
            validate_env_vars()


class TestSetupLogging:
    """Test logging setup."""

    def test_setup_logging_info_level(self):
        """Test logging setup with info level."""
        from cur_report_generator import setup_logging

        setup_logging(debug=False)
        # Just ensure it doesn't crash

    def test_setup_logging_debug_level(self):
        """Test logging setup with debug level."""
        from cur_report_generator import setup_logging

        setup_logging(debug=True)
        # Just ensure it doesn't crash
