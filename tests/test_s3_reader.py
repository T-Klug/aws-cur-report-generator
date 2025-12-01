"""Tests for S3 CUR reader module."""

import os
import sys
from datetime import datetime
from unittest.mock import Mock, patch

import polars as pl
import pytest
from botocore.exceptions import NoCredentialsError

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from s3_reader import CURReader


class TestCURReader:
    """Test cases for CURReader class."""

    def test_initialization_success(self):
        """Test successful initialization of CURReader."""
        with patch("s3_reader.boto3.Session") as mock_session:
            mock_client = Mock()
            mock_session.return_value.client.return_value = mock_client

            reader = CURReader(
                bucket="test-bucket",
                prefix="test-prefix",
                aws_profile="test-profile",
                aws_region="us-east-1",
            )

            assert reader.bucket == "test-bucket"
            assert reader.prefix == "test-prefix"
            mock_session.assert_called_once_with(
                profile_name="test-profile", region_name="us-east-1"
            )

    def test_initialization_no_credentials(self):
        """Test initialization failure when credentials are missing."""
        with patch("s3_reader.boto3.Session") as mock_session:
            mock_session.return_value.client.side_effect = NoCredentialsError()

            with pytest.raises(NoCredentialsError):
                CURReader(bucket="test-bucket", prefix="test-prefix")

    def test_list_report_files_success(self, mock_s3_objects):
        """Test listing CUR report files from S3."""
        with patch("s3_reader.boto3.Session") as mock_session:
            mock_client = Mock()
            mock_paginator = Mock()

            # Setup paginator to return mock objects
            mock_paginator.paginate.return_value = [{"Contents": mock_s3_objects}]
            mock_client.get_paginator.return_value = mock_paginator
            mock_session.return_value.client.return_value = mock_client

            reader = CURReader(bucket="test-bucket", prefix="test-prefix")
            files = reader.list_report_files()

            # Should get all 6 monthly CUR files (Jan-Jun 2024)
            assert len(files) == 6
            assert all(f.endswith(".csv.gz") for f in files)

    def test_list_report_files_with_date_filter(self, mock_s3_objects):
        """Test listing files returns all files (date filtering is done in load_cur_data)."""
        with patch("s3_reader.boto3.Session") as mock_session:
            mock_client = Mock()
            mock_paginator = Mock()

            mock_paginator.paginate.return_value = [{"Contents": mock_s3_objects}]
            mock_client.get_paginator.return_value = mock_paginator
            mock_session.return_value.client.return_value = mock_client

            reader = CURReader(bucket="test-bucket", prefix="test-prefix")

            # list_report_files returns all files; date filtering is done in load_cur_data
            files = reader.list_report_files()

            # Should get all 6 monthly CUR files (date filtering happens later)
            assert len(files) == 6

    def test_list_report_files_empty(self):
        """Test listing files when bucket is empty."""
        with patch("s3_reader.boto3.Session") as mock_session:
            mock_client = Mock()
            mock_paginator = Mock()

            # No contents
            mock_paginator.paginate.return_value = [{}]
            mock_client.get_paginator.return_value = mock_paginator
            mock_session.return_value.client.return_value = mock_client

            reader = CURReader(bucket="test-bucket", prefix="test-prefix")
            files = reader.list_report_files()

            assert len(files) == 0

    def test_load_cur_data_success(self, sample_cur_data, mock_s3_objects):
        """Test loading CUR data from S3."""
        with (
            patch("s3_reader.boto3.Session") as mock_session,
            patch("s3_reader.pl.scan_csv") as mock_scan_csv,
        ):

            mock_client = Mock()
            mock_paginator = Mock()

            # Setup file listing
            mock_paginator.paginate.return_value = [
                {"Contents": mock_s3_objects[:1]}  # Only one file
            ]
            mock_client.get_paginator.return_value = mock_paginator
            mock_session.return_value.client.return_value = mock_client

            # Setup Polars mock
            mock_lf = Mock()
            mock_lf.collect_schema.return_value = pl.Schema(
                {col: pl.Utf8 for col in sample_cur_data.columns}
            )
            # Mock chainable methods
            mock_lf.select.return_value = mock_lf
            mock_lf.filter.return_value = mock_lf
            mock_lf.with_columns.return_value = mock_lf
            mock_lf.unique.return_value = mock_lf

            # Mock collect to return sample data
            mock_lf.collect.return_value = sample_cur_data

            mock_scan_csv.return_value = mock_lf

            reader = CURReader(bucket="test-bucket", prefix="test-prefix")
            # Use explicit dates that match our mock data (6 months)
            df = reader.load_cur_data(
                start_date=datetime(2024, 1, 1), end_date=datetime(2024, 7, 31)
            )

            assert isinstance(df, pl.DataFrame)
            assert len(df) > 0
            assert mock_scan_csv.called

    def test_load_cur_data_with_sample_files(self, sample_cur_data, mock_s3_objects):
        """Test loading CUR data with sample_files limit."""
        with (
            patch("s3_reader.boto3.Session") as mock_session,
            patch("s3_reader.pl.scan_csv") as mock_scan_csv,
            patch("s3_reader.pl.concat") as mock_concat,
        ):

            mock_client = Mock()
            mock_paginator = Mock()

            mock_paginator.paginate.return_value = [{"Contents": mock_s3_objects}]
            mock_client.get_paginator.return_value = mock_paginator
            mock_session.return_value.client.return_value = mock_client

            # Setup Polars mock
            mock_lf = Mock()
            mock_lf.collect_schema.return_value = pl.Schema(
                {col: pl.Utf8 for col in sample_cur_data.columns}
            )
            mock_lf.select.return_value = mock_lf
            mock_lf.filter.return_value = mock_lf
            mock_lf.with_columns.return_value = mock_lf
            mock_lf.unique.return_value = mock_lf
            # mock_lf.collect.return_value = sample_cur_data # Not needed if concat is mocked

            mock_scan_csv.return_value = mock_lf

            # Mock concat result
            mock_combined_lf = Mock()
            mock_combined_lf.unique.return_value = mock_combined_lf
            mock_combined_lf.collect.return_value = sample_cur_data
            mock_concat.return_value = mock_combined_lf

            reader = CURReader(bucket="test-bucket", prefix="test-prefix")
            reader.load_cur_data(
                start_date=datetime(2024, 1, 1), end_date=datetime(2024, 7, 31), sample_files=2
            )

            # Should process 2 files + 1 for schema inference = 3 calls
            assert mock_scan_csv.call_count == 3

    def test_load_cur_data_no_files(self):
        """Test loading CUR data when no files are found."""
        with patch("s3_reader.boto3.Session") as mock_session:
            mock_client = Mock()
            mock_paginator = Mock()

            mock_paginator.paginate.return_value = [{}]
            mock_client.get_paginator.return_value = mock_paginator
            mock_session.return_value.client.return_value = mock_client

            reader = CURReader(bucket="test-bucket", prefix="test-prefix")
            df = reader.load_cur_data()

            assert df.is_empty()
