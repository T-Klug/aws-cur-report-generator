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

            # Setup Polars mock - LazyFrame that returns real DataFrame on collect
            mock_lf = Mock()
            mock_lf.collect_schema.return_value = pl.Schema(
                {col: pl.Utf8 for col in sample_cur_data.columns}
            )
            mock_lf.select.return_value = mock_lf
            mock_lf.filter.return_value = mock_lf
            mock_lf.with_columns.return_value = mock_lf
            mock_lf.unique.return_value = mock_lf
            mock_lf.collect.return_value = sample_cur_data  # Return real DataFrame

            mock_scan_csv.return_value = mock_lf

            # Mock concat to return real DataFrame (not Mock) for proper len() call
            mock_concat.return_value = sample_cur_data

            reader = CURReader(bucket="test-bucket", prefix="test-prefix")
            reader.load_cur_data(
                start_date=datetime(2024, 1, 1), end_date=datetime(2024, 7, 31), sample_files=2
            )

            # With sample_files=2, we scan each file individually (2 files)
            assert mock_scan_csv.call_count == 2

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

    def test_filter_split_cost_duplicates(self):
        """Test that split cost allocation filtering removes parent EC2 rows correctly."""
        with patch("s3_reader.boto3.Session") as mock_session:
            mock_client = Mock()
            mock_session.return_value.client.return_value = mock_client

            reader = CURReader(bucket="test-bucket", prefix="test-prefix")

            # Create test data simulating split cost allocation
            # - Parent EC2 row (should be filtered out): ResourceId = i-xxx, ParentResourceId = null
            # - Split child rows (EKS pods): ResourceId = pod-arn, ParentResourceId = i-xxx
            # - Non-split row (S3): ResourceId = bucket-name, ParentResourceId = null
            test_df = pl.DataFrame(
                {
                    "lineItem/ResourceId": [
                        "i-093f9f99b20291789",  # Parent EC2 (should be removed)
                        "arn:aws:eks:pod/task1",  # Split child 1
                        "arn:aws:eks:pod/task2",  # Split child 2
                        "my-s3-bucket",  # Non-split (should stay)
                        "arn:aws:lambda:func",  # Non-split (should stay)
                    ],
                    "splitLineItem/ParentResourceId": [
                        None,  # Parent has no parent
                        "i-093f9f99b20291789",  # Points to parent EC2
                        "i-093f9f99b20291789",  # Points to parent EC2
                        None,  # S3 has no parent
                        None,  # Lambda has no parent
                    ],
                    "lineItem/UnblendedCost": [
                        100.0,  # Full EC2 cost (duplicate)
                        60.0,  # Split allocation 1
                        40.0,  # Split allocation 2
                        25.0,  # S3 cost
                        15.0,  # Lambda cost
                    ],
                }
            )

            # Apply the filter
            result_df = reader._filter_split_cost_duplicates(test_df)

            # Should have 4 rows (parent EC2 filtered out)
            assert len(result_df) == 4

            # Parent EC2 should be gone
            resource_ids = result_df["lineItem/ResourceId"].to_list()
            assert "i-093f9f99b20291789" not in resource_ids

            # Split children and non-split items should remain
            assert "arn:aws:eks:pod/task1" in resource_ids
            assert "arn:aws:eks:pod/task2" in resource_ids
            assert "my-s3-bucket" in resource_ids
            assert "arn:aws:lambda:func" in resource_ids

            # Total cost should be 60 + 40 + 25 + 15 = 140 (not 240 with parent)
            assert result_df["lineItem/UnblendedCost"].sum() == 140.0

    def test_filter_split_cost_no_splits(self):
        """Test that filtering does nothing when no split allocations exist."""
        with patch("s3_reader.boto3.Session") as mock_session:
            mock_client = Mock()
            mock_session.return_value.client.return_value = mock_client

            reader = CURReader(bucket="test-bucket", prefix="test-prefix")

            # Create test data with no split allocations
            test_df = pl.DataFrame(
                {
                    "lineItem/ResourceId": ["i-xxx", "bucket-1", "func-1"],
                    "splitLineItem/ParentResourceId": [None, None, None],
                    "lineItem/UnblendedCost": [100.0, 50.0, 25.0],
                }
            )

            result_df = reader._filter_split_cost_duplicates(test_df)

            # All rows should remain
            assert len(result_df) == 3
            assert result_df["lineItem/UnblendedCost"].sum() == 175.0

    def test_filter_split_cost_missing_columns(self):
        """Test that filtering handles missing columns gracefully."""
        with patch("s3_reader.boto3.Session") as mock_session:
            mock_client = Mock()
            mock_session.return_value.client.return_value = mock_client

            reader = CURReader(bucket="test-bucket", prefix="test-prefix")

            # Create test data without split columns
            test_df = pl.DataFrame(
                {
                    "lineItem/UnblendedCost": [100.0, 50.0, 25.0],
                }
            )

            result_df = reader._filter_split_cost_duplicates(test_df)

            # All rows should remain unchanged
            assert len(result_df) == 3
            assert result_df["lineItem/UnblendedCost"].sum() == 175.0
