"""Tests for S3 CUR reader module."""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta
import pandas as pd
from botocore.exceptions import ClientError, NoCredentialsError

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from s3_reader import CURReader


class TestCURReader:
    """Test cases for CURReader class."""

    def test_initialization_success(self):
        """Test successful initialization of CURReader."""
        with patch('s3_reader.boto3.Session') as mock_session:
            mock_client = Mock()
            mock_session.return_value.client.return_value = mock_client

            reader = CURReader(
                bucket='test-bucket',
                prefix='test-prefix',
                aws_profile='test-profile',
                aws_region='us-east-1'
            )

            assert reader.bucket == 'test-bucket'
            assert reader.prefix == 'test-prefix'
            mock_session.assert_called_once_with(
                profile_name='test-profile',
                region_name='us-east-1'
            )

    def test_initialization_no_credentials(self):
        """Test initialization failure when credentials are missing."""
        with patch('s3_reader.boto3.Session') as mock_session:
            mock_session.return_value.client.side_effect = NoCredentialsError()

            with pytest.raises(NoCredentialsError):
                CURReader(bucket='test-bucket', prefix='test-prefix')

    def test_list_report_files_success(self, mock_s3_objects):
        """Test listing CUR report files from S3."""
        with patch('s3_reader.boto3.Session') as mock_session:
            mock_client = Mock()
            mock_paginator = Mock()

            # Setup paginator to return mock objects
            mock_paginator.paginate.return_value = [
                {'Contents': mock_s3_objects}
            ]
            mock_client.get_paginator.return_value = mock_paginator
            mock_session.return_value.client.return_value = mock_client

            reader = CURReader(bucket='test-bucket', prefix='test-prefix')
            files = reader.list_report_files()

            # Should get all 6 monthly CUR files (Jan-Jun 2024)
            assert len(files) == 6
            assert all(f.endswith('.csv.gz') for f in files)

    def test_list_report_files_with_date_filter(self, mock_s3_objects):
        """Test listing files with date range filter."""
        with patch('s3_reader.boto3.Session') as mock_session:
            mock_client = Mock()
            mock_paginator = Mock()

            mock_paginator.paginate.return_value = [
                {'Contents': mock_s3_objects}
            ]
            mock_client.get_paginator.return_value = mock_paginator
            mock_session.return_value.client.return_value = mock_client

            reader = CURReader(bucket='test-bucket', prefix='test-prefix')

            # Filter to only January files
            start_date = datetime(2024, 1, 1)
            end_date = datetime(2024, 1, 31)
            files = reader.list_report_files(start_date=start_date, end_date=end_date)

            # Should only get the 1 January file
            assert len(files) == 1

    def test_list_report_files_empty(self):
        """Test listing files when bucket is empty."""
        with patch('s3_reader.boto3.Session') as mock_session:
            mock_client = Mock()
            mock_paginator = Mock()

            # No contents
            mock_paginator.paginate.return_value = [{}]
            mock_client.get_paginator.return_value = mock_paginator
            mock_session.return_value.client.return_value = mock_client

            reader = CURReader(bucket='test-bucket', prefix='test-prefix')
            files = reader.list_report_files()

            assert len(files) == 0

    def test_read_cur_file_csv_gz(self, sample_cur_csv_gz_content):
        """Test reading a gzipped CSV CUR file."""
        with patch('s3_reader.boto3.Session') as mock_session:
            mock_client = Mock()
            mock_response = {
                'Body': Mock()
            }
            mock_response['Body'].read.return_value = sample_cur_csv_gz_content
            mock_client.get_object.return_value = mock_response
            mock_session.return_value.client.return_value = mock_client

            reader = CURReader(bucket='test-bucket', prefix='test-prefix')
            df = reader.read_cur_file('test-file.csv.gz')

            assert isinstance(df, pd.DataFrame)
            assert len(df) > 0
            assert 'line_item_usage_start_date' in df.columns

    def test_read_cur_file_csv(self, sample_cur_csv_content):
        """Test reading a regular CSV CUR file."""
        with patch('s3_reader.boto3.Session') as mock_session:
            mock_client = Mock()
            mock_response = {
                'Body': Mock()
            }
            mock_response['Body'].read.return_value = sample_cur_csv_content
            mock_client.get_object.return_value = mock_response
            mock_session.return_value.client.return_value = mock_client

            reader = CURReader(bucket='test-bucket', prefix='test-prefix')
            df = reader.read_cur_file('test-file.csv')

            assert isinstance(df, pd.DataFrame)
            assert len(df) > 0

    def test_read_cur_file_not_found(self):
        """Test reading a file that doesn't exist."""
        with patch('s3_reader.boto3.Session') as mock_session:
            mock_client = Mock()
            mock_client.get_object.side_effect = ClientError(
                {'Error': {'Code': 'NoSuchKey'}},
                'GetObject'
            )
            mock_session.return_value.client.return_value = mock_client

            reader = CURReader(bucket='test-bucket', prefix='test-prefix')

            with pytest.raises(ClientError):
                reader.read_cur_file('nonexistent-file.csv.gz')

    def test_read_cur_file_unsupported_format(self):
        """Test reading an unsupported file format."""
        with patch('s3_reader.boto3.Session') as mock_session:
            mock_client = Mock()
            mock_response = {
                'Body': Mock()
            }
            mock_response['Body'].read.return_value = b'some data'
            mock_client.get_object.return_value = mock_response
            mock_session.return_value.client.return_value = mock_client

            reader = CURReader(bucket='test-bucket', prefix='test-prefix')

            with pytest.raises(ValueError, match='Unsupported file format'):
                reader.read_cur_file('test-file.txt')

    def test_load_cur_data_success(self, sample_cur_csv_gz_content, mock_s3_objects):
        """Test loading CUR data from S3."""
        with patch('s3_reader.boto3.Session') as mock_session:
            mock_client = Mock()
            mock_paginator = Mock()

            # Setup file listing
            mock_paginator.paginate.return_value = [
                {'Contents': mock_s3_objects[:1]}  # Only one file
            ]
            mock_client.get_paginator.return_value = mock_paginator

            # Setup file reading
            mock_response = {
                'Body': Mock()
            }
            mock_response['Body'].read.return_value = sample_cur_csv_gz_content
            mock_client.get_object.return_value = mock_response

            mock_session.return_value.client.return_value = mock_client

            reader = CURReader(bucket='test-bucket', prefix='test-prefix')
            # Use explicit dates that match our mock data (6 months)
            df = reader.load_cur_data(
                start_date=datetime(2024, 1, 1),
                end_date=datetime(2024, 7, 31)
            )

            assert isinstance(df, pd.DataFrame)
            assert len(df) > 0

    def test_load_cur_data_with_sample_files(self, sample_cur_csv_gz_content, mock_s3_objects):
        """Test loading CUR data with sample_files limit."""
        with patch('s3_reader.boto3.Session') as mock_session:
            mock_client = Mock()
            mock_paginator = Mock()

            mock_paginator.paginate.return_value = [
                {'Contents': mock_s3_objects}
            ]
            mock_client.get_paginator.return_value = mock_paginator

            mock_response = {
                'Body': Mock()
            }
            mock_response['Body'].read.return_value = sample_cur_csv_gz_content
            mock_client.get_object.return_value = mock_response

            mock_session.return_value.client.return_value = mock_client

            reader = CURReader(bucket='test-bucket', prefix='test-prefix')
            # Use explicit dates that match our mock data (6 months)
            df = reader.load_cur_data(
                start_date=datetime(2024, 1, 1),
                end_date=datetime(2024, 7, 31),
                sample_files=2
            )

            # Should only process 2 files
            assert mock_client.get_object.call_count == 2

    def test_load_cur_data_no_files(self):
        """Test loading CUR data when no files are found."""
        with patch('s3_reader.boto3.Session') as mock_session:
            mock_client = Mock()
            mock_paginator = Mock()

            mock_paginator.paginate.return_value = [{}]
            mock_client.get_paginator.return_value = mock_paginator
            mock_session.return_value.client.return_value = mock_client

            reader = CURReader(bucket='test-bucket', prefix='test-prefix')
            df = reader.load_cur_data()

            assert df.empty

    def test_get_date_column_standard(self, sample_cur_data):
        """Test identifying date column in standard CUR format."""
        with patch('s3_reader.boto3.Session') as mock_session:
            mock_client = Mock()
            mock_session.return_value.client.return_value = mock_client

            reader = CURReader(bucket='test-bucket', prefix='test-prefix')
            date_col = reader.get_date_column(sample_cur_data)

            assert date_col == 'line_item_usage_start_date'

    def test_get_date_column_alternative_format(self):
        """Test identifying date column in alternative CUR format."""
        df = pd.DataFrame({
            'lineItem/UsageStartDate': ['2024-01-01'],
            'lineItem/UnblendedCost': [100.0]
        })

        with patch('s3_reader.boto3.Session') as mock_session:
            mock_client = Mock()
            mock_session.return_value.client.return_value = mock_client

            reader = CURReader(bucket='test-bucket', prefix='test-prefix')
            date_col = reader.get_date_column(df)

            assert date_col == 'lineItem/UsageStartDate'

    def test_get_date_column_not_found(self):
        """Test when date column cannot be identified."""
        df = pd.DataFrame({
            'some_column': [1, 2, 3],
            'another_column': [4, 5, 6]
        })

        with patch('s3_reader.boto3.Session') as mock_session:
            mock_client = Mock()
            mock_session.return_value.client.return_value = mock_client

            reader = CURReader(bucket='test-bucket', prefix='test-prefix')

            with pytest.raises(ValueError, match='Could not identify date column'):
                reader.get_date_column(df)
