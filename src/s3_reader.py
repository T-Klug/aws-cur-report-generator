"""S3 CUR Data Reader - Handles downloading and reading AWS Cost and Usage Reports from S3."""

import gzip
import io
import logging
import os
from datetime import datetime, timedelta
from typing import List, Optional

import boto3
import pandas as pd
from botocore.exceptions import ClientError, NoCredentialsError

logger = logging.getLogger(__name__)


class CURReader:
    """Read and process AWS Cost and Usage Reports from S3."""

    def __init__(
        self,
        bucket: str,
        prefix: str,
        aws_profile: Optional[str] = None,
        aws_region: Optional[str] = None,
    ) -> None:
        """
        Initialize the CUR Reader.

        Args:
            bucket: S3 bucket name containing CUR data
            prefix: S3 prefix/path to CUR reports
            aws_profile: AWS profile name (optional)
            aws_region: AWS region (optional)
        """
        self.bucket = bucket
        self.prefix = prefix.rstrip("/")

        # Initialize AWS session
        session_params = {}
        if aws_profile:
            session_params["profile_name"] = aws_profile
        if aws_region:
            session_params["region_name"] = aws_region

        try:
            self.session = boto3.Session(**session_params)
            self.s3_client = self.session.client("s3")
            logger.info(f"Initialized S3 client for bucket: {bucket}")
        except NoCredentialsError:
            logger.error("AWS credentials not found. Please configure credentials.")
            raise
        except Exception as e:
            logger.error(f"Error initializing AWS session: {e}")
            raise

    def list_report_files(
        self, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None
    ) -> List[str]:
        """
        List available CUR report files in S3.

        Args:
            start_date: Start date for filtering reports
            end_date: End date for filtering reports

        Returns:
            List of S3 keys for CUR report files
        """
        try:
            logger.info(f"Listing CUR files in s3://{self.bucket}/{self.prefix}")

            paginator = self.s3_client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self.bucket, Prefix=self.prefix)

            report_files = []
            for page in pages:
                if "Contents" not in page:
                    continue

                for obj in page["Contents"]:
                    key = obj["Key"]
                    # CUR files are typically .csv.gz or .parquet
                    if key.endswith((".csv.gz", ".parquet", ".csv")):
                        # Filter by date if specified
                        if start_date or end_date:
                            last_modified = obj["LastModified"].replace(tzinfo=None)
                            if start_date and last_modified < start_date:
                                continue
                            if end_date and last_modified > end_date:
                                continue
                        report_files.append(key)

            logger.info(f"Found {len(report_files)} CUR report files")
            return sorted(report_files)

        except ClientError as e:
            logger.error(f"Error listing S3 objects: {e}")
            raise

    def read_cur_file(self, s3_key: str) -> pd.DataFrame:
        """
        Read a single CUR file from S3.

        Args:
            s3_key: S3 key of the CUR file

        Returns:
            DataFrame containing the CUR data
        """
        try:
            logger.info(f"Reading CUR file: {s3_key}")

            # Download file content
            response = self.s3_client.get_object(Bucket=self.bucket, Key=s3_key)
            content = response["Body"].read()

            # Handle different file formats
            if s3_key.endswith(".csv.gz"):
                # Decompress gzip
                with gzip.GzipFile(fileobj=io.BytesIO(content)) as gz:
                    df = pd.read_csv(gz, low_memory=False)  # type: ignore[arg-type]
            elif s3_key.endswith(".parquet"):
                df = pd.read_parquet(io.BytesIO(content))
            elif s3_key.endswith(".csv"):
                df = pd.read_csv(io.BytesIO(content), low_memory=False)
            else:
                raise ValueError(f"Unsupported file format: {s3_key}")

            logger.info(f"Successfully read {len(df)} rows from {s3_key}")
            return df

        except ClientError as e:
            logger.error(f"Error reading S3 object {s3_key}: {e}")
            raise
        except Exception as e:
            logger.error(f"Error processing CUR file {s3_key}: {e}")
            raise

    def load_cur_data(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        sample_files: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Load CUR data from S3 for the specified date range.

        Args:
            start_date: Start date for data retrieval
            end_date: End date for data retrieval
            sample_files: If specified, only load this many files (for testing)

        Returns:
            Combined DataFrame with all CUR data
        """
        # Default to last 3 months if no dates specified
        if not end_date:
            end_date = datetime.now()
        if not start_date:
            start_date = end_date - timedelta(days=90)

        logger.info(f"Loading CUR data from {start_date.date()} to {end_date.date()}")

        # List available files
        report_files = self.list_report_files(start_date, end_date)

        if not report_files:
            logger.warning("No CUR files found matching the criteria")
            return pd.DataFrame()

        # Limit files for testing if specified
        if sample_files:
            report_files = report_files[:sample_files]
            logger.info(f"Sampling {sample_files} files for testing")

        # Read and combine all files
        dataframes = []
        for i, s3_key in enumerate(report_files, 1):
            logger.info(f"Processing file {i}/{len(report_files)}: {os.path.basename(s3_key)}")
            try:
                df = self.read_cur_file(s3_key)
                dataframes.append(df)
            except Exception as e:
                logger.warning(f"Skipping file {s3_key} due to error: {e}")
                continue

        if not dataframes:
            logger.error("No data could be loaded from CUR files")
            return pd.DataFrame()

        # Combine all dataframes
        logger.info("Combining all CUR data...")
        combined_df = pd.concat(dataframes, ignore_index=True)
        logger.info(f"Total records loaded: {len(combined_df)}")

        # Deduplicate by line item ID to prevent double counting
        # AWS CUR files can be updated multiple times as charges are finalized
        line_item_id_cols = [
            "identity_line_item_id",
            "identity/LineItemId",
            "lineItem/LineItemId",
        ]

        dedup_col = None
        for col in line_item_id_cols:
            if col in combined_df.columns:
                dedup_col = col
                break

        if dedup_col:
            initial_count = len(combined_df)
            combined_df = combined_df.drop_duplicates(subset=[dedup_col], keep="last")
            deduped_count = initial_count - len(combined_df)
            if deduped_count > 0:
                logger.info(f"Removed {deduped_count} duplicate records based on {dedup_col}")
        else:
            logger.warning(
                "No line item ID column found - skipping deduplication. "
                "This may lead to double counting if files have been updated."
            )

        return combined_df

    def get_date_column(self, df: pd.DataFrame) -> str:
        """
        Identify the date column in the CUR data.
        CUR data can have different column names depending on version.

        Args:
            df: CUR DataFrame

        Returns:
            Name of the date column
        """
        possible_date_columns = [
            "line_item_usage_start_date",
            "lineItem/UsageStartDate",
            "UsageStartDate",
            "bill_billing_period_start_date",
            "identity_time_interval",
        ]

        for col in possible_date_columns:
            if col in df.columns:
                return col

        # If none found, look for any column with 'date' in the name
        date_cols = [col for col in df.columns if "date" in col.lower()]
        if date_cols:
            logger.warning(f"Using fallback date column: {date_cols[0]}")
            return date_cols[0]

        raise ValueError("Could not identify date column in CUR data")
