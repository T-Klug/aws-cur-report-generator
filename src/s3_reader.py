"""S3 CUR Data Reader - Handles downloading and reading AWS Cost and Usage Reports from S3."""

import logging
import os
from datetime import datetime, timedelta
from typing import List, Optional

import boto3
import polars as pl
from botocore.exceptions import ClientError, NoCredentialsError

logger = logging.getLogger(__name__)


class CURReader:
    """Read and process AWS Cost and Usage Reports from S3 using Polars."""

    # Essential columns needed for analysis
    DEFAULT_REQUIRED_COLUMNS = [
        # Cost columns
        "line_item_unblended_cost",
        "lineItem/UnblendedCost",
        "line_item_blended_cost",
        "lineItem/BlendedCost",
        # Date columns
        "line_item_usage_start_date",
        "lineItem/UsageStartDate",
        # Account columns
        "line_item_usage_account_id",
        "lineItem/UsageAccountId",
        # Service columns
        "line_item_product_code",
        "lineItem/ProductCode",
        "product_product_name",
        "product/ProductName",
        # Usage details
        "line_item_usage_type",
        "lineItem/UsageType",
        "line_item_operation",
        "lineItem/Operation",
        # Region
        "product_region",
        "product/region",
        "line_item_availability_zone",
        "lineItem/AvailabilityZone",
        # Deduplication ID
        "identity_line_item_id",
        "identity/LineItemId",
        "lineItem/LineItemId",
    ]

    def __init__(
        self,
        bucket: str,
        prefix: str,
        aws_profile: Optional[str] = None,
        aws_region: Optional[str] = None,
        required_columns: Optional[List[str]] = None,
        chunk_size: int = 100000,  # Kept for compatibility, unused in Polars
        max_workers: int = 4,  # Kept for compatibility, unused in Polars
    ) -> None:
        """
        Initialize the CUR Reader.

        Args:
            bucket: S3 bucket name containing CUR data
            prefix: S3 prefix/path to CUR reports
            aws_profile: AWS profile name (optional)
            aws_region: AWS region (optional)
            required_columns: List of column names to read (None = use defaults)
        """
        self.bucket = bucket
        self.prefix = prefix.rstrip("/")
        self.required_columns = required_columns or self.DEFAULT_REQUIRED_COLUMNS
        self.aws_profile = aws_profile
        self.aws_region = aws_region

        # Initialize S3 session parameters
        self._session_params = {}
        if aws_profile:
            self._session_params["profile_name"] = aws_profile
        if aws_region:
            self._session_params["region_name"] = aws_region

        # Initialize boto3 client for listing files
        try:
            self.session = boto3.Session(**self._session_params)
            self.s3_client = self.session.client("s3")
            logger.info(f"Initialized S3 client for bucket: {bucket}")
        except NoCredentialsError:
            logger.error("AWS credentials not found. Please configure credentials.")
            raise
        except Exception as e:
            logger.error(f"Error initializing AWS session: {e}")
            raise

        # Initialize s3fs for Polars
        # We pass credentials explicitly if available, otherwise s3fs uses default chain
        s3_kwargs = {}

        # If using specific credentials from session (e.g. assumed role), pass them
        creds = self.session.get_credentials()
        if creds:
            s3_kwargs["aws_access_key_id"] = creds.access_key
            s3_kwargs["aws_secret_access_key"] = creds.secret_key
            if creds.token:
                s3_kwargs["aws_session_token"] = creds.token
        elif aws_profile:
            # Only use profile if we don't have explicit credentials
            s3_kwargs["aws_profile"] = aws_profile

        if not aws_region:
            aws_region = self.session.region_name

        if aws_region:
            s3_kwargs["aws_region"] = aws_region

        self.storage_options = s3_kwargs

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

    def load_cur_data(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        sample_files: Optional[int] = None,
    ) -> pl.DataFrame:
        """
        Load CUR data from S3 for the specified date range using Polars.

        Args:
            start_date: Start date for data retrieval
            end_date: End date for data retrieval
            sample_files: If specified, only load this many files (for testing)

        Returns:
            Polars DataFrame with all CUR data
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
            return pl.DataFrame()

        # Limit files for testing if specified
        if sample_files:
            report_files = report_files[:sample_files]
            logger.info(f"Sampling {sample_files} files for testing")

        # Group files by extension
        parquet_files = [f"s3://{self.bucket}/{f}" for f in report_files if f.endswith(".parquet")]
        csv_files = [
            f"s3://{self.bucket}/{f}" for f in report_files if f.endswith((".csv", ".csv.gz"))
        ]

        lazy_frames = []

        # Process Parquet files
        if parquet_files:
            logger.info(f"Processing {len(parquet_files)} Parquet files...")
            try:
                # Scan all parquet files at once - Polars handles parallelism
                lf = pl.scan_parquet(parquet_files, storage_options=self.storage_options, retries=3)

                # Apply optimizations
                lf = self._optimize_lazyframe(lf, start_date, end_date)
                lazy_frames.append(lf)
            except Exception as e:
                logger.error(f"Error scanning Parquet files: {e}")

        # Process CSV files
        if csv_files:
            logger.info(f"Processing {len(csv_files)} CSV files...")
            # Polars scan_csv doesn't support list of S3 files as natively as scan_parquet in all versions,
            # but we can scan them individually and concat
            for file_path in csv_files:
                try:
                    lf = pl.scan_csv(
                        file_path,
                        storage_options=self.storage_options,
                        ignore_errors=True,
                        infer_schema_length=10000,
                    )
                    lf = self._optimize_lazyframe(lf, start_date, end_date)
                    lazy_frames.append(lf)
                except Exception as e:
                    logger.warning(f"Error scanning CSV file {file_path}: {e}")

        if not lazy_frames:
            logger.error("No data could be loaded")
            return pl.DataFrame()

        # Concatenate all lazy frames
        if len(lazy_frames) == 1:
            combined_lf = lazy_frames[0]
        else:
            combined_lf = pl.concat(lazy_frames, how="vertical_relaxed")

        # Deduplicate
        # Find deduplication column
        try:
            schema = combined_lf.collect_schema()
            dedup_col = None
            for col in ["identity_line_item_id", "identity/LineItemId", "lineItem/LineItemId"]:
                if col in schema.names():
                    dedup_col = col
                    break

            if dedup_col:
                logger.info(f"Deduplicating based on {dedup_col}")
                combined_lf = combined_lf.unique(subset=[dedup_col], keep="last")
        except Exception as e:
            logger.warning(f"Could not determine schema for deduplication: {e}")

        # Collect results
        logger.info("Executing query plan (downloading and processing)...")
        try:
            df = combined_lf.collect()
            logger.info(f"Successfully loaded {len(df)} records")
            return df
        except Exception as e:
            logger.error(f"Error executing Polars query: {e}")
            # Fallback or re-raise? Re-raising is better to know what went wrong
            raise

    def _optimize_lazyframe(
        self, lf: pl.LazyFrame, start_date: Optional[datetime], end_date: Optional[datetime]
    ) -> pl.LazyFrame:
        """
        Apply filters and column selection to LazyFrame.
        """
        # Get available columns in this LazyFrame
        # Note: collect_schema() fetches metadata
        try:
            schema = lf.collect_schema()
            available_cols = schema.names()
        except Exception:
            # If schema fetch fails (e.g. empty file), return as is
            return lf

        # Select only required columns that exist
        cols_to_select = [c for c in self.required_columns if c in available_cols]
        if cols_to_select:
            lf = lf.select(cols_to_select)

        # Filter by cost > 0
        cost_col = None
        for col in ["line_item_unblended_cost", "lineItem/UnblendedCost"]:
            if col in available_cols:
                cost_col = col
                break

        if cost_col:
            # Cast to float and filter
            lf = lf.filter(pl.col(cost_col).cast(pl.Float64) > 0)

        # Filter by date
        date_col = None
        for col in ["line_item_usage_start_date", "lineItem/UsageStartDate"]:
            if col in available_cols:
                date_col = col
                break

        if date_col and (start_date or end_date):
            # Check if column is string/utf8 before trying to convert
            col_type = schema.get(date_col)
            if col_type in (pl.String, pl.Utf8):
                # Ensure date column is datetime
                lf = lf.with_columns(pl.col(date_col).str.to_datetime(strict=False))

            if start_date:
                lf = lf.filter(pl.col(date_col) >= start_date)
            if end_date:
                lf = lf.filter(pl.col(date_col) <= end_date)

        return lf
