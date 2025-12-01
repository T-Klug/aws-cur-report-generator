"""S3 CUR Data Reader - Handles downloading and reading AWS Cost and Usage Reports from S3."""

import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import boto3
import polars as pl
from botocore.exceptions import ClientError, NoCredentialsError

logger = logging.getLogger(__name__)


# Check for s3fs availability at import time
try:
    import s3fs  # noqa: F401

    S3FS_AVAILABLE = True
except ImportError:
    S3FS_AVAILABLE = False
    logger.warning("s3fs not installed. S3 file operations may fail.")


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
        max_workers: Optional[int] = None,
    ) -> None:
        """
        Initialize the CUR Reader.

        Args:
            bucket: S3 bucket name containing CUR data
            prefix: S3 prefix/path to CUR reports
            aws_profile: AWS profile name (optional)
            aws_region: AWS region (optional)
            required_columns: List of column names to read (None = use defaults)
            max_workers: Max parallel workers for file processing (None = auto-detect from CPU count)
        """
        self.bucket = bucket
        self.prefix = prefix.rstrip("/")
        self.required_columns = required_columns or self.DEFAULT_REQUIRED_COLUMNS
        self.aws_profile = aws_profile
        self.aws_region = aws_region
        self.max_workers = max_workers or self._get_optimal_workers()

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

        # Validate s3fs is available
        if not S3FS_AVAILABLE:
            raise ImportError(
                "s3fs is required for reading files from S3. " "Install it with: pip install s3fs"
            )

        # Initialize s3fs storage options for Polars
        # IMPORTANT: We do NOT pass static credentials here because they can expire
        # (especially with IAM roles, assumed roles, or MFA).
        # Instead, we let s3fs use the boto3 credential chain which auto-refreshes.
        s3_kwargs: Dict[str, Any] = {}

        if not aws_region:
            aws_region = self.session.region_name

        # Note: s3fs/Polars doesn't allow combining 'profile' with certain options
        # like 'max_concurrency' or 'client_kwargs'. So we need to choose one approach.
        if aws_profile:
            # When using a profile, pass it directly - region comes from profile config
            # Cannot combine with max_concurrency or client_kwargs
            s3_kwargs["profile"] = aws_profile
        else:
            # When not using a profile, we can use additional performance options
            s3_kwargs["default_block_size"] = 5 * 1024 * 1024  # 5MB blocks
            s3_kwargs["default_fill_cache"] = False  # Don't cache (one-time processing)
            s3_kwargs["max_concurrency"] = self.max_workers  # Parallel downloads
            if aws_region:
                s3_kwargs["client_kwargs"] = {"region_name": aws_region}

        self.storage_options = s3_kwargs
        self._schema_cache: Dict[str, pl.Schema] = {}  # Cache for schema lookups
        self._csv_schema_cache: Optional[Dict[str, pl.DataType]] = None  # Cache for CSV schema

    def _get_optimal_workers(self) -> int:
        """Determine optimal number of workers based on CPU count."""
        cpu_count = os.cpu_count() or 4
        # Use 2x CPU count for I/O bound tasks, capped at 32
        return min(cpu_count * 2, 32)

    def _parse_cur_date_range(self, path: str) -> Optional[Tuple[datetime, datetime]]:
        """
        Parse date range from CUR folder path.

        AWS CUR files are organized in folders like:
        - prefix/report-name/20241101-20241201/file.parquet
        - prefix/report-name/year=2024/month=11/file.parquet

        Args:
            path: S3 key path

        Returns:
            Tuple of (start_date, end_date) if parseable, None otherwise
        """
        # Pattern 1: YYYYMMDD-YYYYMMDD format (most common)
        date_range_pattern = r"/(\d{8})-(\d{8})/"
        match = re.search(date_range_pattern, path)
        if match:
            try:
                start = datetime.strptime(match.group(1), "%Y%m%d")
                end = datetime.strptime(match.group(2), "%Y%m%d")
                return (start, end)
            except ValueError:
                pass

        # Pattern 2: year=YYYY/month=MM format (Hive-style partitioning)
        hive_pattern = r"/year=(\d{4})/month=(\d{1,2})/"
        match = re.search(hive_pattern, path)
        if match:
            try:
                year = int(match.group(1))
                month = int(match.group(2))
                start = datetime(year, month, 1)
                # End is first day of next month
                if month == 12:
                    end = datetime(year + 1, 1, 1)
                else:
                    end = datetime(year, month + 1, 1)
                return (start, end)
            except ValueError:
                pass

        return None

    def _filter_files_by_partition(
        self,
        files: List[str],
        start_date: Optional[datetime],
        end_date: Optional[datetime],
    ) -> List[str]:
        """
        Filter files based on partition date ranges parsed from paths.

        Files in folders outside the date range are excluded.
        Files without parseable date ranges are included (conservative).

        Args:
            files: List of S3 keys
            start_date: Start of date range filter
            end_date: End of date range filter

        Returns:
            Filtered list of S3 keys
        """
        if not start_date and not end_date:
            return files

        filtered = []
        skipped_count = 0

        for file_path in files:
            date_range = self._parse_cur_date_range(file_path)

            if date_range is None:
                # Can't determine date from path, include conservatively
                filtered.append(file_path)
                continue

            folder_start, folder_end = date_range

            # Check for overlap between folder range and requested range
            # Folder overlaps if: folder_start < end_date AND folder_end > start_date
            overlaps = True
            if start_date and folder_end <= start_date:
                overlaps = False
            if end_date and folder_start > end_date:
                overlaps = False

            if overlaps:
                filtered.append(file_path)
            else:
                skipped_count += 1

        if skipped_count > 0:
            logger.info(f"Partition filtering: skipped {skipped_count} files outside date range")

        return filtered

    def list_report_files(self) -> List[str]:
        """
        List available CUR report files in S3.

        Note: Date filtering is not done here because S3 LastModified represents
        when files were uploaded, not the billing period. Date filtering is applied
        later when reading the actual data in _optimize_lazyframe.

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
                    # Note: We don't filter by S3 LastModified here because it represents
                    # when the file was uploaded, not the billing period it contains.
                    # Date filtering is done later in _optimize_lazyframe based on actual data.
                    if key.endswith((".csv.gz", ".parquet", ".csv")):
                        report_files.append(key)

            logger.info(f"Found {len(report_files)} CUR report files")
            return sorted(report_files)

        except ClientError as e:
            logger.error(f"Error listing S3 objects: {e}")
            raise

    def _infer_csv_schema(self, file_path: str) -> Optional[Dict[str, pl.DataType]]:
        """
        Infer schema from a CSV file and cache it for reuse.

        Args:
            file_path: S3 path to CSV file

        Returns:
            Dictionary mapping column names to Polars data types
        """
        if self._csv_schema_cache is not None:
            return self._csv_schema_cache

        try:
            # Scan first file to get schema
            lf = pl.scan_csv(
                file_path,
                storage_options=self.storage_options,
                ignore_errors=True,
                infer_schema_length=10000,
            )
            schema = lf.collect_schema()
            self._csv_schema_cache = {name: dtype for name, dtype in schema.items()}
            logger.info(f"Cached CSV schema with {len(self._csv_schema_cache)} columns")
            return self._csv_schema_cache
        except Exception as e:
            logger.warning(f"Failed to infer CSV schema: {e}")
            return None

    def load_cur_data(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        sample_files: Optional[int] = None,
    ) -> pl.DataFrame:
        """
        Load CUR data from S3 for the specified date range using Polars.

        Optimized for hundreds of files with:
        - Partition-aware filtering (skips folders outside date range)
        - Polars native multi-file scanning (handles parallelism internally)
        - Streaming execution for memory efficiency
        - Schema caching (faster CSV scanning)

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

        # Validate date range
        if start_date > end_date:
            raise ValueError(
                f"Invalid date range: start_date ({start_date.date()}) "
                f"is after end_date ({end_date.date()})"
            )

        logger.info(f"Loading CUR data from {start_date.date()} to {end_date.date()}")
        logger.info(f"Using {self.max_workers} workers")

        # List available files
        report_files = self.list_report_files()

        if not report_files:
            logger.warning("No CUR files found matching the criteria")
            return pl.DataFrame()

        # Apply partition-aware filtering BEFORE downloading
        original_count = len(report_files)
        report_files = self._filter_files_by_partition(report_files, start_date, end_date)
        logger.info(f"After partition filtering: {len(report_files)}/{original_count} files")

        if not report_files:
            logger.warning("No CUR files remain after partition filtering")
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

        logger.info(f"Files to process: {len(parquet_files)} Parquet, {len(csv_files)} CSV")

        lazy_frames: List[pl.LazyFrame] = []

        # Process ALL Parquet files at once - Polars handles parallelism internally
        if parquet_files:
            logger.info(f"Scanning {len(parquet_files)} Parquet files...")
            try:
                # Polars can scan all parquet files in one call - much more efficient
                lf = pl.scan_parquet(
                    parquet_files,
                    storage_options=self.storage_options,
                    retries=3,
                    parallel="auto",  # Let Polars decide parallelism
                )
                lf = self._optimize_lazyframe(lf, start_date, end_date)
                lazy_frames.append(lf)
                logger.info("Parquet files scanned successfully")
            except Exception as e:
                logger.error(f"Error scanning Parquet files: {e}")
                raise

        # Process ALL CSV files at once - Polars handles parallelism internally
        if csv_files:
            logger.info(f"Scanning {len(csv_files)} CSV files...")

            # Infer schema from first file for consistent typing
            self._infer_csv_schema(csv_files[0])

            try:
                scan_kwargs: Dict[str, Any] = {
                    "storage_options": self.storage_options,
                    "ignore_errors": True,
                    "glob": False,  # We're passing explicit file list, not glob pattern
                    "retries": 3,
                }
                if self._csv_schema_cache is not None:
                    scan_kwargs["schema"] = self._csv_schema_cache
                else:
                    scan_kwargs["infer_schema_length"] = 10000

                # Polars can scan all CSV files in one call
                lf = pl.scan_csv(csv_files, **scan_kwargs)
                lf = self._optimize_lazyframe(lf, start_date, end_date)
                lazy_frames.append(lf)
                logger.info("CSV files scanned successfully")
            except Exception as e:
                # Fall back to parallel individual scanning if bulk scan fails
                logger.warning(f"Bulk CSV scan failed ({e}), falling back to individual scanning")
                csv_lazy_frames = self._scan_csv_files_parallel(csv_files, start_date, end_date)
                lazy_frames.extend(csv_lazy_frames)
                logger.info(f"CSV files scanned individually: {len(csv_lazy_frames)} successful")

        if not lazy_frames:
            logger.error("No data could be loaded")
            return pl.DataFrame()

        # Combine all lazy frames
        logger.info("Building query plan...")
        if len(lazy_frames) == 1:
            combined_lf = lazy_frames[0]
        else:
            combined_lf = pl.concat(lazy_frames, how="vertical_relaxed")

        # Apply deduplication in the lazy plan
        combined_lf = self._lazy_deduplicate(combined_lf)

        # Execute the query with streaming for memory efficiency
        logger.info("Executing query (downloading and processing)...")
        try:
            df = combined_lf.collect(streaming=True)
            logger.info(f"Successfully loaded {len(df)} records from {len(report_files)} files")
            return df
        except Exception as e:
            logger.error(f"Error executing query with streaming: {e}")
            # Try non-streaming as fallback
            logger.info("Retrying without streaming...")
            try:
                df = combined_lf.collect(streaming=False)
                logger.info(f"Successfully loaded {len(df)} records (non-streaming fallback)")
                return df
            except Exception as e2:
                logger.error(f"Non-streaming also failed: {e2}")
                raise

    def _scan_csv_files_parallel(
        self,
        csv_files: List[str],
        start_date: Optional[datetime],
        end_date: Optional[datetime],
    ) -> List[pl.LazyFrame]:
        """
        Scan CSV files in parallel using ThreadPoolExecutor.

        Returns list of LazyFrames (not collected yet).
        """
        lazy_frames: List[pl.LazyFrame] = []
        errors: List[Tuple[str, str]] = []

        def scan_single_csv(file_path: str) -> Optional[pl.LazyFrame]:
            try:
                scan_kwargs: Dict[str, Any] = {
                    "storage_options": self.storage_options,
                    "ignore_errors": True,
                }
                if self._csv_schema_cache is not None:
                    scan_kwargs["schema"] = self._csv_schema_cache
                else:
                    scan_kwargs["infer_schema_length"] = 10000

                lf = pl.scan_csv(file_path, **scan_kwargs)
                return self._optimize_lazyframe(lf, start_date, end_date)
            except Exception as e:
                errors.append((file_path, str(e)))
                return None

        num_workers = min(self.max_workers, len(csv_files))
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = {executor.submit(scan_single_csv, fp): fp for fp in csv_files}
            for future in as_completed(futures):
                result = future.result()
                if result is not None:
                    lazy_frames.append(result)

        if errors:
            logger.warning(f"Failed to scan {len(errors)} CSV files")
            for fp, err in errors[:5]:  # Log first 5 errors
                logger.debug(f"  {fp}: {err}")

        return lazy_frames

    def _lazy_deduplicate(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        """
        Add deduplication to the lazy query plan.
        """
        try:
            schema = lf.collect_schema()
            dedup_col = None
            for col in ["identity_line_item_id", "identity/LineItemId", "lineItem/LineItemId"]:
                if col in schema.names():
                    dedup_col = col
                    break

            if dedup_col:
                logger.info(f"Adding deduplication on {dedup_col} to query plan")
                return lf.unique(subset=[dedup_col], keep="last")
            else:
                logger.warning("No deduplication column found in schema")
                return lf
        except Exception as e:
            logger.warning(f"Could not add deduplication to plan: {e}")
            return lf

    def _optimize_lazyframe(
        self, lf: pl.LazyFrame, start_date: Optional[datetime], end_date: Optional[datetime]
    ) -> pl.LazyFrame:
        """
        Apply filters and column selection to LazyFrame.

        This method applies:
        - Column selection (only required columns)
        - Cost filtering (> 0)
        - Date range filtering
        """
        # Get available columns in this LazyFrame
        # Note: collect_schema() fetches metadata - we cache results
        try:
            schema = lf.collect_schema()
            available_cols = schema.names()
        except pl.exceptions.ComputeError as e:
            logger.debug(f"Schema fetch failed (possibly empty file): {e}")
            return lf
        except Exception as e:
            logger.debug(f"Unexpected error fetching schema: {e}")
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
                # AWS CUR uses ISO 8601 format with timezone (e.g., 2024-01-15T00:00:00Z)
                # Use %+ format which handles RFC 3339/ISO 8601 with timezone
                lf = lf.with_columns(
                    pl.col(date_col)
                    .str.to_datetime(format="%+", strict=False)
                    .dt.replace_time_zone(
                        None
                    )  # Remove timezone for comparison with naive datetimes
                )

            if start_date:
                lf = lf.filter(pl.col(date_col) >= start_date)
            if end_date:
                lf = lf.filter(pl.col(date_col) <= end_date)

        return lf
