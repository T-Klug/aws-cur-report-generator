"""S3 CUR Data Reader - Handles downloading and reading AWS Cost and Usage Reports from S3."""

import hashlib
import logging
import os
import re
import sys
import tempfile
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import boto3
import polars as pl
from botocore.exceptions import ClientError, NoCredentialsError

logger = logging.getLogger(__name__)

# Default cache directory
DEFAULT_CACHE_DIR = os.path.expanduser("~/.cache/cur-reports")


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
        # Resource ID (needed for split cost allocation filtering)
        "lineItem/ResourceId",
        "line_item_resource_id",
        # Split Cost Allocation columns (to detect and handle split line items)
        "splitLineItem/ParentResourceId",
        "split_line_item_parent_resource_id",
        # Line item type (needed for discount analysis)
        "lineItem/LineItemType",
        "line_item_line_item_type",
    ]

    def __init__(
        self,
        bucket: str,
        prefix: str,
        aws_profile: Optional[str] = None,
        aws_region: Optional[str] = None,
        required_columns: Optional[List[str]] = None,
        max_workers: Optional[int] = None,
        cache_dir: Optional[str] = None,
        use_cache: bool = True,
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
            cache_dir: Directory for caching downloaded files (None = ~/.cache/cur-reports)
            use_cache: Whether to use local file caching (default: True)
        """
        self.bucket = bucket
        self.prefix = prefix.rstrip("/")
        self.required_columns = required_columns or self.DEFAULT_REQUIRED_COLUMNS
        self.aws_profile = aws_profile
        self.aws_region = aws_region
        self.max_workers = max_workers or self._get_optimal_workers()
        self.use_cache = use_cache
        self.cache_dir = Path(cache_dir) if cache_dir else Path(DEFAULT_CACHE_DIR)

        # Create cache directory if caching is enabled
        if self.use_cache:
            self.cache_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"Cache directory: {self.cache_dir}")

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

    def _get_cache_path(self, s3_key: str) -> Path:
        """
        Get local cache path for an S3 key.

        Uses a hash of bucket+key to create a unique filename while preserving extension.
        """
        # Create a hash of the full S3 path for uniqueness
        full_path = f"{self.bucket}/{s3_key}"
        path_hash = hashlib.md5(full_path.encode()).hexdigest()[:16]

        # Preserve the original filename and extension
        original_name = os.path.basename(s3_key)
        # Prepend hash to avoid collisions
        cache_name = f"{path_hash}_{original_name}"

        return self.cache_dir / cache_name

    def _is_cached(self, s3_key: str) -> bool:
        """Check if a file is already cached locally."""
        if not self.use_cache:
            return False
        cache_path = self._get_cache_path(s3_key)
        return cache_path.exists()

    def _download_to_cache(self, s3_key: str) -> Path:
        """
        Download a file from S3 to local cache.

        Returns the local cache path.
        """
        cache_path = self._get_cache_path(s3_key)

        if cache_path.exists():
            logger.debug(f"Cache hit: {s3_key}")
            return cache_path

        logger.debug(f"Cache miss, downloading: {s3_key}")
        try:
            self.s3_client.download_file(self.bucket, s3_key, str(cache_path))
            return cache_path
        except ClientError as e:
            logger.error(f"Failed to download {s3_key}: {e}")
            raise

    def _download_files_to_cache(self, s3_keys: List[str]) -> Tuple[List[str], int, int, int]:
        """
        Download multiple files to cache in parallel.

        Only files from closed months are cached. Current month files are
        downloaded to a temporary location and not cached.

        Returns:
            Tuple of (local_paths, cache_hits, cache_misses, fresh_downloads)
            - cache_hits: Files served from cache
            - cache_misses: Files downloaded and cached (closed months)
            - fresh_downloads: Files downloaded but not cached (current month)
        """
        local_paths: List[str] = []
        cache_hits = 0
        cache_misses = 0
        fresh_downloads = 0
        errors: List[Tuple[str, str]] = []

        def download_single(s3_key: str) -> Tuple[str, str]:
            """
            Download single file.

            Returns:
                (local_path, status) where status is 'cache_hit', 'cached', or 'fresh'
            """
            is_closed = self._is_closed_month(s3_key)
            cache_path = self._get_cache_path(s3_key)

            if is_closed and cache_path.exists():
                # Closed month, already cached
                return str(cache_path), "cache_hit"

            # Create a new S3 client per thread - boto3 clients are not thread-safe
            thread_session = boto3.Session(**self._session_params)
            thread_client = thread_session.client("s3")

            if is_closed:
                # Closed month, download and cache
                try:
                    thread_client.download_file(self.bucket, s3_key, str(cache_path))
                    return str(cache_path), "cached"
                except ClientError as e:
                    errors.append((s3_key, str(e)))
                    raise
            else:
                # Current month - download to temp location, don't cache
                # Use hash + uuid to ensure unique temp file
                original_name = os.path.basename(s3_key)
                temp_name = f"cur_temp_{uuid.uuid4().hex[:8]}_{original_name}"
                temp_path = Path(tempfile.gettempdir()) / temp_name

                try:
                    thread_client.download_file(self.bucket, s3_key, str(temp_path))
                    return str(temp_path), "fresh"
                except ClientError as e:
                    errors.append((s3_key, str(e)))
                    raise

        num_workers = min(self.max_workers, len(s3_keys))
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = {executor.submit(download_single, key): key for key in s3_keys}
            for future in as_completed(futures):
                try:
                    local_path, status = future.result()
                    local_paths.append(local_path)
                    if status == "cache_hit":
                        cache_hits += 1
                    elif status == "cached":
                        cache_misses += 1
                    else:  # fresh
                        fresh_downloads += 1
                except Exception as e:
                    s3_key = futures[future]
                    logger.warning(f"Failed to download {s3_key}: {e}")

        if errors:
            logger.warning(f"Failed to download {len(errors)} files")

        return local_paths, cache_hits, cache_misses, fresh_downloads

    def clear_cache(self) -> int:
        """
        Clear all cached files.

        Returns:
            Number of files deleted
        """
        if not self.cache_dir.exists():
            return 0

        count = 0
        for file in self.cache_dir.iterdir():
            if file.is_file():
                file.unlink()
                count += 1

        logger.info(f"Cleared {count} files from cache")
        return count

    def get_cache_size(self) -> Tuple[int, int]:
        """
        Get cache statistics.

        Returns:
            Tuple of (file_count, total_size_bytes)
        """
        if not self.cache_dir.exists():
            return 0, 0

        file_count = 0
        total_size = 0
        for file in self.cache_dir.iterdir():
            if file.is_file():
                file_count += 1
                total_size += file.stat().st_size

        return file_count, total_size

    def _is_closed_month(self, s3_key: str) -> bool:
        """
        Check if a file belongs to a closed (historical) billing period.

        A month is "closed" if the billing period end date is before the current month.
        This means the data is finalized and safe to cache indefinitely.

        Args:
            s3_key: S3 key path

        Returns:
            True if the file is from a closed month, False otherwise
        """
        date_range = self._parse_cur_date_range(s3_key)
        if date_range is None:
            # Can't determine date - don't cache (conservative)
            return False

        _, folder_end = date_range

        # Get first day of current month
        now = datetime.now()
        current_month_start = datetime(now.year, now.month, 1)

        # Month is closed if its end date is on or before the current month start
        # The folder end date is the first day of the NEXT month (e.g., Nov folder ends 20251201)
        # So if folder_end <= current_month_start, the billing period is complete
        return folder_end <= current_month_start

    def _parse_cur_date_range(self, path: str) -> Optional[Tuple[datetime, datetime]]:
        """
        Parse date range from CUR folder path.

        AWS CUR files are organized in folders like:
        - prefix/report-name/20241101-20241201/file.parquet
        - prefix/report-name/year=2024/month=11/file.parquet
        - prefix/report-name/data/BILLING_PERIOD=2025-11/file.parquet

        Args:
            path: S3 key path

        Returns:
            Tuple of (start_date, end_date) if parseable, None otherwise
        """
        # Pattern 1: YYYYMMDD-YYYYMMDD format
        date_range_pattern = r"/(\d{8})-(\d{8})/"
        match = re.search(date_range_pattern, path)
        if match:
            try:
                start = datetime.strptime(match.group(1), "%Y%m%d")
                end = datetime.strptime(match.group(2), "%Y%m%d")
                return (start, end)
            except ValueError:
                pass

        # Pattern 2: BILLING_PERIOD=YYYY-MM format (CUR 2.0 style)
        billing_period_pattern = r"BILLING_PERIOD[=:](\d{4})-(\d{1,2})"
        match = re.search(billing_period_pattern, path)
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

        # Pattern 3: year=YYYY/month=MM format (Hive-style partitioning)
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

        # Group files by extension (keep S3 keys for caching)
        parquet_keys = [f for f in report_files if f.endswith(".parquet")]
        csv_keys = [f for f in report_files if f.endswith((".csv", ".csv.gz"))]

        logger.info(f"Files to process: {len(parquet_keys)} Parquet, {len(csv_keys)} CSV")

        dataframes: List[pl.DataFrame] = []

        # Process Parquet files - use cache for closed months if enabled
        if parquet_keys:
            if self.use_cache:
                # Download Parquet files to cache (closed months) or temp (current month)
                logger.info(f"Downloading {len(parquet_keys)} Parquet files...")
                local_paths, cache_hits, cache_misses, fresh = self._download_files_to_cache(
                    parquet_keys
                )
                logger.info(
                    f"Cache: {cache_hits} hits, {cache_misses} cached, {fresh} fresh downloads"
                )

                if local_paths:
                    try:
                        lf = pl.scan_parquet(
                            local_paths,
                            parallel="auto",
                        )
                        lf = self._optimize_lazyframe(lf, start_date, end_date)
                        dataframes.append(lf.collect())
                        logger.info("Parquet files read from local storage successfully")
                    except Exception as e:
                        logger.error(f"Error reading local Parquet files: {e}")
                        raise
            else:
                # No caching - read directly from S3
                parquet_files = [f"s3://{self.bucket}/{f}" for f in parquet_keys]
                logger.info(f"Reading {len(parquet_files)} Parquet files from S3...")
                try:
                    lf = pl.scan_parquet(
                        parquet_files,
                        storage_options=self.storage_options,
                        retries=3,
                        parallel="auto",
                    )
                    lf = self._optimize_lazyframe(lf, start_date, end_date)
                    dataframes.append(lf.collect())
                    logger.info("Parquet files read successfully")
                except Exception as e:
                    logger.error(f"Error reading Parquet files: {e}")
                    raise

        # Process CSV files - read each file individually due to varying schemas
        # AWS CUR files can have different column counts across files (AWS adds columns over time)
        if csv_keys:
            if self.use_cache:
                # Download CSV files to local cache first
                logger.info(f"Downloading {len(csv_keys)} CSV files...")
                local_paths, cache_hits, cache_misses, fresh = self._download_files_to_cache(
                    csv_keys
                )
                logger.info(
                    f"Cache: {cache_hits} hits, {cache_misses} cached, {fresh} fresh downloads"
                )

                if not local_paths:
                    logger.warning("No CSV files could be downloaded")
                else:
                    logger.info(f"Reading {len(local_paths)} CSV files from cache...")
                    try:
                        csv_dataframes = self._read_local_csv_files_parallel(
                            local_paths, start_date, end_date
                        )
                        dataframes.extend(csv_dataframes)
                        logger.info(f"CSV files read from cache: {len(csv_dataframes)} successful")
                    except Exception as e:
                        logger.error(f"Error reading local CSV files: {e}")
                        raise
            else:
                # No caching - read directly from S3
                csv_files = [f"s3://{self.bucket}/{f}" for f in csv_keys]
                logger.info(f"Reading {len(csv_files)} CSV files from S3 (no cache)...")

                csv_dataframes = self._read_csv_files_parallel(csv_files, start_date, end_date)
                dataframes.extend(csv_dataframes)
                logger.info(f"CSV files read: {len(csv_dataframes)} successful")

        if not dataframes:
            logger.error("No data could be loaded")
            return pl.DataFrame()

        # Combine all dataframes
        # Use diagonal concat to handle files with different schemas (AWS adds columns over time)
        logger.info("Combining data...")
        if len(dataframes) == 1:
            df = dataframes[0]
        else:
            df = pl.concat(dataframes, how="diagonal_relaxed")

        # Apply deduplication
        df = self._deduplicate(df)

        # Note: We do NOT filter split cost allocation rows here.
        # AWS CUR handles this correctly:
        # - Split children (EKS pods) have UnblendedCost = NULL/0
        # - Parent rows (EC2 instances) have the full cost in UnblendedCost
        # - SplitCost column is for ATTRIBUTION (showing cost per pod), not for totals
        # Summing UnblendedCost gives the correct total without double-counting.

        logger.info(f"Successfully loaded {len(df)} records from {len(report_files)} files")
        return df

    def _read_csv_files_parallel(
        self,
        csv_files: List[str],
        start_date: Optional[datetime],
        end_date: Optional[datetime],
    ) -> List[pl.DataFrame]:
        """
        Read CSV files from S3 in parallel using ThreadPoolExecutor.

        Collects each file eagerly to handle schema differences between files.
        Returns list of DataFrames.
        """
        dataframes: List[pl.DataFrame] = []
        errors: List[Tuple[str, str]] = []

        def read_single_csv(file_path: str) -> Optional[pl.DataFrame]:
            try:
                scan_kwargs: Dict[str, Any] = {
                    "storage_options": self.storage_options,
                    "ignore_errors": True,
                    "infer_schema_length": 10000,
                }

                lf = pl.scan_csv(file_path, **scan_kwargs)
                lf = self._optimize_lazyframe(lf, start_date, end_date)
                # Collect eagerly to get DataFrame with this file's schema
                return lf.collect()
            except Exception as e:
                errors.append((file_path, str(e)))
                return None

        num_workers = min(self.max_workers, len(csv_files))
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = {executor.submit(read_single_csv, fp): fp for fp in csv_files}
            for future in as_completed(futures):
                result = future.result()
                if result is not None:
                    dataframes.append(result)

        if errors:
            logger.warning(f"Failed to read {len(errors)} CSV files")
            for fp, err in errors[:5]:  # Log first 5 errors
                logger.debug(f"  {fp}: {err}")

        return dataframes

    def _infer_csv_schema_local(self, file_path: str) -> Optional[Dict[str, pl.DataType]]:
        """
        Infer schema from a local CSV file and cache it for reuse.

        Args:
            file_path: Local path to CSV file

        Returns:
            Dictionary mapping column names to Polars data types
        """
        if self._csv_schema_cache is not None:
            return self._csv_schema_cache

        try:
            lf = pl.scan_csv(
                file_path,
                ignore_errors=True,
                infer_schema_length=10000,
            )
            schema = lf.collect_schema()
            self._csv_schema_cache = {name: dtype for name, dtype in schema.items()}
            logger.info(f"Cached CSV schema with {len(self._csv_schema_cache)} columns")
            return self._csv_schema_cache
        except Exception as e:
            logger.warning(f"Failed to infer CSV schema from local file: {e}")
            return None

    def _read_local_csv_files_parallel(
        self,
        local_paths: List[str],
        start_date: Optional[datetime],
        end_date: Optional[datetime],
    ) -> List[pl.DataFrame]:
        """
        Read local CSV files in parallel using ThreadPoolExecutor.

        Collects each file eagerly to handle schema differences between files.
        Returns list of DataFrames.
        """
        dataframes: List[pl.DataFrame] = []
        errors: List[Tuple[str, str]] = []

        def read_single_csv(file_path: str) -> Optional[pl.DataFrame]:
            try:
                scan_kwargs: Dict[str, Any] = {
                    "ignore_errors": True,
                    "infer_schema_length": 10000,
                }

                lf = pl.scan_csv(file_path, **scan_kwargs)
                lf = self._optimize_lazyframe(lf, start_date, end_date)
                # Collect eagerly to get DataFrame with this file's schema
                return lf.collect()
            except Exception as e:
                errors.append((file_path, str(e)))
                return None

        # Limit workers for CSV reading to avoid memory exhaustion
        # Each Polars read uses multiple threads internally, so fewer workers is safer
        num_workers = min(self.max_workers, len(local_paths), 8)
        total_files = len(local_paths)
        completed = 0
        logger.info(f"Using {num_workers} workers for CSV reading")
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = {executor.submit(read_single_csv, fp): fp for fp in local_paths}
            for future in as_completed(futures):
                result = future.result()
                completed += 1
                if result is not None:
                    dataframes.append(result)
                # Log progress every 10 files or at the end
                if completed % 10 == 0 or completed == total_files:
                    logger.info(f"CSV read progress: {completed}/{total_files} files processed")
                    # Flush to ensure progress is visible
                    sys.stdout.flush()
                    sys.stderr.flush()

        if errors:
            logger.warning(f"Failed to read {len(errors)} local CSV files")
            for fp, err in errors[:5]:
                logger.debug(f"  {fp}: {err}")

        return dataframes

    def _deduplicate(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Deduplicate DataFrame based on line item ID.
        """
        try:
            dedup_col = None
            for col in ["identity_line_item_id", "identity/LineItemId", "lineItem/LineItemId"]:
                if col in df.columns:
                    dedup_col = col
                    break

            if dedup_col:
                original_count = len(df)
                df = df.unique(subset=[dedup_col], keep="last")
                removed = original_count - len(df)
                if removed > 0:
                    logger.info(f"Deduplication removed {removed} duplicate records")
                return df
            else:
                logger.warning("No deduplication column found in schema")
                return df
        except Exception as e:
            logger.warning(f"Could not deduplicate: {e}")
            return df

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

        # NOTE: Split Cost Allocation filtering is handled in _filter_split_cost_duplicates()
        # after all files are combined. This ensures we can identify parent resources across
        # all files (parent EC2 row might be in a different file than the EKS pod split rows).

        # NOTE: We do NOT filter by cost > 0 here because:
        # 1. Negative costs represent discounts (SavingsPlanNegation, EdpDiscount, Credits, etc.)
        # 2. Filtering them out would inflate total costs
        # 3. We need them for discount analysis/reporting
        # Zero-cost rows are kept for completeness but typically have minimal impact.

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
