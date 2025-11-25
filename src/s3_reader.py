"""S3 CUR Data Reader - Handles downloading and reading AWS Cost and Usage Reports from S3."""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
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

        # Note: s3fs doesn't allow combining 'profile' with 'client_kwargs'
        # So we need to choose one approach or the other
        if aws_profile:
            # When using a profile, pass it directly - region comes from profile config
            s3_kwargs["profile"] = aws_profile
        elif aws_region:
            # When not using a profile, we can pass region via client_kwargs
            s3_kwargs["client_kwargs"] = {"region_name": aws_region}

        self.storage_options = s3_kwargs
        self._schema_cache: Dict[str, pl.Schema] = {}  # Cache for schema lookups

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
                        # S3 LastModified is always UTC, so we normalize comparison dates to UTC
                        if start_date or end_date:
                            last_modified = obj["LastModified"]
                            # Ensure last_modified is timezone-aware (UTC)
                            if last_modified.tzinfo is None:
                                last_modified = last_modified.replace(tzinfo=timezone.utc)

                            # Convert filter dates to UTC for comparison
                            if start_date:
                                start_utc = (
                                    start_date.replace(tzinfo=timezone.utc)
                                    if start_date.tzinfo is None
                                    else start_date
                                )
                                if last_modified < start_utc:
                                    continue
                            if end_date:
                                end_utc = (
                                    end_date.replace(tzinfo=timezone.utc)
                                    if end_date.tzinfo is None
                                    else end_date
                                )
                                if last_modified > end_utc:
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

        # Validate date range
        if start_date > end_date:
            raise ValueError(
                f"Invalid date range: start_date ({start_date.date()}) "
                f"is after end_date ({end_date.date()})"
            )

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
            except (ClientError, NoCredentialsError) as e:
                logger.error(f"AWS error scanning Parquet files: {e}")
                raise
            except pl.exceptions.ComputeError as e:
                logger.error(f"Polars error scanning Parquet files: {e}")
                raise
            except Exception as e:
                logger.error(f"Unexpected error scanning Parquet files: {e}")
                # Re-raise to avoid processing partial data silently
                raise

        # Process CSV files in parallel for better performance
        if csv_files:
            logger.info(f"Processing {len(csv_files)} CSV files...")
            csv_errors: List[Tuple[str, str]] = []

            def scan_csv_file(file_path: str) -> Optional[pl.LazyFrame]:
                """Scan a single CSV file and apply optimizations."""
                try:
                    lf = pl.scan_csv(
                        file_path,
                        storage_options=self.storage_options,
                        ignore_errors=True,
                        infer_schema_length=10000,
                    )
                    return self._optimize_lazyframe(lf, start_date, end_date)
                except (ClientError, NoCredentialsError) as e:
                    csv_errors.append((file_path, str(e)))
                    return None
                except Exception as e:
                    csv_errors.append((file_path, str(e)))
                    return None

            # Use ThreadPoolExecutor for parallel CSV scanning
            max_workers = min(4, len(csv_files))  # Limit concurrent S3 connections
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(scan_csv_file, fp): fp for fp in csv_files}
                for future in as_completed(futures):
                    result = future.result()
                    if result is not None:
                        lazy_frames.append(result)

            # Log any errors that occurred
            for file_path, error in csv_errors:
                logger.warning(f"Error scanning CSV file {file_path}: {error}")

            if csv_errors and not lazy_frames:
                raise RuntimeError(f"Failed to load any CSV files. Last error: {csv_errors[-1][1]}")

        if not lazy_frames:
            logger.error("No data could be loaded")
            return pl.DataFrame()

        # Concatenate all lazy frames
        if len(lazy_frames) == 1:
            combined_lf = lazy_frames[0]
        else:
            combined_lf = pl.concat(lazy_frames, how="vertical_relaxed")

        # Deduplicate
        # Find deduplication column - cache schema to avoid multiple network calls
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
            else:
                logger.warning(
                    "No deduplication column found (identity_line_item_id, identity/LineItemId, "
                    "lineItem/LineItemId). Data may contain duplicates which could inflate costs."
                )
        except pl.exceptions.ComputeError as e:
            logger.warning(f"Could not determine schema for deduplication: {e}")
        except Exception as e:
            logger.warning(f"Unexpected error during deduplication check: {e}")

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
                lf = lf.with_columns(pl.col(date_col).str.to_datetime(strict=False))

            if start_date:
                lf = lf.filter(pl.col(date_col) >= start_date)
            if end_date:
                lf = lf.filter(pl.col(date_col) <= end_date)

        return lf
