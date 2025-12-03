"""Data Processor - Handles processing and aggregation of AWS CUR data using Polars."""

import logging
from typing import Any, Dict, Optional

import pandas as pd
import polars as pl

logger = logging.getLogger(__name__)


class CURDataProcessor:
    """Process and analyze AWS Cost and Usage Report data using Polars."""

    def __init__(self, df: pl.DataFrame) -> None:
        """
        Initialize the data processor.

        Args:
            df: Polars DataFrame containing CUR data
        """
        # No copy needed for Polars (immutable/CoW)
        self.df = df
        self.normalized_columns = self._normalize_column_names()
        # Cache for aggregation results to avoid recomputation
        self._cache: Dict[str, pd.DataFrame] = {}
        logger.info(f"Initialized processor with {len(self.df)} records")

    def _normalize_column_names(self) -> Dict[str, Optional[str]]:
        """
        Normalize column names to handle different CUR versions.
        Creates a mapping of standard names to actual column names.

        Returns:
            Dictionary mapping normalized names to actual column names (or None if not found)
        """
        column_map: Dict[str, Optional[str]] = {
            "cost": None,
            "usage_date": None,
            "account_id": None,
            "service": None,
            "usage_type": None,
            "operation": None,
            "region": None,
            "resource_id": None,
            "line_item_type": None,
        }

        # Define possible column names for each standard field
        column_patterns = {
            "cost": [
                "line_item_unblended_cost",
                "lineItem/UnblendedCost",
                "line_item_blended_cost",
                "lineItem/BlendedCost",
                "cost",
                "unblended_cost",
            ],
            "usage_date": [
                "line_item_usage_start_date",
                "lineItem/UsageStartDate",
                "usage_start_date",
                "bill_billing_period_start_date",
            ],
            "account_id": [
                "line_item_usage_account_id",
                "lineItem/UsageAccountId",
                "usage_account_id",
                "bill_payer_account_id",
            ],
            "service": [
                "line_item_product_code",
                "lineItem/ProductCode",
                "product_product_name",
                "product/ProductName",
                "service",
                "product_name",
            ],
            "usage_type": ["line_item_usage_type", "lineItem/UsageType", "usage_type"],
            "operation": ["line_item_operation", "lineItem/Operation", "operation"],
            "region": [
                "product_region",
                "product/region",
                "line_item_availability_zone",
                "lineItem/AvailabilityZone",
                "region",
            ],
            "resource_id": ["line_item_resource_id", "lineItem/ResourceId", "resource_id"],
            "line_item_type": [
                "lineItem/LineItemType",
                "line_item_line_item_type",
                "line_item_type",
            ],
        }

        # Find actual column names
        available_columns = self.df.columns
        for standard_name, possible_names in column_patterns.items():
            for col_name in possible_names:
                if col_name in available_columns:
                    column_map[standard_name] = col_name
                    break

        # Log found columns
        for standard, actual in column_map.items():
            if actual:
                logger.debug(f"Mapped '{standard}' to '{actual}'")
            else:
                logger.warning(f"Could not find column for '{standard}'")

        return column_map

    def prepare_data(self) -> pl.DataFrame:
        """
        Prepare and clean the CUR data for analysis.

        Returns:
            Cleaned Polars DataFrame with normalized column names
        """
        logger.info("Preparing CUR data...")

        # Build expressions for selection and renaming
        expressions = []
        for standard_name, actual_name in self.normalized_columns.items():
            if actual_name and actual_name in self.df.columns:
                expressions.append(pl.col(actual_name).alias(standard_name))

        if not expressions:
            logger.warning("No valid columns found to process")
            self.prepared_df = pl.DataFrame()
            return self.prepared_df

        # Select and rename columns
        lf = self.df.lazy().select(expressions)

        # Type casting and transformations
        if "cost" in self.normalized_columns and self.normalized_columns["cost"]:
            lf = lf.with_columns(pl.col("cost").cast(pl.Float64, strict=False).fill_null(0.0))
            # Note: Do NOT filter out negative costs - they represent discounts, credits,
            # and negations (SavingsPlanNegation, EdpDiscount, PrivateRateDiscount, etc.)
            # that offset positive usage charges. Filtering them causes double-counting.

        if "usage_date" in self.normalized_columns and self.normalized_columns["usage_date"]:
            # Check if conversion is needed
            original_col = self.normalized_columns["usage_date"]
            if original_col in self.df.columns:
                dtype = self.df.schema[original_col]
                if dtype == pl.Utf8 or dtype == pl.String:
                    # AWS CUR uses ISO 8601 format with timezone (e.g., 2024-01-15T00:00:00Z)
                    # Use %+ format which handles RFC 3339/ISO 8601 with timezone
                    lf = lf.with_columns(
                        [
                            pl.col("usage_date")
                            .str.to_datetime(format="%+", strict=False)
                            .dt.replace_time_zone(None)  # Remove timezone for consistent handling
                            .alias("usage_date"),
                        ]
                    )

            # Add derived date columns
            lf = lf.with_columns(
                [
                    pl.col("usage_date").dt.strftime("%Y-%m").alias("year_month"),
                    pl.col("usage_date").dt.date().alias("date"),
                ]
            )

        # Fill nulls for string columns
        string_columns = [
            "account_id",
            "service",
            "usage_type",
            "operation",
            "region",
            "line_item_type",
        ]
        fill_exprs = []
        for col in string_columns:
            if col in self.normalized_columns and self.normalized_columns[col]:
                fill_exprs.append(pl.col(col).fill_null("Unknown").cast(pl.Categorical))

        if fill_exprs:
            lf = lf.with_columns(fill_exprs)

        # Execute transformations
        self.prepared_df = lf.collect()
        logger.info(f"Prepared {len(self.prepared_df)} records for analysis")
        return self.prepared_df

    def get_total_cost(self) -> float:
        """Get total cost across all data."""
        if not hasattr(self, "prepared_df"):
            self.prepare_data()
        return float(self.prepared_df["cost"].sum())

    def _get_usage_df(self) -> pl.DataFrame:
        """
        Get DataFrame filtered to only Usage and Tax line items.

        This excludes Credits, SavingsPlanNegation, EdpDiscount, etc.
        which are shown separately in discount charts.
        """
        if "line_item_type" not in self.prepared_df.columns:
            # No line_item_type column - return all data
            return self.prepared_df

        # Include only actual charge types - do NOT include SavingsPlanCoveredUsage
        # because it shows the on-demand equivalent cost, not the actual cost.
        # The actual SP cost is in SavingsPlanRecurringFee.
        #
        # AWS CUR Savings Plan line items:
        # - SavingsPlanRecurringFee: The actual hourly cost of your SP commitment
        # - SavingsPlanCoveredUsage: On-demand equivalent (for tracking what SP covered) - DO NOT SUM
        # - SavingsPlanNegation: Offsets SavingsPlanCoveredUsage - only needed if including CoveredUsage
        #
        # Including both Usage AND SavingsPlanCoveredUsage causes double-counting!
        usage_types = [
            "Usage",
            "Tax",
            "Fee",
            "RIFee",
            "SavingsPlanRecurringFee",
            # NOT SavingsPlanCoveredUsage - that would double-count!
        ]
        return self.prepared_df.filter(pl.col("line_item_type").is_in(usage_types))

    def get_cost_by_service(self, top_n: Optional[int] = None) -> pd.DataFrame:
        """
        Aggregate costs by service (Usage charges only, excludes credits/discounts).

        Args:
            top_n: Return only top N services by cost

        Returns:
            Pandas DataFrame with service costs (for visualization compatibility)
        """
        if not hasattr(self, "prepared_df") or self.prepared_df.is_empty():
            self.prepare_data()

        # Check cache first
        cache_key = f"cost_by_service_{top_n}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        # Return empty DataFrame if no data
        if self.prepared_df.is_empty() or "service" not in self.prepared_df.columns:
            logger.warning("No data available for cost by service aggregation")
            return pd.DataFrame(columns=["service", "total_cost"])

        logger.info("Calculating cost by service...")

        # Filter to usage charges only (exclude credits/discounts)
        usage_df = self._get_usage_df()

        result = (
            usage_df.group_by("service")
            .agg(pl.col("cost").sum().alias("total_cost"))
            .sort("total_cost", descending=True)
        )

        if top_n:
            result = result.head(top_n)

        result_df = result.to_pandas()
        self._cache[cache_key] = result_df
        return result_df

    def get_cost_by_account(self, top_n: Optional[int] = None) -> pd.DataFrame:
        """
        Aggregate costs by AWS account (Usage charges only, excludes credits/discounts).

        Args:
            top_n: Return only top N accounts by cost

        Returns:
            Pandas DataFrame with account costs
        """
        if not hasattr(self, "prepared_df") or self.prepared_df.is_empty():
            self.prepare_data()

        # Check cache first
        cache_key = f"cost_by_account_{top_n}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        # Return empty DataFrame if no data
        if self.prepared_df.is_empty() or "account_id" not in self.prepared_df.columns:
            logger.warning("No data available for cost by account aggregation")
            return pd.DataFrame(columns=["account_id", "total_cost"])

        logger.info("Calculating cost by account...")

        # Filter to usage charges only (exclude credits/discounts)
        usage_df = self._get_usage_df()

        result = (
            usage_df.group_by("account_id")
            .agg(pl.col("cost").sum().alias("total_cost"))
            .sort("total_cost", descending=True)
        )

        if top_n:
            result = result.head(top_n)

        result_df = result.to_pandas()
        self._cache[cache_key] = result_df
        return result_df

    def get_discounts_summary(self) -> pd.DataFrame:
        """
        Get summary of all discounts, credits, and rate reductions.

        Returns:
            Pandas DataFrame with discount breakdown by type and service
        """
        if not hasattr(self, "prepared_df") or self.prepared_df.is_empty():
            self.prepare_data()

        # Check cache first
        cache_key = "discounts_summary"
        if cache_key in self._cache:
            return self._cache[cache_key]

        # Return empty DataFrame if no data or missing columns
        if self.prepared_df.is_empty() or "line_item_type" not in self.prepared_df.columns:
            logger.warning("No data available for discounts summary")
            return pd.DataFrame(columns=["discount_type", "total_discount"])

        logger.info("Calculating discounts summary...")

        # Discount-related line item types
        discount_types = [
            "SavingsPlanNegation",
            "EdpDiscount",
            "PrivateRateDiscount",
            "BundledDiscount",
            "Credit",
        ]

        # Filter to discount rows only (negative costs)
        discount_df = self.prepared_df.filter(pl.col("line_item_type").is_in(discount_types))

        if discount_df.is_empty():
            logger.info("No discounts found in the data")
            return pd.DataFrame(columns=["discount_type", "total_discount"])

        # Aggregate by discount type
        result = (
            discount_df.group_by("line_item_type")
            .agg(pl.col("cost").sum().alias("total_discount"))
            .sort("total_discount")  # Most negative (biggest discount) first
            .rename({"line_item_type": "discount_type"})
        )

        result_df = result.to_pandas()
        # Convert to positive values for display (discounts are negative in raw data)
        result_df["total_discount"] = result_df["total_discount"].abs()
        self._cache[cache_key] = result_df
        return result_df

    def get_discounts_by_service(self, top_n: int = 10) -> pd.DataFrame:
        """
        Get discount breakdown by service.

        Args:
            top_n: Number of top services by discount to return

        Returns:
            Pandas DataFrame with discount amounts by service
        """
        if not hasattr(self, "prepared_df") or self.prepared_df.is_empty():
            self.prepare_data()

        # Check cache first
        cache_key = f"discounts_by_service_{top_n}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        # Return empty DataFrame if no data
        if self.prepared_df.is_empty() or "line_item_type" not in self.prepared_df.columns:
            logger.warning("No data available for discounts by service")
            return pd.DataFrame(columns=["service", "total_discount"])

        logger.info("Calculating discounts by service...")

        # Discount-related line item types
        discount_types = [
            "SavingsPlanNegation",
            "EdpDiscount",
            "PrivateRateDiscount",
            "BundledDiscount",
            "Credit",
        ]

        # Filter to discount rows only
        discount_df = self.prepared_df.filter(pl.col("line_item_type").is_in(discount_types))

        if discount_df.is_empty():
            logger.info("No discounts found in the data")
            return pd.DataFrame(columns=["service", "total_discount"])

        # Aggregate by service
        result = (
            discount_df.group_by("service")
            .agg(pl.col("cost").sum().alias("total_discount"))
            .sort("total_discount")  # Most negative first
            .head(top_n)
        )

        result_df = result.to_pandas()
        # Convert to positive values for display
        result_df["total_discount"] = result_df["total_discount"].abs()
        self._cache[cache_key] = result_df
        return result_df

    def get_savings_plan_analysis(self) -> pd.DataFrame:
        """
        Analyze Savings Plan effectiveness by comparing on-demand equivalent cost vs actual cost.

        This shows how much you're saving with Savings Plans by comparing:
        - SavingsPlanCoveredUsage: The on-demand equivalent cost (what you would have paid)
        - SavingsPlanRecurringFee: The actual Savings Plan cost you paid

        Returns:
            Pandas DataFrame with columns:
            - service: The AWS service
            - on_demand_equivalent: What the on-demand cost would have been
            - savings_plan_cost: The actual SP recurring fee (may be 0 if aggregated elsewhere)
            - savings: The amount saved (on_demand_equivalent - actual charges)
        """
        if not hasattr(self, "prepared_df") or self.prepared_df.is_empty():
            self.prepare_data()

        cache_key = "savings_plan_analysis"
        if cache_key in self._cache:
            return self._cache[cache_key]

        if "line_item_type" not in self.prepared_df.columns:
            logger.warning("No line_item_type column - cannot analyze Savings Plans")
            return pd.DataFrame(
                columns=["service", "on_demand_equivalent", "savings_plan_cost", "savings"]
            )

        logger.info("Analyzing Savings Plan effectiveness...")

        # Get SavingsPlanCoveredUsage (on-demand equivalent for covered usage)
        covered_usage = (
            self.prepared_df.filter(pl.col("line_item_type") == "SavingsPlanCoveredUsage")
            .group_by("service")
            .agg(pl.col("cost").sum().alias("on_demand_equivalent"))
        )

        # Get SavingsPlanRecurringFee (actual SP commitment cost)
        recurring_fee = (
            self.prepared_df.filter(pl.col("line_item_type") == "SavingsPlanRecurringFee")
            .group_by("service")
            .agg(pl.col("cost").sum().alias("savings_plan_cost"))
        )

        # Get SavingsPlanNegation (the offset/savings amount - negative values)
        negation = (
            self.prepared_df.filter(pl.col("line_item_type") == "SavingsPlanNegation")
            .group_by("service")
            .agg(pl.col("cost").sum().alias("negation"))
        )

        if covered_usage.is_empty():
            logger.info("No Savings Plan covered usage found in data")
            return pd.DataFrame(
                columns=["service", "on_demand_equivalent", "savings_plan_cost", "savings"]
            )

        # Join the data
        result = (
            covered_usage.join(recurring_fee, on="service", how="left")
            .join(negation, on="service", how="left")
            .with_columns(
                [
                    pl.col("savings_plan_cost").fill_null(0.0),
                    pl.col("negation").fill_null(0.0),
                ]
            )
            # Savings = negation amount (which is negative, so we take abs)
            .with_columns(pl.col("negation").abs().alias("savings"))
            .drop("negation")
            .sort("on_demand_equivalent", descending=True)
        )

        result_df = result.to_pandas()
        self._cache[cache_key] = result_df

        total_on_demand = result_df["on_demand_equivalent"].sum()
        total_savings = result_df["savings"].sum()
        if total_on_demand > 0:
            pct_savings = (total_savings / total_on_demand) * 100
            logger.info(
                f"Savings Plan analysis: ${total_savings:,.2f} saved ({pct_savings:.1f}% of on-demand)"
            )

        return result_df

    def get_savings_plan_summary(self) -> dict:
        """
        Get a summary of Savings Plan effectiveness across all services.

        Returns:
            Dictionary with:
            - on_demand_equivalent: Total on-demand cost that would have been charged
            - savings_plan_cost: Total Savings Plan recurring fees
            - total_savings: Amount saved by using Savings Plans
            - savings_percentage: Percentage saved vs on-demand
        """
        sp_analysis = self.get_savings_plan_analysis()

        if sp_analysis.empty:
            return {
                "on_demand_equivalent": 0.0,
                "savings_plan_cost": 0.0,
                "total_savings": 0.0,
                "savings_percentage": 0.0,
            }

        on_demand = float(sp_analysis["on_demand_equivalent"].sum())
        sp_cost = float(sp_analysis["savings_plan_cost"].sum())
        savings = float(sp_analysis["savings"].sum())
        pct = (savings / on_demand * 100) if on_demand > 0 else 0.0

        return {
            "on_demand_equivalent": on_demand,
            "savings_plan_cost": sp_cost,
            "total_savings": savings,
            "savings_percentage": pct,
        }

    def get_cost_by_account_and_service(
        self, top_accounts: int = 10, top_services: int = 10
    ) -> pd.DataFrame:
        """
        Get detailed breakdown of costs by account and service.

        Args:
            top_accounts: Number of top accounts to include
            top_services: Number of top services to include

        Returns:
            Pandas DataFrame with account-service cost breakdown
        """
        if not hasattr(self, "prepared_df"):
            self.prepare_data()

        logger.info("Calculating cost by account and service...")

        # Get top accounts and services
        top_account_df = self.get_cost_by_account(top_accounts)
        top_service_df = self.get_cost_by_service(top_services)

        top_accounts_list = top_account_df["account_id"].tolist()
        top_services_list = top_service_df["service"].tolist()

        # Filter to usage charges only and aggregate
        usage_df = self._get_usage_df()
        result = (
            usage_df.filter(
                pl.col("account_id").is_in(top_accounts_list)
                & pl.col("service").is_in(top_services_list)
            )
            .group_by(["account_id", "service"])
            .agg(pl.col("cost").sum().alias("total_cost"))
            .sort(["account_id", "total_cost"], descending=[False, True])
        )

        return result.to_pandas()

    def get_cost_trend_by_service(self, top_services: int = 5) -> pd.DataFrame:
        """
        Get monthly cost trends over time for top services.

        Args:
            top_services: Number of top services to include

        Returns:
            Pandas DataFrame with service cost trends by month
        """
        if not hasattr(self, "prepared_df"):
            self.prepare_data()

        logger.info(f"Calculating monthly cost trends for top {top_services} services...")

        # Get top services
        top_service_df = self.get_cost_by_service(top_services)
        top_services_list = top_service_df["service"].tolist()

        # Filter to usage charges only and aggregate
        usage_df = self._get_usage_df()
        result = (
            usage_df.filter(pl.col("service").is_in(top_services_list))
            .group_by(["year_month", "service"])
            .agg(pl.col("cost").sum().alias("total_cost"))
            .sort(["service", "year_month"])
            .with_columns(pl.col("year_month").cast(pl.String).alias("month"))
        )

        return result.to_pandas()

    def get_cost_trend_by_account(self, top_accounts: int = 5) -> pd.DataFrame:
        """
        Get monthly cost trends over time for top accounts.

        Args:
            top_accounts: Number of top accounts to include

        Returns:
            Pandas DataFrame with account cost trends by month
        """
        if not hasattr(self, "prepared_df"):
            self.prepare_data()

        logger.info(f"Calculating monthly cost trends for top {top_accounts} accounts...")

        # Get top accounts
        top_account_df = self.get_cost_by_account(top_accounts)
        top_accounts_list = top_account_df["account_id"].tolist()

        # Filter to usage charges only and aggregate
        usage_df = self._get_usage_df()
        result = (
            usage_df.filter(pl.col("account_id").is_in(top_accounts_list))
            .group_by(["year_month", "account_id"])
            .agg(pl.col("cost").sum().alias("total_cost"))
            .sort(["account_id", "year_month"])
            .with_columns(pl.col("year_month").cast(pl.String).alias("month"))
        )

        return result.to_pandas()

    def get_monthly_summary(self) -> pd.DataFrame:
        """
        Get monthly cost summary (Usage charges only, excludes credits/discounts).

        Returns:
            Pandas DataFrame with monthly aggregated costs
        """
        if not hasattr(self, "prepared_df"):
            self.prepare_data()

        logger.info("Calculating monthly summary...")

        # Filter to usage charges only
        usage_df = self._get_usage_df()

        result = (
            usage_df.group_by("year_month")
            .agg(
                [
                    pl.col("cost").sum().alias("total_cost"),
                    pl.col("cost").mean().alias("avg_record_cost"),
                    pl.col("cost").count().alias("num_records"),
                ]
            )
            .sort("year_month")
            .with_columns(pl.col("year_month").cast(pl.String).alias("month"))
        )

        return result.to_pandas()

    def detect_cost_anomalies(
        self, threshold_std: float = 2.0, top_services: int = 10
    ) -> pd.DataFrame:
        """
        Detect months with anomalous costs by service (statistical outliers).

        Args:
            threshold_std: Number of standard deviations for anomaly detection (must be > 0)
            top_services: Number of top services to analyze for anomalies

        Returns:
            Pandas DataFrame with anomalous service costs by month
        """
        # Validate parameters
        if threshold_std <= 0:
            raise ValueError(f"threshold_std must be positive, got {threshold_std}")

        if not hasattr(self, "prepared_df") or self.prepared_df.is_empty():
            self.prepare_data()

        # Return empty DataFrame if no data
        if self.prepared_df.is_empty():
            logger.warning("No data available for anomaly detection")
            return pd.DataFrame(
                columns=["month", "service", "total_cost", "mean_cost", "z_score", "pct_change"]
            )

        logger.info("Detecting cost anomalies by service and month...")

        # Get top services
        top_service_df = self.get_cost_by_service(top_services)
        if top_service_df.empty:
            return pd.DataFrame(
                columns=["month", "service", "total_cost", "mean_cost", "z_score", "pct_change"]
            )

        top_services_list = top_service_df["service"].tolist()

        # Calculate monthly costs and stats using window functions
        # Polars makes this much cleaner than Pandas self-joins
        # IMPORTANT: Filter std_cost > 0 BEFORE division to avoid division by zero
        result = (
            self.prepared_df.filter(pl.col("service").is_in(top_services_list))
            .group_by(["year_month", "service"])
            .agg(pl.col("cost").sum().alias("total_cost"))
            .with_columns(
                [
                    pl.col("total_cost").mean().over("service").alias("mean_cost"),
                    pl.col("total_cost").std().over("service").alias("std_cost"),
                ]
            )
            # Filter BEFORE division to prevent division by zero
            .filter(pl.col("std_cost") > 0.01)
            .filter(pl.col("mean_cost").abs() > 0.01)  # Also prevent pct_change division by zero
            .with_columns(
                [
                    # Safe division - std_cost > 0.01 guaranteed by filter above
                    ((pl.col("total_cost") - pl.col("mean_cost")) / pl.col("std_cost")).alias(
                        "z_score"
                    ),
                    # Safe division - mean_cost > 0.01 guaranteed by filter above
                    (
                        (pl.col("total_cost") - pl.col("mean_cost")) / pl.col("mean_cost") * 100
                    ).alias("pct_change"),
                    pl.col("year_month").cast(pl.String).alias("month"),
                ]
            )
            .filter(pl.col("z_score").abs() > threshold_std)
            .sort(pl.col("z_score").abs(), descending=True)
            .drop(["std_cost"])
        )

        logger.info(f"Found {len(result)} anomalous service/month combinations")
        return result.to_pandas()

    def get_cost_by_region(self, top_n: Optional[int] = None) -> pd.DataFrame:
        """
        Aggregate costs by region (Usage charges only, excludes credits/discounts).

        Args:
            top_n: Return only top N regions by cost

        Returns:
            Pandas DataFrame with region costs
        """
        if not hasattr(self, "prepared_df"):
            self.prepare_data()

        if "region" not in self.prepared_df.columns:
            logger.warning("Region data not available")
            return pd.DataFrame()

        logger.info("Calculating cost by region...")

        # Filter to usage charges only
        usage_df = self._get_usage_df()

        result = (
            usage_df.group_by("region")
            .agg(pl.col("cost").sum().alias("total_cost"))
            .sort("total_cost", descending=True)
        )

        if top_n:
            result = result.head(top_n)

        return result.to_pandas()

    def get_cost_trend_by_region(self, top_regions: int = 5) -> pd.DataFrame:
        """
        Get monthly cost trends over time for top regions.

        Args:
            top_regions: Number of top regions to include

        Returns:
            Pandas DataFrame with region cost trends by month
        """
        if not hasattr(self, "prepared_df"):
            self.prepare_data()

        if "region" not in self.prepared_df.columns:
            logger.warning("Region data not available")
            return pd.DataFrame(columns=["month", "region", "total_cost"])

        logger.info(f"Calculating monthly cost trends for top {top_regions} regions...")

        # Get top regions
        top_region_df = self.get_cost_by_region(top_regions)
        if top_region_df.empty:
            return pd.DataFrame(columns=["month", "region", "total_cost"])

        top_regions_list = top_region_df["region"].tolist()

        # Filter to usage charges only and aggregate
        usage_df = self._get_usage_df()
        result = (
            usage_df.filter(pl.col("region").is_in(top_regions_list))
            .group_by(["year_month", "region"])
            .agg(pl.col("cost").sum().alias("total_cost"))
            .sort(["region", "year_month"])
            .with_columns(pl.col("year_month").cast(pl.String).alias("month"))
        )

        return result.to_pandas()

    def get_discounts_trend(self) -> pd.DataFrame:
        """
        Get monthly trend of discounts by type.

        Returns:
            Pandas DataFrame with discount trends by month and type
        """
        if not hasattr(self, "prepared_df") or self.prepared_df.is_empty():
            self.prepare_data()

        if "line_item_type" not in self.prepared_df.columns:
            logger.warning("No line_item_type column - cannot analyze discounts trend")
            return pd.DataFrame(columns=["month", "discount_type", "total_discount"])

        logger.info("Calculating monthly discounts trend...")

        # Discount-related line item types
        discount_types = [
            "SavingsPlanNegation",
            "EdpDiscount",
            "PrivateRateDiscount",
            "BundledDiscount",
            "Credit",
        ]

        # Filter to discount rows only
        discount_df = self.prepared_df.filter(pl.col("line_item_type").is_in(discount_types))

        if discount_df.is_empty():
            logger.info("No discounts found in the data")
            return pd.DataFrame(columns=["month", "discount_type", "total_discount"])

        # Aggregate by month and discount type
        result = (
            discount_df.group_by(["year_month", "line_item_type"])
            .agg(pl.col("cost").sum().alias("total_discount"))
            .sort(["line_item_type", "year_month"])
            .with_columns(pl.col("year_month").cast(pl.String).alias("month"))
            .rename({"line_item_type": "discount_type"})
        )

        result_df = result.to_pandas()
        # Convert to positive values for display
        result_df["total_discount"] = result_df["total_discount"].abs()
        return result_df

    def get_discounts_by_service_trend(self, top_n: int = 5) -> pd.DataFrame:
        """
        Get monthly trend of discounts by service.

        Args:
            top_n: Number of top services by discount to include

        Returns:
            Pandas DataFrame with discount trends by month and service
        """
        if not hasattr(self, "prepared_df") or self.prepared_df.is_empty():
            self.prepare_data()

        if "line_item_type" not in self.prepared_df.columns:
            logger.warning("No line_item_type column - cannot analyze discounts by service trend")
            return pd.DataFrame(columns=["month", "service", "total_discount"])

        logger.info(f"Calculating monthly discounts trend for top {top_n} services...")

        # Get top services by discount
        top_service_df = self.get_discounts_by_service(top_n)
        if top_service_df.empty:
            return pd.DataFrame(columns=["month", "service", "total_discount"])

        top_services_list = top_service_df["service"].tolist()

        # Discount-related line item types
        discount_types = [
            "SavingsPlanNegation",
            "EdpDiscount",
            "PrivateRateDiscount",
            "BundledDiscount",
            "Credit",
        ]

        # Filter to discount rows and top services
        discount_df = self.prepared_df.filter(
            pl.col("line_item_type").is_in(discount_types)
            & pl.col("service").is_in(top_services_list)
        )

        if discount_df.is_empty():
            return pd.DataFrame(columns=["month", "service", "total_discount"])

        # Aggregate by month and service
        result = (
            discount_df.group_by(["year_month", "service"])
            .agg(pl.col("cost").sum().alias("total_discount"))
            .sort(["service", "year_month"])
            .with_columns(pl.col("year_month").cast(pl.String).alias("month"))
        )

        result_df = result.to_pandas()
        # Convert to positive values for display
        result_df["total_discount"] = result_df["total_discount"].abs()
        return result_df

    def get_savings_plan_trend(self) -> pd.DataFrame:
        """
        Get monthly trend of Savings Plan effectiveness.

        Returns:
            Pandas DataFrame with columns:
            - month: The billing month
            - on_demand_equivalent: What on-demand would have cost
            - savings: Amount saved that month
            - savings_percentage: Percentage saved vs on-demand
        """
        if not hasattr(self, "prepared_df") or self.prepared_df.is_empty():
            self.prepare_data()

        if "line_item_type" not in self.prepared_df.columns:
            logger.warning("No line_item_type column - cannot analyze Savings Plan trend")
            return pd.DataFrame(
                columns=["month", "on_demand_equivalent", "savings", "savings_percentage"]
            )

        logger.info("Calculating monthly Savings Plan trend...")

        # Get SavingsPlanCoveredUsage by month (on-demand equivalent)
        covered_usage = (
            self.prepared_df.filter(pl.col("line_item_type") == "SavingsPlanCoveredUsage")
            .group_by("year_month")
            .agg(pl.col("cost").sum().alias("on_demand_equivalent"))
        )

        # Get SavingsPlanNegation by month (the savings amount - negative values)
        negation = (
            self.prepared_df.filter(pl.col("line_item_type") == "SavingsPlanNegation")
            .group_by("year_month")
            .agg(pl.col("cost").sum().alias("negation"))
        )

        if covered_usage.is_empty():
            logger.info("No Savings Plan covered usage found in data")
            return pd.DataFrame(
                columns=["month", "on_demand_equivalent", "savings", "savings_percentage"]
            )

        # Join the data
        result = (
            covered_usage.join(negation, on="year_month", how="left")
            .with_columns(pl.col("negation").fill_null(0.0))
            .with_columns(pl.col("negation").abs().alias("savings"))
            .drop("negation")
            # Avoid division by zero - use 0% if on_demand_equivalent is negligible
            .with_columns(
                pl.when(pl.col("on_demand_equivalent") > 0.01)
                .then((pl.col("savings") / pl.col("on_demand_equivalent") * 100))
                .otherwise(0.0)
                .alias("savings_percentage")
            )
            .sort("year_month")
            .with_columns(pl.col("year_month").cast(pl.String).alias("month"))
        )

        return result.to_pandas()

    def get_summary_statistics(self) -> Dict[str, Any]:
        """
        Get overall summary statistics.

        Returns:
            Dictionary with summary statistics including:
            - total_cost: Usage charges only (excludes credits/discounts)
            - net_cost: Total including credits/discounts
            - total_discounts: Sum of all discounts/credits (positive value)
        """
        if not hasattr(self, "prepared_df") or self.prepared_df.is_empty():
            self.prepare_data()

        df = self.prepared_df

        # Handle empty DataFrame case
        if df.is_empty():
            return {
                "total_cost": 0.0,
                "net_cost": 0.0,
                "total_discounts": 0.0,
                "num_accounts": 0,
                "num_services": 0,
                "date_range_start": "N/A",
                "date_range_end": "N/A",
                "total_records": 0,
            }

        # Safely get date range - handle null values
        date_start = "N/A"
        date_end = "N/A"

        if "usage_date" in df.columns:
            min_date = df["usage_date"].min()
            max_date = df["usage_date"].max()

            if min_date is not None:
                date_method = getattr(min_date, "date", None)
                if date_method is not None and callable(date_method):
                    date_start = str(date_method())
                else:
                    date_start = str(min_date)

            if max_date is not None:
                date_method = getattr(max_date, "date", None)
                if date_method is not None and callable(date_method):
                    date_end = str(date_method())
                else:
                    date_end = str(max_date)

        # Calculate usage-only total (for main reporting)
        usage_df = self._get_usage_df()
        usage_total = float(usage_df["cost"].sum()) if "cost" in usage_df.columns else 0.0

        # Calculate net total (including discounts)
        net_total = float(df["cost"].sum()) if "cost" in df.columns else 0.0

        # Calculate total discounts (make it positive for display)
        total_discounts = usage_total - net_total

        summary: Dict[str, Any] = {
            "total_cost": usage_total,  # Usage charges only
            "net_cost": net_total,  # After discounts
            "total_discounts": total_discounts,  # Positive value for display
            "num_accounts": int(df["account_id"].n_unique()) if "account_id" in df.columns else 0,
            "num_services": int(df["service"].n_unique()) if "service" in df.columns else 0,
            "date_range_start": date_start,
            "date_range_end": date_end,
            "total_records": len(df),
        }

        return summary
