"""Data Processor - Handles processing and aggregation of AWS CUR data."""

import logging
from typing import Dict, Optional

import pandas as pd

logger = logging.getLogger(__name__)


class CURDataProcessor:
    """Process and analyze AWS Cost and Usage Report data."""

    def __init__(self, df: pd.DataFrame):
        """
        Initialize the data processor.

        Args:
            df: DataFrame containing CUR data
        """
        self.df = df.copy()
        self.normalized_columns = self._normalize_column_names()
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
        }

        # Find actual column names
        for standard_name, possible_names in column_patterns.items():
            for col_name in possible_names:
                if col_name in self.df.columns:
                    column_map[standard_name] = col_name
                    break

        # Log found columns
        for standard, actual in column_map.items():
            if actual:
                logger.debug(f"Mapped '{standard}' to '{actual}'")
            else:
                logger.warning(f"Could not find column for '{standard}'")

        return column_map

    def prepare_data(self) -> pd.DataFrame:
        """
        Prepare and clean the CUR data for analysis.

        Returns:
            Cleaned DataFrame with normalized column names
        """
        logger.info("Preparing CUR data...")

        # Create working dataframe with normalized columns
        working_df = pd.DataFrame()

        for standard_name, actual_name in self.normalized_columns.items():
            if actual_name and actual_name in self.df.columns:
                working_df[standard_name] = self.df[actual_name]

        # Convert cost to numeric, handling any non-numeric values
        if "cost" in working_df.columns:
            working_df["cost"] = pd.to_numeric(working_df["cost"], errors="coerce")
            working_df["cost"] = working_df["cost"].fillna(0)

        # Convert date columns
        if "usage_date" in working_df.columns:
            working_df["usage_date"] = pd.to_datetime(working_df["usage_date"], errors="coerce")
            working_df["year_month"] = working_df["usage_date"].dt.to_period("M")
            working_df["year_week"] = working_df["usage_date"].dt.to_period("W")
            working_df["date"] = working_df["usage_date"].dt.date

        # Clean string columns
        string_columns = ["account_id", "service", "usage_type", "operation", "region"]
        for col in string_columns:
            if col in working_df.columns:
                working_df[col] = working_df[col].fillna("Unknown")
                working_df[col] = working_df[col].astype(str)

        # Remove rows with zero or negative cost
        if "cost" in working_df.columns:
            initial_rows = len(working_df)
            cost_col: pd.Series = working_df["cost"]  # type: ignore[assignment]
            working_df = working_df[cost_col > 0]
            removed_rows = initial_rows - len(working_df)
            if removed_rows > 0:
                logger.info(f"Removed {removed_rows} rows with zero or negative cost")

        logger.info(f"Prepared {len(working_df)} records for analysis")
        self.prepared_df = working_df
        return pd.DataFrame(working_df)  # Explicit cast for type checker

    def get_total_cost(self) -> float:
        """Get total cost across all data."""
        if not hasattr(self, "prepared_df"):
            self.prepare_data()
        return float(self.prepared_df["cost"].sum())

    def get_cost_by_service(self, top_n: Optional[int] = None) -> pd.DataFrame:
        """
        Aggregate costs by service.

        Args:
            top_n: Return only top N services by cost

        Returns:
            DataFrame with service costs
        """
        if not hasattr(self, "prepared_df"):
            self.prepare_data()

        logger.info("Calculating cost by service...")
        result = self.prepared_df.groupby("service").agg({"cost": "sum"}).reset_index()
        result.columns = ["service", "total_cost"]
        result = result.sort_values("total_cost", ascending=False)

        if top_n:
            result = result.head(top_n)

        return result

    def get_cost_by_account(self, top_n: Optional[int] = None) -> pd.DataFrame:
        """
        Aggregate costs by AWS account.

        Args:
            top_n: Return only top N accounts by cost

        Returns:
            DataFrame with account costs
        """
        if not hasattr(self, "prepared_df"):
            self.prepare_data()

        logger.info("Calculating cost by account...")
        result = self.prepared_df.groupby("account_id").agg({"cost": "sum"}).reset_index()
        result.columns = ["account_id", "total_cost"]
        result = result.sort_values("total_cost", ascending=False)

        if top_n:
            result = result.head(top_n)

        return result

    def get_cost_by_account_and_service(
        self, top_accounts: int = 10, top_services: int = 10
    ) -> pd.DataFrame:
        """
        Get detailed breakdown of costs by account and service.

        Args:
            top_accounts: Number of top accounts to include
            top_services: Number of top services to include

        Returns:
            DataFrame with account-service cost breakdown
        """
        if not hasattr(self, "prepared_df"):
            self.prepare_data()

        logger.info("Calculating cost by account and service...")

        # Get top accounts and services
        top_account_list = self.get_cost_by_account(top_accounts)["account_id"].tolist()
        top_service_list = self.get_cost_by_service(top_services)["service"].tolist()

        # Filter data
        account_col: pd.Series = self.prepared_df["account_id"]  # type: ignore[assignment]
        service_col: pd.Series = self.prepared_df["service"]  # type: ignore[assignment]
        filtered_df: pd.DataFrame = self.prepared_df[  # type: ignore[assignment]
            (account_col.isin(top_account_list)) & (service_col.isin(top_service_list))
        ]

        # Aggregate
        result = filtered_df.groupby(["account_id", "service"]).agg({"cost": "sum"}).reset_index()
        result.columns = ["account_id", "service", "total_cost"]
        result = result.sort_values(["account_id", "total_cost"], ascending=[True, False])

        return result

    def get_cost_trend_by_service(self, top_services: int = 5) -> pd.DataFrame:
        """
        Get cost trends over time for top services.

        Args:
            top_services: Number of top services to include

        Returns:
            DataFrame with service cost trends
        """
        if not hasattr(self, "prepared_df"):
            self.prepare_data()

        logger.info(f"Calculating cost trends for top {top_services} services...")

        # Get top services
        top_service_list = self.get_cost_by_service(top_services)["service"].tolist()

        # Filter and aggregate
        service_col: pd.Series = self.prepared_df["service"]  # type: ignore[assignment]
        filtered_df: pd.DataFrame = self.prepared_df[service_col.isin(top_service_list)]  # type: ignore[assignment]
        result = filtered_df.groupby(["date", "service"]).agg({"cost": "sum"}).reset_index()
        result.columns = ["date", "service", "total_cost"]
        result = result.sort_values(["service", "date"])

        return result

    def get_cost_trend_by_account(self, top_accounts: int = 5) -> pd.DataFrame:
        """
        Get cost trends over time for top accounts.

        Args:
            top_accounts: Number of top accounts to include

        Returns:
            DataFrame with account cost trends
        """
        if not hasattr(self, "prepared_df"):
            self.prepare_data()

        logger.info(f"Calculating cost trends for top {top_accounts} accounts...")

        # Get top accounts
        top_account_list = self.get_cost_by_account(top_accounts)["account_id"].tolist()

        # Filter and aggregate
        account_col: pd.Series = self.prepared_df["account_id"]  # type: ignore[assignment]
        filtered_df: pd.DataFrame = self.prepared_df[account_col.isin(top_account_list)]  # type: ignore[assignment]
        result = filtered_df.groupby(["date", "account_id"]).agg({"cost": "sum"}).reset_index()
        result.columns = ["date", "account_id", "total_cost"]
        result = result.sort_values(["account_id", "date"])

        return result

    def get_monthly_summary(self) -> pd.DataFrame:
        """
        Get monthly cost summary.

        Returns:
            DataFrame with monthly aggregated costs
        """
        if not hasattr(self, "prepared_df"):
            self.prepare_data()

        logger.info("Calculating monthly summary...")
        result = (
            self.prepared_df.groupby("year_month")
            .agg({"cost": ["sum", "mean", "count"]})
            .reset_index()
        )
        result.columns = ["month", "total_cost", "avg_daily_cost", "num_records"]
        result["month"] = result["month"].astype(str)

        return result

    def detect_cost_anomalies(self, threshold_std: float = 2.0) -> pd.DataFrame:
        """
        Detect days with anomalous costs (statistical outliers).

        Args:
            threshold_std: Number of standard deviations for anomaly detection

        Returns:
            DataFrame with anomalous cost days
        """
        if not hasattr(self, "prepared_df"):
            self.prepare_data()

        logger.info("Detecting cost anomalies...")

        # Calculate daily costs
        daily_costs = self.prepared_df.groupby("date").agg({"cost": "sum"}).reset_index()
        daily_costs.columns = ["date", "total_cost"]
        daily_costs = daily_costs.sort_values(by="date")

        # Calculate statistics
        mean_cost = daily_costs["total_cost"].mean()
        std_cost = daily_costs["total_cost"].std()

        # Identify anomalies
        daily_costs["z_score"] = (daily_costs["total_cost"] - mean_cost) / std_cost
        z_score_col: pd.Series = daily_costs["z_score"]  # type: ignore[assignment]
        anomalies: pd.DataFrame = daily_costs[abs(z_score_col) > threshold_std].copy()  # type: ignore[assignment]
        anomalies = anomalies.sort_values(by="date")

        logger.info(f"Found {len(anomalies)} anomalous days")
        return anomalies

    def get_cost_by_region(self, top_n: Optional[int] = None) -> pd.DataFrame:
        """
        Aggregate costs by region.

        Args:
            top_n: Return only top N regions by cost

        Returns:
            DataFrame with region costs
        """
        if not hasattr(self, "prepared_df"):
            self.prepare_data()

        if "region" not in self.prepared_df.columns:
            logger.warning("Region data not available")
            return pd.DataFrame()

        logger.info("Calculating cost by region...")
        result = self.prepared_df.groupby("region").agg({"cost": "sum"}).reset_index()
        result.columns = ["region", "total_cost"]
        result = result.sort_values("total_cost", ascending=False)

        if top_n:
            result = result.head(top_n)

        return result

    def get_summary_statistics(self) -> Dict:
        """
        Get overall summary statistics.

        Returns:
            Dictionary with summary statistics
        """
        if not hasattr(self, "prepared_df"):
            self.prepare_data()

        df = self.prepared_df

        # Type hints for Series to avoid pyright inference issues
        account_col: pd.Series = df["account_id"]  # type: ignore[assignment]
        service_col: pd.Series = df["service"]  # type: ignore[assignment]

        summary = {
            "total_cost": float(df["cost"].sum()),
            "num_accounts": int(account_col.nunique()),
            "num_services": int(service_col.nunique()),
            "date_range_start": str(df["usage_date"].min().date()),
            "date_range_end": str(df["usage_date"].max().date()),
            "total_records": len(df),
        }

        return summary
