"""Visualizer - Creates interactive visualizations and reports from CUR data using Apache ECharts."""

import html
import logging
import math
import os
import re
from datetime import datetime
from typing import Any, List

import pandas as pd
from pyecharts import options as opts
from pyecharts.charts import Bar, HeatMap, Line, Page, Scatter
from pyecharts.commons.utils import JsCode
from pyecharts.globals import ThemeType

logger = logging.getLogger(__name__)


def _safe_round(value: Any, decimals: int = 2) -> float:
    """Safely round a value, handling NaN/None/Inf."""
    if value is None:
        return 0.0
    try:
        float_val = float(value)
        if math.isnan(float_val) or math.isinf(float_val):
            return 0.0
        return round(float_val, decimals)
    except (TypeError, ValueError):
        return 0.0


def _safe_round_list(values: List[Any], decimals: int = 2) -> List[float]:
    """Safely round a list of values."""
    return [_safe_round(v, decimals) for v in values]


def _validate_dataframe(df: pd.DataFrame, required_columns: List[str], context: str) -> bool:
    """
    Validate that DataFrame is not empty and has required columns.

    Args:
        df: DataFrame to validate
        required_columns: List of column names that must be present
        context: Description of the calling context for logging

    Returns:
        True if valid, False otherwise
    """
    if df is None or df.empty:
        logger.warning(f"{context}: DataFrame is empty")
        return False

    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        logger.warning(f"{context}: Missing required columns: {missing_cols}")
        return False

    return True


class CURVisualizer:
    """Generate interactive visualizations for AWS Cost and Usage data using Apache ECharts."""

    def __init__(self, theme: str = "macarons") -> None:
        """
        Initialize the visualizer.

        Args:
            theme: pyecharts theme to use (macarons, shine, roma, vintage, etc.)
        """
        # Map theme names to ThemeType
        theme_map = {
            "macarons": ThemeType.MACARONS,
            "shine": ThemeType.SHINE,
            "roma": ThemeType.ROMA,
            "vintage": ThemeType.VINTAGE,
            "dark": ThemeType.DARK,
            "light": ThemeType.LIGHT,
        }
        self.theme = theme_map.get(theme.lower(), ThemeType.MACARONS)
        self.charts = []

    @staticmethod
    def _get_tooltip_style() -> str:
        """
        Get consistent tooltip styling for all charts.

        Returns:
            CSS style string for tooltips
        """
        return """
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            border: none;
            border-radius: 4px;
            padding: 6px 10px;
            box-shadow: none;
            color: #ffffff;
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            font-size: 12px;
            line-height: 1.4;
            max-width: 200px;
            white-space: normal;
        """

    def create_service_trend_chart(
        self, df: pd.DataFrame, title: str = "Monthly Cost by Service"
    ) -> Bar:
        """
        Create a grouped bar chart showing monthly cost trends for top services.

        Args:
            df: DataFrame with month, service, and total_cost columns
            title: Chart title

        Returns:
            pyecharts Bar chart
        """
        logger.info("Creating service trend chart...")

        # Get unique months and services
        months = sorted(df["month"].unique())
        services = df["service"].unique()

        # Month strings for x-axis
        month_strs = [str(m) for m in months]

        bar = Bar(init_opts=opts.InitOpts(theme=self.theme, height="650px", width="100%"))
        bar.add_xaxis(month_strs)

        # Color palette for better distinction
        colors = [
            "#5470c6",
            "#91cc75",
            "#fac858",
            "#ee6666",
            "#73c0de",
            "#3ba272",
            "#fc8452",
            "#9a60b4",
        ]

        for i, service in enumerate(services):
            service_data = df[df["service"] == service]
            # Create a dict for quick lookup
            cost_by_month = dict(zip(service_data["month"].astype(str), service_data["total_cost"]))
            # Ensure we have values for all months
            values = [round(cost_by_month.get(str(m), 0), 2) for m in months]

            bar.add_yaxis(
                series_name=service,
                y_axis=values,
                label_opts=opts.LabelOpts(is_show=False),
                itemstyle_opts=opts.ItemStyleOpts(color=colors[i % len(colors)]),
            )

        bar.set_global_opts(
            title_opts=opts.TitleOpts(
                title=title,
                title_textstyle_opts=opts.TextStyleOpts(font_size=18, font_weight="bold"),
                pos_top="1%",
            ),
            xaxis_opts=opts.AxisOpts(
                name="Month",
                type_="category",
                axislabel_opts=opts.LabelOpts(rotate=45, interval="auto"),
            ),
            yaxis_opts=opts.AxisOpts(
                name="Total Cost (USD)",
                axislabel_opts=opts.LabelOpts(
                    formatter=JsCode("function(value) { return '$' + value.toLocaleString(); }")
                ),
            ),
            tooltip_opts=opts.TooltipOpts(
                trigger="axis",
                axis_pointer_type="shadow",
                is_confine=True,
                background_color="transparent",
                border_color="transparent",
                border_width=0,
                extra_css_text="box-shadow: none;",
                formatter=JsCode(
                    """function(params) {
                        var style = `"""
                    + self._get_tooltip_style()
                    + """`;
                        var result = '<div style="' + style + '">';
                        result += '<strong style="font-size: 12px;">' + params[0].name + '</strong><br/><br/>';
                        params.forEach(function(item) {
                            result += '<div style="margin: 2px 0;">';
                            result += item.marker + ' ';
                            result += '<span style="opacity: 0.9;">' + item.seriesName + ':</span> ';
                            result += '<strong>$' + item.value.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2}) + '</strong>';
                            result += '</div>';
                        });
                        result += '</div>';
                        return result;
                    }"""
                ),
                textstyle_opts=opts.TextStyleOpts(color="#ffffff"),
            ),
            legend_opts=opts.LegendOpts(
                type_="scroll",
                orient="horizontal",
                pos_top="10%",
                selected_mode="multiple",
            ),
            datazoom_opts=[
                opts.DataZoomOpts(type_="slider", range_start=0, range_end=100),
                opts.DataZoomOpts(type_="inside"),
            ],
            toolbox_opts=opts.ToolboxOpts(
                is_show=True,
                feature=opts.ToolBoxFeatureOpts(
                    save_as_image=opts.ToolBoxFeatureSaveAsImageOpts(title="Save as Image"),
                    restore=opts.ToolBoxFeatureRestoreOpts(title="Restore"),
                    data_zoom=opts.ToolBoxFeatureDataZoomOpts(
                        zoom_title="Zoom", back_title="Reset Zoom"
                    ),
                    data_view=opts.ToolBoxFeatureDataViewOpts(title="Data View"),
                    magic_type=opts.ToolBoxFeatureMagicTypeOpts(
                        line_title="Line", bar_title="Bar", stack_title="Stack"
                    ),
                ),
            ),
        )

        self.charts.append(("service_trend", bar))
        return bar

    def create_account_trend_chart(
        self, df: pd.DataFrame, title: str = "Monthly Cost by Account"
    ) -> Bar:
        """
        Create a grouped bar chart showing monthly cost trends for top accounts.

        Args:
            df: DataFrame with month, account_id, and total_cost columns
            title: Chart title

        Returns:
            pyecharts Bar chart
        """
        logger.info("Creating account trend chart...")

        # Get unique months and accounts
        months = sorted(df["month"].unique())
        accounts = df["account_id"].unique()

        # Month strings for x-axis
        month_strs = [str(m) for m in months]

        bar = Bar(init_opts=opts.InitOpts(theme=self.theme, height="650px", width="100%"))
        bar.add_xaxis(month_strs)

        # Color palette
        colors = [
            "#5470c6",
            "#91cc75",
            "#fac858",
            "#ee6666",
            "#73c0de",
            "#3ba272",
            "#fc8452",
            "#9a60b4",
        ]

        for i, account in enumerate(accounts):
            account_data = df[df["account_id"] == account]
            # Create a dict for quick lookup
            cost_by_month = dict(zip(account_data["month"].astype(str), account_data["total_cost"]))
            # Ensure we have values for all months
            values = [round(cost_by_month.get(str(m), 0), 2) for m in months]

            bar.add_yaxis(
                series_name=str(account),
                y_axis=values,
                label_opts=opts.LabelOpts(is_show=False),
                itemstyle_opts=opts.ItemStyleOpts(color=colors[i % len(colors)]),
            )

        bar.set_global_opts(
            title_opts=opts.TitleOpts(
                title=title,
                title_textstyle_opts=opts.TextStyleOpts(font_size=18, font_weight="bold"),
                pos_top="1%",
            ),
            xaxis_opts=opts.AxisOpts(
                name="Month",
                type_="category",
                axislabel_opts=opts.LabelOpts(rotate=45, interval="auto"),
            ),
            yaxis_opts=opts.AxisOpts(
                name="Total Cost (USD)",
                axislabel_opts=opts.LabelOpts(
                    formatter=JsCode("function(value) { return '$' + value.toLocaleString(); }")
                ),
            ),
            tooltip_opts=opts.TooltipOpts(
                trigger="axis",
                axis_pointer_type="shadow",
                is_confine=True,
                background_color="transparent",
                border_color="transparent",
                border_width=0,
                extra_css_text="box-shadow: none;",
                formatter=JsCode(
                    """function(params) {
                        var style = `"""
                    + self._get_tooltip_style()
                    + """`;
                        var result = '<div style="' + style + '">';
                        result += '<strong style="font-size: 12px;">' + params[0].name + '</strong><br/><br/>';
                        params.forEach(function(item) {
                            result += '<div style="margin: 2px 0;">';
                            result += item.marker + ' ';
                            result += '<span style="opacity: 0.9;">Account ' + item.seriesName + ':</span> ';
                            result += '<strong>$' + item.value.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2}) + '</strong>';
                            result += '</div>';
                        });
                        result += '</div>';
                        return result;
                    }"""
                ),
                textstyle_opts=opts.TextStyleOpts(color="#ffffff"),
            ),
            legend_opts=opts.LegendOpts(
                type_="scroll",
                orient="horizontal",
                pos_top="10%",
                selected_mode="multiple",
            ),
            datazoom_opts=[
                opts.DataZoomOpts(type_="slider", range_start=0, range_end=100),
                opts.DataZoomOpts(type_="inside"),
            ],
            toolbox_opts=opts.ToolboxOpts(
                is_show=True,
                feature=opts.ToolBoxFeatureOpts(
                    save_as_image=opts.ToolBoxFeatureSaveAsImageOpts(title="Save as Image"),
                    restore=opts.ToolBoxFeatureRestoreOpts(title="Restore"),
                    data_zoom=opts.ToolBoxFeatureDataZoomOpts(
                        zoom_title="Zoom", back_title="Reset Zoom"
                    ),
                    data_view=opts.ToolBoxFeatureDataViewOpts(title="Data View"),
                    magic_type=opts.ToolBoxFeatureMagicTypeOpts(
                        line_title="Line", bar_title="Bar", stack_title="Stack"
                    ),
                ),
            ),
        )

        self.charts.append(("account_trend", bar))
        return bar

    def create_account_service_heatmap(
        self, df: pd.DataFrame, title: str = "Cost Heatmap: Account vs Service"
    ) -> HeatMap:
        """
        Create a heatmap showing costs by account and service.

        Args:
            df: DataFrame with account_id, service, and total_cost columns
            title: Chart title

        Returns:
            pyecharts HeatMap
        """
        logger.info("Creating account-service heatmap...")

        # Pivot the data
        pivot_df = df.pivot(index="account_id", columns="service", values="total_cost")
        pivot_df = pivot_df.fillna(0)

        # Prepare data for heatmap
        accounts = pivot_df.index.astype(str).tolist()
        services = pivot_df.columns.tolist()
        data = []
        max_value = 0
        for i, account in enumerate(accounts):
            for j, service in enumerate(services):
                # Use values array for cleaner type handling
                value = round(float(pivot_df.values[i, j]), 2)
                data.append([j, i, value])
                max_value = max(max_value, value)

        heatmap = (
            HeatMap(init_opts=opts.InitOpts(theme=self.theme, height="600px", width="100%"))
            .add_xaxis(services)
            .add_yaxis(
                "Account",
                accounts,
                data,
                label_opts=opts.LabelOpts(
                    is_show=True,
                    position="inside",
                    formatter=JsCode(
                        "function(params) { return params.value[2] > 0 ? '$' + params.value[2].toLocaleString() : ''; }"
                    ),
                    font_size=10,
                ),
            )
            .set_global_opts(
                title_opts=opts.TitleOpts(
                    title=title,
                    title_textstyle_opts=opts.TextStyleOpts(font_size=18, font_weight="bold"),
                    pos_top="1%",
                ),
                xaxis_opts=opts.AxisOpts(
                    name="Service",
                    type_="category",
                    axislabel_opts=opts.LabelOpts(rotate=45, interval=0),
                ),
                yaxis_opts=opts.AxisOpts(
                    name="Account ID",
                    type_="category",
                ),
                visualmap_opts=opts.VisualMapOpts(
                    min_=0,
                    max_=max_value,
                    is_calculable=True,
                    orient="horizontal",
                    pos_left="center",
                    pos_bottom="0%",
                    range_color=["#eef5ff", "#5470c6"],
                ),
                tooltip_opts=opts.TooltipOpts(
                    trigger="item",
                    is_confine=True,
                    background_color="transparent",
                    border_color="transparent",
                    border_width=0,
                    extra_css_text="box-shadow: none;",
                    formatter=JsCode(
                        """function(params) {
                            var style = `"""
                        + self._get_tooltip_style()
                        + """`;
                            return '<div style="' + style + '">' +
                                '<strong style="font-size: 12px;">Cost Breakdown</strong><br/><br/>' +
                                '<div style="margin: 2px 0;"><span style="opacity: 0.9;">Account: </span><strong>' + params.name + '</strong></div>' +
                                '<div style="margin: 2px 0;"><span style="opacity: 0.9;">Service: </span><strong>' + params.value[0] + '</strong></div>' +
                                '<div style="margin: 2px 0;"><span style="opacity: 0.9;">Total Cost: </span><strong>$' + params.value[2].toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2}) + '</strong></div>' +
                                '</div>';
                        }"""
                    ),
                    textstyle_opts=opts.TextStyleOpts(color="#ffffff"),
                ),
                toolbox_opts=opts.ToolboxOpts(
                    is_show=True,
                    feature=opts.ToolBoxFeatureOpts(
                        save_as_image=opts.ToolBoxFeatureSaveAsImageOpts(title="Save as Image"),
                        restore=opts.ToolBoxFeatureRestoreOpts(title="Restore"),
                        data_view=opts.ToolBoxFeatureDataViewOpts(title="Data View"),
                    ),
                ),
            )
        )

        self.charts.append(("account_service_heatmap", heatmap))
        return heatmap

    def create_monthly_summary_chart(
        self, df: pd.DataFrame, title: str = "Monthly Cost Summary"
    ) -> Bar:
        """
        Create an enhanced bar chart showing monthly cost summary.

        Args:
            df: DataFrame with month and total_cost columns
            title: Chart title

        Returns:
            pyecharts Bar chart with line overlay
        """
        logger.info("Creating monthly summary chart...")

        months = df["month"].tolist()
        costs = [round(x, 2) for x in df["total_cost"].tolist()]

        bar = (
            Bar(init_opts=opts.InitOpts(theme=self.theme, height="600px", width="100%"))
            .add_xaxis(months)
            .add_yaxis(
                "Monthly Cost",
                costs,
                label_opts=opts.LabelOpts(
                    is_show=True,
                    position="top",
                    formatter=JsCode(
                        "function(params) { return '$' + params.value.toLocaleString(); }"
                    ),
                ),
                itemstyle_opts=opts.ItemStyleOpts(
                    color="#5470c6",
                    border_radius=8,
                ),
            )
        )

        # Add trend line if we have enough data points
        if len(df) >= 2:
            line = (
                Line()
                .add_xaxis(months)
                .add_yaxis(
                    "Trend",
                    costs,
                    is_smooth=True,
                    symbol_size=10,
                    linestyle_opts=opts.LineStyleOpts(width=3, type_="dashed"),
                    itemstyle_opts=opts.ItemStyleOpts(color="#ee6666"),
                    label_opts=opts.LabelOpts(is_show=False),
                )
            )
            bar.overlap(line)

        bar.set_global_opts(
            title_opts=opts.TitleOpts(
                title=title,
                title_textstyle_opts=opts.TextStyleOpts(font_size=18, font_weight="bold"),
                pos_top="1%",
                item_gap=15,
            ),
            xaxis_opts=opts.AxisOpts(
                name="Month",
                axislabel_opts=opts.LabelOpts(rotate=45),
            ),
            yaxis_opts=opts.AxisOpts(
                name="Total Cost (USD)",
                axislabel_opts=opts.LabelOpts(
                    formatter=JsCode("function(value) { return '$' + value.toLocaleString(); }")
                ),
            ),
            tooltip_opts=opts.TooltipOpts(
                trigger="axis",
                is_confine=True,
                background_color="transparent",
                border_color="transparent",
                border_width=0,
                extra_css_text="box-shadow: none;",
                formatter=JsCode(
                    """function(params) {
                        var style = `"""
                    + self._get_tooltip_style()
                    + """`;
                        var result = '<div style="' + style + '">';
                        result += '<strong style="font-size: 12px;">' + params[0].name + '</strong><br/><br/>';
                        params.forEach(function(item) {
                            result += '<div style="margin: 2px 0;">';
                            result += item.marker + ' ';
                            result += '<span style="opacity: 0.9;">' + item.seriesName + ':</span> ';
                            result += '<strong>$' + item.value.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2}) + '</strong>';
                            result += '</div>';
                        });
                        result += '</div>';
                        return result;
                    }"""
                ),
                textstyle_opts=opts.TextStyleOpts(color="#ffffff"),
            ),
            legend_opts=opts.LegendOpts(pos_top="8%"),
            toolbox_opts=opts.ToolboxOpts(
                is_show=True,
                feature=opts.ToolBoxFeatureOpts(
                    save_as_image=opts.ToolBoxFeatureSaveAsImageOpts(title="Save as Image"),
                    restore=opts.ToolBoxFeatureRestoreOpts(title="Restore"),
                    data_view=opts.ToolBoxFeatureDataViewOpts(title="Data View"),
                ),
            ),
        )

        self.charts.append(("monthly_summary", bar))
        return bar

    def create_anomaly_chart(
        self, df: pd.DataFrame, title: str = "Cost Anomalies by Service"
    ) -> Scatter:
        """
        Create a chart highlighting cost anomalies by service and month.

        Args:
            df: DataFrame with month, service, total_cost, mean_cost, z_score, pct_change columns
            title: Chart title

        Returns:
            pyecharts Scatter chart
        """
        logger.info("Creating cost anomaly chart...")

        if df.empty:
            logger.warning("No anomalies to visualize")
            # Return empty chart
            scatter = Scatter(
                init_opts=opts.InitOpts(theme=self.theme, height="600px", width="100%")
            )
            return scatter

        # Get unique months and services
        months = sorted(df["month"].unique())
        services = df["service"].unique()

        # Color palette for services
        colors = [
            "#ee6666",  # Red for anomalies
            "#fac858",  # Orange
            "#91cc75",  # Green
            "#73c0de",  # Blue
            "#5470c6",  # Dark blue
            "#9a60b4",  # Purple
            "#fc8452",  # Orange-red
            "#3ba272",  # Teal
        ]

        scatter = Scatter(init_opts=opts.InitOpts(theme=self.theme, height="650px", width="100%"))
        scatter.add_xaxis(months)

        # Add series for each service
        for i, service in enumerate(services):
            service_data = df[df["service"] == service]

            # Prepare data points with all information
            data = []
            for _, row in service_data.iterrows():
                month_str = str(row["month"])
                z_score = row["z_score"]

                # Size based on severity
                if abs(z_score) < 2.5:
                    symbol_size = 12
                elif abs(z_score) < 3:
                    symbol_size = 16
                else:
                    symbol_size = 20

                data.append(
                    {
                        "value": [
                            month_str,
                            round(row["total_cost"], 2),
                            round(z_score, 2),
                            round(row["mean_cost"], 2),
                            round(row["pct_change"], 1),
                        ],
                        "symbolSize": symbol_size,
                    }
                )

            scatter.add_yaxis(
                series_name=service,
                y_axis=data,
                symbol="circle",
                label_opts=opts.LabelOpts(is_show=False),
                itemstyle_opts=opts.ItemStyleOpts(color=colors[i % len(colors)]),
            )

        scatter.set_global_opts(
            title_opts=opts.TitleOpts(
                title=title,
                subtitle="Months where service costs deviate significantly from average",
                title_textstyle_opts=opts.TextStyleOpts(font_size=18, font_weight="bold"),
                pos_top="1%",
                item_gap=15,
            ),
            xaxis_opts=opts.AxisOpts(
                name="Month",
                type_="category",
                axislabel_opts=opts.LabelOpts(rotate=45, interval="auto"),
            ),
            yaxis_opts=opts.AxisOpts(
                name="Cost (USD)",
                axislabel_opts=opts.LabelOpts(
                    formatter=JsCode("function(value) { return '$' + value.toLocaleString(); }")
                ),
            ),
            tooltip_opts=opts.TooltipOpts(
                trigger="axis",
                is_confine=True,
                background_color="transparent",
                border_color="transparent",
                border_width=0,
                extra_css_text="box-shadow: none;",
                formatter=JsCode(
                    """function(params) {
                        var value = params.value;
                        var style = `"""
                    + self._get_tooltip_style()
                    + """`;
                        var result = '<div style="' + style + '">';
                        result += '<strong style="font-size: 12px;">' + params.seriesName + '</strong><br/><br/>';
                        result += '<div style="margin: 2px 0;"><span style="opacity: 0.9;">Month: </span><strong>' + value[0] + '</strong></div>';
                        result += '<div style="margin: 2px 0;"><span style="opacity: 0.9;">Cost: </span><strong>$' + value[1].toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2}) + '</strong></div>';
                        result += '<div style="margin: 2px 0;"><span style="opacity: 0.9;">Average: </span><strong>$' + value[3].toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2}) + '</strong></div>';
                        result += '<div style="margin: 2px 0;"><span style="opacity: 0.9;">Change: </span><strong style="color: ' + (value[4] > 0 ? '#ff6b6b' : '#51cf66') + ';">' + (value[4] > 0 ? '+' : '') + value[4].toFixed(1) + '%</strong></div>';
                        result += '<div style="margin: 2px 0;"><span style="opacity: 0.9;">Z-Score: </span><strong>' + value[2].toFixed(2) + '</strong></div>';
                        result += '</div>';
                        return result;
                    }"""
                ),
                textstyle_opts=opts.TextStyleOpts(color="#ffffff"),
            ),
            legend_opts=opts.LegendOpts(
                type_="scroll",
                orient="horizontal",
                pos_top="12%",
                selected_mode="multiple",
            ),
            datazoom_opts=[
                opts.DataZoomOpts(type_="slider", range_start=0, range_end=100),
                opts.DataZoomOpts(type_="inside"),
            ],
            toolbox_opts=opts.ToolboxOpts(
                is_show=True,
                feature=opts.ToolBoxFeatureOpts(
                    save_as_image=opts.ToolBoxFeatureSaveAsImageOpts(title="Save as Image"),
                    restore=opts.ToolBoxFeatureRestoreOpts(title="Restore"),
                    data_zoom=opts.ToolBoxFeatureDataZoomOpts(
                        zoom_title="Zoom", back_title="Reset Zoom"
                    ),
                    data_view=opts.ToolBoxFeatureDataViewOpts(title="Data View"),
                ),
            ),
        )

        self.charts.append(("anomalies", scatter))
        return scatter

    def create_region_trend_chart(
        self, df: pd.DataFrame, title: str = "Monthly Cost by Region"
    ) -> Bar:
        """
        Create a grouped bar chart showing monthly cost trends for top regions.

        Args:
            df: DataFrame with month, region, and total_cost columns
            title: Chart title

        Returns:
            pyecharts Bar chart
        """
        logger.info("Creating region trend chart...")

        if not _validate_dataframe(df, ["month", "region", "total_cost"], "Region trend chart"):
            bar = Bar(init_opts=opts.InitOpts(theme=self.theme, height="650px", width="100%"))
            bar.set_global_opts(
                title_opts=opts.TitleOpts(title=title, subtitle="No data available")
            )
            self.charts.append(("region_trend", bar))
            return bar

        # Get unique months and regions
        months = sorted(df["month"].unique())
        regions = df["region"].unique()

        # Month strings for x-axis
        month_strs = [str(m) for m in months]

        bar = Bar(init_opts=opts.InitOpts(theme=self.theme, height="650px", width="100%"))
        bar.add_xaxis(month_strs)

        # Color palette
        colors = [
            "#5470c6",
            "#91cc75",
            "#fac858",
            "#ee6666",
            "#73c0de",
            "#3ba272",
            "#fc8452",
            "#9a60b4",
        ]

        for i, region in enumerate(regions):
            region_data = df[df["region"] == region]
            cost_by_month = dict(zip(region_data["month"].astype(str), region_data["total_cost"]))
            values = [round(cost_by_month.get(str(m), 0), 2) for m in months]

            bar.add_yaxis(
                series_name=str(region),
                y_axis=values,
                label_opts=opts.LabelOpts(is_show=False),
                itemstyle_opts=opts.ItemStyleOpts(color=colors[i % len(colors)]),
            )

        bar.set_global_opts(
            title_opts=opts.TitleOpts(
                title=title,
                title_textstyle_opts=opts.TextStyleOpts(font_size=18, font_weight="bold"),
                pos_top="1%",
            ),
            xaxis_opts=opts.AxisOpts(
                name="Month",
                type_="category",
                axislabel_opts=opts.LabelOpts(rotate=45, interval="auto"),
            ),
            yaxis_opts=opts.AxisOpts(
                name="Total Cost (USD)",
                axislabel_opts=opts.LabelOpts(
                    formatter=JsCode("function(value) { return '$' + value.toLocaleString(); }")
                ),
            ),
            tooltip_opts=opts.TooltipOpts(
                trigger="axis",
                axis_pointer_type="shadow",
                is_confine=True,
                background_color="transparent",
                border_color="transparent",
                border_width=0,
                extra_css_text="box-shadow: none;",
                formatter=JsCode(
                    """function(params) {
                        var style = `"""
                    + self._get_tooltip_style()
                    + """`;
                        var result = '<div style="' + style + '">';
                        result += '<strong style="font-size: 12px;">' + params[0].name + '</strong><br/><br/>';
                        params.forEach(function(item) {
                            result += '<div style="margin: 2px 0;">';
                            result += item.marker + ' ';
                            result += '<span style="opacity: 0.9;">' + item.seriesName + ':</span> ';
                            result += '<strong>$' + item.value.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2}) + '</strong>';
                            result += '</div>';
                        });
                        result += '</div>';
                        return result;
                    }"""
                ),
                textstyle_opts=opts.TextStyleOpts(color="#ffffff"),
            ),
            legend_opts=opts.LegendOpts(
                type_="scroll",
                orient="horizontal",
                pos_top="10%",
                selected_mode="multiple",
            ),
            datazoom_opts=[
                opts.DataZoomOpts(type_="slider", range_start=0, range_end=100),
                opts.DataZoomOpts(type_="inside"),
            ],
            toolbox_opts=opts.ToolboxOpts(
                is_show=True,
                feature=opts.ToolBoxFeatureOpts(
                    save_as_image=opts.ToolBoxFeatureSaveAsImageOpts(title="Save as Image"),
                    restore=opts.ToolBoxFeatureRestoreOpts(title="Restore"),
                    data_zoom=opts.ToolBoxFeatureDataZoomOpts(
                        zoom_title="Zoom", back_title="Reset Zoom"
                    ),
                    data_view=opts.ToolBoxFeatureDataViewOpts(title="Data View"),
                    magic_type=opts.ToolBoxFeatureMagicTypeOpts(
                        line_title="Line", bar_title="Bar", stack_title="Stack"
                    ),
                ),
            ),
        )

        self.charts.append(("region_trend", bar))
        return bar

    def create_discounts_trend_chart(
        self, df: pd.DataFrame, title: str = "Monthly Discounts by Type"
    ) -> Bar:
        """
        Create a stacked bar chart showing monthly discount trends by type.

        Args:
            df: DataFrame with month, discount_type, and total_discount columns
            title: Chart title

        Returns:
            pyecharts Bar chart
        """
        logger.info("Creating discounts trend chart...")

        if not _validate_dataframe(
            df, ["month", "discount_type", "total_discount"], "Discounts trend chart"
        ):
            bar = Bar(init_opts=opts.InitOpts(theme=self.theme, height="650px", width="100%"))
            bar.set_global_opts(
                title_opts=opts.TitleOpts(title=title, subtitle="No discount data available")
            )
            self.charts.append(("discounts_trend", bar))
            return bar

        # Map to friendly names
        friendly_names = {
            "SavingsPlanNegation": "Savings Plans",
            "EdpDiscount": "Enterprise Discount (EDP)",
            "PrivateRateDiscount": "Private Rate Discount",
            "BundledDiscount": "Bundled Discount",
            "Credit": "Credits",
        }
        df = df.copy()
        df["display_name"] = df["discount_type"].map(
            lambda x: friendly_names.get(x, x) if pd.notna(x) else "Unknown"
        )

        months = sorted(df["month"].unique())
        discount_types = df["display_name"].unique()
        month_strs = [str(m) for m in months]

        bar = Bar(init_opts=opts.InitOpts(theme=self.theme, height="650px", width="100%"))
        bar.add_xaxis(month_strs)

        colors = ["#91cc75", "#5470c6", "#ee6666", "#fac858", "#73c0de"]

        for i, dtype in enumerate(discount_types):
            type_data = df[df["display_name"] == dtype]
            discount_by_month = dict(
                zip(type_data["month"].astype(str), type_data["total_discount"])
            )
            values = [round(discount_by_month.get(str(m), 0), 2) for m in months]

            bar.add_yaxis(
                series_name=str(dtype),
                y_axis=values,
                stack="discounts",
                label_opts=opts.LabelOpts(is_show=False),
                itemstyle_opts=opts.ItemStyleOpts(color=colors[i % len(colors)]),
            )

        bar.set_global_opts(
            title_opts=opts.TitleOpts(
                title=title,
                subtitle="How your discounts and credits trend over time",
                title_textstyle_opts=opts.TextStyleOpts(font_size=18, font_weight="bold"),
                pos_top="1%",
            ),
            xaxis_opts=opts.AxisOpts(
                name="Month",
                type_="category",
                axislabel_opts=opts.LabelOpts(rotate=45, interval="auto"),
            ),
            yaxis_opts=opts.AxisOpts(
                name="Discount Amount (USD)",
                axislabel_opts=opts.LabelOpts(
                    formatter=JsCode("function(value) { return '$' + value.toLocaleString(); }")
                ),
            ),
            tooltip_opts=opts.TooltipOpts(
                trigger="axis",
                axis_pointer_type="shadow",
                is_confine=True,
                background_color="transparent",
                border_color="transparent",
                border_width=0,
                extra_css_text="box-shadow: none;",
                formatter=JsCode(
                    """function(params) {
                        var style = `"""
                    + self._get_tooltip_style()
                    + """`;
                        var result = '<div style="' + style + '">';
                        result += '<strong style="font-size: 12px;">' + params[0].name + '</strong><br/><br/>';
                        var total = 0;
                        params.forEach(function(item) {
                            total += item.value;
                            result += '<div style="margin: 2px 0;">';
                            result += item.marker + ' ';
                            result += '<span style="opacity: 0.9;">' + item.seriesName + ':</span> ';
                            result += '<strong style="color: #91cc75;">$' + item.value.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2}) + '</strong>';
                            result += '</div>';
                        });
                        result += '<hr style="margin: 4px 0; border-color: rgba(255,255,255,0.2);"/>';
                        result += '<div><strong>Total: $' + total.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2}) + '</strong></div>';
                        result += '</div>';
                        return result;
                    }"""
                ),
                textstyle_opts=opts.TextStyleOpts(color="#ffffff"),
            ),
            legend_opts=opts.LegendOpts(
                type_="scroll",
                orient="horizontal",
                pos_top="12%",
                selected_mode="multiple",
            ),
            datazoom_opts=[
                opts.DataZoomOpts(type_="slider", range_start=0, range_end=100),
                opts.DataZoomOpts(type_="inside"),
            ],
            toolbox_opts=opts.ToolboxOpts(
                is_show=True,
                feature=opts.ToolBoxFeatureOpts(
                    save_as_image=opts.ToolBoxFeatureSaveAsImageOpts(title="Save as Image"),
                    restore=opts.ToolBoxFeatureRestoreOpts(title="Restore"),
                    data_view=opts.ToolBoxFeatureDataViewOpts(title="Data View"),
                ),
            ),
        )

        self.charts.append(("discounts_trend", bar))
        return bar

    def create_discounts_by_service_trend_chart(
        self, df: pd.DataFrame, title: str = "Monthly Discounts by Service"
    ) -> Bar:
        """
        Create a grouped bar chart showing monthly discount trends by service.

        Args:
            df: DataFrame with month, service, and total_discount columns
            title: Chart title

        Returns:
            pyecharts Bar chart
        """
        logger.info("Creating discounts by service trend chart...")

        if not _validate_dataframe(
            df, ["month", "service", "total_discount"], "Discounts by service trend chart"
        ):
            bar = Bar(init_opts=opts.InitOpts(theme=self.theme, height="650px", width="100%"))
            bar.set_global_opts(
                title_opts=opts.TitleOpts(title=title, subtitle="No discount data available")
            )
            self.charts.append(("discounts_by_service_trend", bar))
            return bar

        months = sorted(df["month"].unique())
        services = df["service"].unique()
        month_strs = [str(m) for m in months]

        bar = Bar(init_opts=opts.InitOpts(theme=self.theme, height="650px", width="100%"))
        bar.add_xaxis(month_strs)

        colors = [
            "#91cc75",
            "#5470c6",
            "#fac858",
            "#ee6666",
            "#73c0de",
            "#3ba272",
            "#fc8452",
            "#9a60b4",
        ]

        for i, service in enumerate(services):
            service_data = df[df["service"] == service]
            discount_by_month = dict(
                zip(service_data["month"].astype(str), service_data["total_discount"])
            )
            values = [round(discount_by_month.get(str(m), 0), 2) for m in months]

            bar.add_yaxis(
                series_name=str(service),
                y_axis=values,
                label_opts=opts.LabelOpts(is_show=False),
                itemstyle_opts=opts.ItemStyleOpts(color=colors[i % len(colors)]),
            )

        bar.set_global_opts(
            title_opts=opts.TitleOpts(
                title=title,
                title_textstyle_opts=opts.TextStyleOpts(font_size=18, font_weight="bold"),
                pos_top="1%",
            ),
            xaxis_opts=opts.AxisOpts(
                name="Month",
                type_="category",
                axislabel_opts=opts.LabelOpts(rotate=45, interval="auto"),
            ),
            yaxis_opts=opts.AxisOpts(
                name="Discount Amount (USD)",
                axislabel_opts=opts.LabelOpts(
                    formatter=JsCode("function(value) { return '$' + value.toLocaleString(); }")
                ),
            ),
            tooltip_opts=opts.TooltipOpts(
                trigger="axis",
                axis_pointer_type="shadow",
                is_confine=True,
                background_color="transparent",
                border_color="transparent",
                border_width=0,
                extra_css_text="box-shadow: none;",
                formatter=JsCode(
                    """function(params) {
                        var style = `"""
                    + self._get_tooltip_style()
                    + """`;
                        var result = '<div style="' + style + '">';
                        result += '<strong style="font-size: 12px;">' + params[0].name + '</strong><br/><br/>';
                        params.forEach(function(item) {
                            result += '<div style="margin: 2px 0;">';
                            result += item.marker + ' ';
                            result += '<span style="opacity: 0.9;">' + item.seriesName + ':</span> ';
                            result += '<strong style="color: #91cc75;">$' + item.value.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2}) + '</strong>';
                            result += '</div>';
                        });
                        result += '</div>';
                        return result;
                    }"""
                ),
                textstyle_opts=opts.TextStyleOpts(color="#ffffff"),
            ),
            legend_opts=opts.LegendOpts(
                type_="scroll",
                orient="horizontal",
                pos_top="10%",
                selected_mode="multiple",
            ),
            datazoom_opts=[
                opts.DataZoomOpts(type_="slider", range_start=0, range_end=100),
                opts.DataZoomOpts(type_="inside"),
            ],
            toolbox_opts=opts.ToolboxOpts(
                is_show=True,
                feature=opts.ToolBoxFeatureOpts(
                    save_as_image=opts.ToolBoxFeatureSaveAsImageOpts(title="Save as Image"),
                    restore=opts.ToolBoxFeatureRestoreOpts(title="Restore"),
                    data_view=opts.ToolBoxFeatureDataViewOpts(title="Data View"),
                ),
            ),
        )

        self.charts.append(("discounts_by_service_trend", bar))
        return bar

    def create_savings_plan_trend_chart(
        self, df: pd.DataFrame, title: str = "Savings Plan Effectiveness Over Time"
    ) -> Bar:
        """
        Create a bar chart showing monthly Savings Plan effectiveness.

        Args:
            df: DataFrame with month, on_demand_equivalent, savings, savings_percentage columns
            title: Chart title

        Returns:
            pyecharts Bar chart with line overlay for savings percentage
        """
        logger.info("Creating Savings Plan trend chart...")

        if not _validate_dataframe(
            df, ["month", "on_demand_equivalent", "savings"], "Savings Plan trend chart"
        ):
            bar = Bar(init_opts=opts.InitOpts(theme=self.theme, height="600px", width="100%"))
            bar.set_global_opts(
                title_opts=opts.TitleOpts(title=title, subtitle="No Savings Plan data available")
            )
            self.charts.append(("savings_plan_trend", bar))
            return bar

        months = df["month"].tolist()
        on_demand = _safe_round_list(df["on_demand_equivalent"].tolist())
        savings = _safe_round_list(df["savings"].tolist())
        savings_pct = _safe_round_list(df["savings_percentage"].tolist(), 1)

        bar = (
            Bar(init_opts=opts.InitOpts(theme=self.theme, height="600px", width="100%"))
            .add_xaxis(months)
            .add_yaxis(
                "On-Demand Equivalent",
                on_demand,
                label_opts=opts.LabelOpts(is_show=False),
                itemstyle_opts=opts.ItemStyleOpts(color="#ee6666", opacity=0.7),
            )
            .add_yaxis(
                "Savings",
                savings,
                label_opts=opts.LabelOpts(
                    is_show=True,
                    position="top",
                    formatter=JsCode(
                        "function(params) { return '$' + params.value.toLocaleString(); }"
                    ),
                ),
                itemstyle_opts=opts.ItemStyleOpts(color="#91cc75"),
            )
            .extend_axis(
                yaxis=opts.AxisOpts(
                    name="Savings %",
                    position="right",
                    axislabel_opts=opts.LabelOpts(formatter="{value}%"),
                    min_=0,
                    max_=100,
                )
            )
        )

        # Add savings percentage line
        line = (
            Line()
            .add_xaxis(months)
            .add_yaxis(
                "Savings %",
                savings_pct,
                yaxis_index=1,
                is_smooth=True,
                symbol_size=10,
                linestyle_opts=opts.LineStyleOpts(width=3, type_="dashed"),
                itemstyle_opts=opts.ItemStyleOpts(color="#fac858"),
                label_opts=opts.LabelOpts(
                    is_show=True,
                    formatter=JsCode("function(params) { return params.value + '%'; }"),
                ),
            )
        )
        bar.overlap(line)

        bar.set_global_opts(
            title_opts=opts.TitleOpts(
                title=title,
                subtitle="Monthly Savings Plan savings compared to on-demand pricing",
                title_textstyle_opts=opts.TextStyleOpts(font_size=18, font_weight="bold"),
                pos_top="1%",
            ),
            xaxis_opts=opts.AxisOpts(
                name="Month",
                axislabel_opts=opts.LabelOpts(rotate=45),
            ),
            yaxis_opts=opts.AxisOpts(
                name="Cost (USD)",
                axislabel_opts=opts.LabelOpts(
                    formatter=JsCode("function(value) { return '$' + value.toLocaleString(); }")
                ),
            ),
            tooltip_opts=opts.TooltipOpts(
                trigger="axis",
                is_confine=True,
                background_color="transparent",
                border_color="transparent",
                border_width=0,
                extra_css_text="box-shadow: none;",
                formatter=JsCode(
                    """function(params) {
                        var style = `"""
                    + self._get_tooltip_style()
                    + """`;
                        var result = '<div style="' + style + '">';
                        result += '<strong style="font-size: 12px;">' + params[0].name + '</strong><br/><br/>';
                        params.forEach(function(item) {
                            result += '<div style="margin: 2px 0;">';
                            result += item.marker + ' ';
                            result += '<span style="opacity: 0.9;">' + item.seriesName + ':</span> ';
                            if (item.seriesName === 'Savings %') {
                                result += '<strong style="color: #fac858;">' + item.value + '%</strong>';
                            } else {
                                result += '<strong>$' + item.value.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2}) + '</strong>';
                            }
                            result += '</div>';
                        });
                        result += '</div>';
                        return result;
                    }"""
                ),
                textstyle_opts=opts.TextStyleOpts(color="#ffffff"),
            ),
            legend_opts=opts.LegendOpts(pos_top="12%"),
            toolbox_opts=opts.ToolboxOpts(
                is_show=True,
                feature=opts.ToolBoxFeatureOpts(
                    save_as_image=opts.ToolBoxFeatureSaveAsImageOpts(title="Save as Image"),
                    restore=opts.ToolBoxFeatureRestoreOpts(title="Restore"),
                    data_view=opts.ToolBoxFeatureDataViewOpts(title="Data View"),
                ),
            ),
        )

        self.charts.append(("savings_plan_trend", bar))
        return bar

    def generate_html_report(
        self, output_path: str, summary_stats: dict, title: str = "AWS Cost and Usage Report"
    ) -> str:
        """
        Generate a comprehensive HTML report with all visualizations.

        Args:
            output_path: Path to save the HTML report
            summary_stats: Dictionary with summary statistics
            title: Report title

        Returns:
            Path to the generated HTML file
        """
        logger.info(f"Generating HTML report: {output_path}")

        # Create Page object to combine all charts
        page = Page(layout=Page.SimplePageLayout)

        # Add all charts to the page
        for name, chart in self.charts:
            page.add(chart)

        # Save the page
        page.render(output_path)

        # Read the generated HTML and inject our custom header
        with open(output_path, "r", encoding="utf-8") as f:
            chart_html = f.read()

        # Extract script dependencies from head (ECharts libraries)
        scripts = []
        head_match = re.search(r"<head>(.*?)</head>", chart_html, re.DOTALL)
        if head_match:
            head_content = head_match.group(1)
            # Find all script tags (both src and inline)
            scripts = re.findall(r"<script[^>]*>.*?</script>", head_content, re.DOTALL)

        script_block = "\n".join(scripts)

        # Extract the chart content (everything after <body>)
        # Use regex to find body tag with potential attributes
        body_match = re.search(r"<body[^>]*>", chart_html)
        if body_match:
            chart_content = chart_html[body_match.end() :]  # Skip <body> tag

            # Remove closing tags to append footer properly
            chart_content = chart_content.replace("</body>", "").replace("</html>", "")

            # Replace Chinese locale with English
            chart_content = chart_content.replace("locale: 'ZH'", "locale: 'EN'")
            chart_content = chart_content.replace('locale: "ZH"', 'locale: "EN"')
        else:
            # Fallback if no body tag found (unlikely with pyecharts)
            chart_content = chart_html
            # Replace Chinese locale with English
            chart_content = chart_content.replace("locale: 'ZH'", "locale: 'EN'")
            chart_content = chart_content.replace('locale: "ZH"', 'locale: "EN"')

        # Escape user-provided content to prevent XSS
        safe_title = html.escape(str(title))
        safe_total_cost = _safe_round(summary_stats.get("total_cost", 0), 2)
        safe_num_accounts = int(summary_stats.get("num_accounts", 0))
        safe_num_services = int(summary_stats.get("num_services", 0))
        safe_date_start = html.escape(str(summary_stats.get("date_range_start", "N/A")))
        safe_date_end = html.escape(str(summary_stats.get("date_range_end", "N/A")))
        safe_total_records = int(summary_stats.get("total_records", 0))

        # Create summary HTML
        summary_html = f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>{safe_title}</title>
            {script_block}
            <style>
                * {{
                    margin: 0;
                    padding: 0;
                    box-sizing: border-box;
                }}
                body {{
                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    min-height: 100vh;
                    padding: 40px 20px;
                    overflow-x: hidden;
                }}
                .main-container {{
                    max-width: 1600px;
                    margin: 0 auto;
                    background: #ffffff;
                    border-radius: 20px;
                    box-shadow: 0 20px 60px rgba(0, 0, 0, 0.3);
                    overflow: hidden;
                }}
                .header {{
                    background: linear-gradient(135deg, #5470c6 0%, #91cc75 100%);
                    color: white;
                    padding: 60px 40px;
                    text-align: center;
                    overflow-wrap: break-word;
                }}
                .header h1 {{
                    font-size: 48px;
                    font-weight: 700;
                    margin-bottom: 15px;
                    text-shadow: 2px 2px 4px rgba(0,0,0,0.2);
                    word-wrap: break-word;
                }}
                .header p {{
                    font-size: 20px;
                    opacity: 0.95;
                    font-weight: 300;
                    word-wrap: break-word;
                }}
                .content {{
                    padding: 60px 40px;
                }}
                .summary-section {{
                    margin-bottom: 60px;
                }}
                .summary-section h2 {{
                    color: #2c3e50;
                    font-size: 32px;
                    font-weight: 600;
                    margin-bottom: 30px;
                    padding-bottom: 15px;
                    border-bottom: 3px solid #5470c6;
                }}
                .summary-grid {{
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
                    gap: 25px;
                    margin-bottom: 40px;
                }}
                .summary-card {{
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    color: white;
                    padding: 30px;
                    border-radius: 15px;
                    box-shadow: 0 10px 30px rgba(102, 126, 234, 0.3);
                    transition: transform 0.3s ease, box-shadow 0.3s ease;
                    overflow: hidden;
                    word-wrap: break-word;
                }}
                .summary-card:hover {{
                    transform: translateY(-5px);
                    box-shadow: 0 15px 40px rgba(102, 126, 234, 0.4);
                }}
                .summary-card h3 {{
                    font-size: 14px;
                    font-weight: 500;
                    margin-bottom: 15px;
                    opacity: 0.9;
                    text-transform: uppercase;
                    letter-spacing: 1px;
                    word-wrap: break-word;
                }}
                .summary-card .value {{
                    font-size: 36px;
                    font-weight: 700;
                    text-shadow: 1px 1px 2px rgba(0,0,0,0.1);
                    word-wrap: break-word;
                    overflow-wrap: break-word;
                }}
                .summary-card.small-text .value {{
                    font-size: 18px;
                    font-weight: 600;
                    line-height: 1.4;
                }}
                .charts-section {{
                    margin-top: 40px;
                }}
                .charts-section h2 {{
                    color: #2c3e50;
                    font-size: 32px;
                    font-weight: 600;
                    margin-bottom: 40px;
                    padding-bottom: 15px;
                    border-bottom: 3px solid #91cc75;
                }}
                .chart-container {{
                    background: #ffffff;
                    padding: 100px 30px 30px 100px;
                    border-radius: 15px;
                    box-shadow: 0 5px 20px rgba(0, 0, 0, 0.08);
                    margin-bottom: 40px;
                    border: 1px solid #e8e8e8;
                    overflow-x: auto;
                    overflow-y: hidden;
                }}
                .chart-container > div {{
                    min-width: 600px;
                }}
                .footer {{
                    text-align: center;
                    padding: 30px;
                    background: #f8f9fa;
                    color: #6c757d;
                    font-size: 14px;
                    border-top: 1px solid #dee2e6;
                }}
                .footer strong {{
                    color: #495057;
                }}
            </style>
        </head>
        <body>
            <div class="main-container">
                <div class="header">
                    <h1>{safe_title}</h1>
                    <p>Comprehensive analysis of AWS costs and usage patterns</p>
                </div>
                <div class="content">
                    <div class="summary-section">
                        <h2>Executive Summary</h2>
                        <div class="summary-grid">
                            <div class="summary-card">
                                <h3>Total Cost</h3>
                                <div class="value">${safe_total_cost:,.2f}</div>
                            </div>
                            <div class="summary-card">
                                <h3>Number of Accounts</h3>
                                <div class="value">{safe_num_accounts}</div>
                            </div>
                            <div class="summary-card">
                                <h3>Number of Services</h3>
                                <div class="value">{safe_num_services}</div>
                            </div>
                            <div class="summary-card small-text">
                                <h3>Date Range</h3>
                                <div class="value">{safe_date_start}<br>to<br>{safe_date_end}</div>
                            </div>
                            <div class="summary-card">
                                <h3>Total Records</h3>
                                <div class="value">{safe_total_records:,}</div>
                            </div>
                        </div>
                    </div>
                    <div class="charts-section">
                        <h2>Detailed Analysis</h2>
        """

        # Combine our custom HTML with the charts
        footer_html = f"""
                    </div>
                </div>
                <div class="footer">
                    <strong>Report generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</strong><br>
                    Powered by Apache ECharts | AWS Cost and Usage Report Generator
                </div>
            </div>
        </body>
        </html>
        """

        final_html = summary_html + chart_content + footer_html

        # Write the final HTML
        os.makedirs(
            os.path.dirname(output_path) if os.path.dirname(output_path) else ".", exist_ok=True
        )
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(final_html)

        logger.info(f"HTML report generated successfully: {output_path}")
        return output_path
