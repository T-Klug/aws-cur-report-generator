"""Visualizer - Creates interactive visualizations and reports from CUR data."""

import logging
import os
from datetime import datetime
from typing import Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

logger = logging.getLogger(__name__)


class CURVisualizer:
    """Generate interactive visualizations for AWS Cost and Usage data."""

    def __init__(self, theme: str = 'plotly_white'):
        """
        Initialize the visualizer.

        Args:
            theme: Plotly theme to use
        """
        self.theme = theme
        self.figures: list[tuple[str, go.Figure]] = []

    def create_cost_by_service_chart(self, df: pd.DataFrame, top_n: int = 10,
                                     title: str = "Cost by Service") -> go.Figure:
        """
        Create a bar chart of costs by service.

        Args:
            df: DataFrame with service and total_cost columns
            top_n: Number of services to show
            title: Chart title

        Returns:
            Plotly figure
        """
        logger.info(f"Creating cost by service chart (top {top_n})...")

        # Take top N
        plot_df = df.head(top_n).copy()

        # Create figure
        fig = go.Figure()

        fig.add_trace(go.Bar(
            x=plot_df['service'],
            y=plot_df['total_cost'],
            text=[f'${x:,.2f}' for x in plot_df['total_cost']],
            textposition='auto',
            marker_color='#3498db',
            hovertemplate='<b>%{x}</b><br>Cost: $%{y:,.2f}<extra></extra>'
        ))

        fig.update_layout(
            title=title,
            xaxis_title="Service",
            yaxis_title="Total Cost (USD)",
            template=self.theme,
            height=500,
            showlegend=False
        )

        self.figures.append(('cost_by_service', fig))
        return fig

    def create_cost_by_account_chart(self, df: pd.DataFrame, top_n: int = 10,
                                     title: str = "Cost by Account") -> go.Figure:
        """
        Create a bar chart of costs by account.

        Args:
            df: DataFrame with account_id and total_cost columns
            top_n: Number of accounts to show
            title: Chart title

        Returns:
            Plotly figure
        """
        logger.info(f"Creating cost by account chart (top {top_n})...")

        # Take top N
        plot_df = df.head(top_n).copy()

        fig = go.Figure()

        fig.add_trace(go.Bar(
            x=plot_df['account_id'],
            y=plot_df['total_cost'],
            text=[f'${x:,.2f}' for x in plot_df['total_cost']],
            textposition='auto',
            marker_color='#2ecc71',
            hovertemplate='<b>Account: %{x}</b><br>Cost: $%{y:,.2f}<extra></extra>'
        ))

        fig.update_layout(
            title=title,
            xaxis_title="Account ID",
            yaxis_title="Total Cost (USD)",
            template=self.theme,
            height=500,
            showlegend=False
        )

        self.figures.append(('cost_by_account', fig))
        return fig

    def create_daily_trend_chart(self, df: pd.DataFrame,
                                 title: str = "Daily Cost Trend") -> go.Figure:
        """
        Create a line chart showing daily cost trends with moving averages.

        Args:
            df: DataFrame with date, total_cost, and moving average columns
            title: Chart title

        Returns:
            Plotly figure
        """
        logger.info("Creating daily cost trend chart...")

        fig = go.Figure()

        # Daily cost
        fig.add_trace(go.Scatter(
            x=df['date'],
            y=df['total_cost'],
            mode='lines',
            name='Daily Cost',
            line=dict(color='#3498db', width=1),
            hovertemplate='<b>%{x}</b><br>Cost: $%{y:,.2f}<extra></extra>'
        ))

        # 7-day moving average
        if '7_day_ma' in df.columns:
            fig.add_trace(go.Scatter(
                x=df['date'],
                y=df['7_day_ma'],
                mode='lines',
                name='7-Day Average',
                line=dict(color='#e74c3c', width=2, dash='dash'),
                hovertemplate='<b>%{x}</b><br>7-Day Avg: $%{y:,.2f}<extra></extra>'
            ))

        # 30-day moving average
        if '30_day_ma' in df.columns:
            fig.add_trace(go.Scatter(
                x=df['date'],
                y=df['30_day_ma'],
                mode='lines',
                name='30-Day Average',
                line=dict(color='#9b59b6', width=2, dash='dot'),
                hovertemplate='<b>%{x}</b><br>30-Day Avg: $%{y:,.2f}<extra></extra>'
            ))

        fig.update_layout(
            title=title,
            xaxis_title="Date",
            yaxis_title="Total Cost (USD)",
            template=self.theme,
            height=500,
            hovermode='x unified'
        )

        self.figures.append(('daily_trend', fig))
        return fig

    def create_service_trend_chart(self, df: pd.DataFrame,
                                   title: str = "Cost Trends by Service") -> go.Figure:
        """
        Create a line chart showing cost trends for top services.

        Args:
            df: DataFrame with date, service, and total_cost columns
            title: Chart title

        Returns:
            Plotly figure
        """
        logger.info("Creating service trend chart...")

        fig = go.Figure()

        # Get unique services
        services = df['service'].unique()

        # Define colors
        colors = px.colors.qualitative.Set2

        for i, service in enumerate(services):
            service_data = df[df['service'] == service]
            fig.add_trace(go.Scatter(
                x=service_data['date'],
                y=service_data['total_cost'],
                mode='lines+markers',
                name=service,
                line=dict(color=colors[i % len(colors)], width=2),
                marker=dict(size=4),
                hovertemplate=f'<b>{service}</b><br>Date: %{{x}}<br>Cost: $%{{y:,.2f}}<extra></extra>'
            ))

        fig.update_layout(
            title=title,
            xaxis_title="Date",
            yaxis_title="Total Cost (USD)",
            template=self.theme,
            height=600,
            hovermode='x unified',
            legend=dict(
                orientation="v",
                yanchor="top",
                y=1,
                xanchor="left",
                x=1.02
            )
        )

        self.figures.append(('service_trend', fig))
        return fig

    def create_account_trend_chart(self, df: pd.DataFrame,
                                   title: str = "Cost Trends by Account") -> go.Figure:
        """
        Create a line chart showing cost trends for top accounts.

        Args:
            df: DataFrame with date, account_id, and total_cost columns
            title: Chart title

        Returns:
            Plotly figure
        """
        logger.info("Creating account trend chart...")

        fig = go.Figure()

        # Get unique accounts
        accounts = df['account_id'].unique()

        # Define colors
        colors = px.colors.qualitative.Plotly

        for i, account in enumerate(accounts):
            account_data = df[df['account_id'] == account]
            fig.add_trace(go.Scatter(
                x=account_data['date'],
                y=account_data['total_cost'],
                mode='lines+markers',
                name=account,
                line=dict(color=colors[i % len(colors)], width=2),
                marker=dict(size=4),
                hovertemplate=f'<b>Account: {account}</b><br>Date: %{{x}}<br>Cost: $%{{y:,.2f}}<extra></extra>'
            ))

        fig.update_layout(
            title=title,
            xaxis_title="Date",
            yaxis_title="Total Cost (USD)",
            template=self.theme,
            height=600,
            hovermode='x unified',
            legend=dict(
                orientation="v",
                yanchor="top",
                y=1,
                xanchor="left",
                x=1.02
            )
        )

        self.figures.append(('account_trend', fig))
        return fig

    def create_account_service_heatmap(self, df: pd.DataFrame,
                                       title: str = "Cost Heatmap: Account vs Service") -> go.Figure:
        """
        Create a heatmap showing costs by account and service.

        Args:
            df: DataFrame with account_id, service, and total_cost columns
            title: Chart title

        Returns:
            Plotly figure
        """
        logger.info("Creating account-service heatmap...")

        # Pivot the data
        pivot_df = df.pivot(index='account_id', columns='service', values='total_cost')
        pivot_df = pivot_df.fillna(0)

        # Convert to lists for better browser compatibility (avoid binary encoding issues)
        z_values = pivot_df.values.tolist()
        x_labels = pivot_df.columns.tolist()
        y_labels = pivot_df.index.tolist()

        fig = go.Figure(data=go.Heatmap(
            z=z_values,
            x=x_labels,
            y=y_labels,
            colorscale='Blues',
            hovertemplate='Account: %{y}<br>Service: %{x}<br>Cost: $%{z:,.2f}<extra></extra>',
            colorbar=dict(title="Cost (USD)")
        ))

        fig.update_layout(
            title=title,
            xaxis_title="Service",
            yaxis_title="Account ID",
            template=self.theme,
            height=max(400, len(pivot_df.index) * 40)
        )

        self.figures.append(('account_service_heatmap', fig))
        return fig

    def create_cost_distribution_pie(self, df: pd.DataFrame, category: str,
                                     top_n: int = 10,
                                     title: Optional[str] = None) -> go.Figure:
        """
        Create a pie chart showing cost distribution.

        Args:
            df: DataFrame with category and total_cost columns
            category: Name of the category (for labeling)
            top_n: Number of items to show (rest grouped as "Other")
            title: Chart title

        Returns:
            Plotly figure
        """
        logger.info(f"Creating cost distribution pie chart for {category}...")

        if title is None:
            title = f"Cost Distribution by {category.replace('_', ' ').title()}"

        # Take top N and group the rest
        plot_df = df.copy()
        if len(plot_df) > top_n:
            top_items = plot_df.head(top_n)
            other_cost = plot_df.iloc[top_n:]['total_cost'].sum()
            first_col = str(plot_df.columns[0])  # Ensure it's a string for dict key
            other_row = pd.DataFrame([{first_col: 'Other', 'total_cost': other_cost}])
            plot_df = pd.concat([top_items, other_row], ignore_index=True)

        fig = go.Figure(data=[go.Pie(
            labels=plot_df.iloc[:, 0],
            values=plot_df['total_cost'],
            hovertemplate='<b>%{label}</b><br>Cost: $%{value:,.2f}<br>Percentage: %{percent}<extra></extra>',
            textinfo='label+percent',
            textposition='auto'
        )])

        fig.update_layout(
            title=title,
            template=self.theme,
            height=500
        )

        self.figures.append((f'{category}_pie', fig))
        return fig

    def create_monthly_summary_chart(self, df: pd.DataFrame,
                                     title: str = "Monthly Cost Summary") -> go.Figure:
        """
        Create a bar chart showing monthly cost summary.

        Args:
            df: DataFrame with month and total_cost columns
            title: Chart title

        Returns:
            Plotly figure
        """
        logger.info("Creating monthly summary chart...")

        fig = go.Figure()

        fig.add_trace(go.Bar(
            x=df['month'],
            y=df['total_cost'],
            text=[f'${x:,.2f}' for x in df['total_cost']],
            textposition='auto',
            marker_color='#16a085',
            hovertemplate='<b>%{x}</b><br>Total Cost: $%{y:,.2f}<extra></extra>'
        ))

        fig.update_layout(
            title=title,
            xaxis_title="Month",
            yaxis_title="Total Cost (USD)",
            template=self.theme,
            height=500,
            showlegend=False
        )

        self.figures.append(('monthly_summary', fig))
        return fig

    def create_anomaly_chart(self, df: pd.DataFrame,
                            title: str = "Cost Anomalies Detection") -> go.Figure:
        """
        Create a chart highlighting cost anomalies.

        Args:
            df: DataFrame with date, total_cost, and z_score columns
            title: Chart title

        Returns:
            Plotly figure
        """
        logger.info("Creating cost anomaly chart...")

        fig = go.Figure()

        # Highlight anomalies
        fig.add_trace(go.Scatter(
            x=df['date'],
            y=df['total_cost'],
            mode='markers',
            name='Anomalous Days',
            marker=dict(
                size=12,
                color=df['z_score'],
                colorscale='RdYlGn',
                reversescale=True,
                showscale=True,
                colorbar=dict(title="Z-Score"),
                line=dict(width=2, color='darkred')
            ),
            hovertemplate='<b>%{x}</b><br>Cost: $%{y:,.2f}<br>Z-Score: %{marker.color:.2f}<extra></extra>'
        ))

        fig.update_layout(
            title=title,
            xaxis_title="Date",
            yaxis_title="Total Cost (USD)",
            template=self.theme,
            height=500
        )

        self.figures.append(('anomalies', fig))
        return fig

    def create_region_chart(self, df: pd.DataFrame, top_n: int = 10,
                           title: str = "Cost by Region") -> go.Figure:
        """
        Create a bar chart of costs by region.

        Args:
            df: DataFrame with region and total_cost columns
            top_n: Number of regions to show
            title: Chart title

        Returns:
            Plotly figure
        """
        logger.info(f"Creating cost by region chart (top {top_n})...")

        plot_df = df.head(top_n).copy()

        fig = go.Figure()

        fig.add_trace(go.Bar(
            x=plot_df['region'],
            y=plot_df['total_cost'],
            text=[f'${x:,.2f}' for x in plot_df['total_cost']],
            textposition='auto',
            marker_color='#e67e22',
            hovertemplate='<b>%{x}</b><br>Cost: $%{y:,.2f}<extra></extra>'
        ))

        fig.update_layout(
            title=title,
            xaxis_title="Region",
            yaxis_title="Total Cost (USD)",
            template=self.theme,
            height=500,
            showlegend=False
        )

        self.figures.append(('cost_by_region', fig))
        return fig

    def generate_html_report(self, output_path: str, summary_stats: dict,
                            title: str = "AWS Cost and Usage Report") -> str:
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

        # Create HTML content
        html_parts = [
            f"""
            <!DOCTYPE html>
            <html>
            <head>
                <meta charset="UTF-8">
                <title>{title}</title>
                <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
                <style>
                    body {{
                        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                        margin: 0;
                        padding: 20px;
                        background-color: #f5f5f5;
                    }}
                    .container {{
                        max-width: 1400px;
                        margin: 0 auto;
                        background-color: white;
                        padding: 30px;
                        border-radius: 10px;
                        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                    }}
                    h1 {{
                        color: #2c3e50;
                        border-bottom: 3px solid #3498db;
                        padding-bottom: 10px;
                    }}
                    h2 {{
                        color: #34495e;
                        margin-top: 30px;
                        border-left: 4px solid #3498db;
                        padding-left: 10px;
                    }}
                    .summary {{
                        display: grid;
                        grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
                        gap: 20px;
                        margin: 20px 0;
                    }}
                    .summary-card {{
                        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                        color: white;
                        padding: 20px;
                        border-radius: 8px;
                        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
                    }}
                    .summary-card h3 {{
                        margin: 0 0 10px 0;
                        font-size: 14px;
                        opacity: 0.9;
                    }}
                    .summary-card p {{
                        margin: 0;
                        font-size: 24px;
                        font-weight: bold;
                    }}
                    .chart {{
                        margin: 20px 0;
                        background: white;
                        padding: 10px;
                        border-radius: 5px;
                    }}
                    .timestamp {{
                        text-align: right;
                        color: #7f8c8d;
                        font-size: 12px;
                        margin-top: 30px;
                    }}
                </style>
            </head>
            <body>
                <div class="container">
                    <h1>{title}</h1>
                    <p>Comprehensive analysis of AWS costs and usage patterns</p>

                    <h2>Summary Statistics</h2>
                    <div class="summary">
                        <div class="summary-card">
                            <h3>Total Cost</h3>
                            <p>${summary_stats.get('total_cost', 0):,.2f}</p>
                        </div>
                        <div class="summary-card">
                            <h3>Average Daily Cost</h3>
                            <p>${summary_stats.get('average_daily_cost', 0):,.2f}</p>
                        </div>
                        <div class="summary-card">
                            <h3>Number of Accounts</h3>
                            <p>{summary_stats.get('num_accounts', 0)}</p>
                        </div>
                        <div class="summary-card">
                            <h3>Number of Services</h3>
                            <p>{summary_stats.get('num_services', 0)}</p>
                        </div>
                        <div class="summary-card">
                            <h3>Date Range</h3>
                            <p style="font-size: 16px;">{summary_stats.get('date_range_start', 'N/A')} to {summary_stats.get('date_range_end', 'N/A')}</p>
                        </div>
                        <div class="summary-card">
                            <h3>Peak Daily Cost</h3>
                            <p>${summary_stats.get('max_daily_cost', 0):,.2f}</p>
                        </div>
                    </div>

                    <h2>Detailed Analysis</h2>
            """
        ]

        # Add all figures
        for name, fig in self.figures:
            html_parts.append(f'<div class="chart" id="{name}"></div>')
            html_parts.append('<script>')
            html_parts.append(f'var {name}_data = {fig.to_json()};')
            html_parts.append(f'Plotly.newPlot("{name}", {name}_data.data, {name}_data.layout);')
            html_parts.append('</script>')

        # Close HTML
        html_parts.append(f"""
                    <div class="timestamp">
                        Report generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                    </div>
                </div>
            </body>
            </html>
        """)

        # Write to file
        os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else '.', exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(html_parts))

        logger.info(f"HTML report generated successfully: {output_path}")
        return output_path
