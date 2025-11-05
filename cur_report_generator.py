#!/usr/bin/env python3
"""
AWS CUR Report Generator - Main CLI Entry Point

Generate comprehensive visual reports from AWS Cost and Usage Reports stored in S3.
"""

import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import click
from colorama import Fore, Style
from colorama import init as colorama_init
from dotenv import load_dotenv
from tqdm import tqdm

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from data_processor import CURDataProcessor
from s3_reader import CURReader
from visualizer import CURVisualizer

# Initialize colorama
colorama_init()

# Configure logging
def setup_logging(debug: bool = False):
    """Configure logging based on debug flag."""
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )

logger = logging.getLogger(__name__)


def print_banner():
    """Print application banner."""
    banner = f"""
{Fore.CYAN}╔══════════════════════════════════════════════════════════════╗
║                                                              ║
║          AWS Cost and Usage Report Generator                ║
║                                                              ║
║     Generate in-depth visual analytics from your CUR data   ║
║                                                              ║
╚══════════════════════════════════════════════════════════════╝{Style.RESET_ALL}
    """
    print(banner)


def validate_env_vars():
    """Validate required environment variables."""
    required_vars = ['CUR_BUCKET', 'CUR_PREFIX']
    missing_vars = [var for var in required_vars if not os.getenv(var)]

    if missing_vars:
        print(f"{Fore.RED}Error: Missing required environment variables:{Style.RESET_ALL}")
        for var in missing_vars:
            print(f"  - {var}")
        print(f"\n{Fore.YELLOW}Please set these in your .env file or environment.{Style.RESET_ALL}")
        print(f"{Fore.YELLOW}See .env.example for reference.{Style.RESET_ALL}")
        sys.exit(1)


@click.command()
@click.option('--start-date', '-s', type=str, help='Start date (YYYY-MM-DD). Default: 90 days ago')
@click.option('--end-date', '-e', type=str, help='End date (YYYY-MM-DD). Default: today')
@click.option('--output-dir', '-o', type=str, help='Output directory for reports. Default: reports/')
@click.option('--top-n', '-n', type=int, help='Number of top items to show in reports. Default: 10')
@click.option('--generate-html/--no-html', default=True, help='Generate HTML report. Default: True')
@click.option('--generate-csv/--no-csv', default=False, help='Generate CSV exports. Default: False')
@click.option('--sample-files', type=int, help='Limit number of CUR files to process (for testing)')
@click.option('--debug', is_flag=True, help='Enable debug logging')
def generate_report(start_date, end_date, output_dir, top_n, generate_html,
                   generate_csv, sample_files, debug):
    """
    Generate comprehensive AWS Cost and Usage Reports.

    This tool reads CUR data from S3 and generates detailed visual analytics including:
    - Cost trends over time
    - Cost breakdown by service and account
    - Daily, weekly, and monthly aggregations
    - Cost anomaly detection
    - Interactive visualizations

    Configuration is done via environment variables (see .env.example).
    """
    # Setup
    setup_logging(debug)
    print_banner()

    # Load environment variables
    load_dotenv()

    # Validate configuration
    validate_env_vars()

    # Get configuration from environment
    bucket = os.getenv('CUR_BUCKET')
    prefix = os.getenv('CUR_PREFIX')
    aws_profile = os.getenv('AWS_PROFILE')
    aws_region = os.getenv('AWS_REGION', 'us-east-1')

    # Use provided values or defaults from env
    output_dir = output_dir or os.getenv('OUTPUT_DIR', 'reports')
    top_n = top_n or int(os.getenv('TOP_N', '10'))

    # Parse dates
    try:
        if start_date:
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        else:
            start_env = os.getenv('START_DATE')
            if start_env:
                start_dt = datetime.strptime(start_env, '%Y-%m-%d')
            else:
                start_dt = datetime.now() - timedelta(days=90)

        if end_date:
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        else:
            end_env = os.getenv('END_DATE')
            if end_env:
                end_dt = datetime.strptime(end_env, '%Y-%m-%d')
            else:
                end_dt = datetime.now()
    except ValueError:
        print(f"{Fore.RED}Error: Invalid date format. Use YYYY-MM-DD{Style.RESET_ALL}")
        sys.exit(1)

    print(f"{Fore.CYAN}Configuration:{Style.RESET_ALL}")
    print(f"  S3 Bucket: {bucket}")
    print(f"  S3 Prefix: {prefix}")
    print(f"  Date Range: {start_dt.date()} to {end_dt.date()}")
    print(f"  Output Directory: {output_dir}")
    print(f"  Top N Items: {top_n}")
    if sample_files:
        print(f"  {Fore.YELLOW}Sample Mode: Processing only {sample_files} files{Style.RESET_ALL}")
    print()

    try:
        # Step 1: Read CUR data from S3
        print(f"{Fore.GREEN}[1/4] Reading CUR data from S3...{Style.RESET_ALL}")
        reader = CURReader(bucket=bucket, prefix=prefix,
                          aws_profile=aws_profile, aws_region=aws_region)

        cur_data = reader.load_cur_data(start_date=start_dt, end_date=end_dt,
                                       sample_files=sample_files)

        if cur_data.empty:
            print(f"{Fore.RED}Error: No CUR data found for the specified date range{Style.RESET_ALL}")
            sys.exit(1)

        print(f"{Fore.GREEN}✓ Loaded {len(cur_data)} records{Style.RESET_ALL}\n")

        # Step 2: Process data
        print(f"{Fore.GREEN}[2/4] Processing and analyzing data...{Style.RESET_ALL}")
        processor = CURDataProcessor(cur_data)
        processor.prepare_data()

        # Get all analytics
        processor.get_total_cost()
        cost_by_service = processor.get_cost_by_service(top_n=top_n)
        cost_by_account = processor.get_cost_by_account(top_n=top_n)
        cost_by_account_service = processor.get_cost_by_account_and_service(
            top_accounts=top_n, top_services=top_n
        )
        daily_trend = processor.get_daily_cost_trend()
        service_trend = processor.get_cost_trend_by_service(top_services=5)
        account_trend = processor.get_cost_trend_by_account(top_accounts=5)
        monthly_summary = processor.get_monthly_summary()
        anomalies = processor.detect_cost_anomalies()
        cost_by_region = processor.get_cost_by_region(top_n=top_n)
        summary_stats = processor.get_summary_statistics()

        print(f"{Fore.GREEN}✓ Analysis complete{Style.RESET_ALL}\n")

        # Step 3: Generate visualizations
        print(f"{Fore.GREEN}[3/4] Creating visualizations...{Style.RESET_ALL}")
        visualizer = CURVisualizer()

        with tqdm(total=11, desc="Generating charts", bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt}') as pbar:
            visualizer.create_cost_by_service_chart(cost_by_service, top_n=top_n)
            pbar.update(1)

            visualizer.create_cost_by_account_chart(cost_by_account, top_n=top_n)
            pbar.update(1)

            visualizer.create_daily_trend_chart(daily_trend)
            pbar.update(1)

            if not service_trend.empty:
                visualizer.create_service_trend_chart(service_trend)
            pbar.update(1)

            if not account_trend.empty:
                visualizer.create_account_trend_chart(account_trend)
            pbar.update(1)

            if not cost_by_account_service.empty:
                visualizer.create_account_service_heatmap(cost_by_account_service)
            pbar.update(1)

            visualizer.create_cost_distribution_pie(cost_by_service, 'service', top_n=top_n)
            pbar.update(1)

            visualizer.create_cost_distribution_pie(cost_by_account, 'account', top_n=top_n)
            pbar.update(1)

            visualizer.create_monthly_summary_chart(monthly_summary)
            pbar.update(1)

            if not anomalies.empty:
                visualizer.create_anomaly_chart(anomalies)
            pbar.update(1)

            if not cost_by_region.empty:
                visualizer.create_region_chart(cost_by_region, top_n=top_n)
            pbar.update(1)

        print(f"{Fore.GREEN}✓ Visualizations created{Style.RESET_ALL}\n")

        # Step 4: Generate output files
        print(f"{Fore.GREEN}[4/4] Generating output files...{Style.RESET_ALL}")

        # Create output directory
        Path(output_dir).mkdir(parents=True, exist_ok=True)

        output_files = []

        # Generate HTML report
        if generate_html:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            html_path = os.path.join(output_dir, f'cur_report_{timestamp}.html')
            visualizer.generate_html_report(html_path, summary_stats)
            output_files.append(html_path)
            print(f"{Fore.GREEN}✓ HTML report: {html_path}{Style.RESET_ALL}")

        # Generate CSV exports
        if generate_csv:
            csv_dir = os.path.join(output_dir, 'csv')
            Path(csv_dir).mkdir(parents=True, exist_ok=True)

            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

            csv_files = {
                'cost_by_service': cost_by_service,
                'cost_by_account': cost_by_account,
                'daily_trend': daily_trend,
                'monthly_summary': monthly_summary,
            }

            for name, df in csv_files.items():
                csv_path = os.path.join(csv_dir, f'{name}_{timestamp}.csv')
                df.to_csv(csv_path, index=False)
                output_files.append(csv_path)

            print(f"{Fore.GREEN}✓ CSV files exported to: {csv_dir}{Style.RESET_ALL}")

        # Print summary
        print(f"\n{Fore.CYAN}{'='*60}{Style.RESET_ALL}")
        print(f"{Fore.CYAN}Report Summary:{Style.RESET_ALL}")
        print(f"{Fore.CYAN}{'='*60}{Style.RESET_ALL}")
        print(f"Total Cost: {Fore.YELLOW}${summary_stats['total_cost']:,.2f}{Style.RESET_ALL}")
        print(f"Average Daily Cost: {Fore.YELLOW}${summary_stats['average_daily_cost']:,.2f}{Style.RESET_ALL}")
        print(f"Peak Daily Cost: {Fore.YELLOW}${summary_stats['max_daily_cost']:,.2f}{Style.RESET_ALL}")
        print(f"Number of Accounts: {summary_stats['num_accounts']}")
        print(f"Number of Services: {summary_stats['num_services']}")
        print(f"Date Range: {summary_stats['date_range_start']} to {summary_stats['date_range_end']}")

        if anomalies is not None and not anomalies.empty:
            print(f"\n{Fore.YELLOW}⚠ Found {len(anomalies)} days with anomalous costs{Style.RESET_ALL}")

        print(f"\n{Fore.GREEN}✓ Report generation complete!{Style.RESET_ALL}")
        print(f"\n{Fore.CYAN}Generated files:{Style.RESET_ALL}")
        for file in output_files:
            print(f"  - {file}")

    except Exception as e:
        logger.exception("Error generating report")
        print(f"\n{Fore.RED}Error: {str(e)}{Style.RESET_ALL}")
        if debug:
            raise
        sys.exit(1)


if __name__ == '__main__':
    generate_report()
