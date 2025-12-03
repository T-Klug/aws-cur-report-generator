# AWS CUR Report Generator

[![Tests](https://github.com/yourusername/aws-cur-report-generator/actions/workflows/test.yml/badge.svg)](https://github.com/yourusername/aws-cur-report-generator/actions/workflows/test.yml)
[![Coverage](https://codecov.io/gh/yourusername/aws-cur-report-generator/branch/main/graph/badge.svg)](https://codecov.io/gh/yourusername/aws-cur-report-generator)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A powerful, easy-to-use tool for generating comprehensive visual analytics from AWS Cost and Usage Reports (CUR) stored in S3. Create in-depth cost analysis reports with interactive visualizations that go far beyond standard AWS billing reports.

## Features

**Easy to Use** - Simple CLI interface with environment-based configuration

**Rich Visualizations** - 11 interactive chart types powered by Apache ECharts (via pyecharts)
- Monthly cost trends by service, account, and region
- Multi-dimensional analysis (account x service heatmap)
- Statistical anomaly detection with z-scores
- Monthly discount trends by type and service
- Savings plan effectiveness tracking over time
- Distribution views for quick cost overview

**Deep Analysis** - Goes beyond basic billing reports:
- Statistical anomaly detection for unusual spending patterns
- Trend analysis across configurable time periods
- Heatmaps showing cost relationships between accounts and services
- Top cost drivers identification
- Moving average calculations for trend smoothing
- Savings plan effectiveness analysis
- Discount and credit tracking

**High Performance** - Built for large datasets:
- Polars DataFrame library for fast data processing
- Parallel S3 file downloads with configurable workers
- Smart local file caching to avoid re-downloads
- Streaming support for memory efficiency

**Secure** - Uses environment variables for credentials (no secrets in code)

**Flexible Output** - Generate self-contained HTML reports and/or CSV exports

## See It In Action

Check out the **[example report](examples/example_report.html)** generated with mock data to see what the output looks like!

The example includes:
- 11 interactive visualizations with monthly trends
- 6 months of sample data across 2 accounts
- All chart types (bar, line, pie, heatmap, scatter)
- Self-contained HTML file

You can also explore the [example CSV exports](examples/) for data analysis.

## Prerequisites

- Python 3.9 or higher
- AWS account with Cost and Usage Reports enabled
- S3 bucket with CUR data
- AWS credentials with S3 read access

## AWS CUR Setup

Before using this tool, you need to set up Cost and Usage Reports in AWS:

1. **Enable CUR in AWS Console:**
   - Go to AWS Billing Console → Cost & Usage Reports
   - Click "Create report"
   - Configure report settings:
     - Report name: Choose a name (e.g., "my-cur-report")
     - Time granularity: Daily or Monthly
     - Enable "Resource IDs" for detailed analysis
     - Enable "Split cost allocation data"
   - Choose S3 bucket for delivery
   - Select report format: **Parquet** (recommended) or CSV with Gzip compression

2. **Note the following for configuration:**
   - S3 bucket name
   - S3 prefix/path where reports are stored
   - AWS region of the S3 bucket

3. **Grant S3 Access:**
   - Ensure your AWS credentials have `s3:GetObject` and `s3:ListBucket` permissions for the CUR bucket

## Installation

### Using uv (Recommended - Fast & Modern)

1. **Install uv** (if not already installed):
   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

2. **Clone and setup:**
   ```bash
   git clone https://github.com/yourusername/aws-cur-report-generator.git
   cd aws-cur-report-generator

   # Install dependencies (uv handles everything automatically)
   uv sync
   ```

3. **Configure environment variables:**
   ```bash
   cp .env.example .env
   ```

   Edit `.env` and set your configuration:
   ```bash
   # AWS Configuration
   AWS_PROFILE=default          # Or use AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY
   AWS_REGION=us-east-1

   # S3 CUR Configuration
   CUR_BUCKET=your-cur-bucket-name
   CUR_PREFIX=your-cur-prefix

   # Optional: Report Configuration
   OUTPUT_DIR=reports
   TOP_N=10
   ```

### Using pip

```bash
git clone https://github.com/yourusername/aws-cur-report-generator.git
cd aws-cur-report-generator

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -e ".[dev]"
```

## Configuration

### Environment Variables

| Variable | Required | Description | Default |
|----------|----------|-------------|---------|
| `CUR_BUCKET` | Yes | S3 bucket containing CUR data | - |
| `CUR_PREFIX` | Yes | S3 prefix/path to CUR reports | - |
| `AWS_PROFILE` | Either | AWS profile name (recommended) | `default` |
| `AWS_ACCESS_KEY_ID` | Either | AWS access key (use profile instead) | - |
| `AWS_SECRET_ACCESS_KEY` | Either | AWS secret key (use profile instead) | - |
| `AWS_REGION` | No | AWS region | `us-east-1` |
| `START_DATE` | No | Analysis start date (YYYY-MM-DD) | 90 days ago |
| `END_DATE` | No | Analysis end date (YYYY-MM-DD) | Today |
| `OUTPUT_DIR` | No | Output directory for reports | `reports` |
| `TOP_N` | No | Number of top items in reports | `10` |
| `DEBUG` | No | Enable debug logging | `false` |

**Note:** Use either `AWS_PROFILE` (recommended) or `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`. For production, use IAM roles instead of access keys.

## Usage

### Basic Usage

Generate a report for the last 90 days (default):

```bash
# Using uv (recommended)
uv run python cur_report_generator.py

# Or if using pip/venv
python cur_report_generator.py
```

### Advanced Usage

**Custom date range:**
```bash
uv run python cur_report_generator.py --start-date 2024-01-01 --end-date 2024-03-31
```

**Generate CSV exports:**
```bash
uv run python cur_report_generator.py --generate-csv
```

**Custom output directory:**
```bash
uv run python cur_report_generator.py --output-dir ./my-reports
```

**Show top 20 items in charts:**
```bash
uv run python cur_report_generator.py --top-n 20
```

**Parallel downloads with caching:**
```bash
uv run python cur_report_generator.py --max-workers 8 --cache-dir .cache
```

**Clear cache and regenerate:**
```bash
uv run python cur_report_generator.py --clear-cache
```

**Test with limited data:**
```bash
uv run python cur_report_generator.py --sample-files 5
```

**Enable debug logging:**
```bash
uv run python cur_report_generator.py --debug
```

### CLI Options

```
Options:
  -s, --start-date TEXT        Start date (YYYY-MM-DD). Default: 90 days ago
  -e, --end-date TEXT          End date (YYYY-MM-DD). Default: today
  -o, --output-dir TEXT        Output directory for reports. Default: reports/
  -n, --top-n INTEGER          Number of top items to show. Default: 10
  -w, --max-workers INTEGER    Parallel workers for S3 downloads. Default: 4
  --cache-dir TEXT             Local cache directory for downloaded files
  --no-cache                   Disable file caching
  --clear-cache                Clear cache before running
  --generate-html / --no-html  Generate HTML report. Default: True
  --generate-csv / --no-csv    Generate CSV exports. Default: False
  --sample-files INTEGER       Limit files to process (for testing)
  --debug                      Enable debug logging
  --help                       Show this message and exit
```

## Output

### HTML Report

The tool generates a comprehensive, self-contained HTML report with:

- **Summary Dashboard** - Key metrics at a glance
  - Total cost for the period
  - Number of accounts and services
  - Date range covered
  - Total records analyzed

- **Interactive Visualizations** (9 chart types, all with monthly context as bar charts)
  1. Service Cost Trends (grouped bar chart by service)
  2. Account Cost Trends (grouped bar chart by account)
  3. Account vs Service Heatmap (cross-dimensional breakdown)
  4. Monthly Summary (bar chart)
  5. Cost Anomalies (scatter plot with z-scores)
  6. Region Cost Trends (grouped bar chart by region)
  7. Discounts Trend by Type (stacked bar chart)
  8. Discounts Trend by Service (grouped bar chart)
  9. Savings Plan Effectiveness (bar chart with savings %)

All charts are interactive - hover for details, zoom, pan, and download as PNG.

### CSV Exports

When enabled with `--generate-csv`, the tool exports:
- `cost_by_service_[timestamp].csv`
- `cost_by_account_[timestamp].csv`
- `daily_trend_[timestamp].csv`
- `monthly_summary_[timestamp].csv`

## Example Workflow

```bash
# 1. Install uv (if needed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# 2. Clone and setup
git clone https://github.com/yourusername/aws-cur-report-generator.git
cd aws-cur-report-generator
uv sync

# 3. Configure environment
cp .env.example .env
vim .env  # Edit with your AWS configuration

# 4. Generate a test report with sample data
uv run python cur_report_generator.py --sample-files 3 --debug

# 5. Generate a full report for last quarter
uv run python cur_report_generator.py \
  --start-date 2024-10-01 \
  --end-date 2024-12-31 \
  --top-n 15 \
  --generate-csv \
  --max-workers 8

# 6. Open the HTML report in your browser
open reports/cur_report_*.html
```

## Architecture

The tool is organized into modular components:

```
aws-cur-report-generator/
├── src/
│   ├── __init__.py           # Package initialization
│   ├── s3_reader.py          # S3 data reading and CUR file handling
│   ├── data_processor.py     # Data processing and aggregation
│   └── visualizer.py         # Visualization generation
├── tests/
│   ├── conftest.py           # Shared fixtures and test configuration
│   ├── test_s3_reader.py     # S3 reader tests
│   ├── test_data_processor.py # Data processor tests
│   ├── test_visualizer.py    # Visualizer tests
│   ├── test_cli.py           # CLI tests
│   └── test_examples.py      # Example report generation tests
├── examples/
│   ├── example_report.html   # Sample HTML report
│   └── *.csv                 # Sample CSV exports
├── cur_report_generator.py   # Main CLI interface
├── .env.example              # Environment template
└── README.md                 # This file
```

### Components

- **S3 Reader** (`src/s3_reader.py`)
  - Downloads CUR files from S3 with parallel execution
  - Handles multiple file formats (CSV, gzipped CSV, Parquet)
  - Manages AWS credentials and sessions
  - Implements smart file caching

- **Data Processor** (`src/data_processor.py`)
  - Normalizes CUR data across different AWS versions
  - Performs aggregations and calculations
  - Detects cost anomalies using statistical methods
  - Generates summary statistics and trends
  - Analyzes discounts and savings plans

- **Visualizer** (`src/visualizer.py`)
  - Creates interactive Apache ECharts visualizations
  - Generates self-contained HTML reports
  - Handles multiple chart types and themes

- **CLI** (`cur_report_generator.py`)
  - Command-line interface with Click
  - Configuration management
  - Orchestrates the report generation workflow

## Troubleshooting

### No CUR files found

**Issue:** "No CUR files found matching the criteria"

**Solutions:**
- Verify `CUR_BUCKET` and `CUR_PREFIX` are correct
- Check that CUR data exists for the date range
- Ensure AWS credentials have S3 read permissions
- Try listing files manually: `aws s3 ls s3://your-bucket/your-prefix/`

### AWS Credentials Error

**Issue:** "AWS credentials not found"

**Solutions:**
- Set `AWS_PROFILE` in `.env` file
- Or configure AWS CLI: `aws configure`
- Or set `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` (not recommended)
- For EC2/ECS, ensure IAM role is attached

### Permission Denied

**Issue:** "Access Denied" when reading from S3

**Solutions:**
- Verify IAM policy includes:
  ```json
  {
    "Effect": "Allow",
    "Action": [
      "s3:GetObject",
      "s3:ListBucket"
    ],
    "Resource": [
      "arn:aws:s3:::your-bucket",
      "arn:aws:s3:::your-bucket/*"
    ]
  }
  ```

### Memory Issues

**Issue:** Out of memory with large datasets

**Solutions:**
- Use `--sample-files` to limit data
- Reduce date range with `--start-date` and `--end-date`
- Enable caching with `--cache-dir` to avoid reprocessing
- The tool uses Polars for memory-efficient processing

### Column Not Found

**Issue:** "Could not identify column for X"

**Solutions:**
- Your CUR version may use different column names
- Check the CUR file structure
- Update column mappings in `data_processor.py`
- Report the issue with your CUR version details

## Security Best Practices

**Never commit secrets to version control!**

**Do:**
- Use environment variables for configuration
- Use AWS IAM roles when running on EC2/ECS/Lambda
- Use `AWS_PROFILE` for local development
- Restrict S3 bucket access to specific IAM users/roles
- Use read-only permissions for CUR bucket

**Don't:**
- Commit `.env` files
- Hard-code AWS credentials
- Use root AWS account credentials
- Share AWS access keys
- Grant broad S3 permissions

## Performance Tips

- **Use Parquet format** - Faster to read and smaller than CSV
- **Enable caching** - Use `--cache-dir` to avoid re-downloading files
- **Parallel downloads** - Increase `--max-workers` for faster S3 access
- **Limit date range** - Process only the time period you need
- **Use sample mode** - Test with `--sample-files` first
- **Run on EC2** - Better bandwidth to S3 in same region

## Development & Testing

### Running Tests

The project includes a comprehensive test suite with 96% coverage.

**Using uv (recommended):**
```bash
# Install with dev dependencies
uv sync --all-extras

# Run all tests
uv run pytest

# Run tests with coverage report
uv run pytest --cov=src --cov-report=html

# Run specific test file
uv run pytest tests/test_s3_reader.py

# Run specific test
uv run pytest tests/test_s3_reader.py::TestCURReader::test_initialization_success

# Generate example reports
uv run pytest tests/test_examples.py -v
```

### Test Structure

```
tests/
├── conftest.py              # Shared fixtures with 6 months of mock data
├── test_s3_reader.py        # S3 reader tests
├── test_data_processor.py   # Data processor tests
├── test_visualizer.py       # Visualizer tests
├── test_cli.py              # CLI tests
└── test_examples.py         # Example report generation tests
```

### Code Quality

**Format code with black:**
```bash
uv run black src tests cur_report_generator.py
```

**Lint with ruff:**
```bash
uv run ruff check src tests
```

**Fix linting issues automatically:**
```bash
uv run ruff check --fix src tests
```

### CI/CD

The project uses GitHub Actions for continuous integration:
- Tests run on Python 3.9, 3.10, 3.11, and 3.12
- Tests run on Ubuntu, macOS, and Windows
- Automatic code coverage reporting via Codecov
- Linting checks with ruff and black

## Claude Code Integration

This project includes comprehensive Claude Code configuration for AI-assisted development.

### Slash Commands

| Command | Description |
|---------|-------------|
| `/test` | Run the test suite with coverage reporting |
| `/lint` | Run ruff and black checks on the codebase |
| `/generate-example` | Regenerate the example HTML report and CSVs |
| `/check-all` | Run all quality checks before committing |
| `/add-chart [name] [type]` | Guide for adding a new chart type |
| `/debug-report` | Debug HTML report generation issues |

### Specialized Agents

| Agent | Use Case |
|-------|----------|
| `cost-analyzer` | Analyze CUR data for cost patterns, anomalies, and optimization opportunities |
| `report-builder` | Create and customize HTML reports and visualizations |
| `test-runner` | Run tests, analyze failures, and fix test issues |
| `code-reviewer` | Review code for quality, security, and best practices |

### Skills

| Skill | Knowledge Domain |
|-------|------------------|
| `cur-data` | CUR file formats, column names, data analysis patterns |
| `visualization` | pyecharts charts, HTML reports, styling best practices |

### Configuration

The `.claude/` directory contains:
- `agents/` - Specialized subagent definitions
- `commands/` - Custom slash commands
- `skills/` - Reusable knowledge bases
- `settings.local.json` - Permission configuration

See [Claude.md](Claude.md) for detailed AI agent context and development guidelines.

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Run tests (`uv run pytest`)
5. Format code (`uv run black src tests cur_report_generator.py`)
6. Run linters (`uv run ruff check src tests`)
7. Commit your changes (`git commit -m 'Add amazing feature'`)
8. Push to the branch (`git push origin feature/amazing-feature`)
9. Open a Pull Request

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Support

For issues, questions, or feature requests, please [open an issue](https://github.com/yourusername/aws-cur-report-generator/issues) on GitHub.

## Acknowledgments

Built with:
- [Boto3](https://boto3.amazonaws.com/) - AWS SDK for Python
- [Polars](https://pola.rs/) - Fast DataFrame library
- [Pandas](https://pandas.pydata.org/) - Data analysis
- [Apache ECharts](https://echarts.apache.org/) (via [pyecharts](https://pyecharts.org/)) - Interactive visualizations
- [Click](https://click.palletsprojects.com/) - CLI framework
- [uv](https://github.com/astral-sh/uv) - Fast Python package manager

## Roadmap

Future enhancements:
- [ ] Cost forecasting with ML models
- [ ] Budget alerts and notifications
- [ ] Cost optimization recommendations
- [ ] Multi-account consolidation views
- [ ] Slack/email report delivery
- [ ] Scheduled report generation
- [ ] Custom tagging analysis
- [ ] Reserved Instance utilization analysis
- [ ] Savings Plan recommendations engine
- [ ] Docker containerization

---

**Happy Cost Analyzing!**
