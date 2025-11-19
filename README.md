# AWS CUR Report Generator

A powerful, easy-to-use tool for generating comprehensive visual analytics from AWS Cost and Usage Reports (CUR) stored in S3. Create in-depth cost analysis reports with interactive visualizations that go far beyond standard AWS billing reports.

## Features

🚀 **Easy to Use** - Simple CLI interface with environment-based configuration

📊 **Rich Visualizations** - Interactive charts and graphs powered by Apache ECharts
- Cost trends over time
- Cost breakdown by AWS account
- Cost breakdown by service
- Multi-dimensional analysis (account × service)
- Regional cost distribution
- Cost anomaly detection
- Monthly aggregations

🔍 **Deep Analysis** - Goes beyond basic billing reports:
- Statistical anomaly detection for unusual spending
- Trend analysis across time periods
- Heatmaps showing cost relationships between accounts and services
- Top cost drivers identification
- Time-series analysis

🔒 **Secure** - Uses environment variables for credentials (no secrets in code)

📈 **Flexible Output** - Generate HTML reports and/or CSV exports

## 🎯 See It In Action

Check out the **[example report](examples/example_report.html)** generated with mock data to see what the output looks like!

The example includes:
- 11 interactive visualizations
- Complete cost analysis for a sample month
- All chart types (bar, line, pie, heatmap, scatter)
- ~95KB self-contained HTML file

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
     - Time granularity: Monthly
     - Enable "Resource IDs" for detailed analysis
     - Enable "Split cost allocation data"
   - Choose S3 bucket for delivery
   - Select report format: Parquet or CSV with Gzip compression

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

## Configuration

### Environment Variables

| Variable | Required | Description | Default |
|----------|----------|-------------|---------|
| `CUR_BUCKET` | ✅ | S3 bucket containing CUR data | - |
| `CUR_PREFIX` | ✅ | S3 prefix/path to CUR reports | - |
| `AWS_PROFILE` | ⚠️ | AWS profile name (alternative to keys) | `default` |
| `AWS_ACCESS_KEY_ID` | ⚠️ | AWS access key (not recommended) | - |
| `AWS_SECRET_ACCESS_KEY` | ⚠️ | AWS secret key (not recommended) | - |
| `AWS_REGION` | ❌ | AWS region | `us-east-1` |
| `START_DATE` | ❌ | Analysis start date (YYYY-MM-DD) | 90 days ago |
| `END_DATE` | ❌ | Analysis end date (YYYY-MM-DD) | Today |
| `OUTPUT_DIR` | ❌ | Output directory for reports | `reports` |
| `TOP_N` | ❌ | Number of top items in reports | `10` |
| `DEBUG` | ❌ | Enable debug logging | `false` |

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
  -s, --start-date TEXT      Start date (YYYY-MM-DD). Default: 90 days ago
  -e, --end-date TEXT        End date (YYYY-MM-DD). Default: today
  -o, --output-dir TEXT      Output directory for reports. Default: reports/
  -n, --top-n INTEGER        Number of top items to show. Default: 10
  --generate-html / --no-html   Generate HTML report. Default: True
  --generate-csv / --no-csv     Generate CSV exports. Default: False
  --sample-files INTEGER     Limit files to process (for testing)
  --debug                    Enable debug logging
  --help                     Show this message and exit
```

## Output

### HTML Report

The tool generates a comprehensive HTML report with:

- **Summary Dashboard** - Key metrics at a glance
  - Total cost
  - Number of accounts and services
  - Date range covered
  - Total records analyzed

- **Interactive Visualizations**
  - Cost by Service (bar chart)
  - Cost by Account (bar chart)
  - Cost Trends by Service (multi-line time series)
  - Cost Trends by Account (multi-line time series)
  - Account vs Service Heatmap (detailed breakdown)
  - Cost Distribution Pie Charts
  - Monthly Summary (bar chart with trend line)
  - Cost Anomalies (scatter plot with z-scores)
  - Cost by Region (bar chart)

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
  --generate-csv

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
│   ├── data_processor.py    # Data processing and aggregation
│   └── visualizer.py         # Visualization generation
├── cur_report_generator.py  # Main CLI interface
├── .env.example             # Environment template
├── .gitignore               # Git ignore rules
└── README.md                # This file
```

### Components

- **S3 Reader** (`s3_reader.py`)
  - Downloads CUR files from S3
  - Handles multiple file formats (CSV, Parquet, gzipped)
  - Manages AWS credentials and sessions

- **Data Processor** (`data_processor.py`)
  - Normalizes CUR data across different versions
  - Performs aggregations and calculations
  - Detects cost anomalies
  - Generates summary statistics

- **Visualizer** (`visualizer.py`)
  - Creates interactive Plotly charts
  - Generates HTML reports
  - Handles multiple visualization types

- **CLI** (`cur_report_generator.py`)
  - Command-line interface
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
- Process data in smaller chunks
- Increase available system memory

### Column Not Found

**Issue:** "Could not identify column for X"

**Solutions:**
- Your CUR version may use different column names
- Check the CUR file structure
- Update column mappings in `data_processor.py`
- Report the issue with your CUR version details

## Security Best Practices

⚠️ **Never commit secrets to version control!**

✅ **Do:**
- Use environment variables for configuration
- Use AWS IAM roles when running on EC2/ECS/Lambda
- Use `AWS_PROFILE` for local development
- Restrict S3 bucket access to specific IAM users/roles
- Use read-only permissions for CUR bucket

❌ **Don't:**
- Commit `.env` files
- Hard-code AWS credentials
- Use root AWS account credentials
- Share AWS access keys
- Grant broad S3 permissions

## Performance Tips

- **Use Parquet format** - Faster to read and smaller than CSV
- **Limit date range** - Process only the time period you need
- **Use sample mode** - Test with `--sample-files` first
- **Run on EC2** - Better bandwidth to S3
- **Filter data** - Modify code to filter specific accounts/services if needed

## Development & Testing

### Running Tests

The project includes a comprehensive test suite with >90% coverage.

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
├── conftest.py              # Shared fixtures and test configuration
├── test_s3_reader.py       # Tests for S3 reader module
├── test_data_processor.py  # Tests for data processor module
├── test_visualizer.py      # Tests for visualizer module
└── test_cli.py             # Tests for CLI interface
```

### Code Quality

**Format code with black:**
```bash
uv run black src tests
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
- Tests run on Python 3.8-3.12
- Tests run on Ubuntu, macOS, and Windows
- Automatic code coverage reporting
- Linting checks with ruff and black

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Run tests (`uv run pytest`)
5. Format code (`uv run black src tests`)
6. Run linters (`uv run ruff check src tests`)
7. Commit your changes (`git commit -m 'Add amazing feature'`)
8. Push to the branch (`git push origin feature/amazing-feature`)
9. Open a Pull Request

## License

MIT License - see LICENSE file for details

## Support

For issues, questions, or feature requests, please open an issue on GitHub.

## Acknowledgments

Built with:
- [Boto3](https://boto3.amazonaws.com/) - AWS SDK for Python
- [Pandas](https://pandas.pydata.org/) - Data analysis
- [Apache ECharts (pyecharts)](https://echarts.apache.org/) - Interactive visualizations
- [Click](https://click.palletsprojects.com/) - CLI framework

## Roadmap

Future enhancements:
- [ ] Cost forecasting with ML models
- [ ] Budget alerts and notifications
- [ ] Cost optimization recommendations
- [ ] Multi-account consolidation
- [ ] Slack/email report delivery
- [ ] Scheduled report generation
- [ ] Custom tagging analysis
- [ ] Reserved Instance utilization analysis
- [ ] Savings Plan recommendations

---

**Happy Cost Analyzing!** 💰📊
