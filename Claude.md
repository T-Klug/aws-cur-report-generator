# AWS CUR Report Generator - AI Agent Context

## Quick Reference

```bash
# Essential commands
uv sync                           # Install dependencies
uv run pytest                     # Run all tests
uv run pytest --cov=src           # Run tests with coverage
uv run ruff check src tests       # Lint code
uv run black src tests            # Format code

# Generate reports
uv run python cur_report_generator.py --help
uv run python cur_report_generator.py --sample-files 5 --debug

# Regenerate example report
uv run pytest tests/test_examples.py::TestExampleReports::test_generate_example_html_report
```

## Available Slash Commands

Use these commands for common tasks:

| Command | Description |
|---------|-------------|
| `/test` | Run the test suite with coverage reporting |
| `/lint` | Run ruff and black checks on the codebase |
| `/generate-example` | Regenerate the example HTML report and CSVs |
| `/check-all` | Run all quality checks before committing |
| `/add-chart [name] [type]` | Guide for adding a new chart type |
| `/debug-report` | Debug HTML report generation issues |

## Available Subagents

Use these specialized agents for specific tasks:

| Agent | When to Use |
|-------|-------------|
| `cost-analyzer` | Analyzing CUR data for cost patterns, anomalies, optimization opportunities |
| `report-builder` | Creating/customizing HTML reports and visualizations |
| `test-runner` | Running tests, analyzing failures, fixing test issues |
| `code-reviewer` | Code reviews, refactoring suggestions, PR reviews |

## Available Skills

These skills provide domain knowledge:

| Skill | Knowledge Domain |
|-------|------------------|
| `cur-data` | CUR file formats, column names, data analysis patterns |
| `visualization` | pyecharts charts, HTML reports, styling best practices |

---

## Project Overview

**What it does:** Generates comprehensive, interactive HTML reports from AWS Cost and Usage Reports (CUR) data stored in S3. Provides 13 different visualizations, statistical anomaly detection, and trend analysis.

**Key requirements:**
- Easy CLI interface for end users
- Self-contained HTML reports (no external dependencies)
- Environment variable configuration (NO secrets in code)
- Deep cost analysis beyond standard AWS billing

**Target users:** DevOps engineers, FinOps teams, cloud architects analyzing AWS spending

## Architecture

```
aws-cur-report-generator/
├── cur_report_generator.py    # CLI entry point (Click-based)
├── src/
│   ├── s3_reader.py          # S3 file handling, caching, parallel downloads
│   ├── data_processor.py     # CUR data normalization, aggregation, anomaly detection
│   └── visualizer.py         # pyecharts chart generation, HTML report creation
├── tests/
│   ├── conftest.py           # Fixtures with 6 months of realistic mock data
│   ├── test_s3_reader.py     # S3 and file format tests
│   ├── test_data_processor.py # Data processing tests
│   ├── test_visualizer.py    # Chart and report tests
│   ├── test_cli.py           # CLI interface tests
│   └── test_examples.py      # End-to-end example generation
├── examples/                  # Generated sample outputs
└── .claude/
    ├── agents/               # Specialized subagents
    ├── commands/             # Slash commands
    ├── skills/               # Reusable knowledge
    └── settings.local.json   # Permissions config
```

## Technology Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| Package Manager | uv | Fast Python package management |
| CLI | Click + colorama | Command-line interface |
| AWS Access | boto3 | S3 operations |
| Data Processing | Polars (primary), Pandas | DataFrame operations |
| Visualization | pyecharts (Apache ECharts) | Interactive charts |
| File Formats | pyarrow | Parquet support |
| Testing | pytest + moto | Unit tests with AWS mocking |
| Linting | ruff | Code quality |
| Formatting | black | Code style |

## Module Details

### 1. S3 Reader (`src/s3_reader.py`)

**Class:** `CURReader`

**Responsibilities:**
- List CUR files in S3 bucket with date filtering
- Download files with parallel ThreadPoolExecutor
- Smart local file caching
- Support CSV, CSV.GZ, and Parquet formats
- Handle AWS credentials (profile, keys, IAM roles)

**Key methods:**
```python
reader = CURReader(bucket, prefix, region, profile)
reader.load_cur_data(start_date, end_date)  # Main entry point
reader.list_report_files()                   # List available files
reader.clear_cache()                         # Clear local cache
```

**CUR column variants handled:**
- Old format: `lineItem/UnblendedCost`, `lineItem/UsageAccountId`
- New format: `line_item_unblended_cost`, `line_item_usage_account_id`

### 2. Data Processor (`src/data_processor.py`)

**Class:** `CURDataProcessor`

**Responsibilities:**
- Normalize column names across CUR versions
- Aggregate costs by service, account, region
- Calculate moving averages (7-day, 30-day)
- Detect anomalies using z-scores
- Analyze discounts and savings plans

**Key methods:**
```python
processor = CURDataProcessor(df)
processor.prepare_data()                    # Clean and normalize
processor.get_cost_by_service(top_n=10)     # Service aggregation
processor.get_cost_by_account()             # Account aggregation
processor.get_daily_cost_trend()            # Daily trends with moving avg
processor.detect_cost_anomalies()           # Statistical outliers
processor.get_discounts_summary()           # Discount analysis
processor.get_savings_plan_analysis()       # Savings plan effectiveness
```

### 3. Visualizer (`src/visualizer.py`)

**Class:** `CURVisualizer`

**13 chart types:**
1. Cost by Service (bar)
2. Cost by Account (bar)
3. Daily Cost Trends with Moving Averages (line)
4. Service Cost Trends (multi-line)
5. Account Cost Trends (multi-line)
6. Account vs Service Heatmap
7. Service Cost Distribution (pie)
8. Account Cost Distribution (pie)
9. Monthly Summary (bar with trend)
10. Cost Anomalies (scatter with z-scores)
11. Cost by Region (bar)
12. Discounts/Credits Analysis (bar)
13. Savings Plan Effectiveness (bar)

**Key method:**
```python
visualizer = CURVisualizer(processor)
visualizer.generate_html_report(output_path, top_n=10)
```

**Theme options:** macarons (default), shine, roma, vintage, dark, light

### 4. CLI (`cur_report_generator.py`)

**Entry point:** `generate_report()` decorated with Click

**Workflow:**
1. Load environment variables
2. Validate required config (bucket, prefix)
3. Initialize S3 reader
4. Load CUR data for date range
5. Process data with aggregations
6. Generate visualizations
7. Write HTML report and/or CSV exports

## Mock Data Structure

The test fixtures in `conftest.py` generate 6 months of realistic data:

**Production Account (111111111111) - 87% of costs:**
- EC2: $8-13K/day, steady growth (cloud migration)
- RDS: $6.5-7.5K/day, stable (production DBs)
- S3: $2.8-4.2K/day, growing (data lake)
- CloudFront: $2-4.5K/day, weekday peaks
- DynamoDB: $2.2-3.7K/day, scaling up
- Lambda: ~$1K/day, consistent

**Development Account (210987654321) - 13% of costs:**
- EC2: Baseline + 5-8x spikes every 14 days (load testing)
- RDS: Baseline + 3-4x spikes every 10 days
- Lambda: Baseline + 8-15x spikes (integration tests)
- CloudFront: Testing only, OFF on weekends
- DynamoDB: Declining (migrating to RDS)
- S3: Stable storage

**Total:** ~$6.2M over 182 days

## Common Development Tasks

### Adding a new chart type

Use the `/add-chart` command or follow these steps:

1. Add method to `visualizer.py`:
```python
def create_my_new_chart(self, data: pd.DataFrame) -> Bar:
    chart = Bar()
    chart.add_xaxis(data['x'].tolist())
    chart.add_yaxis("Series", data['y'].tolist())
    return chart
```

2. Add to `generate_html_report()` method

3. Add test to `test_visualizer.py`:
```python
def test_create_my_new_chart(self, visualizer):
    chart = visualizer.create_my_new_chart(self.sample_data)
    assert chart is not None
```

4. Update `test_examples.py` if it should appear in example report

### Adding a new aggregation

1. Add method to `data_processor.py`:
```python
def get_my_aggregation(self) -> pd.DataFrame:
    return self.df.groupby(['column']).agg({'cost': 'sum'}).reset_index()
```

2. Add caching if expensive:
```python
if self._my_aggregation_cache is not None:
    return self._my_aggregation_cache
# ... compute ...
self._my_aggregation_cache = result
return result
```

3. Add tests to `test_data_processor.py`

### Supporting new CUR column formats

1. Update column mappings in `data_processor.py`:
```python
COLUMN_VARIANTS = {
    'cost': ['line_item_unblended_cost', 'lineItem/UnblendedCost', 'new_format'],
    # ...
}
```

2. Update `_normalize_column_names()` method

3. Add test cases with the new format

## Type Checking Notes

The codebase uses pandas extensively, which has incomplete type stubs. Common patterns:

**DataFrame column access:**
```python
# Use explicit type annotation to avoid ndarray inference
cost_col: pd.Series = df['cost']  # type: ignore[assignment]
```

**Filtering:**
```python
filtered: pd.DataFrame = df[df['col'] > 0]  # type: ignore[assignment]
```

**GzipFile with pandas:**
```python
# pandas stubs don't recognize GzipFile, but it works at runtime
pd.read_csv(gzip_file)  # type: ignore[arg-type]
```

**sort_values:**
```python
# Always use by= parameter for type safety
df.sort_values(by='column')  # Not df.sort_values('column')
```

## Testing Strategy

Use the `/test` command or the `test-runner` agent for comprehensive testing.

**Coverage target:** 96%+ (currently achieved)

**Test isolation:** Each test uses fixtures, no shared state

**Mock patterns:**
- S3: Use `moto` for AWS mocking
- Dates: Use `freezegun` or parametrized fixtures
- Files: Use `tmp_path` pytest fixture

**Running specific tests:**
```bash
# Single test file
uv run pytest tests/test_visualizer.py -v

# Single test class
uv run pytest tests/test_data_processor.py::TestCostAggregations -v

# Single test method
uv run pytest tests/test_cli.py::TestCLI::test_generate_report_success -v

# With print output
uv run pytest tests/test_examples.py -v -s
```

## Known Issues & Workarounds

1. **Import resolution in tests:** Tests use `sys.path.insert()` which type checkers can't resolve - this is expected and doesn't affect functionality

2. **pyecharts chart rendering:** Charts must use `.tolist()` for data arrays to ensure browser compatibility (not numpy arrays)

3. **Large file handling:** For very large CUR files (>1GB), consider using `--sample-files` or smaller date ranges

4. **AWS credentials:** If using MFA, ensure session tokens are current before running

## Environment Configuration

**Required:**
```bash
CUR_BUCKET=my-cur-bucket      # S3 bucket with CUR data
CUR_PREFIX=cur-reports/       # S3 prefix path
```

**Optional:**
```bash
AWS_PROFILE=default           # AWS credentials profile
AWS_REGION=us-east-1          # AWS region
OUTPUT_DIR=reports            # Output directory
START_DATE=2024-01-01         # Analysis start (YYYY-MM-DD)
END_DATE=2024-12-31           # Analysis end (YYYY-MM-DD)
TOP_N=10                      # Top items in charts
DEBUG=false                   # Enable debug logging
```

## CI/CD Pipeline

GitHub Actions workflow (`.github/workflows/test.yml`):
- **Matrix:** Python 3.9, 3.10, 3.11, 3.12 x Ubuntu, macOS, Windows
- **Steps:** Install uv → sync deps → run pytest → upload coverage
- **Lint job:** ruff check + black --check

## Code Style Guidelines

Use the `/lint` command or `code-reviewer` agent for code quality checks.

- **Line length:** 100 characters (configured in pyproject.toml)
- **Imports:** Sorted by ruff (isort-compatible)
- **Docstrings:** Required for public methods
- **Type hints:** Preferred but not strictly enforced
- **Comments:** Explain "why", not "what"

## Debugging Tips

Use the `/debug-report` command for common issues.

**Empty charts in browser:**
1. Check browser console for JavaScript errors
2. Hard refresh (Ctrl+Shift+R)
3. Verify data exists in HTML source (search for chart name)
4. Ensure `.tolist()` is called on numpy arrays

**S3 access issues:**
1. Test with AWS CLI: `aws s3 ls s3://bucket/prefix/`
2. Check IAM permissions for GetObject and ListBucket
3. Verify bucket region matches configuration

**Memory issues:**
1. Use `--sample-files` to limit data
2. Enable caching with `--cache-dir`
3. Reduce date range

## Resources

- [AWS CUR Documentation](https://docs.aws.amazon.com/cur/latest/userguide/what-is-cur.html)
- [pyecharts Documentation](https://pyecharts.org/)
- [Polars User Guide](https://pola.rs/user-guide/)
- [uv Documentation](https://github.com/astral-sh/uv)
