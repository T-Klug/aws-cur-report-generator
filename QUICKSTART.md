# Quick Start Guide

Get up and running with AWS CUR Report Generator in 5 minutes!

## Prerequisites Check

Before starting, ensure you have:
- [ ] Python 3.8+ installed (`python --version`)
- [ ] AWS CUR enabled and exporting to S3
- [ ] AWS credentials configured
- [ ] S3 bucket name and prefix for your CUR data

## Step-by-Step Setup

### 1. Install

```bash
# Install uv (if not already installed) - blazingly fast! ⚡
curl -LsSf https://astral.sh/uv/install.sh | sh

# Clone the repository
git clone https://github.com/yourusername/aws-cur-report-generator.git
cd aws-cur-report-generator

# Install dependencies (uv handles everything automatically)
uv sync
```

<details>
<summary>Using pip instead? Click here for traditional installation</summary>

```bash
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```
</details>

### 2. Configure

```bash
# Copy the example environment file
cp .env.example .env

# Edit .env with your favorite editor
nano .env  # or vim, code, etc.
```

**Minimum required configuration in `.env`:**
```bash
AWS_PROFILE=default              # Your AWS profile name
AWS_REGION=us-east-1            # AWS region
CUR_BUCKET=my-cur-bucket        # S3 bucket with CUR data
CUR_PREFIX=cur-reports/my-cur   # S3 prefix to CUR files
```

### 3. Test Run

```bash
# Test with limited data first
uv run python cur_report_generator.py --sample-files 3 --debug
```

If successful, you'll see:
- ✓ Files read from S3
- ✓ Data processed
- ✓ Visualizations created
- ✓ HTML report generated in `reports/` directory

### 4. Generate Full Report

```bash
# Generate full report for last 90 days
uv run python cur_report_generator.py

# Or specify custom date range
uv run python cur_report_generator.py --start-date 2024-01-01 --end-date 2024-12-31
```

### 5. View Report

```bash
# Open the HTML report
open reports/cur_report_*.html  # macOS
xdg-open reports/cur_report_*.html  # Linux
start reports/cur_report_*.html  # Windows
```

## Common First-Time Issues

### "No CUR files found"
**Problem:** Can't find CUR data in S3

**Quick Fix:**
```bash
# Test S3 access manually
aws s3 ls s3://YOUR_BUCKET/YOUR_PREFIX/

# Verify your .env file has correct values
cat .env | grep CUR
```

### "AWS credentials not found"
**Problem:** AWS credentials not configured

**Quick Fix:**
```bash
# Option 1: Configure AWS CLI (recommended)
aws configure

# Option 2: Set profile in .env
echo "AWS_PROFILE=your-profile-name" >> .env

# Verify credentials work
aws sts get-caller-identity
```

### "Permission denied"
**Problem:** Missing S3 permissions

**Quick Fix:**
Ensure your AWS user/role has this IAM policy:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::YOUR_BUCKET",
        "arn:aws:s3:::YOUR_BUCKET/*"
      ]
    }
  ]
}
```

## Usage Examples

### Last 30 Days
```bash
uv run python cur_report_generator.py --start-date $(date -d '30 days ago' +%Y-%m-%d)
```

### Specific Month
```bash
uv run python cur_report_generator.py --start-date 2024-10-01 --end-date 2024-10-31
```

### With CSV Export
```bash
uv run python cur_report_generator.py --generate-csv
```

### Top 20 Services
```bash
uv run python cur_report_generator.py --top-n 20
```

## Next Steps

Now that you have the basic report working:

1. **Customize date ranges** to analyze specific periods
2. **Adjust TOP_N** to show more/fewer services
3. **Enable CSV exports** for data analysis in Excel/other tools
4. **Schedule regular reports** with cron or AWS EventBridge
5. **Share reports** with your team (HTML files are self-contained)

## Get Help

- 📖 See full documentation in [README.md](README.md)
- 🐛 Found a bug? Open an issue on GitHub
- 💡 Have a feature idea? We'd love to hear it!

---

**You're all set! Happy cost analyzing!** 🚀
