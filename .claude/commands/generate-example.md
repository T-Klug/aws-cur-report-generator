---
description: Regenerate the example HTML report and CSV exports
allowed-tools: Bash(uv run pytest:*)
---

Regenerate the example report files in the examples/ directory:

Run: `uv run pytest tests/test_examples.py -v -s`

This will:
1. Generate a new example_report.html with 6 months of mock data
2. Create CSV exports (cost_by_service, cost_by_account, monthly_summary)
3. Verify all 13 chart types render correctly

After generation, confirm the files were updated and report their sizes.
