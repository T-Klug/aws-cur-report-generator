---
description: Debug issues with generated HTML reports
allowed-tools: Bash(uv run python:*), Bash(ls:*), Bash(wc:*)
---

Debug HTML report generation issues:

## Common Issues to Check

### 1. Empty Charts
- Verify data arrays use `.tolist()` not numpy arrays
- Check browser console for JavaScript errors
- Ensure pyecharts is generating valid ECharts config

### 2. Missing Data
- Check if CUR data was loaded correctly
- Verify date range includes data
- Check column normalization in data_processor.py

### 3. Report Not Generating
- Check for Python exceptions
- Verify output directory exists and is writable
- Check disk space

## Diagnostic Steps

1. **Generate with debug logging:**
   ```
   uv run python cur_report_generator.py --debug --sample-files 3
   ```

2. **Check recent reports:**
   List files in reports directory and check sizes

3. **Validate HTML structure:**
   Check if ECharts script tags are present

What specific issue are you seeing with the report?
