---
description: Run all quality checks (tests, lint, format) before committing
allowed-tools: Bash(uv run pytest:*), Bash(uv run ruff:*), Bash(uv run black:*)
---

Run comprehensive quality checks before committing:

## 1. Run Tests
`uv run pytest --cov=src -v`

## 2. Check Linting
`uv run ruff check src tests cur_report_generator.py`

## 3. Check Formatting
`uv run black --check src tests cur_report_generator.py`

## Summary
Report:
- Test results (pass/fail count, coverage percentage)
- Linting issues (count and severity)
- Formatting issues (files needing changes)

If all checks pass, the code is ready to commit. If not, list the issues to fix.
