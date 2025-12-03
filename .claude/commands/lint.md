---
description: Run linting and formatting checks on the codebase
allowed-tools: Bash(uv run ruff:*), Bash(uv run black:*)
---

Run code quality checks:

1. **Ruff linting**: `uv run ruff check src tests cur_report_generator.py`
2. **Black formatting check**: `uv run black --check src tests cur_report_generator.py`

If issues are found:
- For ruff: Run `uv run ruff check --fix src tests` to auto-fix
- For black: Run `uv run black src tests cur_report_generator.py` to format

Report all issues found and whether they were auto-fixed.
