---
description: Run the test suite with coverage reporting
allowed-tools: Bash(uv run pytest:*), Bash(uv sync:*)
---

Run the full test suite with coverage reporting:

1. Ensure dependencies are installed
2. Run pytest with coverage
3. Report any failures with details
4. Summarize coverage statistics

Execute: `uv run pytest --cov=src --cov-report=term-missing -v`

If tests fail, analyze the failures and suggest fixes.
