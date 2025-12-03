---
description: Guide for adding a new chart type to the visualizer
allowed-tools: Bash(uv run pytest:*)
argument-hint: [chart-name] [chart-type]
---

Guide for adding a new chart to the AWS CUR Report Generator.

**Chart to add:** $ARGUMENTS

## Steps to implement:

### 1. Add chart method to `src/visualizer.py`
Create a new method following the pattern:
```python
def create_[chart_name]_chart(self, data: pd.DataFrame) -> [ChartType]:
    chart = [ChartType]()
    chart.add_xaxis(data['x_column'].tolist())
    chart.add_yaxis("Label", data['y_column'].tolist())
    chart.set_global_opts(
        title_opts=opts.TitleOpts(title="Chart Title"),
        tooltip_opts=opts.TooltipOpts(trigger="axis"),
    )
    return chart
```

### 2. Add to `generate_html_report()` method
Include the new chart in the HTML report generation.

### 3. Add test to `tests/test_visualizer.py`
```python
def test_create_[chart_name]_chart(self, visualizer):
    chart = visualizer.create_[chart_name]_chart()
    assert chart is not None
```

### 4. Update example report
Add to `tests/test_examples.py` if the chart should appear in examples.

### 5. Run tests
`uv run pytest tests/test_visualizer.py -v`

Would you like me to implement this chart type?
