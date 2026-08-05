# Regression Detection

**Goal**: Compare current test results with previous runs to detect regressions and track quality trends.

## Step 1: Load Current and Previous Results

- Load the most recent test execution report
- Load the previous test execution report(s) from test artifacts
- If no previous report exists, note this as the baseline run

## Step 2: Compare Results

For each test that exists in both runs:
- **New Failure**: Test passed before, fails now → REGRESSION
- **Fixed**: Test failed before, passes now → IMPROVEMENT
- **Still Failing**: Test failed in both runs → PERSISTENT
- **Still Passing**: Test passes in both runs → STABLE
- **New Test**: Test didn't exist before → NEW
- **Removed Test**: Test existed before, gone now → REMOVED

## Step 3: Analyze Regressions

For each regression:
- Identify the test name and location
- Analyze the failure to determine likely cause
- Check git log for recent changes to affected files
- Assess severity (critical path vs edge case)
- Recommend fix approach

## Step 4: Track Quality Trends

Compare across available runs:
- Pass rate trend (improving, stable, declining)
- Coverage trend
- Test count trend (growing, stable, shrinking)
- Execution time trend (getting faster/slower)
- Flaky test rate trend

## Step 5: Generate Regression Report

Create report with:
- Regression list with severity and likely cause
- Improvements list (fixes since last run)
- Quality trend summary
- Action items for regressions
- Confidence assessment (is the codebase getting better or worse?)

Write to: {default_output_file}
Log to audit trail
