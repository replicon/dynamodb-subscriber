# Coverage Report Generation

**Goal**: Analyze test coverage data to identify gaps, risk areas, and provide improvement recommendations.

## Step 1: Collect Coverage Data

- Run tests with coverage enabled
- Parse coverage output (lcov, istanbul, coverage.py)
- Extract per-file and per-function coverage metrics

## Step 2: Analyze Coverage by Module

For each module/directory:
- Calculate line coverage, branch coverage, function coverage
- Identify files below the target threshold ({{coverage_target}}%)
- Rank modules by risk (low coverage + high complexity = high risk)

## Step 3: Identify Critical Gaps

Flag uncovered code that poses the highest risk:
- Error handling paths (catch blocks, error callbacks)
- Authentication and authorization logic
- Data validation and sanitization
- Payment or financial processing
- API endpoint handlers
- Database mutations (writes, deletes)

## Step 4: Generate Improvement Recommendations

For each gap:
- Specific test suggestion (what to test, how to test it)
- Priority based on risk (critical path vs utility code)
- Estimated effort to achieve target coverage

## Step 5: Create Coverage Report

Generate the report with:
- Overall coverage metrics vs targets
- Per-module breakdown
- Critical gap list with priorities
- Recommended tests to write
- Coverage trend (if previous reports exist)

Write to: {default_output_file}
Log to audit trail
