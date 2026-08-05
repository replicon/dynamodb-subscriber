# Test Analysis Validation Checklist

## Classification
- [ ] Every failed test is categorized (no unclassified failures)
- [ ] Categories are correct (test bug vs code bug vs environment vs flaky)
- [ ] Severity assigned to each failure
- [ ] Fix complexity assessed

## Code Bugs
- [ ] Root cause identified for each code bug
- [ ] File path and line number provided
- [ ] Suggested fix is actionable and specific
- [ ] Regression test recommended

## Test Bugs
- [ ] Issue clearly described (wrong assertion, missing mock, etc.)
- [ ] Fix is specific to the test (not changing production code)

## Patterns
- [ ] Failure clusters identified
- [ ] Common root causes grouped
- [ ] Systemic issues flagged

## Recommendations
- [ ] Priority fix order is logical (code bugs first)
- [ ] Regression risk assessed
- [ ] Re-run recommendation provided
