---
stepsCompleted: []
inputDocuments: []
workflowType: 'test-analysis'
date: '{{date}}'
---

# Test Analysis Report

**Analyzer:** Trent (Test Architect)
**Date:** {{date}}
**Based On:** {{execution_report_path}}

---

## Executive Summary

| Metric | Value |
|--------|-------|
| Total Tests | {{total_tests}} |
| Passed | {{passed}} ({{pass_rate}}%) |
| Failed | {{failed}} |
| Skipped | {{skipped}} |
| Coverage | {{coverage}}% |

### Failure Breakdown

| Category | Count | Severity | Action |
|----------|-------|----------|--------|
| Test Bugs | {{test_bug_count}} | {{test_bug_severity}} | Fix tests |
| Code Bugs | {{code_bug_count}} | {{code_bug_severity}} | Fix code |
| Environment | {{env_count}} | {{env_severity}} | Fix setup |
| Flaky | {{flaky_count}} | {{flaky_severity}} | Stabilize |

---

## Detailed Failure Analysis

### Code Bugs (Fix These First)

{{#each code_bugs}}
#### {{this.test_name}}
- **File:** {{this.file_path}}:{{this.line}}
- **Error:** {{this.error_message}}
- **Root Cause:** {{this.root_cause}}
- **Severity:** {{this.severity}}
- **Suggested Fix:**
```
{{this.suggested_fix}}
```
{{/each}}

### Test Bugs

{{#each test_bugs}}
#### {{this.test_name}}
- **Test File:** {{this.test_file}}
- **Error:** {{this.error_message}}
- **Issue:** {{this.issue_description}}
- **Fix:** {{this.fix_description}}
{{/each}}

### Environment Issues

{{#each env_issues}}
#### {{this.test_name}}
- **Error:** {{this.error_message}}
- **Missing:** {{this.missing_dependency}}
- **Fix:** {{this.fix_steps}}
{{/each}}

### Flaky Tests

{{#each flaky_tests}}
#### {{this.test_name}}
- **Pattern:** {{this.flaky_pattern}}
- **Recommendation:** {{this.recommendation}}
{{/each}}

---

## Pattern Analysis

### Failure Clusters
{{failure_clusters}}

### Root Cause Summary
{{root_cause_summary}}

---

## Priority Fix Order

1. {{fix_priority_1}}
2. {{fix_priority_2}}
3. {{fix_priority_3}}
4. {{fix_priority_4}}

---

## Regression Risk

{{regression_risk_assessment}}
