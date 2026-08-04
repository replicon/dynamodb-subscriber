---
stepsCompleted: []
workflowType: 'coverage-report'
date: '{{date}}'
---

# Test Coverage Report

**Analyst:** Trent (Test Architect)
**Date:** {{date}}
**Coverage Target:** {{coverage_target}}%

---

## Overall Coverage

| Metric | Current | Target | Status |
|--------|---------|--------|--------|
| Line Coverage | {{line_coverage}}% | {{coverage_target}}% | {{line_status}} |
| Branch Coverage | {{branch_coverage}}% | {{coverage_target}}% | {{branch_status}} |
| Function Coverage | {{function_coverage}}% | {{coverage_target}}% | {{function_status}} |

## Coverage by Module

| Module | Lines | Branches | Functions | Risk Level |
|--------|-------|----------|-----------|------------|
{{module_coverage_table}}

## Critical Gaps (High Risk, Low Coverage)

{{#each critical_gaps}}
### {{this.file_path}}
- **Current Coverage:** {{this.coverage}}%
- **Risk:** {{this.risk_level}} — {{this.risk_reason}}
- **Uncovered Lines:** {{this.uncovered_lines}}
- **Recommended Tests:**
  {{this.recommended_tests}}
{{/each}}

## Coverage Improvement Plan

| Priority | Module | Current | Target | Tests Needed | Effort |
|----------|--------|---------|--------|-------------|--------|
{{improvement_plan_table}}

## Coverage Trend

{{coverage_trend}}

---

## Next Steps

1. {{next_step_1}}
2. {{next_step_2}}
3. {{next_step_3}}
