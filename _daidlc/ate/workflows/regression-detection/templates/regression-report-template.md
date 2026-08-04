---
stepsCompleted: []
workflowType: 'regression-detection'
date: '{{date}}'
---

# Regression Detection Report

**Analyst:** Trent (Test Architect)
**Date:** {{date}}
**Compared:** {{current_run}} vs {{previous_run}}

---

## Summary

| Category | Count |
|----------|-------|
| Regressions (new failures) | {{regression_count}} |
| Improvements (new passes) | {{improvement_count}} |
| Persistent failures | {{persistent_count}} |
| Stable (still passing) | {{stable_count}} |
| New tests added | {{new_test_count}} |
| Tests removed | {{removed_test_count}} |

## Quality Trend

| Metric | Previous | Current | Trend |
|--------|----------|---------|-------|
| Pass Rate | {{prev_pass_rate}}% | {{curr_pass_rate}}% | {{pass_trend}} |
| Coverage | {{prev_coverage}}% | {{curr_coverage}}% | {{coverage_trend}} |
| Test Count | {{prev_count}} | {{curr_count}} | {{count_trend}} |
| Suite Duration | {{prev_duration}} | {{curr_duration}} | {{duration_trend}} |

**Assessment:** {{quality_assessment}}

---

## Regressions (Action Required)

{{#each regressions}}
### {{this.test_name}}
- **Status:** Was PASSING, now FAILING
- **Error:** {{this.error_message}}
- **Likely Cause:** {{this.likely_cause}}
- **Affected File:** {{this.affected_file}}
- **Severity:** {{this.severity}}
- **Recommended Fix:** {{this.fix_recommendation}}
{{/each}}

## Improvements

{{#each improvements}}
- **{{this.test_name}}** — Was failing, now passing
{{/each}}

## Persistent Failures

{{#each persistent}}
- **{{this.test_name}}** — Still failing ({{this.failing_since}})
{{/each}}

---

## Action Items

1. {{action_1}}
2. {{action_2}}
3. {{action_3}}
