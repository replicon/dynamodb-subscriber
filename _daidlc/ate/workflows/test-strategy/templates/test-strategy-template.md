---
stepsCompleted: []
inputDocuments: []
workflowType: 'test-strategy'
project_name: '{{project_name}}'
date: '{{date}}'
---

# Test Strategy: {{project_name}}

**Author:** Trent (Test Architect)
**Date:** {{date}}
**Status:** Draft

---

## 1. Test Ecosystem

| Aspect | Value |
|--------|-------|
| Language | {{language}} |
| Framework | {{framework}} |
| Unit Test Framework | {{unit_test_framework}} |
| Integration Test Framework | {{integration_test_framework}} |
| E2E Test Framework | {{e2e_test_framework}} |
| Coverage Tool | {{coverage_tool}} |
| CI Platform | {{ci_platform}} |

## 2. Test Pyramid

```
        /  E2E  \           {{e2e_percentage}}% - Critical user journeys
       /----------\
      / Integration \       {{integration_percentage}}% - API & service interactions
     /----------------\
    /    Unit Tests     \   {{unit_percentage}}% - Functions & components
   /--------------------\
```

### Coverage Targets

| Level | Coverage Target | Speed Target | Test Count Estimate |
|-------|----------------|-------------|-------------------|
| Unit | {{unit_coverage}}% | < 10ms/test | {{unit_count}} |
| Integration | {{integration_coverage}}% | < 500ms/test | {{integration_count}} |
| E2E | Critical paths | < 30s/test | {{e2e_count}} |
| **Overall** | **{{overall_coverage}}%** | **< {{suite_time}}** | **{{total_count}}** |

## 3. Feature Test Map

| Feature/Module | Unit | Integration | E2E | Priority |
|---------------|------|-------------|-----|----------|
{{feature_test_map}}

## 4. Test Infrastructure

### Test Database
{{test_database_strategy}}

### Mocking Strategy
{{mocking_strategy}}

### Test Fixtures
{{fixture_strategy}}

### CI Configuration
{{ci_test_config}}

## 5. Test Quality Standards

### Naming Convention
{{naming_convention}}

### Assertion Rules
- One logical assertion per test
- Use descriptive assertion messages
- Prefer specific assertions over generic ones

### Isolation Rules
- No shared mutable state between tests
- Each test sets up and tears down its own data
- Tests must pass when run in any order

### Flaky Test Policy
{{flaky_test_policy}}

### Performance Budget
- Full unit suite: < {{unit_suite_time}}
- Full integration suite: < {{integration_suite_time}}
- Full E2E suite: < {{e2e_suite_time}}

## 6. Test Data Strategy

{{test_data_strategy}}

---

## Approval

- [ ] Test pyramid ratios appropriate for project type
- [ ] Coverage targets are achievable and meaningful
- [ ] Feature test map covers all critical paths
- [ ] Test infrastructure is feasible with current tech stack
- [ ] Quality standards are clear and enforceable
