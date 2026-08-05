# Test Generation Validation Checklist

## Framework Compliance
- [ ] Tests use the project's existing test framework
- [ ] Test file naming follows project conventions
- [ ] Test directory structure matches project layout
- [ ] No custom test utilities introduced

## Unit Test Quality
- [ ] Each function has happy path test(s)
- [ ] Edge cases covered (null, empty, boundary)
- [ ] Error paths tested (exceptions, error returns)
- [ ] External dependencies are mocked
- [ ] One logical assertion per test
- [ ] Descriptive test names that explain behavior

## Integration Test Quality
- [ ] API endpoints tested with correct status codes
- [ ] Response body structure validated
- [ ] Auth enforcement tested (401/403 for protected routes)
- [ ] Input validation tested (400 for bad input)
- [ ] Database operations verified

## E2E Test Quality
- [ ] Critical user journeys covered
- [ ] Semantic locators used (not CSS selectors)
- [ ] Proper waits used (not hardcoded timeouts)
- [ ] Tests are independent and can run in any order

## Report
- [ ] All generated files listed with paths
- [ ] Test counts by type are accurate
- [ ] Coverage estimate provided
