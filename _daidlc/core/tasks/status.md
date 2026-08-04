# D-AIDLC Status Display

**Goal**: Show current project status in a clear, actionable format.

## Instructions

1. Load `aidlc-docs/aidlc-state.md`
2. Display status in this format:

```
D-AIDLC Project Status: {project_name}
================================================
Type: {project_type} | Governance: {governance_level}

INCEPTION PHASE
  [x] Workspace Detection          complete
  [x] Reverse Engineering          complete (brownfield) / skipped (greenfield)
  [x] Requirements Analysis        complete
  [ ] User Stories                  pending
  [ ] Workflow Planning             pending
  [ ] Application Design           pending
  [ ] NFR Requirements             pending
  [ ] NFR Design                   pending
  [ ] Infrastructure Design        pending

CONSTRUCTION PHASE
  [ ] Sprint Planning              pending
  [ ] Code Generation              pending
  [ ] Build & Test                 pending

TESTING PHASE
  [ ] Test Strategy                pending
  [ ] Test Generation              pending
  [ ] Test Execution               pending
  [ ] Coverage Report              pending
  [ ] Regression Detection         pending

COMPLIANCE (optional)
  [ ] Security Review              pending
  [ ] Compliance Check             pending
  [ ] Tech Stack Validation        pending

GOVERNANCE
  Audit entries: {count}
  Stage gates passed: {count}
  Questions pending: {count}

Current Stage: {current_stage}
Next Action: {recommended_next_action}
================================================
```

3. Highlight the current stage with an arrow (-->)
4. Show any blocked items with reasons
5. Recommend the next action
