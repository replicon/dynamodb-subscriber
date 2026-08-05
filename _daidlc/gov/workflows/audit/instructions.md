# D-AIDLC Audit Trail Management

**Goal**: Maintain a complete, timestamped audit trail of all D-AIDLC workflow activities.

## Audit Entry Format

Every audit entry MUST follow this exact format:

```markdown
### [ISO-8601-TIMESTAMP] | [PHASE] | [STAGE] | [ACTION]

- **Agent:** [Agent Name]
- **User Input:** [Complete user input, never summarized]
- **Action Taken:** [What the agent did]
- **Artifacts Created/Modified:** [List of file paths]
- **Approval Status:** pending | approved | rejected | override
- **Notes:** [Any additional context]

---
```

## Automatic Logging Rules

### When to Log (MANDATORY)

1. **Stage Start** — When any workflow stage begins execution
2. **Stage Complete** — When any workflow stage completes
3. **Artifact Creation** — When any document is created or significantly modified
4. **Question File Created** — When a questions file is created for user
5. **User Approval** — When user approves or rejects a stage
6. **Stage Skip** — When a stage is skipped (with override reason)
7. **Error/Rollback** — When an error occurs or work is rolled back

### How to Log

1. Read the current audit file (create if it doesn't exist)
2. Append the new entry at the end
3. Never modify existing entries
4. Never summarize user input — capture it complete

## Querying the Audit Trail

When user requests audit information:

### Step 1: Load Audit File
- Read `{audit_file}`
- If not found, report "No audit trail exists yet"

### Step 2: Filter and Present
Based on user request, filter entries by:
- Date range
- Phase (inception, construction, testing)
- Stage (requirements, architecture, etc.)
- Agent (which agent performed the action)
- Approval status (pending, approved, rejected)

### Step 3: Summary
Present a summary with:
- Total entries matching filter
- Timeline of actions
- Any pending approvals
- Any overrides used
