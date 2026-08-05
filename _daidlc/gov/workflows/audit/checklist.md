# Audit Trail Validation Checklist

## Entry Completeness
- [ ] Every entry has ISO 8601 timestamp
- [ ] Every entry has Phase identifier
- [ ] Every entry has Stage identifier
- [ ] Every entry has Action description
- [ ] Agent name is specified for every action
- [ ] User input is captured complete (never summarized)
- [ ] Artifacts list is accurate and complete
- [ ] Approval status is set correctly

## Trail Integrity
- [ ] Entries are in chronological order
- [ ] No existing entries have been modified
- [ ] No gaps in stage transition logging
- [ ] All stage skips have override reasons documented
- [ ] All approvals and rejections are logged

## Governance Compliance
- [ ] All question files referenced in audit exist
- [ ] All artifact files referenced in audit exist
- [ ] Stage gates are logged before stage transitions
- [ ] Error entries include resolution or escalation
