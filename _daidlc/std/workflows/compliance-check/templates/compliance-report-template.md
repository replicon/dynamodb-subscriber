---
stepsCompleted: []
workflowType: 'compliance-check'
date: '{{date}}'
---

# Compliance Report

**Reviewer:** Clio (Compliance Officer)
**Date:** {{date}}
**Standards Checked:** {{standards_list}}

---

## Executive Summary

| Standard | Requirements | Compliant | Non-Compliant | Partial | Score |
|----------|-------------|-----------|---------------|---------|-------|
{{summary_table}}

---

{{#if gdpr_enabled}}
## GDPR Compliance

| # | Requirement | Status | Evidence | Remediation |
|---|-------------|--------|----------|-------------|
| 1 | Data Inventory | {{gdpr_1_status}} | {{gdpr_1_evidence}} | {{gdpr_1_remediation}} |
| 2 | Consent Management | {{gdpr_2_status}} | {{gdpr_2_evidence}} | {{gdpr_2_remediation}} |
| 3 | Right to Access | {{gdpr_3_status}} | {{gdpr_3_evidence}} | {{gdpr_3_remediation}} |
| 4 | Right to Erasure | {{gdpr_4_status}} | {{gdpr_4_evidence}} | {{gdpr_4_remediation}} |
| 5 | Data Minimization | {{gdpr_5_status}} | {{gdpr_5_evidence}} | {{gdpr_5_remediation}} |
| 6 | Data Protection | {{gdpr_6_status}} | {{gdpr_6_evidence}} | {{gdpr_6_remediation}} |
| 7 | Breach Notification | {{gdpr_7_status}} | {{gdpr_7_evidence}} | {{gdpr_7_remediation}} |
| 8 | Privacy by Design | {{gdpr_8_status}} | {{gdpr_8_evidence}} | {{gdpr_8_remediation}} |
| 9 | Cross-Border Transfer | {{gdpr_9_status}} | {{gdpr_9_evidence}} | {{gdpr_9_remediation}} |
{{/if}}

{{#if soc2_enabled}}
## SOC 2 Compliance

| # | Trust Criteria | Status | Evidence | Remediation |
|---|---------------|--------|----------|-------------|
{{soc2_table}}
{{/if}}

{{#if hipaa_enabled}}
## HIPAA Compliance

| # | Requirement | Status | Evidence | Remediation |
|---|-------------|--------|----------|-------------|
{{hipaa_table}}
{{/if}}

---

## Non-Compliant Items (Action Required)

{{non_compliant_items}}

## Remediation Priority

1. {{priority_1}}
2. {{priority_2}}
3. {{priority_3}}

---

## Next Review Date

{{next_review_date}}
