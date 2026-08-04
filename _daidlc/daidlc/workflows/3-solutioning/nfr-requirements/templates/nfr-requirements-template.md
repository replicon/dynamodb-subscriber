---
stepsCompleted: []
inputDocuments: []
workflowType: 'nfr-requirements'
project_name: '{{project_name}}'
date: '{{date}}'
---

# Non-Functional Requirements: {{project_name}}

**Author:** {{user_name}}
**Date:** {{date}}
**Status:** Draft

---

## Overview

This document defines the non-functional requirements (quality attributes) for {{project_name}}.
Each requirement has a unique identifier, measurable target, priority, and traceability to
functional requirements.

## Priority Legend

| Priority | Meaning |
|----------|---------|
| **Critical** | System cannot launch without meeting this |
| **High** | Must be met for production readiness |
| **Medium** | Should be met, can be deferred to next release |
| **Low** | Nice to have, optimize when resources allow |

---

## 1. Performance Requirements

### NFR-PERF-001: API Response Time
- **Target:** p50 < {{p50_target}}ms, p95 < {{p95_target}}ms, p99 < {{p99_target}}ms
- **Priority:** {{priority}}
- **Measurement:** Application Performance Monitoring (APM)
- **Related FRs:** {{related_frs}}

### NFR-PERF-002: Throughput
- **Target:** {{throughput_target}} requests/second sustained
- **Priority:** {{priority}}
- **Measurement:** Load testing with {{concurrent_users}} concurrent users
- **Related FRs:** {{related_frs}}

### NFR-PERF-003: Database Query Performance
- **Target:** All queries < {{query_target}}ms, no N+1 queries
- **Priority:** {{priority}}
- **Measurement:** Query profiling, slow query log
- **Related FRs:** {{related_frs}}

---

## 2. Security Requirements

### NFR-SEC-001: Authentication
- **Standard:** {{auth_standard}}
- **Target:** {{auth_target}}
- **Priority:** Critical
- **OWASP Reference:** A07:2021 - Identification and Authentication Failures
- **Related FRs:** {{related_frs}}

### NFR-SEC-002: Input Validation
- **Standard:** OWASP Input Validation Cheat Sheet
- **Target:** All user inputs validated and sanitized; parameterized queries for all database access
- **Priority:** Critical
- **OWASP Reference:** A03:2021 - Injection
- **Related FRs:** {{related_frs}}

### NFR-SEC-003: Data Encryption
- **Target:** TLS 1.2+ for transit; AES-256 for data at rest
- **Priority:** {{priority}}
- **OWASP Reference:** A02:2021 - Cryptographic Failures
- **Related FRs:** {{related_frs}}

### NFR-SEC-004: Authorization
- **Target:** {{authz_model}} with principle of least privilege
- **Priority:** Critical
- **OWASP Reference:** A01:2021 - Broken Access Control
- **Related FRs:** {{related_frs}}

---

## 3. Scalability Requirements

### NFR-SCALE-001: Horizontal Scaling
- **Target:** {{min_instances}} to {{max_instances}} instances, auto-scale at {{scale_trigger}}% CPU
- **Priority:** {{priority}}
- **Related FRs:** {{related_frs}}

### NFR-SCALE-002: Data Volume
- **Target:** Support {{data_volume}} records with < {{degradation_threshold}}% performance degradation
- **Priority:** {{priority}}
- **Related FRs:** {{related_frs}}

---

## 4. Reliability Requirements

### NFR-REL-001: Availability
- **Target:** {{uptime_sla}} uptime ({{downtime_budget}} downtime per month)
- **Priority:** {{priority}}
- **Related FRs:** {{related_frs}}

### NFR-REL-002: Recovery
- **Target:** RTO: {{rto}}, RPO: {{rpo}}
- **Priority:** {{priority}}
- **Related FRs:** {{related_frs}}

### NFR-REL-003: Data Durability
- **Target:** {{durability_target}}
- **Priority:** {{priority}}
- **Related FRs:** {{related_frs}}

---

## 5. Observability Requirements

### NFR-OBS-001: Logging
- **Target:** Structured JSON logging, correlation IDs on all requests
- **Priority:** {{priority}}
- **Related FRs:** {{related_frs}}

### NFR-OBS-002: Metrics
- **Target:** Application metrics (latency, error rate, throughput) with {{metrics_retention}} retention
- **Priority:** {{priority}}
- **Related FRs:** {{related_frs}}

### NFR-OBS-003: Alerting
- **Target:** Alerts fire within {{alert_latency}} of threshold breach, PagerDuty/Slack integration
- **Priority:** {{priority}}
- **Related FRs:** {{related_frs}}

---

## NFR Traceability Matrix

| NFR ID | Category | Priority | Related FRs | Status |
|--------|----------|----------|-------------|--------|
{{traceability_matrix}}

---

## Approval

- [ ] Performance targets reviewed and achievable
- [ ] Security requirements aligned with organizational standards
- [ ] Scalability targets match business growth projections
- [ ] Reliability targets match SLA commitments
- [ ] Observability requirements support operational needs
