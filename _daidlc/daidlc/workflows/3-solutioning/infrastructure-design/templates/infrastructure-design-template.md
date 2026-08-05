---
stepsCompleted: []
inputDocuments: []
workflowType: 'infrastructure-design'
project_name: '{{project_name}}'
date: '{{date}}'
---

# Infrastructure Design: {{project_name}}

**Author:** {{user_name}}
**Date:** {{date}}
**Status:** Draft

---

## 1. Service Topology

### Architecture Overview

```mermaid
{{service_topology_diagram}}
```

### Service Inventory

| Service | Type | Platform | Network Zone | Port | Dependencies |
|---------|------|----------|-------------|------|--------------|
{{service_inventory_table}}

### Inter-Service Communication

| From | To | Protocol | Auth | Pattern |
|------|----|----------|------|---------|
{{communication_table}}

---

## 2. Data Infrastructure

### Databases

| Database | Engine | Purpose | Size Estimate | Replication | Backup |
|----------|--------|---------|--------------|-------------|--------|
{{database_table}}

### Cache Layer

| Cache | Engine | Mode | Max Memory | Eviction | Persistence |
|-------|--------|------|-----------|----------|-------------|
{{cache_table}}

### Message Queues

| Queue | Engine | Purpose | Partitions | Consumers | DLQ |
|-------|--------|---------|-----------|-----------|-----|
{{queue_table}}

### Object Storage

| Bucket | Purpose | Access | Lifecycle | Encryption |
|--------|---------|--------|-----------|------------|
{{storage_table}}

---

## 3. CI/CD Pipeline

### Pipeline Flow

```mermaid
{{cicd_pipeline_diagram}}
```

### Pipeline Stages

| Stage | Tools | Duration Target | Gate Criteria |
|-------|-------|----------------|---------------|
| Source | {{source_tools}} | — | Branch policy |
| Build | {{build_tools}} | < {{build_target}} | Compile success |
| Unit Test | {{test_tools}} | < {{unit_test_target}} | 100% pass, > {{coverage_target}}% coverage |
| Integration Test | {{integration_tools}} | < {{integration_target}} | All pass |
| Security Scan | {{security_tools}} | < {{scan_target}} | No critical/high findings |
| Deploy Staging | {{deploy_tools}} | < {{deploy_target}} | Health checks pass |
| E2E Test | {{e2e_tools}} | < {{e2e_target}} | All scenarios pass |
| Deploy Production | {{deploy_tools}} | < {{deploy_target}} | Approval + health checks |

### Rollback Strategy

{{rollback_strategy}}

---

## 4. Environment Matrix

| Aspect | Development | Staging | Production |
|--------|-------------|---------|------------|
| **Purpose** | {{dev_purpose}} | {{staging_purpose}} | {{prod_purpose}} |
| **Scale** | {{dev_scale}} | {{staging_scale}} | {{prod_scale}} |
| **Data** | {{dev_data}} | {{staging_data}} | {{prod_data}} |
| **Access** | {{dev_access}} | {{staging_access}} | {{prod_access}} |
| **Monitoring** | {{dev_monitoring}} | {{staging_monitoring}} | {{prod_monitoring}} |
| **Cost Estimate** | {{dev_cost}} | {{staging_cost}} | {{prod_cost}} |

---

## 5. Infrastructure as Code

- **Tool:** {{iac_tool}}
- **Module Structure:** {{module_structure}}
- **State Management:** {{state_management}}
- **Secret Injection:** {{secret_injection}}
- **Environment Variables:** {{env_var_management}}

---

## 6. Cost Estimate

| Component | Monthly Cost (Dev) | Monthly Cost (Staging) | Monthly Cost (Prod) |
|-----------|--------------------|----------------------|---------------------|
{{cost_table}}

**Total Monthly Estimate:** {{total_monthly}}

---

## Approval

- [ ] Service topology reviewed and approved
- [ ] Data infrastructure meets NFR requirements
- [ ] CI/CD pipeline covers all quality gates
- [ ] Environment matrix is complete
- [ ] Cost estimates are within budget
