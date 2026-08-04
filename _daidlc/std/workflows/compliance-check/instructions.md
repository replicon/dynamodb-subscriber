# Compliance Check

**Goal**: Validate the project against selected regulatory compliance standards.

## Step 1: Identify Applicable Standards

Load the compliance_standards configuration. For each selected standard, load the corresponding checklist.

## Step 2: GDPR Compliance (if selected)

Check against GDPR requirements:
- **Data Inventory**: Is there a record of all personal data processing?
- **Consent Management**: Is user consent obtained and recorded?
- **Right to Access**: Can users export their personal data?
- **Right to Erasure**: Can users request data deletion?
- **Data Minimization**: Is only necessary data collected?
- **Data Protection**: Is personal data encrypted at rest and in transit?
- **Breach Notification**: Is there a breach notification process?
- **DPO**: Is a Data Protection Officer designated (if required)?
- **Privacy by Design**: Are privacy controls built into the architecture?
- **Cross-Border Transfer**: Are data transfer mechanisms in place?

## Step 3: SOC 2 Compliance (if selected)

Check against SOC 2 Trust Service Criteria:
- **Security**: Access controls, network security, encryption
- **Availability**: Uptime commitments, disaster recovery, monitoring
- **Processing Integrity**: Data validation, error handling, quality assurance
- **Confidentiality**: Data classification, access restrictions, encryption
- **Privacy**: Notice, consent, collection, retention, disposal

## Step 4: HIPAA Compliance (if selected)

Check against HIPAA requirements:
- **PHI Identification**: Is Protected Health Information identified and classified?
- **Access Controls**: Is PHI access role-based with minimum necessary?
- **Audit Controls**: Are all PHI accesses logged?
- **Transmission Security**: Is PHI encrypted in transit?
- **Integrity Controls**: Are there mechanisms to prevent unauthorized PHI alteration?
- **BAA**: Are Business Associate Agreements in place?
- **Breach Notification**: Is there a breach notification process?
- **Risk Assessment**: Is a risk assessment documented?

## Step 5: Generate Compliance Report

For each standard:
- List of requirements checked
- Status per requirement (Compliant / Non-Compliant / Partial / N/A)
- Evidence references for compliant items
- Remediation steps for non-compliant items
- Overall compliance score

Write to: {default_output_file}
Log to audit trail
