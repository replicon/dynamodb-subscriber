# Security Review Validation Checklist

## OWASP Coverage
- [ ] A01: Broken Access Control checked
- [ ] A02: Cryptographic Failures checked
- [ ] A03: Injection checked
- [ ] A04: Insecure Design checked
- [ ] A05: Security Misconfiguration checked
- [ ] A06: Vulnerable Components checked
- [ ] A07: Authentication Failures checked
- [ ] A08: Software and Data Integrity checked
- [ ] A09: Logging Failures checked
- [ ] A10: SSRF checked

## Finding Quality
- [ ] Every finding has file path and line number
- [ ] Every finding has severity assigned
- [ ] Every finding has specific remediation steps
- [ ] No generic "improve security" recommendations
- [ ] CWE references provided where applicable

## Report
- [ ] Summary counts are accurate
- [ ] Findings grouped by OWASP category
- [ ] Critical findings highlighted at top
- [ ] Dependency vulnerabilities included (if standard/deep scan)
