# OWASP Top 10 (2021) Quick Reference

## A01:2021 - Broken Access Control
- [ ] Every protected endpoint has authentication middleware
- [ ] Authorization checks on every data access (not just UI)
- [ ] No direct object references without ownership verification
- [ ] CORS policy is restrictive (not wildcard)
- [ ] File upload restrictions enforce allowed types and sizes
- [ ] Rate limiting on authentication endpoints

## A02:2021 - Cryptographic Failures
- [ ] No hardcoded secrets, API keys, or passwords
- [ ] TLS 1.2+ enforced for all connections
- [ ] Passwords hashed with bcrypt/scrypt/argon2 (not MD5/SHA1)
- [ ] Sensitive data encrypted at rest (AES-256)
- [ ] No sensitive data in URLs or logs

## A03:2021 - Injection
- [ ] All SQL queries use parameterized statements
- [ ] User input sanitized before use in commands
- [ ] Template engines configured for auto-escaping
- [ ] No eval() or equivalent with user input

## A04:2021 - Insecure Design
- [ ] Rate limiting on all public endpoints
- [ ] Input validation on all user-supplied data
- [ ] Business logic tested for abuse scenarios
- [ ] Threat model documented for critical features

## A05:2021 - Security Misconfiguration
- [ ] No default credentials in production
- [ ] Error messages don't expose stack traces in production
- [ ] Unnecessary HTTP methods disabled
- [ ] Security headers set (CSP, HSTS, X-Frame-Options, etc.)

## A06:2021 - Vulnerable and Outdated Components
- [ ] No known CVEs in production dependencies
- [ ] Dependency update policy in place
- [ ] Automated vulnerability scanning in CI/CD

## A07:2021 - Identification and Authentication Failures
- [ ] Strong password policy enforced
- [ ] Brute force protection (lockout/delay)
- [ ] Session management is secure (HttpOnly, Secure, SameSite)
- [ ] Multi-factor authentication available for sensitive operations

## A08:2021 - Software and Data Integrity Failures
- [ ] Dependencies installed from trusted sources
- [ ] CI/CD pipeline has integrity checks
- [ ] No insecure deserialization of untrusted data

## A09:2021 - Security Logging and Monitoring Failures
- [ ] Authentication events logged (success and failure)
- [ ] Authorization failures logged
- [ ] Input validation failures logged
- [ ] No sensitive data in log entries

## A10:2021 - Server-Side Request Forgery (SSRF)
- [ ] URL inputs validated against allowlist
- [ ] Internal service endpoints not accessible from user input
- [ ] DNS resolution restricted for user-supplied URLs
