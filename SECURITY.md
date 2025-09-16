# Security Policy

## Supported Versions

We support security updates for the following versions:

| Version | Supported          |
| ------- | ------------------ |
| 1.x.x   | :white_check_mark: |
| < 1.0   | :x:                |

## Reporting a Vulnerability

The Noesis Connectors team takes security seriously. If you discover a security vulnerability, please report it privately.

### How to Report

**Do not report security vulnerabilities through public GitHub issues.**

Instead, please send an email to: **security@noesis.dev**

Include the following information:
- Description of the vulnerability
- Steps to reproduce the issue
- Potential impact
- Any suggested fixes (if you have them)

### What to Expect

1. **Acknowledgment**: We will acknowledge receipt of your report within 48 hours
2. **Assessment**: We will assess the vulnerability and determine severity
3. **Fix Development**: We will develop and test a fix
4. **Disclosure**: We will coordinate disclosure with you

### Timeline

- **Initial Response**: Within 48 hours
- **Status Update**: Within 7 days
- **Fix Timeline**: Varies by severity
  - Critical: 1-7 days
  - High: 7-14 days
  - Medium: 14-30 days
  - Low: 30-90 days

### Recognition

We appreciate security researchers who help keep our community safe. With your permission, we will:
- Credit you in our security advisory
- Include you in our security hall of fame
- Provide a reference letter upon request

## Security Best Practices

When developing connectors:

1. **Input Validation**: Always validate and sanitize inputs
2. **Authentication**: Use secure authentication mechanisms
3. **Secrets Management**: Never hardcode secrets or credentials
4. **Network Security**: Use TLS for all communications
5. **Dependencies**: Keep dependencies up to date
6. **Logging**: Avoid logging sensitive information

## Scope

This security policy applies to:
- Core API definitions
- All language SDKs
- Reference connector implementations
- Development tools and utilities
- Documentation examples

## Contact

For general security questions (not vulnerability reports):
- Open a GitHub Discussion
- Email: security@noesis.dev