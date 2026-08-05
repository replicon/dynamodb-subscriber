# Technology Stack
**Project:** `@replicon/dynamodb-subscriber` v1.7.2
**Confidence:** High
**Generated:** 2026-08-04

---

## Language & Runtime

| Technology | Version | Notes |
|-----------|---------|-------|
| **JavaScript** | ES2015+ (ES6) | Uses `class`, arrow functions, `const`/`let`, destructuring, template literals |
| **Node.js** | Not explicitly specified | AWS SDK v3 requires Node.js ≥14.x; `async` v2.6.x supports Node.js ≥0.12. Effective minimum: **Node.js 14** |
| **Module system** | CommonJS (`require`/`module.exports`) | No ESM (`import`/`export`). No transpilation step. |

---

## Core Runtime Dependencies

| Package | Version Spec | Actual Role | Freshness |
|---------|-------------|------------|-----------|
| `@aws-sdk/client-dynamodb` | `^3.0.0` | DynamoDB `DescribeTable` + `ListStreams` for ARN resolution | **Current** — AWS SDK v3 is actively maintained |
| `@aws-sdk/client-dynamodb-streams` | `^3.0.0` | Core stream API: `DescribeStream`, `GetShardIterator`, `GetRecords` | **Current** |
| `@aws-sdk/util-dynamodb` | `^3.0.0` | `unmarshall()` — converts `AttributeValue` format to plain JS | **Current** |
| `async` | `^2.6.4` | Control flow: `series`, `doWhilst`, `each`, `map` | **Outdated** — v2.x (latest is v3.3.x). v2 still receives security patches but v3 has a cleaner API |
| `debug` | `^2.6.8` | Conditional debug logging via `DEBUG=DynamodDBSubscriber` | **Outdated** — v2.x (latest is v4.4.x). v4 added performance improvements and security fixes |
| `ms` | `^2.0.0` | Human-readable interval parsing (`'10s'` → `10000`) | **Current** — v2.x is the latest stable |
| `tempus-fugit` | `~2.3.1` | Periodic job scheduling | **Unmaintained risk** — small package, last published 2015–2016 era; no ESM support |

---

## Development Dependencies

| Package | Version Spec | Role | Freshness |
|---------|-------------|------|-----------|
| `mocha` | `~2.2.5` | Test runner | **Very outdated** — v2.x (2015); latest is v10.x. Missing async test improvements, `--exit` flag behavior differs |
| `chai` | `~3.5.0` | Assertion library | **Very outdated** — v3.x (2016); latest is v5.x |
| `sinon` | `^21.0.1` | Spies, stubs, mocks | **Current** |
| `aws-sdk-client-mock` | `^4.0.0` | AWS SDK v3 mock utilities | **Current** but **unused** — imported in `package.json` but not referenced in any test file |

---

## Build & Tooling

| Tool | Config File | Purpose | Notes |
|------|------------|---------|-------|
| **No bundler** | N/A | Zero build step — raw CommonJS shipped as-is | Library is published directly from source |
| **No transpiler** | N/A | No Babel, tsc, or SWC | Must maintain ES6 features that the target Node.js version supports natively |
| **JSHint** | `.jshintrc` | Static analysis / linting | **Legacy** — JSHint is largely superseded by ESLint. Config has `camelcase: false`, `curly: false`, `evil: true` (allows `eval`) |
| **npm scripts** | `package.json:13` | `"test": "mocha"` | Runs mocha against `test/` directory |
| **No CI/CD pipeline** | N/A | No GitHub Actions, no `.travis.yml` (excluded from `.npmignore` suggesting it existed historically) | `.travis.yml` is listed in `.npmignore` but no such file exists in the repo |

---

## Package Distribution

| Attribute | Value |
|-----------|-------|
| **Registry** | GitHub Packages (`https://npm.pkg.github.com`) — NOT the public npm registry |
| **Package name** | `@replicon/dynamodb-subscriber` |
| **Main entry** | `index.js` |
| **Files excluded from publish** | `test/`, `.github/`, `.gitmodules`, `.idea/`, `.travis.yml`, `.vscode/` (via `.npmignore`) |
| **Version** | `1.7.2` — indicates active use over time (not a prototype) |

---

## Infrastructure

No containerization, no cloud deployment, no database — this is a pure library, not a running service.

| Infrastructure | Present | Notes |
|---------------|---------|-------|
| Docker / containers | No | N/A for a library |
| Kubernetes | No | N/A |
| Database | No | Consumes DynamoDB via AWS SDK; does not own data |
| CI/CD | No active configuration | `.travis.yml` referenced in `.npmignore` but file doesn't exist — CI likely removed |
| Environment configuration | None | No `.env` files; AWS credentials handled by SDK credential chain |

---

## Stack Assessment

| Dimension | Rating | Notes |
|-----------|--------|-------|
| Language modernity | Medium | ES6 classes are fine; lack of async/await and Promises is dated |
| Runtime compatibility | Medium | No explicit `engines` field in `package.json` — consumers have no guidance on supported Node versions |
| Dependency freshness | Low | `mocha` and `chai` are 8+ years old; `debug` and `async` are one major version behind |
| Build simplicity | High | Zero-build is an advantage for a library of this size |
| Test infrastructure | Low | Mocha v2 is end-of-life; missing `aws-sdk-client-mock` usage despite being installed |

---

## Techniques Applied

- Build System Analysis (#34): Confirmed zero-build, CommonJS, npm scripts
- Environment Variable Mapping (#33): Confirmed zero direct env var usage; documented SDK-level vars
- CI/CD Pipeline Analysis (#31): Confirmed no active CI; historic Travis CI reference found
- Third-Party SDK Inventory (#38): Catalogued all AWS SDK usage
- Dependency Vulnerability Check (#20): Assessed each package for freshness and CVE risk
