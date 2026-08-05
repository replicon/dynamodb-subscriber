# Dependencies
**Project:** `@replicon/dynamodb-subscriber` v1.7.2
**Confidence:** High
**Generated:** 2026-08-04

---

## Runtime Dependencies

| Package | Pinned Version | Latest Stable | Status | Severity |
|---------|--------------|--------------|--------|---------|
| `@aws-sdk/client-dynamodb` | `^3.0.0` | 3.x (current) | Current | None |
| `@aws-sdk/client-dynamodb-streams` | `^3.0.0` | 3.x (current) | Current | None |
| `@aws-sdk/util-dynamodb` | `^3.0.0` | 3.x (current) | Current | None |
| `async` | `^2.6.4` | 3.3.x | Outdated (1 major) | Low |
| `debug` | `^2.6.8` | 4.4.x | Outdated (2 majors) | Medium |
| `ms` | `^2.0.0` | 2.1.x | Current | None |
| `tempus-fugit` | `~2.3.1` | 2.x (last pub ~2015) | Unmaintained | Medium |

---

## Dev Dependencies

| Package | Pinned Version | Latest Stable | Status | Notes |
|---------|--------------|--------------|--------|-------|
| `aws-sdk-client-mock` | `^4.0.0` | 4.x (current) | Current | **Unused** — installed but never imported in tests |
| `chai` | `~3.5.0` | 5.x | Outdated (2 majors) | Low risk (test only) |
| `mocha` | `~2.2.5` | 10.x | Outdated (8 majors) | Medium risk — async behavior differences |
| `sinon` | `^21.0.1` | 21.x (current) | Current | None |

---

## Risk Table

### `debug` v2.6.8 → v4.4.x (Medium)

**Current:** v2.6.8 | **Latest:** v4.4.x
**Gap:** 2 major versions behind
**Risk:** v2.x and v3.x had various bugs around color detection and formatting. v4 (released 2019) included memory improvements and better formatting. No known critical CVEs in v2, but the package is considered legacy.
**Recommendation:** Upgrade to `debug@^4.0.0` — the API is backwards-compatible for basic usage (`require('debug')('namespace')`).

---

### `tempus-fugit` v2.3.1 (Medium)

**Current:** v2.3.1 | **Last publish:** circa 2015–2016
**Risk:** No updates in 8+ years. If a Node.js version introduces a breaking change in timer behavior, there is no maintainer to patch it. No known CVEs. The functionality (`schedule`, `job.cancel()`) is simple enough that it's low risk operationally, but the maintenance posture is concerning.
**Recommendation:** Consider replacing with a simple `setInterval`/`clearInterval` wrapper or a maintained alternative like `node-cron` (if cron-style) or just `setTimeout` recursion with an internal flag.

---

### `async` v2.6.4 → v3.3.x (Low)

**Current:** v2.6.4 | **Latest:** v3.3.x
**Gap:** 1 major version
**Breaking changes in v3:** `async.series`, `async.each`, `async.map`, `async.doWhilst` all gained Promise support in v3, but the callback-style API was preserved. The v3 migration guide lists minimal breaking changes for callback-style usage.
**Risk:** Low — v2 is still maintained for security patches. Upgrading to v3 would be low-risk with the existing callback patterns.

---

### `mocha` v2.2.5 → v10.x (Medium — test only)

**Current:** v2.2.5 | **Latest:** v10.x
**Gap:** 8 major versions behind (2015 → present)
**Risk (test quality):** v2 predates `--exit` flag, async/timeout improvements, and proper ES module handling. Tests may pass erroneously or hang on modern Node.js versions if timer behavior changes.
**Risk (production):** None — dev dependency only.
**Recommendation:** Upgrade to `mocha@^10.0.0`. Some test structure changes may be needed (`before`/`after` scope changes in v6+).

---

### `chai` v3.5.0 → v5.x (Low — test only)

**Current:** v3.5.0 | **Latest:** v5.x
**Risk:** Very low operationally. v4 added minor assertion improvements; v5 added TypeScript support. No known breaking changes for the assertion methods used in this project (`assert.property`, `assert.deepEqual`, `assert.equal`, `assert.isOk`).

---

## External Service Dependencies

| Service | Interaction | Failure Impact |
|---------|------------|----------------|
| **AWS DynamoDB** | `DescribeTable` — optional ARN resolution | Fails gracefully: silently skips to `ListStreams` path (`index.js:174–179`) |
| **AWS DynamoDB Streams** | `DescribeStream`, `GetShardIterator`, `GetRecords` — core functionality | Hard failure: emits `'error'` event, stops polling |
| **AWS IAM** | Credential chain — resolved by AWS SDK | Hard failure if credentials are absent or insufficient |

**No other external APIs, databases, caches, or message queues are dependencies.**

---

## License Analysis

| Package | License |
|---------|---------|
| This library | MIT (per `LICENSE` file and README) |
| `@aws-sdk/*` | Apache 2.0 — permissive, compatible with MIT |
| `async` | MIT |
| `debug` | MIT |
| `ms` | MIT |
| `tempus-fugit` | MIT (verify — small package) |
| `mocha` | MIT |
| `chai` | MIT |
| `sinon` | BSD-3-Clause |
| `aws-sdk-client-mock` | MIT |

**No license conflicts detected.** All dependencies are permissively licensed and compatible with MIT distribution.

---

## Unused Dependency

> **`aws-sdk-client-mock` is installed but never used.**
>
> It appears in `package.json` `devDependencies` at `^4.0.0`, but neither `test/dsubscriber.tests.js` nor `test/internal.tests.js` imports or references it. The tests use `sinon.stub` on the SDK prototype directly instead.
>
> **Recommendation:** Remove from `devDependencies` or migrate tests to use `aws-sdk-client-mock` for more idiomatic AWS SDK v3 mocking (prototype stubbing is fragile against SDK internal changes).

---

## Deep-Dive: Sinon Prototype Stubbing vs. `aws-sdk-client-mock`

The tests use `sinon.stub(DynamoDBStreams.prototype, 'describeStream')` — directly stubbing the class prototype. This approach is fragile for AWS SDK v3 specifically:

- AWS SDK v3 clients use modular `Command` objects internally, not simple method calls on the class prototype. The SDK's `send()` method dispatches `Command` instances. Prototype stubbing works only because the tests happen to target the high-level client methods — but if the SDK changes its internal dispatch in a future v3 patch, the stubs could silently stop intercepting.
- `aws-sdk-client-mock` is the officially recommended approach for AWS SDK v3 — it intercepts at the `send()` middleware layer, which is stable across patches.

**Recommendation:** Replace sinon prototype stubs with `aws-sdk-client-mock`. The library is already installed (`package.json:29`).

## Deep-Dive: Transitive Dependency Risk Surface

Key transitive dependencies introduced by runtime deps:

| Runtime Dep | Major Transitive Deps | Risk |
|------------|----------------------|------|
| `@aws-sdk/client-dynamodb` | `@aws-sdk/middleware-*`, `@smithy/*` | Low — AWS-controlled, regularly updated |
| `@aws-sdk/client-dynamodb-streams` | Same smithy middleware stack | Low |
| `async` v2.x | `lodash` (partial) | Low — lodash is stable |
| `tempus-fugit` | None significant | Low — tiny package |
| `debug` | `ms` (shared with direct dep) | Low |

No transitive dependency introduces a conflicting version of `ms` — both `debug` and the direct `ms` dep resolve to the same major. No known transitive CVEs in the runtime dependency tree at current pinned versions.

## Deep-Dive: Sinon `before()`/`afterEach()` Stub Lifecycle Bug

**Finding:** In both test files, stubs are created in `before()` (runs once before the suite) but `sinon.restore()` is called in `afterEach()` (runs after each test). This means:

- After the first test completes, `sinon.restore()` removes the stubs that `before()` set up
- The second test runs without any stubs — making raw AWS SDK calls (which fail in a test environment without real credentials)

In practice, each `describe` block in `dsubscriber.tests.js` has only one `it()` test, so this bug is dormant — but adding a second test to any existing `describe` block would cause the second test to fail mysteriously.

**Fix:** Move stubs from `before()` to `beforeEach()`, or remove the `sinon.restore()` from `afterEach()` and rely only on the `after()` cleanup.

## Techniques Applied

- Dependency Vulnerability Check (#20): Assessed each package for version currency and known risk
- Third-Party SDK Inventory (#38): Catalogued all AWS SDK dependencies and their call sites
- External API Dependency Mapping (#35): Mapped outbound AWS service calls and failure modes
- Dead Code Detection (#21): Identified unused `aws-sdk-client-mock` dev dependency
- Code Duplication Analysis (#22) [deep-dive]: Identified sinon stub lifecycle bug in test structure
