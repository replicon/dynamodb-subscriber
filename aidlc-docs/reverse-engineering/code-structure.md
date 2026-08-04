# Code Structure
**Project:** `@replicon/dynamodb-subscriber` v1.7.2
**Confidence:** High
**Generated:** 2026-08-04

---

## Directory Tree

```
dynamodb-subscriber/
│
├── index.js                     # [MAIN] 251 lines — entire library implementation
│   ├── require block (lines 1–9)
│   ├── class DynamodDBSubscriber extends EventEmitter (lines 13–226)
│   │   ├── constructor(params)          lines 14–42
│   │   ├── _getOpenShards(callback)     lines 44–93
│   │   ├── _process(job)                lines 95–159
│   │   ├── start()                      lines 161–221
│   │   └── stop()                       lines 223–225
│   ├── class DynamodDBReadable extends Readable (lines 230–248)
│   │   ├── constructor(options)         lines 231–243
│   │   └── _read()                      lines 245–247
│   └── module.exports (lines 250–251)
│
├── test/
│   ├── dsubscriber.tests.js     # Integration-style tests (172 lines)
│   │   ├── describe: subscribing to stream with 1 record (ARN)
│   │   └── describe: subscribing by tablename + TrimmedDataAccessException recovery
│   └── internal.tests.js        # Unit test for _getOpenShards() (45 lines)
│
├── package.json                 # npm manifest — published to @replicon scope
├── README.md                    # Usage documentation
├── LICENSE                      # MIT license
├── .gitignore                   # ⚠ Includes 'test*.js' — see note below
├── .npmignore                   # Excludes test/, .github/, .idea/ from published package
└── .jshintrc                    # JSHint linter config (legacy — camelcase disabled, esnext enabled)
```

---

## Naming Conventions

### Dominant Patterns

| Scope | Convention | Examples |
|-------|-----------|---------|
| Files | `kebab-case` | `index.js`, `dsubscriber.tests.js`, `internal.tests.js` |
| Classes | `PascalCase` | `DynamodDBSubscriber`, `DynamodDBReadable` |
| Public methods | `camelCase` | `start()`, `stop()` |
| Private methods | `_camelCase` | `_getOpenShards()`, `_process()` |
| Private fields | `_camelCase` | `_region`, `_table`, `_streamArn`, `_shards`, `_job`, `_ddbStream`, `_endpoint`, `_interval` |
| Event names | `lowercase string` | `'record'`, `'error'` |
| Variables | `camelCase` | `shards`, `openShards`, `shard`, `callback` |

### Convention Violations / Inconsistencies

| Issue | Location | Severity |
|-------|----------|---------|
| **Typo: "DynamodDB" vs "DynamoDB"** | Class names throughout | Medium — misleading but internally consistent |
| **Typo in debug namespace** | `index.js:11` `'DynamodDBSubscriber'` | Low |
| `LastEvaluatedShardId` is declared with `var` instead of `const`/`let` | `index.js:46` | Low |
| Mix of `var` and `const`/`let` | Multiple locations | Low |

---

## Entry Points

| Entry Point | Export | Description |
|------------|--------|-------------|
| `require('dynamodb-subscriber')` | `DynamodDBSubscriber` class | `index.js:250` — Main EventEmitter subscriber |
| `require('dynamodb-subscriber').Stream` | `DynamodDBReadable` class | `index.js:251` — Readable stream adapter |

---

## Module Organization Pattern

This library uses the **Single File Module** pattern — a common pattern for small libraries. All implementation is in `index.js`. There is no `src/`, no `lib/`, no split by responsibility.

**Trade-off:** Acceptable for a library this small. Would become unmaintainable above ~500 lines or if adding new stream types, retry strategies, or metrics hooks.

---

## Environment Variables

The library **does not directly reference any environment variables**. AWS credentials and region are handled by the AWS SDK's default credential chain, which reads from:

| Env Var | Handled By | Purpose |
|---------|-----------|---------|
| `AWS_ACCESS_KEY_ID` | AWS SDK v3 (implicit) | AWS credential |
| `AWS_SECRET_ACCESS_KEY` | AWS SDK v3 (implicit) | AWS credential |
| `AWS_SESSION_TOKEN` | AWS SDK v3 (implicit) | For temporary credentials |
| `AWS_REGION` | AWS SDK v3 (implicit) | Default region if not passed in `params.region` |
| `DEBUG` | `debug` package (implicit) | Set to `DynamodDBSubscriber` to enable debug output |

---

## Dead Code Analysis

| Finding | Evidence | Verdict |
|---------|----------|---------|
| `aws-sdk-client-mock` dev dep | Listed in `package.json:29` but not imported in any test file | **Dead dependency** — installed but unused |
| `DynamodDBReadable` | Exported at `index.js:251`, used in README examples | **Active** |
| All `DynamodDBSubscriber` methods | Tested or exercised in startup flow | **Active** |

No orphaned source files or dead exports detected in the source. The dead item is an unused dev dependency.

---

## `.gitignore` Anomaly

> **WARNING**: `test*.js` is listed at line 44 of `.gitignore`. This pattern would exclude files named `test*.js` in the **root directory** from git tracking. The test files are in `test/` (a subdirectory) so they are currently tracked — but any test file created at the root level would be silently ignored. This line appears to be a leftover from the gitignore.io generator and should be reviewed.

---

## Deep-Dive: Complexity Metrics Per Function

| Function | Lines | Nesting Depth | Cyclomatic Complexity | Rating |
|----------|-------|---------------|----------------------|--------|
| `constructor` | 28 (14–42) | 2 | ~3 | Good |
| `_getOpenShards` | 49 (44–93) | **5** | **~7** | **Poor** — callback hell |
| `_process` | 64 (95–159) | **4** | **~8** | **Poor** — complex branching |
| `start` | 60 (161–221) | 3 | ~5 | Fair |
| `stop` | 2 (223–225) | 1 | 1 | Excellent |
| `DynamodDBReadable constructor` | 12 (231–243) | 2 | ~2 | Good |
| `DynamodDBReadable._read` | 3 (245–247) | 1 | 1 | Excellent |

**Hotspots:** `_process()` and `_getOpenShards()` are the two functions that contain all the logic complexity, all the known bugs (REL-01, REL-06), and are the least testable in isolation. Both are candidates for extraction and refactoring.

## Deep-Dive: Variable Shadowing

**Finding:** `err` is shadowed inside `_getOpenShards` callback chains:

```javascript
// outer callback
async.doWhilst((cb) => {
  this._ddbStream.describeStream({...}, (err, data) => {  // ← first `err`
    async.map(openShards, (shard, cb) => {
      this._ddbStream.getShardIterator({...}, (err, data) => {  // ← shadows outer err
```

Both the outer `describeStream` error and the inner `getShardIterator` error are named `err`. JavaScript closures handle this correctly (inner `err` shadows outer `err`), but it is a readability hazard. Similarly, `data` is shadowed at the same levels.

In `_process`:
```javascript
this._getOpenShards((err, shards) => {  // err shadows outer scope err
```

**Recommendation:** Use distinct names: `describeErr`, `iteratorErr`, `recordsErr`.

## Deep-Dive: `_process` Accesses `this._shards` After `delete`

**Finding (race condition):**

At `index.js:147`: `delete this._shards`
At `index.js:148–155`: `_getOpenShards((err, shards) => { this._shards = shards; this._process(job); })`

Between the `delete` and the re-assignment, `this._shards` is `undefined`. The scheduler calls `_process` at each interval via `job.done()` — but `job.done()` is NOT called until the re-assignment completes (step 3 at line 154 calls `this._process(job)` directly). So the scheduler does not fire a second concurrent `_process` during the re-fetch window.

**Assessment:** Safe — the scheduler's next firing is blocked because `job.done()` hasn't been called yet. Not a real race condition with the current `tempus-fugit` implementation, but this is an implicit invariant that is not documented and would break if the scheduler were replaced with a naive `setInterval`.

## Techniques Applied

- Entry Point Discovery (#5): Mapped all exported symbols
- Dead Code Detection (#21): Identified unused dev dependency
- Naming Convention Audit (#25): Documented all naming patterns and violations
- Environment Variable Mapping (#33): Confirmed no direct env var usage
- Build System Analysis (#34): Confirmed no build step (raw CommonJS, no transpilation)
- Complexity Metrics Scan (#23) [deep-dive]: Per-function complexity table
- Async Flow Analysis (#48) [deep-dive]: Variable shadowing and delete/re-assign race analysis
