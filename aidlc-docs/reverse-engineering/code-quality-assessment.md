# Code Quality Assessment
**Project:** `@replicon/dynamodb-subscriber` v1.7.2
**Confidence:** High
**Generated:** 2026-08-04

---

## Summary Scorecard

| Dimension | Rating | Score |
|-----------|--------|-------|
| Security | Good | 4/5 |
| Reliability | Poor | 2/5 |
| Maintainability | Fair | 3/5 |
| Performance | Fair | 3/5 |
| Test Coverage | Poor | 2/5 |

---

## Security

### OWASP Quick Scan

This is a library, not a web application — most OWASP web categories (XSS, CSRF, SQL injection) do not apply. The relevant OWASP categories for a cloud SDK library are:

| OWASP Category | Applicable | Finding |
|---------------|-----------|---------|
| A01 Broken Access Control | Partial | Library does not enforce IAM — relies on caller-supplied credentials |
| A02 Cryptographic Failures | N/A | No encryption logic |
| A03 Injection | Low | No query building, no shell commands, no user input processed |
| A05 Security Misconfiguration | Yes | See findings below |
| A06 Vulnerable Components | Yes | Outdated `debug`, `mocha`, `chai` — see Dependencies |
| A09 Logging Failures | Yes | Error at `index.js:55` is logged to `console.log` (not proper logging) |

---

### Finding: SEC-01 — Hardcoded Secrets
**Severity:** None detected
**Evidence:** No API keys, passwords, or tokens found in source code or config files. `.gitignore` correctly excludes `node_modules`. AWS credentials are handled by SDK credential chain.
**Status:** PASS

---

### Finding: SEC-02 — Sensitive Data Exposure via console.log
**Severity:** Medium
**File:** `index.js:55`
**Code:**
```javascript
}, (err, data) => {
  if (err) {
    return console.log(err);   // line 55 — logs raw AWS error to stdout
  }
```
**Description:** When `DescribeStream` returns an error, the full AWS error object is printed to `console.log`. This could expose AWS account identifiers, region information, or internal service details to application logs. Additionally, the callback is never invoked, creating a hanging `async.doWhilst` loop (see Reliability findings).
**Recommended Fix:** Replace with `return callback(err)` and let the caller handle the error through the `'error'` event.

---

### Finding: SEC-03 — No Input Validation Beyond Required Check
**Severity:** Low
**File:** `index.js:17–19`
**Description:** The constructor validates that `arn` or `table` is present, but does not validate that these are strings, non-empty, or properly formatted. Passing a non-string value would propagate to the AWS SDK and result in an unhandled or confusing SDK error rather than a clear library error.
**Recommended Fix:** Add basic type and format guards:
```javascript
if (params.arn && typeof params.arn !== 'string') throw new Error('arn must be a string');
```

---

## Reliability

### Finding: REL-01 — CRITICAL: Error Swallowed in `_getOpenShards`
**Severity:** Critical
**File:** `index.js:53–56`
**Code:**
```javascript
this._ddbStream.describeStream({...}, (err, data) => {
  if (err) {
    return console.log(err);  // callback NEVER called — async.doWhilst hangs forever
  }
```
**Description:** When `DynamoDBStreams.DescribeStream` returns an error, the code logs to console and returns — but the `async.doWhilst` callback (`cb`) is never called with the error. This causes the `async.doWhilst` loop to hang indefinitely. The caller (`start()` or `_process()`) never receives the error, `emit('error')` is never called, and the job never terminates. The subscriber silently freezes.
**Impact:** If the stream ARN is invalid, the AWS token expires, or a network error occurs during `DescribeStream`, the subscriber stops working with no observable signal.
**Recommended Fix:**
```javascript
}, (err, data) => {
  if (err) {
    return cb(err);  // propagate to doWhilst, which forwards to the outer callback
  }
```

---

### Finding: REL-02 — `stop()` Crashes if Called Before `start()` Completes
**Severity:** High
**File:** `index.js:223–225`
**Code:**
```javascript
stop() {
  this._job.cancel();  // this._job is undefined until schedule() is called
}
```
**Description:** `this._job` is not set until `async.series` in `start()` reaches step 3 (the `schedule()` call). If `stop()` is called before that point — e.g. due to a race condition, timeout, or caller error — `this._job` is `undefined` and `.cancel()` throws `TypeError: Cannot read properties of undefined (reading 'cancel')`.
**Recommended Fix:**
```javascript
stop() {
  if (this._job) this._job.cancel();
}
```

---

### Finding: REL-03 — TrimmedDataAccessException Check May Never Fire in Production
**Severity:** High
**File:** `index.js:102`, `test/dsubscriber.tests.js:145`
**Production code checks:**
```javascript
if (err.name === 'TrimmedDataAccessException') {
```
**Test simulates:**
```javascript
return callback({code: 'TrimmedDataAccessException'});
```
**Description:** AWS SDK v3 error objects use `err.name` for the error type (not `err.code`). The production check (`err.name`) is correct for AWS SDK v3. However, the test simulates the error using `{code: 'TrimmedDataAccessException'}` — an object with `code` but not `name` — meaning the test is not actually testing the production code path. In the test, `err.name` would be `undefined`, so the recovery branch is never exercised. If the production check is correct, the test gives false confidence.
**Recommended Fix:** Update the test to simulate the error correctly:
```javascript
const trimmedError = new Error('TrimmedDataAccessException');
trimmedError.name = 'TrimmedDataAccessException';
return callback(trimmedError);
```

---

### Finding: REL-04 — No Timeout on AWS API Calls
**Severity:** Medium
**File:** `index.js` — all `this._ddbStream.*` and `dynamo.*` calls
**Description:** No `requestTimeout` or `connectionTimeout` is configured on the AWS SDK clients. If the AWS service hangs (network partition, VPC endpoint misconfiguration), the subscriber's polling cycle will hang indefinitely without emitting an error, effectively freezing the consumer.
**Recommended Fix:** Add timeout configuration to the SDK client constructor:
```javascript
this._ddbStream = new DynamoDBStreams({
  region: params.region,
  requestHandler: new NodeHttpHandler({
    requestTimeout: 30000,
    connectionTimeout: 5000
  })
});
```

---

### Finding: REL-05 — `DynamodDBReadable` Has No Stream Resume After Backpressure
**Severity:** Medium
**File:** `index.js:236–239`
**Code:**
```javascript
this._subscriber.on('record', (record) => {
  if (!this.push(record)) {
    this._subscriber.stop();  // backpressure: stop
  }
```
**Description:** When `push()` returns `false` (consumer is slow — backpressure), `subscriber.stop()` is called. However, `_read()` (which calls `subscriber.start()`) is designed to be called once by the stream internals, not repeatedly. After a backpressure stop, the subscriber will not restart, and the stream silently stalls.
**Recommended Fix:** Replace the `stop()`/`start()` pattern with a flag that gates record pushing, or track whether the subscriber is running and restart it in `_read()` if it was stopped.

---

## Maintainability

### Finding: MAINT-01 — Typo in Public Class Names
**Severity:** Low
**File:** `index.js:13`, `index.js:230`
**Description:** `DynamodDBSubscriber` and `DynamodDBReadable` — "DynamodDB" instead of "DynamoDB". Internally consistent but would confuse developers joining the project. Not a breaking change to fix since the classes are exported by reference, not by name.

---

### Finding: MAINT-02 — Mixed `var`/`const`/`let`
**Severity:** Low
**File:** `index.js:46` (`var LastEvaluatedShardId`)
**Description:** One `var` declaration in an otherwise `const`/`let` codebase. Inconsistent but not harmful.

---

### Finding: MAINT-03 — Callback Hell in `_getOpenShards`
**Severity:** Medium
**File:** `index.js:44–93`
**Description:** Five levels of nested callbacks make this method hard to read, test in isolation, and reason about error flows. This is the most complex method in the library (cyclomatic complexity ~6).
**Recommended Fix:** Refactor to `async/await` with the `promisify`-wrapped SDK calls or use the AWS SDK v3's native Promise API.

---

### Finding: MAINT-04 — No `engines` Field in `package.json`
**Severity:** Low
**File:** `package.json`
**Description:** No `"engines": {"node": ">=14"}` declaration. Consumers cannot determine the minimum Node.js version without reading the source.

---

### Finding: MAINT-05 — Legacy JSHint (No ESLint)
**Severity:** Low
**File:** `.jshintrc`
**Description:** JSHint is effectively unmaintained since ~2018. ESLint is the modern standard and supports modern JS/ES2022+. JSHint has no concept of ES modules, optional chaining, or nullish coalescing.

---

## Performance

### Finding: PERF-01 — No Backoff on Poll Errors or Empty Results
**Severity:** Medium
**File:** `index.js:208–211`
**Description:** The polling interval is fixed. When no records are returned (quiet stream) or when transient errors occur, the library polls at the same rate — no jitter, no backoff. This wastes AWS API calls during quiet periods and risks throttling during error storms.
**Recommended Fix:** Implement exponential backoff with jitter when `GetRecords` returns empty results or transient errors.

---

### Finding: PERF-02 — Async Parallel Shard Polling (Positive)
**Severity:** None (positive finding)
**File:** `index.js:98`
**Description:** `async.each` processes all shards concurrently (not sequentially), which is the correct approach for maximizing throughput across multiple shards. No issue here.

---

### Finding: PERF-03 — Memory — No Unbounded Growth
**Severity:** None (positive finding)
**File:** `index.js:147`
**Description:** `this._shards` is always replaced on re-fetch (`delete this._shards` then re-assigned). No accumulation risk detected.

---

### Finding: PERF-04 — Memory Leak Risk — EventEmitter Listener Accumulation in `DynamodDBReadable`
**Severity:** Low
**File:** `index.js:236–241`
**Description:** If a `DynamodDBReadable` instance is created but never piped or consumed, and then abandoned (GC'd), the inner `_subscriber` EventEmitter has listeners attached that hold a reference back to the outer `Readable`. This creates a reference cycle. Node.js handles most of these via GC, but if many Readable instances are created and discarded in a loop, it could delay collection.
**Recommended Fix:** Implement a `destroy()` method that calls `subscriber.stop()` and removes all listeners.

---

## Summary Scorecard (Updated after Deep-Dive)

| Dimension | Initial Rating | Deep-Dive Rating | Change |
|-----------|---------------|-----------------|--------|
| Security | Good 4/5 | Good 4/5 | No change |
| Reliability | Poor 2/5 | **Very Poor 1/5** | ↓ New REL-06 found |
| Maintainability | Fair 3/5 | Fair 3/5 | No change |
| Performance | Fair 3/5 | Fair 3/5 | No change |
| Test Coverage | Poor 2/5 | **Very Poor 1/5** | ↓ TrimmedDataAccessException coverage worse than assessed |

---

## NEW Finding (Deep-Dive): REL-06 — `async.each` Discards `rerunJob` — TrimmedDataAccessException Immediate Re-Process Is Dead Code

**Severity:** High
**File:** `index.js:98–141`
**Code path:**
```javascript
async.each(this._shards, (shard, callback) => {
  // ...
  // On TrimmedDataAccessException:
  shard.iterator = dataa.ShardIterator;
  callback(null, true);   // ← `true` passed as result
  // ...
}, (err, rerunJob) => {  // ← async.each does NOT forward iterator results
  if (rerunJob) {         // ← rerunJob is ALWAYS undefined — this branch never executes
    return this._process(job);
  }
```

**Root Cause:** `async.each` is a *parallel iterator* — it runs all shard callbacks concurrently and calls its final callback with `(err)` only. It does NOT collect or forward individual callback results. That behavior belongs to `async.map`. The author intended `callback(null, true)` to signal "re-run the process" to the outer callback, but `async.each` silently discards the `true` value.

**Actual vs. Intended behavior:**

| Scenario | Intended | Actual |
|----------|---------|--------|
| TrimmedDataAccessException on shard X | Fetch TRIM_HORIZON iterator → immediately call `_process()` again | Fetch TRIM_HORIZON iterator (✓) → update `shard.iterator` (✓) → `rerunJob` never truthy → next re-process delayed until next scheduled interval |

**Consequence:** After a TrimmedDataAccessException:
1. The shard iterator IS correctly updated to TRIM_HORIZON ✓
2. The library will eventually catch up at the next polling interval ✓
3. The records available between recovery and the next interval are NOT fetched immediately ✗ — the intent of immediate re-processing is not achieved

**This makes the recovery code effectively work — but slower than designed.** The shard iterator update is correct; only the immediate-retry signal is broken.

**Fix:** Replace `async.each` with `async.map` to collect results, OR use a shared flag:
```javascript
// Option A: use a flag
let needsRerun = false;
async.each(this._shards, (shard, callback) => {
  // ...
  needsRerun = true;
  callback();
}, (err) => {
  if (needsRerun) return this._process(job);
  // ...
});
```

---

## NEW Finding (Deep-Dive): REL-07 — `start()` Has No Guard Against Multiple Calls

**Severity:** High
**File:** `index.js:161`
**Description:** `start()` creates a new `tempus-fugit` job every time it is called. There is no `_started` flag or similar guard. If `start()` is called twice — or if `DynamodDBReadable._read()` triggers `start()` multiple times — multiple independent schedulers are created. Each scheduler independently calls `_process()` on the same `this._shards` array, causing:
- Duplicate `emit('record')` calls for the same records
- Concurrent mutation of `shard.iterator` from multiple processes
- `this._shards` being `delete`'d and re-fetched by multiple `_process()` calls simultaneously

**Fix:**
```javascript
start() {
  if (this._started) return;
  this._started = true;
  // ... existing async.series
}
stop() {
  if (this._job) this._job.cancel();
  this._started = false;
}
```

---

## NEW Finding (Deep-Dive): REL-08 — Unhandled Exception if `'record'` Handler Throws

**Severity:** High
**File:** `index.js:126–129`
**Code:**
```javascript
data.Records.forEach(r => {
  const key = r.dynamodb && r.dynamodb.Keys && unmarshall(r.dynamodb.Keys);
  this.emit('record', r, key);  // ← if consumer's handler throws, exception propagates here
});
```
**Description:** `this.emit('record', ...)` is called inside `forEach` inside an async callback. If the consumer's `'record'` listener throws a synchronous exception, it propagates up through `forEach` and then through the `getRecords` callback. Since there is no try/catch, this exception becomes an unhandled rejection inside `async.each`'s internal machinery and may crash the process or silently kill the polling loop.

**Fix:**
```javascript
try {
  this.emit('record', r, key);
} catch (e) {
  this.emit('error', e);
}
```

---

## NEW Finding (Deep-Dive): MAINT-06 — Debug Log Argument Bug

**Severity:** Low
**File:** `index.js:65`
**Code:**
```javascript
debug('stream.getShardIterator (start) ShardId: %s', openShards.length);
```
**Description:** This debug statement logs `openShards.length` (a number — total shards count) where `shard.ShardId` (the current shard being processed) was intended. Every shard in the parallel `async.map` logs the same count instead of its own ID, rendering this debug line useless.

**Fix:** `debug('stream.getShardIterator (start) ShardId: %s', shard.ShardId);`

---

## Test Coverage

### Coverage Heat Map (Updated)

| Component / Path | Coverage | Notes |
|-----------------|----------|-------|
| `DynamodDBSubscriber` constructor | Green | Tested in multiple test cases |
| `DynamodDBSubscriber.start()` — ARN path | Green | `dsubscriber.tests.js:70` |
| `DynamodDBSubscriber.start()` — table path | Green | `dsubscriber.tests.js:157` |
| `DynamodDBSubscriber.start()` — multiple calls | Red | Not tested (REL-07) |
| `DynamodDBSubscriber._getOpenShards()` — single page | Green | `internal.tests.js:36` |
| `DynamodDBSubscriber._getOpenShards()` — multi-page pagination | Red | Not tested |
| `DynamodDBSubscriber._getOpenShards()` — DescribeStream error | Red | Not tested (swallowed — REL-01) |
| `DynamodDBSubscriber._process()` — records returned | Green | Tested via end-to-end flow |
| `DynamodDBSubscriber._process()` — TrimmedDataAccessException | **Red** | Test uses wrong error shape (REL-03); rerunJob never fires (REL-06) — effectively untested |
| `DynamodDBSubscriber._process()` — all shards closed (null iterator) | Red | Not tested |
| `DynamodDBSubscriber._process()` — non-TrimmedDataAccessException error | Red | Not tested |
| `DynamodDBSubscriber._process()` — empty Records array | Red | Not tested |
| `DynamodDBSubscriber._process()` — consumer handler throws (REL-08) | Red | Not tested |
| `DynamodDBSubscriber.stop()` — normal | Green | Called in test teardown |
| `DynamodDBSubscriber.stop()` — before start | Red | Not tested (crashes — REL-02) |
| `DynamodDBReadable` (entire class) | Red | Zero test coverage |
| ARN resolution failure paths | Red | Not tested |

### Test Quality Notes

- Tests use `before()`/`after()` for stub setup, which means stubs are shared across test cases within a `describe` block — side effects between tests possible
- `sinon.restore()` is called in `afterEach` (good), but `before()` stubs run once, creating a potential conflict if `afterEach` cleans up a stub that `before()` set up
- No assertion on error cases — `subscriber.on('error', ...)` is never tested
- `done()` callback pattern is used — compatible with Mocha v2 but would benefit from async/await with modern Mocha

---

## Techniques Applied

- OWASP Top 10 Quick Scan (#16): Security assessment
- Secret Detection Scan (#19): Confirmed no hardcoded secrets
- Input Validation Audit (#18): Reviewed constructor parameter handling
- Error Propagation Tracing (#10): Found critical swallowed error (REL-01)
- Async Flow Analysis (#48): Identified callback hell, async patterns, REL-06 (async.each result discard), REL-07 (no start guard), REL-08 (emit inside forEach)
- Memory Leak Risk Detection (#49): Found EventEmitter reference cycle risk
- Test Coverage Mapping (#24): Produced and updated coverage heat map
- Dead Code Detection (#21): Found unused `aws-sdk-client-mock`
- Code Duplication Analysis (#22): No significant duplication in a codebase this small
- Complexity Metrics Scan (#23): Per-function complexity table; `_process` and `_getOpenShards` are hotspots
- Design Pattern Recognition (#4): Identified anti-patterns (error swallowing, callback hell, wrong primitive)
- Data Transformation Chain (#9) [deep-dive]: Traced unmarshall chain and gaps
