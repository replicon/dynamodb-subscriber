# API Documentation
**Project:** `@replicon/dynamodb-subscriber` v1.7.2
**Confidence:** High
**Generated:** 2026-08-04

---

> This is a **library** — there are no HTTP routes, REST endpoints, or network-facing APIs.
> This document covers the **public JavaScript API surface** exposed to consumers.

---

## Public Class: `DynamodDBSubscriber`

**Exported as:** `require('@replicon/dynamodb-subscriber')` (`index.js:250`)
**Extends:** `EventEmitter` (Node.js core)
**Purpose:** Polls one or more DynamoDB stream shards periodically and emits each incoming record as an event.

---

### Constructor

```javascript
new DynamodDBSubscriber(params)
```

**Throws** `Error('arn or table are required')` if neither `arn` nor `table` is provided (`index.js:17–19`).

#### Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `arn` | `string` | One of `arn`/`table` | — | Full DynamoDB stream ARN. Example: `arn:aws:dynamodb:us-west-1:XXXX:table/YYYY/stream/ZZZZ` |
| `table` | `string` | One of `arn`/`table` | — | DynamoDB table name. Stream ARN is auto-discovered via `DescribeTable` or `ListStreams` |
| `region` | `string` | No | AWS SDK default | AWS region code (e.g. `'us-east-1'`) |
| `endpoint` | `string` | No | AWS default | Custom endpoint URL — use for local DynamoDB (e.g. `'http://localhost:8000'`) |
| `interval` | `number` \| `string` | No | `10000` (10s) | Polling interval. Number = milliseconds; string = `ms`-package format (e.g. `'1s'`, `'500ms'`, `'2m'`) |

#### Validation Behavior

| Condition | Result |
|-----------|--------|
| Neither `arn` nor `table` | Throws synchronously |
| Both `arn` and `table` provided | `arn` takes precedence (table ARN discovery is skipped if `_streamArn` is set at start) |
| Invalid `interval` type | Falls back to default 10s silently (`index.js:26–30`) |

---

### Methods

#### `start()`

```javascript
subscriber.start() → void
```

Begins the subscription lifecycle:

1. **ARN resolution** (only if `table` provided, not `arn`):
   - Attempts `DynamoDB.DescribeTable` → extracts `LatestStreamArn` (`index.js:174–179`)
   - Falls back to `DynamoDBStreams.ListStreams` → uses `Streams[0].StreamArn` (`index.js:188–194`)
2. **Shard discovery**: Calls `_getOpenShards()` to find all open shards and their LATEST iterators
3. **Polling start**: Schedules `_process()` to run every `interval` milliseconds via `tempus-fugit`

Errors during initialization are emitted as `'error'` events (`index.js:217–219`).

**Caution:** Calling `start()` multiple times creates multiple polling jobs (no guard against double-start).

---

#### `stop()`

```javascript
subscriber.stop() → void
```

Cancels the polling scheduler (`index.js:224`).

**Caution:** Calling `stop()` before `start()` completes results in `TypeError: Cannot read properties of undefined (reading 'cancel')` because `this._job` is undefined until the scheduler is started.

---

### Events

#### `'record'`

```javascript
subscriber.on('record', (record, key) => { ... })
```

Emitted for each record returned from `DynamoDBStreams.GetRecords` (`index.js:128–129`).

| Argument | Type | Description |
|----------|------|-------------|
| `record` | `object` | Raw DynamoDB stream record — the full object from the AWS API, including `eventName`, `eventSource`, `dynamodb` sub-object |
| `key` | `object` | Unmarshalled key of the changed item — plain JavaScript object (AWS `AttributeValue` format decoded via `@aws-sdk/util-dynamodb`'s `unmarshall`) |

**Example record structure:**
```javascript
{
  eventID: 'xxxxx',
  eventName: 'INSERT',            // INSERT | MODIFY | REMOVE
  eventVersion: '1.1',
  eventSource: 'aws:dynamodb',
  awsRegion: 'us-west-1',
  dynamodb: {
    ApproximateCreationDateTime: Date,
    Keys: { name: { S: 'YYYYZZZ' } },   // raw AttributeValue format
    SequenceNumber: '4324698400043243243243246',
    SizeBytes: 35,
    StreamViewType: 'KEYS_ONLY'          // depends on table stream config
  }
}
```

**Example key structure** (after unmarshalling):
```javascript
{ name: 'YYYYZZZ' }
```

**Note:** `key` is produced by `unmarshall(r.dynamodb.Keys)`. If `r.dynamodb` or `r.dynamodb.Keys` is absent, `key` will be `undefined` — the library does not guard against this (`index.js:127`).

---

#### `'error'`

```javascript
subscriber.on('error', (err) => { ... })
```

Emitted on unrecoverable AWS API errors (`index.js:136–137`).

**Important:** In Node.js, if an `'error'` event is emitted on an EventEmitter with no listener, the process crashes with an unhandled error. Consumers MUST attach an `'error'` listener.

| Trigger | Details |
|---------|---------|
| `GetRecords` fails with non-TrimmedDataAccessException error | Emitted and polling stops (`job.done()` called) |
| `_getOpenShards` callback errors | Emitted and polling stops |
| `start()` series errors | Emitted, no polling started |

**Not** emitted for: `DescribeStream` errors (swallowed at `index.js:55` — see Code Quality findings).

---

## Public Class: `DynamodDBReadable`

**Exported as:** `require('@replicon/dynamodb-subscriber').Stream` (`index.js:251`)
**Extends:** `stream.Readable` (Node.js core, `objectMode: true`)
**Purpose:** Wraps `DynamodDBSubscriber` in a Node.js Readable stream — enables `.pipe()` and stream composition.

---

### Constructor

```javascript
new DynamodDBReadable(options)
```

Accepts the same `options` as `DynamodDBSubscriber`. Merges `{ objectMode: true }` into the options before passing to `Readable` super constructor (`index.js:232`).

---

### Inherited Stream Interface

Being a `Readable` in `objectMode`, it pushes raw DynamoDB record objects (not strings or Buffers).

| Method | Behavior |
|--------|---------|
| `_read()` | Calls `this._subscriber.start()` — stream pull triggers subscription start |
| `push(record)` | Called internally on each `'record'` event — returns `false` when consumer is slow, triggering `subscriber.stop()` (backpressure) |
| `pipe(destination)` | Standard Node.js stream pipe |

**Backpressure:** When `push()` returns `false` (consumer is slower than production), `subscriber.stop()` is called (`index.js:237–239`). However, there is no `resume()` mechanism — once stopped this way, the stream will not restart. This is a **known limitation** for high-throughput scenarios.

---

## Internal Methods (Not Public API)

Documented for maintainers only.

| Method | Signature | Description |
|--------|-----------|-------------|
| `_getOpenShards` | `(callback: (err, shards[]) => void) → void` | Paginates `DescribeStream`, filters to open shards, fetches `LATEST` iterator per shard. Uses `async.doWhilst` for pagination, `async.map` for parallel iterator fetching |
| `_process` | `(job: TempusFugitJob) → void` | Polls each shard via `GetRecords` in parallel (`async.each`). Handles `TrimmedDataAccessException`, `NextShardIterator` advancement, and re-fetches shards when iterators expire |

---

## AWS IAM Requirements

Consumers must grant the following IAM permissions to the AWS credentials used:

| Permission | Resource | Required When |
|-----------|----------|---------------|
| `dynamodb:DescribeStream` | Stream ARN | Always |
| `dynamodb:GetShardIterator` | Stream ARN | Always |
| `dynamodb:GetRecords` | Stream ARN | Always |
| `dynamodb:ListStreams` | Stream ARN | When using `table` param (fallback path) |
| `dynamodb:DescribeTable` | Table ARN | When using `table` param (preferred path) |

---

## Deep-Dive: Data Transformation Chain

Tracing data from AWS wire format to consumer-visible value (technique #9):

```
AWS GetRecords response
  └─ data.Records[]                          (raw DynamoDB stream record array)
       └─ record                             (single record: eventID, eventName, dynamodb, ...)
            └─ record.dynamodb.Keys          (AWS AttributeValue format: { foo: { S: 'bar' } })
                 └─ unmarshall(Keys)         (@aws-sdk/util-dynamodb — index.js:127)
                      └─ key                 (plain JS: { foo: 'bar' })

emit('record', record, key)                  (consumer receives both raw and unmarshalled)
```

**Transformation gaps:**
- `record.dynamodb.NewImage` and `record.dynamodb.OldImage` are NOT unmarshalled — only `Keys` is. Consumers must call `unmarshall()` themselves for image data.
- If `record.dynamodb` is absent (malformed record from AWS), `key` evaluates to `undefined` — no error is thrown, `emit('record', record, undefined)` is called silently.
- `data.Records` is checked for existence and length at line 125 (`if (data.Records && data.Records.length > 0)`), but individual record fields are NOT validated before unmarshalling.

## Deep-Dive: Undocumented API Behaviors

| Behavior | Location | Documented? |
|----------|----------|-------------|
| `start()` called with `arn` skips both ARN-resolution steps silently | `index.js:164,186` — `if (this._streamArn) { return cb(); }` | Not documented |
| `start()` called with invalid table: no error emitted if `DescribeTable` fails — silently tries `ListStreams` | `index.js:174–179` — error ignored on describeTable | Not documented — could hide misconfiguration |
| `start()` called with invalid table AND `ListStreams` fails: emits error with message starting `"Cannot retrieve the stream arn of..."` | `index.js:190–193` | Not documented |
| `start()` can be called multiple times — creates duplicate polling jobs | No guard in `start()` | Not documented — dangerous |
| `_read()` may call `start()` multiple times on a `DynamodDBReadable` | Node.js calls `_read()` when buffer drains | Not documented |
| Records from ALL open shards are interleaved in the `'record'` event | `async.each` processes all shards concurrently — arrival order is non-deterministic | Not documented |
| Ordering within a single shard is preserved | `getRecords` returns records in sequence order per shard | Not documented but implied |

## Techniques Applied

- Entry Point Discovery (#5): Located both exported classes
- API Contract Extraction (#14): Documented all constructor params, return shapes, event payloads
- Event Flow Analysis (#8): Documented emitter → listener contract and error propagation
- Input-to-Output Tracing (#6): Traced from `start()` call to `emit('record')` with full data shape
- Data Transformation Chain (#9) [deep-dive]: Full unmarshall chain documented
- API Versioning Detection (#15) [deep-dive]: N/A — no versioning; single export
