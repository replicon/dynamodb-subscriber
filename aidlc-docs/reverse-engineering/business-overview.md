# Business Overview
**Project:** `@replicon/dynamodb-subscriber` v1.7.2
**Confidence:** High
**Generated:** 2026-08-04

---

## Purpose

`dynamodb-subscriber` is a Node.js library that abstracts the complexity of the AWS DynamoDB Streams API. It continuously polls DynamoDB stream shards for new records and exposes them through two familiar Node.js idioms: an **EventEmitter** interface and a **Readable stream** interface.

The library was created to remove boilerplate from applications that need to react to DynamoDB change events (INSERT, MODIFY, REMOVE) — such as cache invalidation pipelines, audit logging, cross-region replication, and event-driven microservice coordination.

---

## Business Domain

**Domain:** AWS Cloud Integration / Change Data Capture (CDC)

DynamoDB Streams is AWS's native CDC mechanism. It records every write to a table as a time-ordered sequence of stream records, retained for 24 hours. This library makes that stream consumable with minimal setup.

---

## Target Users

- **Backend developers** building event-driven architectures on AWS
- **Teams using DynamoDB** who need to react to data changes without polling the table directly
- **Internal Replicon teams** — published to GitHub Packages under the `@replicon` scope, indicating internal/organizational use rather than general public distribution

---

## Key User Workflows

### Workflow 1: Subscribe by Stream ARN
```javascript
const subscriber = new DynamoDBSubscriber({ arn: 'arn:aws:dynamodb:...', interval: '1s' });
subscriber.on('record', (record, keys) => { /* handle */ });
subscriber.start();
```
User provides the stream ARN directly → subscriber polls open shards → emits each record.

### Workflow 2: Subscribe by Table Name
```javascript
const subscriber = new DynamoDBSubscriber({ table: 'my-table', interval: '10s' });
subscriber.on('record', (record, keys) => { /* handle */ });
subscriber.start();
```
User provides the table name → library auto-discovers the stream ARN (via `DescribeTable` or `ListStreams`) → same polling flow.

### Workflow 3: Node.js Stream Interface
```javascript
const DynamoDBStream = require('dynamodb-subscriber').Stream;
const stream = new DynamoDBStream({ arn: '...', interval: '1s' });
stream.pipe(stringify).pipe(process.stdout);
```
Records flow through a standard `Readable` stream — integrates with any stream-based pipeline.

### Workflow 4: Stop Subscription
```javascript
subscriber.stop();
```
Cancels the polling job cleanly.

---

## Business Rules (Embedded in Code)

| Rule | Location | Description |
|------|----------|-------------|
| ARN or table required | `index.js:17–19` | Constructor throws if neither `arn` nor `table` is provided |
| Default poll interval | `index.js:30` | 10 seconds if `interval` not specified |
| Open shards only | `index.js:61` | Closed shards (those with `EndingSequenceNumber`) are filtered and never polled |
| LATEST iterator on start | `index.js:68` | New subscriptions start from the tip of the stream — historical records are not replayed |
| TrimmedDataAccessException recovery | `index.js:103–115` | If stream data is trimmed (> 24h old), iterator is reset to TRIM_HORIZON to catch up |
| Shard re-discovery on expiry | `index.js:145–156` | If all shard iterators expire (shard closed), open shards are re-discovered before resuming |

---

## Domain Terminology (Ubiquitous Language)

| Term | Definition |
|------|-----------|
| **Stream ARN** | Amazon Resource Name identifying a specific DynamoDB stream |
| **Shard** | A partition of a stream; a stream has one or more shards |
| **Open Shard** | A shard with no `EndingSequenceNumber` — still receiving writes |
| **Closed Shard** | A shard that has been retired — no more records will appear |
| **Shard Iterator** | A cursor pointing to a position in a shard (`LATEST`, `TRIM_HORIZON`, `AT_SEQUENCE_NUMBER`) |
| **TrimmedDataAccessException** | AWS error when requesting records older than the 24-hour stream retention window |
| **Record** | A single DynamoDB stream event — contains the item keys and optionally old/new images |
| **Unmarshalling** | Converting DynamoDB's `AttributeValue` wire format (e.g. `{S: "foo"}`) to plain JavaScript objects |

---

## Business Value

- **Reduces boilerplate:** Encapsulates AWS Streams pagination, iterator management, shard lifecycle, and error recovery into a single `start()` call
- **Resilient by default:** Handles TrimmedDataAccessException, closed shard recovery, and iterator expiry automatically
- **Dual interface:** Works as an EventEmitter (familiar to most Node.js developers) and as a Readable stream (integrates with pipelines)
- **Configurable polling:** Interval accepts both millisecond numbers and human-readable strings via the `ms` package

---

## Deep-Dive: Jobs-to-Be-Done Analysis

Beyond the documented README use cases, the code reveals implicit jobs consumers are hiring this library to do:

| Job | Signal in Code | Implication |
|-----|---------------|-------------|
| **"Don't make me manage shard lifecycle"** | `_getOpenShards()` + shard re-discovery loop | The hardest part of the DynamoDB Streams API — automatic pagination, open/closed filtering, iterator management — is fully encapsulated |
| **"Alert me the moment a record appears"** | `ShardIteratorType: 'LATEST'` (index.js:70) | Library always starts from the current tip; it is NOT a replay tool |
| **"Don't lose records if the stream gets trimmed"** | TrimmedDataAccessException recovery at index.js:103 | 24-hour retention window is handled automatically — library can survive brief outages |
| **"Work with my existing Node.js stream pipeline"** | `DynamodDBReadable` extending `Readable` | Composable with `pipe()`, `Transform`, and other Node.js stream primitives |
| **"Don't force me to know the stream ARN"** | Table-name resolution path at index.js:165–198 | Consumer only needs IAM access; ARN lookup is automatic |

## Deep-Dive: Business Rules Gap Analysis

Business rules that are MISSING or UNDERSPECIFIED:

| Gap | Description | Risk |
|-----|-------------|------|
| **No replay capability** | `ShardIteratorType: 'LATEST'` is hardcoded — there is no option to start from `AT_SEQUENCE_NUMBER` or `AT_TIMESTAMP` | Medium — a consumer restarting after a gap has no way to catch up on missed records |
| **No record deduplication** | If `_process` is called twice (e.g. on a re-run after recovery), the same record could be emitted twice | Medium — consumers must handle idempotency themselves; this is not documented |
| **No delivery guarantee** | If the consumer's `'record'` handler throws, the error propagates to the `forEach` loop (index.js:126) and the exception is unhandled — could crash the process | High — no documentation warning consumers about this |
| **Interval is not jittered** | Multiple instances of this library running against the same stream will poll at exactly the same time, creating synchronized bursts of AWS API calls | Low |

## Techniques Applied

- Input-to-Output Tracing (#6): Traced the record lifecycle from AWS API call to `emit('record')`
- Entry Point Discovery (#5): Identified both exported classes and their initialization flows
- Event Flow Analysis (#8): Mapped the EventEmitter pattern and stream adapter composition
- Data Transformation Chain (#9) [deep-dive]: Traced data from AWS AttributeValue wire format → `unmarshall()` → plain JS object

