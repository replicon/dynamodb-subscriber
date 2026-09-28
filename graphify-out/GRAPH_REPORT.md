# Graph Report - dynamodb-subscriber  (2026-09-28)

## Corpus Check
- cluster-only mode — file stats not available

## Summary
- 63 nodes · 81 edges · 9 communities (6 shown, 3 thin omitted)
- Extraction: 100% EXTRACTED · 0% INFERRED · 0% AMBIGUOUS
- Token cost: 381 input · 77 output

## Graph Freshness
- Built from commit: `c7125cfc`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- DynamoDB Subscriber Tests
- DynamoDB Stream Reader
- Stream Index Exports
- Package Metadata
- Package Dependencies
- Dev Dependencies
- Publish Config
- Repository Info
- Test Scripts

## God Nodes (most connected - your core abstractions)
1. `DynamodDBSubscriber` - 7 edges
2. `@aws-sdk/client-dynamodb-streams` - 4 edges
3. `debug` - 3 edges
4. `DynamodDBReadable` - 3 edges
5. `@aws-sdk/client-dynamodb` - 3 edges
6. `chai` - 3 edges
7. `sinon` - 3 edges
8. `ms` - 2 edges
9. `repository` - 2 edges
10. `publishConfig` - 2 edges

## Surprising Connections (you probably didn't know these)
- None detected - all connections are within the same source files.

## Import Cycles
- None detected.

## Communities (9 total, 3 thin omitted)

### Community 0 - "DynamoDB Subscriber Tests"
Cohesion: 0.18
Nodes (11): @aws-sdk/client-dynamodb, @aws-sdk/client-dynamodb-streams, chai, sinon, { DynamoDB }, { DynamoDBStreams }, DynamoDBSubscriber, sinon (+3 more)

### Community 1 - "DynamoDB Stream Reader"
Cohesion: 0.24
Nodes (4): debug, DynamodDBReadable, DynamodDBSubscriber, ms

### Community 2 - "Stream Index Exports"
Cohesion: 0.20
Nodes (9): async, { DynamoDB }, { DynamoDBStreams }, { unmarshall }, async, @aws-sdk/util-dynamodb, ref_events, ref_stream (+1 more)

### Community 3 - "Package Metadata"
Cohesion: 0.20
Nodes (9): author, description, main, name, version, aws-sdk-client-mock, debug, mocha (+1 more)

### Community 4 - "Package Dependencies"
Cohesion: 0.25
Nodes (8): dependencies, async, @aws-sdk/client-dynamodb, @aws-sdk/client-dynamodb-streams, @aws-sdk/util-dynamodb, debug, ms, tempus-fugit

### Community 5 - "Dev Dependencies"
Cohesion: 0.40
Nodes (5): devDependencies, aws-sdk-client-mock, chai, mocha, sinon

## Knowledge Gaps
- **33 isolated node(s):** `{ DynamoDB }`, `{ DynamoDBStreams }`, `{ unmarshall }`, `async`, `name` (+28 more)
  These have ≤1 connection - possible missing edges. (Counts symbols only; 37 node(s) total have ≤1 connection when file, concept and rationale nodes are included.)
- **3 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `dependencies` connect `Package Dependencies` to `Package Metadata`?**
  _High betweenness centrality (0.215) - this node is a cross-community bridge._
- **Why does `devDependencies` connect `Dev Dependencies` to `Package Metadata`?**
  _High betweenness centrality (0.126) - this node is a cross-community bridge._
- **Why does `DynamodDBSubscriber` connect `DynamoDB Stream Reader` to `Stream Index Exports`?**
  _High betweenness centrality (0.124) - this node is a cross-community bridge._
- **What connects `{ DynamoDB }`, `{ DynamoDBStreams }`, `{ unmarshall }` to the rest of the system?**
  _33 weakly-connected nodes found - possible documentation gaps or missing edges._