# dynamodb-subscriber

Node.js library that subscribes to AWS DynamoDB Streams and emits records via EventEmitter or Readable stream. Published as `@replicon/dynamodb-subscriber` to GitHub Packages.

## Project Layout

```
index.js          # Entire library — DynamodDBSubscriber + DynamodDBReadable
test/             # Mocha tests (dsubscriber.tests.js, internal.tests.js)
package.json      # Published to https://npm.pkg.github.com
```

## Commands

```bash
npm test          # Run mocha test suite
```

## D-AIDLC Documentation

This project uses the **D-AIDLC AI-Driven Development Lifecycle**. All generated artifacts live in `aidlc-docs/`.

### Lifecycle State

`aidlc-docs/aidlc-state.md` — current phase, stage, and completion status for all lifecycle stages.

### Reverse Engineering Artifacts

Located in `aidlc-docs/reverse-engineering/` — produced by Aria (Research Analyst) with deep analysis:

| Artifact | Contents |
|----------|---------|
| `business-overview.md` | Purpose, user workflows, business rules, domain terminology |
| `architecture.md` | Layered architecture, Mermaid diagrams, subscriber state machine, design patterns |
| `code-structure.md` | Directory map, naming conventions, complexity metrics, dead code |
| `api-documentation.md` | Full public API — constructor params, events, methods, data shapes |
| `component-inventory.md` | Class inventory, AWS API calls, scheduler contract, event listeners |
| `technology-stack.md` | Full stack assessment, build tooling, dependency freshness |
| `dependencies.md` | Runtime/dev deps, CVE risk, license analysis, transitive dependencies |
| `code-quality-assessment.md` | All findings (1 Critical, 6 High, 8 Medium, 8 Low) with file:line citations |
| `cross-validation.md` | Cross-artifact consistency checks — discrepancies and new bugs surfaced |
| `reverse-engineering-summary.md` | Executive summary, full findings register, prioritized fix list |

### Key Findings (Read Before Coding)

Before modifying `index.js`, read `aidlc-docs/reverse-engineering/code-quality-assessment.md`. Critical known issues:

- **REL-01** (`index.js:55`): `console.log(err)` in `_getOpenShards` swallows errors — subscriber hangs forever
- **REL-06** (`index.js:134`): `async.each` discards results — TrimmedDataAccessException re-process branch never executes
- **REL-07** (`index.js:161`): `start()` has no guard — calling twice creates duplicate polling schedulers
- **REL-08** (`index.js:126`): Consumer `'record'` handler exception propagates unhandled

### Audit Trail

`aidlc-docs/audit/audit.md` — chronological log of all D-AIDLC actions and findings.
