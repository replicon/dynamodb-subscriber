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

<!-- graphify-managed-start v2 -->
## MANDATORY: Query the knowledge graph before reading source files

An **AST knowledge graph** of this codebase lives in `graphify-out/` — 63 nodes, 81
edges, 9 named communities across `index.js`, `package.json`, and the two mocha suites.
Query it before reading any source file; the graph surfaces call and import
relationships you would miss by reading files directly.

This repo indexes the **AST layer only** — there is no semantic layer, so the graph
answers *where is X / what calls Y / what breaks if I change Z*, not *why was this
designed this way*. For the "why", read `aidlc-docs/reverse-engineering/`.

**Mandatory workflow:**

1. **Graph first** — run `graphify query` to get relevant nodes with file paths and line numbers
2. **Targeted read** — use those line numbers to read only the specific lines needed
3. **Source only** — go to source directly ONLY for runtime/AWS state the graph cannot know

**How to write a good query — semantic, not keyword:**

| ❌ Keyword (avoid) | ✅ Semantic (use this) |
|---|---|
| `"ShardIterator"` | `"how a shard is opened and read position is carried between polls"` |
| `"emit"` | `"how stream records reach the consumer as events"` |
| `"interval"` | `"how polling is scheduled and stopped"` |

Use `--context` to narrow traversal to a specific relationship type:
```bash
graphify query "how records are emitted to consumers" --context "calls" --budget 6000
```

**Budget:** this graph is ~46 KB, so **start at `--budget 6000`**. If the output shows
`[!] TRUNCATED`, step up: 6000 → 16000 → 32000. If still truncated at 32000 the query is
too broad — add `--context` or split it.

**DO NOT read source files when the last query was TRUNCATED.** This is enforced by
`.claude/hooks/graphify-guard.py` — source reads are blocked until a non-truncated query
runs. The guard covers `index.js` and `test/`; docs, `_daidlc/` and config are never blocked.

**Other useful commands:**

```bash
graphify explain "DynamodDBSubscriber"            # node + its neighbors in plain language
graphify affected "DynamodDBSubscriber"           # what breaks if this changes
graphify path "DynamodDBSubscriber" "debug"       # shortest path between two nodes
graphify god-nodes                                # most connected architectural hubs
```

**Keeping the graph fresh — adhoc, not CI:**

This repo has **no CI graph publishing and no merge workflow**. `graphify-out/graph.json`
is committed directly and is rebuilt **on demand** when the code has drifted enough to
matter (a refactor, new module, renamed symbols):

```bash
graphify update . --no-gitignore            # incremental AST re-extract
graphify cluster-only . --backend=claude    # re-cluster + re-label communities
git add graphify-out/ && git commit -m "chore: refresh graphify AST graph"
```

Use `graphify update . --no-gitignore --force` after a refactor that deletes code, so a
smaller node count does not get rejected. What is indexed is controlled by `.graphifyignore`.

**Prerequisites:**

- **Python 3.9+** on `PATH` — the hooks are Python scripts.
- **Windows:** hooks invoke `sh` to run the graphify wrapper. `sh` comes from
  **Git for Windows** — install with the "Git from the command line and also from
  3rd-party software" option so `sh` is on `PATH`. Run `where sh` to verify.
- **graphify CLI** (`pip install graphifyy`) — *optional*. Without it the guard
  detects the missing binary and disables itself rather than blocking you.

**Turning the guard off:**

```bash
GRAPHIFY_GUARD_DISABLE=1    # disables graph-first enforcement entirely
GRAPHIFY_NO_TELEMETRY=1     # disables metrics logging only (guard stays on)
```

Session metrics are written to `graphify-out/metrics/` (gitignored, never committed)
and are not consumed by any CI or dashboard in this repo.
<!-- graphify-managed-end -->
