# D-AIDLC Help System

**Goal**: Provide context-aware guidance based on current project state.

## Instructions

1. Load `aidlc-docs/aidlc-state.md` to determine current phase and stage
2. Based on the current state, provide relevant guidance:

### If No State File Exists
> "Welcome to D-AIDLC! Let's get started. I'll scan your workspace to determine
> if this is a new project or you're extending existing code."
>
> **Next Step:** Workspace Detection (automatic)

### If In Inception Phase
Show which inception stages are complete and what's next:
- Workspace Detection → Requirements Analysis → User Stories → Workflow Planning
- Architecture Design → NFR Requirements → NFR Design → Infrastructure Design

### If In Construction Phase
Show sprint status and current story progress:
- Current sprint status
- Next story ready for development
- Pending code reviews

### If In Testing Phase
Show test status:
- Test strategy completion
- Test generation progress
- Latest execution results
- Coverage vs targets

### General Help
List available agents and their commands:
- **Aria** (Analyst): BS, MR, DR, TR, CB, RE
- **Parker** (PM): CP, VP, EP, CE, IR
- **Atlas** (Architect): CA, NR, ND, ID
- **Uma** (UX): UX
- **Scout** (SM): SP, CS, RT, CC
- **Dash** (Developer): DS, CR
- **Vera** (QA): QA
- **Flash** (Quick Dev): QS, QD
- **Sage** (Tech Writer): WD, MG, VD
- **Trent** (Test Architect): TS, GT, ET, AR, COV, RD
- **Grace** (Governance): AU, VG, CV
- **Clio** (Compliance): SR, CC, TV
