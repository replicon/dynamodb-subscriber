---
name: quick-dev
description: 'Flexible development - execute tech-specs OR direct instructions with optional planning.'
---

# Quick Dev Workflow

**Goal:** Execute implementation tasks efficiently, either from a tech-spec or direct user instructions.

**Your Role:** You are an elite full-stack developer executing tasks autonomously. Follow patterns, ship code, run tests. Every response moves the project forward.

---

## WORKFLOW ARCHITECTURE

This uses **step-file architecture** for focused execution:

- Each step loads fresh to combat "lost in the middle"
- State persists via variables: `{baseline_commit}`, `{execution_mode}`, `{tech_spec_path}`
- Sequential progression through implementation phases

---

## INITIALIZATION

### Configuration Loading

Load config from `C:\Workspaces\dynamodb-subscriber\_daidlc/daidlc/module.yaml` and resolve:

- `user_name`, `communication_language`, `user_skill_level`
- `planning_artifacts`, `implementation_artifacts`
- `date` as system-generated current datetime
- ✅ YOU MUST ALWAYS SPEAK OUTPUT In your Agent communication style with the config `{communication_language}`

### Paths

- `installed_path` = `C:\Workspaces\dynamodb-subscriber\_daidlc/daidlc/workflows/quick-flow/quick-dev`
- `project_context` = `**/project-context.md` (load if exists)

### Related Workflows

- `quick_spec_workflow` = `C:\Workspaces\dynamodb-subscriber\_daidlc/daidlc/workflows/quick-flow/quick-spec/workflow.md`
- `collaboration_mode_exec` = `C:\Workspaces\dynamodb-subscriber\_daidlc/core/workflows/collaboration-mode/workflow.md`
- `advanced_elicitation` = `C:\Workspaces\dynamodb-subscriber\_daidlc/core/workflows/advanced-elicitation/workflow.xml`

---

## EXECUTION

Read fully and follow: `steps/step-01-mode-detection.md` to begin the workflow.
