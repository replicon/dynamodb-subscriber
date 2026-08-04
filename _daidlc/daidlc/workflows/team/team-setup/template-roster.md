# Team Roster Template

This template shows the expected structure for `aidlc-docs/team/team-roster.yaml`.

## team-roster.yaml

```yaml
# D-AIDLC Team Roster
# Generated: 2026-02-20
# Product: ERP System

product: "ERP System"
created: "2026-02-20"
updated: "2026-02-20"

teams:
  - name: "Team Alpha"
    lead: "alice"
    sprint_cadence: "2-week"
    members:
      - name: "alice"
        role: developer
        ide: claude-code
      - name: "bob"
        role: developer
        ide: claude-code
      - name: "carol"
        role: qa
        ide: claude-code
      - name: "dave"
        role: architect
        ide: claude-code
      - name: "eve"
        role: pm
        ide: claude-code
    modules:
      - src/modules/auth/
      - src/modules/rbac/
      - src/modules/user-management/
    story_prefix: "1-"

  - name: "Team Beta"
    lead: "frank"
    sprint_cadence: "2-week"
    members:
      - name: "frank"
        role: developer
        ide: claude-code
      - name: "grace"
        role: developer
        ide: claude-code
      - name: "heidi"
        role: developer
        ide: claude-code
      - name: "ivan"
        role: qa
        ide: claude-code
      - name: "judy"
        role: sm
        ide: claude-code
    modules:
      - src/modules/billing/
      - src/modules/invoicing/
      - src/modules/payments/
    story_prefix: "2-"

  - name: "Platform Core"
    lead: "karl"
    sprint_cadence: "3-week"
    members:
      - name: "karl"
        role: architect
        ide: claude-code
      - name: "liam"
        role: developer
        ide: claude-code
      - name: "mia"
        role: developer
        ide: claude-code
      - name: "nina"
        role: qa
        ide: claude-code
    modules:
      - src/core/
      - src/shared/
      - src/infrastructure/
    story_prefix: "P-"
```

## module-owners.yaml

```yaml
# D-AIDLC Module Ownership Map
# Generated: 2026-02-20
# Product: ERP System

product: "ERP System"
generated: "2026-02-20"

modules:
  src/modules/auth/:
    team: "Team Alpha"
    lead: "alice"
    reviewers:
      - "alice"
      - "bob"
      - "dave"

  src/modules/rbac/:
    team: "Team Alpha"
    lead: "alice"
    reviewers:
      - "alice"
      - "bob"
      - "dave"

  src/modules/user-management/:
    team: "Team Alpha"
    lead: "alice"
    reviewers:
      - "alice"
      - "bob"
      - "dave"

  src/modules/billing/:
    team: "Team Beta"
    lead: "frank"
    reviewers:
      - "frank"
      - "grace"
      - "heidi"

  src/modules/invoicing/:
    team: "Team Beta"
    lead: "frank"
    reviewers:
      - "frank"
      - "grace"
      - "heidi"

  src/modules/payments/:
    team: "Team Beta"
    lead: "frank"
    reviewers:
      - "frank"
      - "grace"
      - "heidi"

  src/core/:
    team: "Platform Core"
    lead: "karl"
    reviewers:
      - "karl"
      - "liam"
      - "mia"

  src/shared/:
    team: "Platform Core"
    lead: "karl"
    reviewers:
      - "karl"
      - "liam"
      - "mia"

  src/infrastructure/:
    team: "Platform Core"
    lead: "karl"
    reviewers:
      - "karl"
      - "liam"
      - "mia"
```

## CODEOWNERS

```
# D-AIDLC Generated CODEOWNERS
# Generated: 2026-02-20
# Product: ERP System

# Team Alpha
src/modules/auth/       @alice @bob @dave
src/modules/rbac/       @alice @bob @dave
src/modules/user-management/  @alice @bob @dave

# Team Beta
src/modules/billing/    @frank @grace @heidi
src/modules/invoicing/  @frank @grace @heidi
src/modules/payments/   @frank @grace @heidi

# Platform Core
src/core/               @karl @liam @mia
src/shared/             @karl @liam @mia
src/infrastructure/     @karl @liam @mia
```

## Field Reference

### Roles
| Value | Description |
|-------|-------------|
| `pm` | Product Manager |
| `developer` | Software Developer |
| `qa` | QA Engineer |
| `sm` | Scrum Master |
| `architect` | Software Architect |
| `writer` | Technical Writer |

### IDEs
| Value | Description |
|-------|-------------|
| `claude-code` | Anthropic Claude Code CLI |
| `other` | Other IDE |

### Sprint Cadence
| Value | Description |
|-------|-------------|
| `1-week` | Weekly sprints |
| `2-week` | Bi-weekly sprints (most common) |
| `3-week` | Three-week sprints |
