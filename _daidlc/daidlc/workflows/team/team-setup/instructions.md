# Team Setup - Define Team Structure and Module Ownership

<critical>The workflow execution engine is governed by: C:\Workspaces\dynamodb-subscriber\_daidlc/core/tasks/workflow.xml</critical>
<critical>You MUST have already loaded and processed: C:\Workspaces\dynamodb-subscriber\_daidlc/daidlc/workflows/team/team-setup/workflow.yaml</critical>
<critical>Communicate all responses in {communication_language}</critical>
<critical>Generate all documents in {document_output_language}</critical>

## Purpose

This workflow enables org-wide team management for 10-40 person development teams. It captures team structure, member roles, module ownership, and generates CODEOWNERS for GitHub-based review routing. Scout (Scrum Master) facilitates this setup to ensure clear ownership and sprint boundaries across sub-teams.

<workflow>

<step n="1" goal="Check for existing team roster and determine create vs update mode">
<action>Communicate in {communication_language} with {user_name}</action>
<action>Load {project_context} for project-wide patterns and conventions (if exists)</action>
<action>Check if {roster_file} already exists</action>

<check if="{roster_file} exists">
  <action>Load and parse {roster_file}</action>
  <action>Display current team summary to {user_name}</action>

  <output>
  **Existing Team Roster Found**

  - **Product:** {{product_name}}
  - **Last Updated:** {{updated_date}}
  - **Teams:** {{team_count}}
  - **Total Members:** {{member_count}}

  | Team | Lead | Members | Modules | Sprint |
  |------|------|---------|---------|--------|
  {{#each team}}
  | {{name}} | {{lead}} | {{member_count}} | {{module_count}} dirs | {{sprint_cadence}} |
  {{/each}}

  **Options:**
  1. **Update** - Modify existing roster (add/remove members, reassign modules)
  2. **Replace** - Start fresh with a new roster
  3. **Cancel** - Keep current roster unchanged
  </output>

  <action>WAIT for {user_name} to choose an option</action>

  <check if="user chooses Cancel">
    <action>HALT - no changes needed</action>
  </check>

  <check if="user chooses Replace">
    <action>Continue to Step 2 (start fresh)</action>
  </check>

  <check if="user chooses Update">
    <action>Ask {user_name} what they want to update: add team, remove team, modify members, reassign modules, change cadence</action>
    <action>WAIT for {user_name} to describe changes</action>
    <action>Apply changes to existing roster data and skip to Step 4 for regeneration</action>
  </check>
</check>

<check if="{roster_file} does NOT exist">
  <output>
  No existing team roster found. Starting fresh setup.

  This workflow will:
  1. Collect org-level product information
  2. Define sub-teams with members and roles
  3. Assign module ownership to each team
  4. Generate `team-roster.yaml`, `module-owners.yaml`, and `CODEOWNERS`
  </output>

  <action>Continue to Step 2</action>
</check>
</step>

<step n="2" goal="Collect org-level product information">

<output>
**Product & Organization Setup**

I need some high-level information about your product and team structure.
</output>

<action>Ask {user_name} for the following information:</action>

<output>
**Question 1:** What is the product name?
(e.g., "ERP System", "Customer Portal", "Data Platform")

**Question 2:** What is the total team size? (number of people across all sub-teams)
(Supported range: 10-40 for org-wide team management)

**Question 3:** How many sub-teams should the product be split into?
(Typical: 2-6 sub-teams, each owning specific modules or domains)

Provide your answers below or respond conversationally.
</output>

<action>WAIT for {user_name} to provide product name, team size, and sub-team count</action>

<action>Validate inputs:</action>
- Product name must be non-empty
- Team size should be between 1 and 100 (warn if outside 10-40 range)
- Sub-team count should be between 1 and 10 (warn if team-size / sub-teams < 2)

<action>Store validated values:</action>
- {{product_name}} = user-provided product name
- {{total_team_size}} = user-provided team size
- {{sub_team_count}} = user-provided sub-team count

<output>
**Confirmed:**
- Product: {{product_name}}
- Total Size: {{total_team_size}} members
- Sub-teams: {{sub_team_count}}

Now let's define each sub-team.
</output>
</step>

<step n="3" goal="Collect sub-team details for each team">

<action>For each sub-team (1 through {{sub_team_count}}), collect the following information from {user_name}:</action>

<output>
**Sub-Team {{team_index}} of {{sub_team_count}}**

Please provide the following for this sub-team:

**Team Name:** (e.g., "Team Alpha", "Auth Squad", "Platform Core")

**Team Lead:** (name of the person leading this team)

**Members:** List each member with their role and IDE preference.

Supported roles: `pm` | `developer` | `qa` | `sm` | `architect` | `writer`
Supported IDEs: `claude-code` | `other`

Format (one per line):
```
name, role, ide
```

Example:
```
alice, developer, claude-code
bob, developer, claude-code
carol, qa, claude-code
```

**Module Ownership:** Which `src/` directories does this team own?
(One directory path per line, e.g., `src/modules/auth/`, `src/modules/rbac/`)

**Sprint Cadence:** How long are this team's sprints?
- A) 1-week
- B) 2-week
- C) 3-week

**Story Prefix:** What prefix should stories from this team use?
(e.g., "1-" for Team 1, "A-" for Team Alpha, "AUTH-" for Auth Squad)
</output>

<action>WAIT for {user_name} to provide sub-team details</action>

<action>Validate sub-team data:</action>
- Team name must be non-empty and unique across all sub-teams
- Team lead must be listed as one of the members
- Each member must have a valid role (pm, developer, qa, sm, architect, writer)
- Each member must have an IDE specified
- At least one module must be assigned
- Sprint cadence must be 1-week, 2-week, or 3-week
- Story prefix must be unique across all sub-teams

<check if="validation errors found">
  <output>
  **Validation Issues:**
  {{list_validation_errors}}

  Please correct and resubmit.
  </output>

  <action>WAIT for {user_name} to fix validation issues</action>
</check>

<action>Store validated sub-team data in {{teams}} array</action>

<check if="more sub-teams remain">
  <action>Repeat Step 3 for next sub-team</action>
</check>

<action>After all sub-teams collected, verify totals:</action>

<output>
**Team Setup Summary**

| Team | Lead | Members | Modules | Sprint | Prefix |
|------|------|---------|---------|--------|--------|
{{#each teams}}
| {{name}} | {{lead}} | {{members.length}} | {{modules.length}} dirs | {{sprint_cadence}} | {{story_prefix}} |
{{/each}}

**Total Members:** {{total_members}} (expected: {{total_team_size}})

{{#if total_members != total_team_size}}
**Warning:** Total members ({{total_members}}) does not match declared team size ({{total_team_size}}). Proceed anyway?
{{/if}}

**Module Coverage Check:**
{{list_all_modules_with_owners}}

{{#if modules_without_owners}}
**Warning:** The following directories have no team ownership:
{{list_unowned_modules}}
{{/if}}

{{#if modules_with_multiple_owners}}
**Warning:** The following directories are owned by multiple teams:
{{list_shared_modules}}
(Shared ownership is allowed but review routing will use the first listed team.)
{{/if}}

Does this look correct? Confirm to proceed with file generation.
</output>

<action>WAIT for {user_name} to confirm or request changes</action>

<check if="user requests changes">
  <action>Apply changes and re-display summary</action>
  <action>WAIT for confirmation again</action>
</check>
</step>

<step n="4" goal="Generate team-roster.yaml">
<action>Ensure {team_dir} directory exists, create if needed</action>

<action>Generate {roster_file} with the following YAML structure:</action>

```yaml
# D-AIDLC Team Roster
# Generated: {date}
# Product: {{product_name}}

product: "{{product_name}}"
created: "{date}"
updated: "{date}"

teams:
  - name: "{{team_1_name}}"
    lead: "{{team_1_lead}}"
    sprint_cadence: "{{team_1_cadence}}"
    members:
      - name: "{{member_name}}"
        role: {{member_role}}
        ide: {{member_ide}}
      # ... additional members
    modules:
      - {{module_path_1}}
      - {{module_path_2}}
    story_prefix: "{{team_1_prefix}}"

  # ... additional teams
```

<action>Write {roster_file}</action>

<output>
**Generated:** {roster_file}
</output>
</step>

<step n="5" goal="Generate module-owners.yaml">
<action>Build a directory-to-team mapping from all team module assignments</action>

<action>Generate {module_owners_file} with the following YAML structure:</action>

```yaml
# D-AIDLC Module Ownership Map
# Generated: {date}
# Product: {{product_name}}
#
# Maps source directories to owning teams.
# Used by D-AIDLC workflows for routing reviews, assigning stories,
# and determining which team handles changes in a given area.

product: "{{product_name}}"
generated: "{date}"

modules:
  {{module_path_1}}:
    team: "{{owning_team_name}}"
    lead: "{{owning_team_lead}}"
    reviewers:
      - "{{team_member_1}}"
      - "{{team_member_2}}"

  {{module_path_2}}:
    team: "{{owning_team_name}}"
    lead: "{{owning_team_lead}}"
    reviewers:
      - "{{team_member_1}}"

  # ... additional modules
```

<action>For each module, the reviewers list should include all developers and architects from the owning team</action>
<action>Write {module_owners_file}</action>

<output>
**Generated:** {module_owners_file}
</output>
</step>

<step n="6" goal="Generate CODEOWNERS file in GitHub format">
<action>Generate {codeowners_file} in GitHub CODEOWNERS format</action>

<action>The CODEOWNERS file maps file paths to GitHub usernames or team handles for automatic review assignment</action>

<action>Generate {codeowners_file} with the following format:</action>

```
# D-AIDLC Generated CODEOWNERS
# Generated: {date}
# Product: {{product_name}}
#
# This file is auto-generated by the D-AIDLC team-setup workflow.
# It maps source directories to team members for automatic PR review assignment.
# Move this file to the repository root or .github/ directory to activate.
#
# Format: <pattern> <owner1> <owner2> ...
# GitHub docs: https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/customizing-your-repository/about-code-owners

# {{team_1_name}}
{{module_path_1}}    @{{team_1_lead}} @{{team_1_developer_1}} @{{team_1_developer_2}}
{{module_path_2}}    @{{team_1_lead}} @{{team_1_developer_1}}

# {{team_2_name}}
{{module_path_3}}    @{{team_2_lead}} @{{team_2_developer_1}}

# ... additional teams and modules
```

<action>Include only members with roles: developer, architect, lead (skip pm, qa, sm, writer for code review routing)</action>
<action>Team lead is always listed first as a reviewer</action>
<action>Write {codeowners_file}</action>

<output>
**Generated:** {codeowners_file}

**Note:** To activate CODEOWNERS, copy this file to your repository root or `.github/` directory:
```
cp aidlc-docs/team/CODEOWNERS .github/CODEOWNERS
```
</output>
</step>

<step n="7" goal="Configure git workflow and branching strategy">

<output>
**Git Workflow Configuration**

How does your team manage branches and PRs? Choose the strategy that best fits your team, or skip to configure later.

1. **GitHub Flow** (Recommended for small teams / simple projects)
   - `main` is always deployable
   - Developers branch from `main`, open PR back to `main`
   - No `develop` or `release` branches — simple and fast

2. **Git Flow** (Recommended for larger teams / release cycles)
   - `main` ← production releases (tagged)
   - `develop` ← integration branch
   - `release/vX.Y` ← release candidates
   - `story/{story-id}` ← per-story feature branches
   - `bugfix/{story-id}-{desc}` ← bug fixes
   - `hotfix/{desc}` ← emergency production fixes

3. **Trunk-Based Development** (For CI/CD-heavy teams)
   - `main` is the only long-lived branch
   - Very short-lived feature branches (< 1 day)
   - Feature flags for incomplete work

4. **Custom** — I'll describe our branching strategy

5. **Skip** — I'll configure git workflow separately
</output>

<action>WAIT for {user_name} to choose an option</action>

<check if="user chooses Skip">
  <action>Record git_workflow_configured = false</action>
  <action>Skip to Step 8 (no git workflow files generated)</action>
</check>

<check if="user chooses GitHub Flow (option 1)">
  <action>Generate {git_workflow_file} using {template_git_workflow} with these values:</action>

  <action>Variables:
    - strategy_name: "GitHub Flow"
    - strategy_description: "Simple branch-per-feature model. `main` is always deployable. Developers create feature branches from `main` and open PRs back to `main`. No long-lived integration branches."
    - branch_diagram:
      ```
      main ─────●─────●─────●─────●───── (always deployable)
                 \   /       \   /
                  ●─●         ●─●
              story/1-1   story/2-1
      ```
    - branch_conventions:
      | Story branch | `story/{story-id}` | `story/1-2-user-auth` | `main` | `main` |
      | Bug fix | `bugfix/{story-id}-{desc}` | `bugfix/1-2-login-crash` | `main` | `main` |
      | Hotfix | `hotfix/{desc}` | `hotfix/security-patch` | `main` | `main` |
    - base_branch: main
    - pr_target_branch: main
    - protected_branch: main
    - min_approvals: 1
    - merge_method: "Squash and merge (recommended for clean history)"
    - additional_protection_rules: ""
  </action>
</check>

<check if="user chooses Git Flow (option 2)">
  <action>Generate {git_workflow_file} using {template_git_workflow} with these values:</action>

  <action>Variables:
    - strategy_name: "Git Flow"
    - strategy_description: "Branch model with dedicated integration and release branches. `main` contains production releases. `develop` is the integration branch where features merge. Release branches stabilize before merging to `main`."
    - branch_diagram:
      ```
      main ─────────────────●───────────●───── (production releases)
                           / \         / \
      release/v1.0 ──────●   \       ●   \
                         /     \     /     \
      develop ──●──●──●──●──●──●──●──●──●──── (integration)
                 \  / \  /     \  /
                  ●●   ●●      ●●
             story/1-1  story/1-2  story/2-1
      ```
    - branch_conventions:
      | Story branch | `story/{story-id}` | `story/1-2-user-auth` | `develop` | `develop` |
      | Bug fix | `bugfix/{story-id}-{desc}` | `bugfix/1-2-login-crash` | `develop` | `develop` |
      | Release | `release/v{X.Y}` | `release/v1.0` | `develop` | `main` + `develop` |
      | Hotfix | `hotfix/{desc}` | `hotfix/security-patch` | `main` | `main` + `develop` |
    - base_branch: develop
    - pr_target_branch: develop
    - protected_branch: main and develop
    - min_approvals: 1 (2 recommended for main)
    - merge_method: "Merge commit for develop, squash for main"
    - additional_protection_rules: |
      ### `develop` branch:
      - Require pull request before merging
      - Require 1 approval
      - Require status checks to pass
  </action>
</check>

<check if="user chooses Trunk-Based (option 3)">
  <action>Generate {git_workflow_file} using {template_git_workflow} with these values:</action>

  <action>Variables:
    - strategy_name: "Trunk-Based Development"
    - strategy_description: "All development happens on or very close to `main`. Feature branches are short-lived (ideally < 1 day). Incomplete features use feature flags. Continuous integration is essential."
    - branch_diagram:
      ```
      main ──●──●──●──●──●──●──●──●──●──── (trunk, always green)
              \/ \/ \/     \/ \/
              ●  ●  ●      ●  ●
           (short-lived feature branches, < 1 day)
      ```
    - branch_conventions:
      | Story branch | `story/{story-id}` | `story/1-2-user-auth` | `main` | `main` |
      | Bug fix | `fix/{desc}` | `fix/login-crash` | `main` | `main` |
    - base_branch: main
    - pr_target_branch: main
    - protected_branch: main
    - min_approvals: 1
    - merge_method: "Squash and merge (keeps trunk clean)"
    - additional_protection_rules: |
      ### Additional Notes:
      - Feature branches should be merged within 1 day
      - Use feature flags for incomplete work
      - CI must pass before merge — no exceptions
  </action>
</check>

<check if="user chooses Custom (option 4)">
  <action>Ask {user_name} to describe their branching strategy:</action>

  <output>
  **Custom Branching Strategy**

  Please describe your team's branching strategy. I'll generate a `git-workflow.md` based on your description.

  What I need to know:
  1. What long-lived branches do you have? (e.g., main, develop, staging)
  2. Where do developers branch FROM for new stories?
  3. Where do PRs target? (which branch do PRs merge into?)
  4. Do you use release branches? If so, what's the naming convention?
  5. How do you handle hotfixes?
  6. Preferred merge method? (squash, merge commit, rebase)
  7. Minimum PR approvals required?
  </output>

  <action>WAIT for {user_name} to describe their strategy</action>
  <action>Generate {git_workflow_file} using {template_git_workflow} with user-provided values</action>
  <action>The generated document should reflect THEIR strategy, not a predefined one</action>
</check>

<check if="git workflow was generated (not skipped)">
  <action>Generate {pr_template_file} using {template_pr} as the base template</action>
  <action>Ensure the `.github/` directory exists, create if needed</action>
  <action>Write {pr_template_file}</action>

  <output>
  **Git Workflow Configured**

  **Generated:**
  - `{git_workflow_file}` — Branching strategy and developer guide
  - `{pr_template_file}` — Pull request template with story linkage

  These files are **starting points** — customize them as your team's workflow evolves.
  </output>
</check>
</step>

<step n="8" goal="Log to audit trail">
  <action>Log the following to the audit trail:
    - Timestamp: {date}
    - Action: Team setup completed
    - Product: {{product_name}}
    - Teams: {{sub_team_count}}
    - Total members: {{total_members}}
    - Git workflow: {{strategy_name or "skipped"}}
    - Files generated: roster, module-owners, CODEOWNERS, git-workflow (if configured), PR template (if configured)
  </action>
</step>

<step n="9" goal="Present summary and request approval">

<output>
**Team Setup Complete**

**Files Generated:**
1. `{roster_file}` — Full team roster with members, roles, and module ownership
2. `{module_owners_file}` — Directory-to-team mapping for routing and assignment
3. `{codeowners_file}` — GitHub CODEOWNERS for automatic PR review assignment
{{#if git_workflow_configured}}
4. `{git_workflow_file}` — Branching strategy: {{strategy_name}}
5. `{pr_template_file}` — Pull request template with story linkage
{{/if}}

**Team Overview:**

| Team | Lead | Members | Modules | Sprint | Prefix |
|------|------|---------|---------|--------|--------|
{{#each teams}}
| {{name}} | {{lead}} | {{members.length}} | {{modules.length}} dirs | {{sprint_cadence}} | {{story_prefix}} |
{{/each}}

**Total:** {{total_members}} members across {{sub_team_count}} teams

**Module Coverage:** {{covered_module_count}} directories assigned to teams

{{#if git_workflow_configured}}
**Git Strategy:** {{strategy_name}}
**PR Target:** `{{pr_target_branch}}`
**Story Branch Convention:** `story/{story-id}`
{{/if}}

**Next Steps:**
1. Review the generated files
2. Copy CODEOWNERS to `.github/CODEOWNERS` to activate GitHub review routing
{{#if git_workflow_configured}}
3. Review `{git_workflow_file}` — customize for your team if needed
4. The PR template is already at `.github/pull-request-template.md` and will auto-populate new PRs
{{/if}}
5. Use the team roster during sprint planning to assign stories by module ownership
6. Run this workflow again (`[TEAM] Team Setup`) to update the roster as the team evolves
</output>

<action>WAIT for {user_name} to review and approve</action>

<check if="user requests changes">
  <action>Apply changes to the relevant files</action>
  <action>Re-display summary</action>
  <action>WAIT for approval</action>
</check>

<check if="user approves">
  <output>
  **Team Setup Approved**

  Your team structure is now defined and ready for use across D-AIDLC workflows.
  Sprint planning, story creation, and code reviews will reference these ownership mappings.
  {{#if git_workflow_configured}}
  Developers can refer to `{git_workflow_file}` for branching and PR guidelines.
  {{/if}}
  </output>
</check>
</step>

</workflow>

## Additional Notes

### Role Definitions

| Role | Description |
|------|-------------|
| `pm` | Product Manager - owns requirements and priorities |
| `developer` | Developer - implements features and fixes |
| `qa` | QA Engineer - testing, quality assurance |
| `sm` | Scrum Master - sprint management, ceremonies |
| `architect` | Architect - system design, technical decisions |
| `writer` | Technical Writer - documentation, API docs |

### IDE Support

The `ide` field tracks which AI-assisted IDE each team member uses. This helps D-AIDLC optimize workflow instructions and context delivery:

| IDE | Description |
|-----|-------------|
| `claude-code` | Anthropic Claude Code CLI |
| `other` | Other IDE or no AI assistance |

### Module Ownership Rules

1. **Single ownership preferred** - Each directory should ideally be owned by exactly one team
2. **Shared ownership allowed** - Multiple teams can own the same directory, but the first listed team is primary
3. **Nested ownership** - A team owning `src/modules/auth/` implicitly owns all subdirectories
4. **Coverage gaps** - Directories without owners will not have automatic review routing
