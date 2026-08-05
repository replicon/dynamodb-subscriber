# Publish Stories — GitHub Issue Sync & Project Board Workflow

<critical>The workflow execution engine is governed by: C:\Workspaces\dynamodb-subscriber\_daidlc/core/tasks/workflow.xml</critical>
<critical>You MUST have already loaded and processed: C:\Workspaces\dynamodb-subscriber\_daidlc/daidlc/workflows/team/publish-stories/workflow.yaml</critical>
<critical>Communicate all responses in {communication_language}</critical>
<critical>Generate all documents in {document_output_language}</critical>

<workflow>

<step n="1" goal="Verify gh CLI is authenticated">
  <action>Run `gh auth status` to verify the GitHub CLI is installed and authenticated</action>

  <action>If `gh` is not found or not authenticated:
    - STOP and inform the user:
      "GitHub CLI (`gh`) is required for publishing stories to GitHub Issues.
       Install: https://cli.github.com/
       Then run: `gh auth login`"
    - Do NOT proceed without `gh` authentication.
  </action>

  <action>Run `gh repo view --json nameWithOwner -q .nameWithOwner` to get the current repo name (e.g., `OrgName/RepoName`)</action>
  <action>Split into owner and repo parts for API calls</action>

  <action>If not in a git repo or no remote configured:
    - STOP and inform the user:
      "This directory is not a GitHub repository. Please initialize a repo and push to GitHub first."
    - Do NOT proceed.
  </action>

  <output>
    ## GitHub Authentication Verified

    **Repository:** {{repo_name}}
    **Authenticated as:** {{gh_username}}
  </output>
</step>

<step n="2" goal="Load sprint status and story files">
  <action>Load sprint status from {sprint_status_file}</action>

  <action>If {sprint_status_file} does not exist:
    - STOP and inform the user:
      "Sprint status not found at {sprint_status_file}. Run Sprint Planning first (/daidlc-agent-scrum-master → SP)."
    - Do NOT proceed.
  </action>

  <action>Extract all stories from the `development_status` section</action>
  <action>For each story entry (skip entries starting with `epic-` and `*-retrospective`):
    - Parse the story key: `{{epic_num}}-{{story_num}}-{{story_title}}`
    - Record the current status (backlog, ready-for-dev, in-progress, review, done)
  </action>

  <action>For each story, check if a story file exists at {story_dir}/{{story_key}}.md</action>
  <action>If the story file exists, extract:
    - Full title from the `# Story` heading
    - User story text (As a... I want... so that...)
    - Acceptance criteria (numbered list)
    - Tasks/subtasks (checkbox list)
    - Dev notes (if present)
  </action>

  <output>
    ## Sprint Status Loaded

    **Stories found:** {{total_stories}}
    **With story files:** {{stories_with_files}}
    **Without story files (backlog only):** {{stories_without_files}}
  </output>
</step>

<step n="3" goal="Load assignments and team roster for enrichment">
  <action>Check if {assignments_file} exists</action>
  <action>If it exists, load assignments and build a map: story_key → developer_github_username</action>

  <action>Check if {roster_file} exists</action>
  <action>If it exists, load roster and build a map: developer_name → github_username</action>

  <action>Cross-reference assignments with roster to get GitHub usernames for assignees</action>

  <output>
    ## Assignment Data Loaded

    **Assignments found:** {{assignment_count}}
    **Team members with GitHub usernames:** {{members_with_github}}
  </output>
</step>

<step n="4" goal="Load existing issue map for idempotent updates">
  <action>Check if {issue_map_file} exists</action>

  <check if="{issue_map_file} exists">
    <action>Load the issue map — this maps story keys to GitHub Issue numbers and stores the project board ID</action>
    <action>For stories that already have issue numbers, we will UPDATE instead of CREATE</action>
    <action>If `project_number` exists in the map, we will reuse the existing project board</action>
    <output>
      ## Existing Issue Map Found

      **Previously published stories:** {{existing_count}}
      **New stories to publish:** {{new_count}}
      **Project board:** {{project_number or "will be created"}}
    </output>
  </check>

  <check if="{issue_map_file} does NOT exist">
    <action>No previous issue map — all stories will be created as new issues and a new project board will be created</action>
    <output>
      ## No Existing Issue Map

      All {{total_stories}} stories will be created as new GitHub Issues.
      A new GitHub Project board will be created.
    </output>
  </check>
</step>

<step n="5" goal="Ensure GitHub labels exist">
  <action>Create the following labels in the repo if they do not already exist. Use `gh label create` with `--force` to skip errors for existing labels:</action>

  <action>Status labels (color: status-themed):
    - `status:backlog` (color: `CCCCCC`, description: "Story is in the backlog")
    - `status:ready-for-dev` (color: `0E8A16`, description: "Story is ready for development")
    - `status:in-progress` (color: `FBCA04`, description: "Story is being implemented")
    - `status:review` (color: `1D76DB`, description: "Story is in code review")
    - `status:done` (color: `6F42C1`, description: "Story is complete")
  </action>

  <action>D-AIDLC label:
    - `d-aidlc` (color: `F97316`, description: "Managed by D-AIDLC framework")
  </action>

  <action>Story point labels (color: `E4E669`):
    - `points:1`, `points:2`, `points:3`, `points:5`, `points:8`, `points:13`
  </action>

  <action>Use this command pattern for each label:
    `gh label create "label-name" --color "HEXCOLOR" --description "desc" --force`
  </action>

  <output>
    ## Labels Configured

    Created/verified {{label_count}} labels in {{repo_name}}.
  </output>
</step>

<step n="6" goal="Create or load GitHub Project board">
  <action>Check if {issue_map_file} contains a `project_number` field</action>

  <check if="project_number exists in issue map">
    <action>Verify the project still exists:
      `gh project view {{project_number}} --owner {{owner}} --format json`
    </action>
    <action>If project exists, reuse it. Record the project number.</action>
    <action>If project was deleted, proceed to create a new one (below).</action>
  </check>

  <check if="no project_number in issue map OR project was deleted">
    <action>Create a new GitHub Project board:
      `gh project create --owner {{owner}} --title "D-AIDLC: {project_name}" --format json`
    </action>
    <action>Capture the project number from the output</action>
  </check>

  <action>Get the project's "Status" field ID and its option IDs:
    `gh project field-list {{project_number}} --owner {{owner}} --format json`
  </action>

  <action>Look for the built-in "Status" single-select field. Record:
    - field_id: the Status field's ID
    - option_ids: map of option name → option ID
  </action>

  <action>Check if the Status field has these options. If any are missing, add them using the GraphQL API.
    The D-AIDLC status columns should be:
    - **Backlog** — stories not yet ready for dev
    - **Ready for Dev** — stories prepared with full context
    - **In Progress** — developer actively working
    - **Review** — code review / QA
    - **Done** — story complete

    To add a missing option, use:
    `gh api graphql -f query='mutation { updateProjectV2Field(input: { projectId: "{{project_node_id}}", fieldId: "{{status_field_id}}", singleSelectOptions: [{{existing_options}}, {name: "{{new_option}}", color: "{{color}}"}] }) { projectV2Field { ... on ProjectV2SingleSelectField { options { id name } } } } }'`

    If the project already has default columns like "Todo" / "In Progress" / "Done", rename or map them:
    - "Todo" → use for "Backlog" stories
    - Keep "In Progress" as-is
    - Keep "Done" as-is
    - Add "Ready for Dev" and "Review" as new columns

    Record the final mapping: D-AIDLC status → project status option ID
  </action>

  <output>
    ## GitHub Project Board Ready

    **Project:** D-AIDLC: {project_name}
    **Project Number:** {{project_number}}
    **Status Columns:** Backlog | Ready for Dev | In Progress | Review | Done
  </output>
</step>

<step n="7" goal="Publish each story as a GitHub Issue">
  <action>For each story in the sprint status, process in order (epic 1 stories first, then epic 2, etc.):</action>

  <action>**Build the issue title:**
    `[Story {{epic_num}}.{{story_num}}] {{story_title_humanized}}`
    Example: `[Story 1.2] User Authentication`
  </action>

  <action>**Build the issue body** using this template:

    ```markdown
    ## Story {{epic_num}}.{{story_num}}: {{story_title}}

    **Epic:** {{epic_num}} — {{epic_name}}
    **Status:** {{status}}
    **Story Points:** {{points}}
    **Assigned To:** {{developer_name}} (@{{github_username}})
    **Branch:** `story/{{story_key}}`

    ---

    ### User Story

    {{user_story_text_or_"See story file for details"}}

    ### Acceptance Criteria

    {{acceptance_criteria_as_checklist_or_"To be defined when story is created"}}

    ### Tasks

    {{tasks_checklist_or_"To be defined when story is created"}}

    ---

    > **D-AIDLC Story File:** `{{story_file_path}}`
    > Generated by [D-AIDLC](https://github.com/DLTKGouravk04/D-AIDLC-V20) framework
    ```
  </action>

  <action>**Build the labels list:**
    - Always add: `d-aidlc`
    - Add status label: `status:{{status}}`
    - Add epic label: `epic:{{epic_num}}`  (create dynamically if needed with color `D4C5F9`)
    - Add points label: `points:{{story_points}}` (if known)
    - Add team label: `team:{{sub_team_name}}` (if from roster, create dynamically with color `BFD4F2`)
  </action>

  <action>**Determine assignee:**
    - From {assignments_file}: find the developer assigned to this story
    - From {roster_file}: get their GitHub username
    - If no assignment or no GitHub username, leave unassigned
  </action>

  <action>**Create or Update the issue:**

    **If story key is NOT in the issue map (new issue):**
    Run: `gh issue create --title "{{title}}" --body "{{body}}" --label "{{labels}}" [--assignee "{{github_username}}"]`
    Capture the issue number from the output.

    **If story key IS in the issue map (update existing):**
    Run: `gh issue edit {{issue_number}} --title "{{title}}" --body "{{body}}" --add-label "{{new_labels}}" --remove-label "{{old_status_label}}" [--add-assignee "{{github_username}}"]`

    **If story status is "done":**
    After creating/updating, close the issue:
    `gh issue close {{issue_number}} --reason completed`

    **If story was previously "done" but status changed back:**
    Reopen the issue:
    `gh issue reopen {{issue_number}}`
  </action>

  <action>After each issue is created/updated, record the mapping: story_key → issue_number</action>

  <action>Display progress after each story:
    `Published {{current}}/{{total}}: [Story {{epic}}.{{story}}] {{title}} → #{{issue_number}}`
  </action>
</step>

<step n="8" goal="Add issues to the GitHub Project board and set status">
  <action>For each published issue, add it to the project board and set its status column:</action>

  <action>**Add issue to project (if not already added):**
    `gh project item-add {{project_number}} --owner {{owner}} --url https://github.com/{{owner}}/{{repo}}/issues/{{issue_number}} --format json`
    Capture the item ID from the output.
  </action>

  <action>**Set the Status field for the item:**
    Map the D-AIDLC story status to the project board column:
    - `backlog` → **Backlog** option
    - `ready-for-dev` → **Ready for Dev** option
    - `in-progress` → **In Progress** option
    - `review` → **Review** option
    - `done` → **Done** option

    Use:
    `gh project item-edit --project-id {{project_node_id}} --id {{item_id}} --field-id {{status_field_id}} --single-select-option-id {{option_id}}`
  </action>

  <action>Display progress:
    `Board: {{current}}/{{total}}: #{{issue_number}} → {{status_column}}`
  </action>
</step>

<step n="9" goal="Create epic-level milestones and assign issues">
  <action>For each unique epic found in the stories:</action>

  <action>Check if a milestone named `Epic {{epic_num}}: {{epic_name}}` exists:
    `gh api repos/{{owner}}/{{repo}}/milestones --jq '.[] | select(.title | startswith("Epic {{epic_num}}"))' `
  </action>

  <action>If milestone does not exist, create it:
    `gh api repos/{{owner}}/{{repo}}/milestones --method POST -f title="Epic {{epic_num}}: {{epic_name}}" -f state="open" -f description="D-AIDLC Epic {{epic_num}}"`
  </action>

  <action>Assign all issues for this epic to the milestone:
    `gh issue edit {{issue_number}} --milestone "Epic {{epic_num}}: {{epic_name}}"`
  </action>
</step>

<step n="10" goal="Write the issue map file for idempotent future runs">
  <action>Write the issue map to {issue_map_file} with this structure:</action>

  <action>YAML structure:
    ```yaml
    # D-AIDLC GitHub Issue & Project Map
    # generated: {date}
    # project: {project_name}
    # repository: {{repo_name}}
    #
    # This file maps D-AIDLC story keys to GitHub Issue numbers and tracks
    # the GitHub Project board. Do NOT edit manually — maintained by the
    # publish-stories workflow.
    # Running publish-stories again will update existing issues and board status.

    repository: "{{repo_name}}"
    last_published: "{date}"

    # GitHub Project board
    project:
      number: {{project_number}}
      node_id: "{{project_node_id}}"
      title: "D-AIDLC: {project_name}"
      url: "https://github.com/orgs/{{owner}}/projects/{{project_number}}"
      status_field_id: "{{status_field_id}}"
      status_options:
        backlog: "{{option_id}}"
        ready-for-dev: "{{option_id}}"
        in-progress: "{{option_id}}"
        review: "{{option_id}}"
        done: "{{option_id}}"

    # Story → GitHub Issue mapping
    issues:
      {{story_key_1}}: {{issue_number_1}}
      {{story_key_2}}: {{issue_number_2}}
      # ... one entry per published story

    # Story → Project board item mapping
    project_items:
      {{story_key_1}}: "{{item_id_1}}"
      {{story_key_2}}: "{{item_id_2}}"
      # ... one entry per story on the board

    # Epic → Milestone mapping
    milestones:
      epic-{{num}}: {{milestone_id}}
      # ... one entry per epic milestone
    ```
  </action>

  <action>Write the file to {default_output_file}</action>
</step>

<step n="11" goal="Log to audit trail">
  <action>Log the following to the audit trail:
    - Timestamp: {date}
    - Action: Published stories to GitHub Issues & Project Board
    - Repository: {{repo_name}}
    - Project board: #{{project_number}}
    - Stories created: {{created_count}}
    - Stories updated: {{updated_count}}
    - Stories closed: {{closed_count}}
    - Board items updated: {{board_items_count}}
    - Milestones created: {{milestones_created}}
    - Issue map saved to: {issue_map_file}
  </action>
</step>

<step n="12" goal="Present publish summary">
  <output>
    # D-AIDLC: Stories Published to GitHub

    ## Summary

    | Metric | Count |
    |--------|-------|
    | Issues created | {{created_count}} |
    | Issues updated | {{updated_count}} |
    | Issues closed (done) | {{closed_count}} |
    | Board items set | {{board_items_count}} |
    | Milestones created | {{milestones_created}} |
    | Total published | {{total_published}} |

    ## GitHub Project Board

    **Project:** D-AIDLC: {project_name}
    **URL:** {{project_url}}

    | Backlog | Ready for Dev | In Progress | Review | Done |
    |---------|---------------|-------------|--------|------|
    {{board_column_counts}}

    The Scrum Master and engineering leaders can open the project board URL above
    to see a Kanban view of all stories with their current status, assignees,
    and epic milestones.

    ## Published Issues

    | Story | GitHub Issue | Status | Assignee | Milestone |
    |-------|-------------|--------|----------|-----------|
    {{published_issues_table}}

    ## Who Should Run This and When

    | Who | When | Why |
    |-----|------|-----|
    | **PM (Parker)** | After creating/updating epics and stories | Initial publish, new stories |
    | **SM (Scout)** | During sprint, after status changes | Sync progress to board |
    | **SM (Scout)** | After story assignments change | Update assignees on issues |

    ## Re-running This Workflow

    This workflow is **idempotent**. Running it again will:
    - Update existing issues (new status, assignment changes, content updates)
    - Move issues to the correct column on the project board
    - Create issues only for new stories
    - Close issues for stories marked "done"
    - Never create duplicate issues or board items

    **Artifacts Created/Updated:**
    - {issue_map_file}

    > **REVIEW REQUIRED:** Check the project board on GitHub
    >
    > **WHAT'S NEXT?**
    > 1. Open Project Board — {{project_url}}
    > 2. Re-publish — Run again after status changes to sync updates
    > 3. Return to Sprint — Continue with development work
  </output>

  <action>Log completion to audit trail</action>
</step>

</workflow>

## Prerequisites

### Required
- **`gh` CLI** installed and authenticated (`gh auth login`)
- **GitHub repository** initialized with remote configured
- **Sprint status file** at {sprint_status_file} (run Sprint Planning first)

### Optional (enriches issues and board)
- **Story files** in {story_dir} (creates richer issue bodies with AC and tasks)
- **Sprint assignments** at {assignments_file} (adds assignees to issues)
- **Team roster** at {roster_file} (maps developer names to GitHub usernames)

## Error Handling

### Rate Limiting
- GitHub API has rate limits (~5000 requests/hour for authenticated users)
- For large projects (50+ stories), add a 1-second delay between issue operations
- If rate limited, pause and inform the user of the retry time

### Project Board Permissions
- Creating a GitHub Project requires admin or write access to the org/user
- If project creation fails due to permissions, inform the user and continue with issues-only mode
- The workflow should still create issues even if the project board cannot be created

### Partial Failures
- If a single issue fails to create/update, log the error and continue with remaining stories
- If a board item fails to add, log and continue — the issue still exists
- Report all failures in the summary
- The issue map will still be saved with successful entries, allowing retry of failed stories

### Network Errors
- If `gh` commands fail due to network issues, stop and inform the user
- The issue map from the last successful run ensures no duplicates on retry
