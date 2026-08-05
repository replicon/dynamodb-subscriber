# Team Status - Team-Wide Dashboard

<critical>The workflow execution engine is governed by: C:\Workspaces\dynamodb-subscriber\_daidlc/core/tasks/workflow.xml</critical>
<critical>You MUST have already loaded and processed: C:\Workspaces\dynamodb-subscriber\_daidlc/daidlc/workflows/team/team-status/workflow.yaml</critical>
<critical>This is a READ-ONLY dashboard. Do NOT modify any source data files. Only write files during explicit report export (Step 6).</critical>
<critical>No specific agent persona required. This workflow serves PMs, SMs, and managers.</critical>

<workflow>

<step n="1" goal="Load all data files">
  <action>Load {project_context} for project-wide patterns and conventions (if exists)</action>
  <action>Communicate in {communication_language} with {user_name}</action>

  <action>Load team roster from {team_dir}/team-roster.yaml</action>
  <check if="team-roster.yaml not found">
    <output>
Team roster not found at {team_dir}/team-roster.yaml.
Run `/daidlc-agent-scrum-master` and select Team Setup, or create the roster manually.
    </output>
    <action>Exit workflow</action>
  </check>
  <action>Parse team_roster: extract teams[], each with name, focus, lead, and members[]</action>
  <action>For each member, extract: name, role, handle (if present)</action>

  <action>Load sprint assignments from {team_dir}/sprint-assignments.yaml</action>
  <check if="sprint-assignments.yaml not found">
    <output>
Sprint assignments not found at {team_dir}/sprint-assignments.yaml.
Run `/daidlc-agent-scrum-master` and select Assign Stories, or create the file manually.
    </output>
    <action>Exit workflow</action>
  </check>
  <action>Parse sprint_assignments: extract sprint metadata (sprint_number, start_date, end_date, sprint_days)</action>
  <action>Parse assignments map: developer_handle -> list of { story_key, points }</action>

  <action>Load sprint status from {implementation_artifacts}/sprint-status.yaml</action>
  <check if="sprint-status.yaml not found">
    <output>
Sprint status not found at {implementation_artifacts}/sprint-status.yaml.
Run `/daidlc-agent-scrum-master` and select Sprint Planning to generate it.
    </output>
    <action>Exit workflow</action>
  </check>
  <action>Parse sprint_status: extract development_status map (story_key -> status)</action>
  <action>Extract metadata: project, project_key, generated timestamp</action>
  <action>Map legacy statuses: "drafted" -> "ready-for-dev", "contexted" -> "in-progress"</action>

  <action>Load project state from C:\Workspaces\dynamodb-subscriber/aidlc-docs/aidlc-state.md</action>
  <check if="aidlc-state.md not found">
    <action>Set lifecycle_state = "unknown"</action>
    <action>Continue (non-blocking)</action>
  </check>
  <check if="aidlc-state.md found">
    <action>Parse current_phase, current_stage from state file</action>
  </check>

  <action>Load audit trail from C:\Workspaces\dynamodb-subscriber/aidlc-docs/audit/audit.md</action>
  <check if="audit.md not found">
    <action>Set last_audit_entry = "none"</action>
    <action>Continue (non-blocking)</action>
  </check>
  <check if="audit.md found">
    <action>Extract the 5 most recent audit entries (by timestamp)</action>
    <action>Store as recent_audit_entries[]</action>
  </check>

  <action>Load velocity history from {team_dir}/velocity-history.yaml (optional)</action>
  <check if="velocity-history.yaml not found">
    <action>Set has_velocity_history = false</action>
    <action>Continue (non-blocking)</action>
  </check>
  <check if="velocity-history.yaml found">
    <action>Set has_velocity_history = true</action>
    <action>Parse sprint_velocities[]: each with sprint_number, points_completed, points_committed</action>
  </check>

  <action>Continue to Step 2</action>
</step>

<step n="2" goal="Compute metrics per team">
  <action>For each team in team_roster.teams[]:</action>

  <action>Collect all stories assigned to team members using sprint_assignments</action>
  <note>A story belongs to a team if any team member is assigned to it</note>

  <action>For each team story, look up its status in sprint_status.development_status</action>
  <action>Classify story statuses into buckets:</action>
  - done: status == "done"
  - in_progress: status == "in-progress"
  - review: status == "review"
  - ready: status == "ready-for-dev"
  - blocked: status == "blocked"
  - backlog: status == "backlog"

  <action>Compute team story counts:</action>
  - team.stories_total = count of all team stories
  - team.stories_done = count where status == "done"
  - team.stories_in_progress = count where status == "in-progress" OR status == "review"
  - team.stories_ready = count where status == "ready-for-dev"
  - team.stories_blocked = count where status == "blocked"
  - team.stories_backlog = count where status == "backlog"

  <action>Compute team point totals:</action>
  - team.points_committed = sum of points for all team stories
  - team.points_completed = sum of points for done stories
  - team.points_in_progress = sum of points for in-progress + review stories

  <action>Compute team velocity (if has_velocity_history == true):</action>
  <check if="has_velocity_history == true">
    <action>Filter velocity entries for this team (if per-team velocity exists)</action>
    <action>Compute average velocity over last 3 sprints</action>
    <action>Compute projected completion = points_completed + (points_in_progress * days_remaining / days_elapsed)</action>
    <action>Compute velocity_trend: "above target", "on target", "below target"</action>
    <action>Compute velocity_delta_pct = ((current_projected - avg_velocity) / avg_velocity) * 100</action>
  </check>

  <action>Collect blockers for this team:</action>
  - For each story with status == "blocked", record: story_key, assignee, block_reason (from sprint-status comments or assignments)
  - If a blocker references another story, resolve the dependency: blocked_by_story, blocked_by_assignee, blocked_by_team

  <action>Store computed team metrics in teams_computed[]</action>
  <action>Continue to Step 3</action>
</step>

<step n="3" goal="Compute metrics per developer">
  <action>For each developer across all teams:</action>

  <action>From sprint_assignments, collect all stories assigned to this developer</action>
  <action>For each assigned story, look up status in sprint_status.development_status</action>

  <action>Compute developer metrics:</action>
  - dev.assigned_count = total stories assigned
  - dev.assigned_points = total points assigned
  - dev.done_count = stories with status "done"
  - dev.done_points = points from done stories
  - dev.in_progress_count = stories with status "in-progress" or "review"
  - dev.remaining_count = assigned_count - done_count
  - dev.remaining_points = assigned_points - done_points

  <action>Identify current active story:</action>
  - dev.active_story = first story with status "in-progress" (sorted by story key)
  - If no "in-progress" story, dev.active_story = first "ready-for-dev" story
  - If none, dev.active_story = "idle"

  <action>Compute completion percentage:</action>
  - dev.completion_pct = (done_points / assigned_points) * 100
  - Round to nearest integer

  <action>Detect capacity signals:</action>
  - If dev.done_count == dev.assigned_count: mark as "complete" (has capacity)
  - If dev.remaining_points == 0 AND dev.in_progress_count == 0: mark as "available"
  - If dev has any blocked story: mark as "has_blocker"

  <action>Store computed developer metrics in devs_computed[]</action>
  <action>Continue to Step 4</action>
</step>

<step n="4" goal="Display the Team Dashboard">
  <action>Compute sprint-level aggregate metrics:</action>
  - total_stories = sum of all team stories (deduplicated)
  - total_done = sum of all done stories
  - total_points_committed = sum of all team points committed
  - total_points_completed = sum of all team points completed
  - days_elapsed = (today - start_date) in days
  - days_total = sprint_days from sprint_assignments
  - days_remaining = days_total - days_elapsed

  <action>Build progress bar strings (10 chars wide):</action>
  - stories_bar = filled blocks for (total_done / total_stories * 10), empty blocks for remainder
  - points_bar = filled blocks for (total_points_completed / total_points_committed * 10), empty blocks for remainder
  - days_bar = filled blocks for (days_elapsed / days_total * 10), empty blocks for remainder

  <action>Collect all blockers across teams into all_blockers[]</action>

  <action>Generate recommended actions based on analysis:</action>
  - If any blocker exists where the blocking story is assigned to someone: "Prioritize {assignee}'s {story_key} to unblock {blocked_assignee}"
  - If any developer has capacity (complete): "Consider pulling next backlog story for {dev_name} (has capacity)"
  - If any team's velocity_trend == "below target": "{team_name} may need scope reduction — {remaining_points} pts remaining in {days_remaining} days"
  - If days_elapsed > (days_total * 0.8) AND total_points_completed < (total_points_committed * 0.6): "Sprint at risk — consider scope negotiation"
  - If any story status == "review" for more than 2 days (from audit timestamps): "Code review may be bottleneck — {story_key} in review since {date}"

  <action>Display the dashboard:</action>

  <output>
```
═══════════════════════════════════════════════════════
  D-AIDLC TEAM DASHBOARD — {{project_name}}
  Sprint {{sprint_number}} | {{start_date}} → {{end_date}}
═══════════════════════════════════════════════════════

SPRINT OVERVIEW
  Stories: [{{stories_bar}}] {{total_done}}/{{total_stories}} ({{stories_pct}}%)
  Points:  [{{points_bar}}] {{total_points_completed}}/{{total_points_committed}} ({{points_pct}}%)
  Days:    [{{days_bar}}] {{days_elapsed}}/{{days_total}} ({{days_pct}}%)

TEAM STATUS
┌─────────────────────────────────────────────────────┐
{{#each teams_computed}}
│ {{team_name}} ({{team_focus}})          Lead: {{team_lead}}
│ Done: {{stories_done}}  In Progress: {{stories_in_progress}}  Ready: {{stories_ready}}  Blocked: {{stories_blocked}}
│ Points: {{points_completed}}/{{points_committed}}
│
{{#each members_computed}}
│   {{dev_name}}  [{{dev_bar}}] {{done_count}} done{{#if in_progress_count}}, {{in_progress_count}} in-progress{{/if}}{{#if has_blocker}}, BLOCKED{{/if}}{{#if is_complete}} (capacity){{/if}}
{{#if blocked_stories}}
│     Blocked: {{blocked_story_key}} ({{block_reason}})
{{/if}}
{{/each}}
├─────────────────────────────────────────────────────┤
{{/each}}
└─────────────────────────────────────────────────────┘

{{#if all_blockers}}
BLOCKERS & RISKS
{{#each all_blockers}}
  {{@index}}. {{assignee}}/{{story_key}} — {{block_reason}}{{#if blocked_by}} (blocked by {{blocked_by_assignee}}/{{blocked_by_story}}){{/if}}
{{/each}}
{{#each risks}}
  {{@index}}. {{description}}
{{/each}}
{{/if}}

{{#if has_velocity_history}}
VELOCITY TREND
{{#each sprint_velocities}}
  Sprint {{sprint_number}}: {{points_completed}} pts
{{/each}}
  Sprint {{current_sprint}}: {{total_points_completed}}/{{total_points_committed}} pts (projected: {{projected_points}})
{{/if}}

RECOMMENDED ACTIONS
{{#each recommendations}}
  {{@index}}. {{this}}
{{/each}}
```
  </output>

  <note>Render progress bars using block characters: filled = unicode full block, empty = unicode light shade. Example: [########--] for 80%</note>
  <note>For story and point counts, use actual computed values from Steps 2-3</note>
  <note>If a team has no blockers, omit the blocker line for that team</note>
  <note>If a developer has completed all stories, append "(capacity)" indicator</note>

  <action>Continue to Step 5</action>
</step>

<step n="5" goal="Offer drill-down options">
  <ask>
Select an option:
1) View specific team details
2) View specific developer's stories
3) View all blockers
4) View velocity chart
5) Export as markdown report
6) Exit

Choice:
  </ask>

  <check if="choice == 1">
    <ask>Which team? (enter team name or number from the dashboard)</ask>
    <action>Display detailed view for selected team:</action>
    <output>
### {{team_name}} — Detailed View

**Lead:** {{team_lead}}
**Focus:** {{team_focus}}
**Points:** {{points_completed}}/{{points_committed}} ({{points_pct}}%)

| Developer | Story | Status | Points | Notes |
|-----------|-------|--------|--------|-------|
{{#each team_stories_expanded}}
| {{assignee}} | {{story_key}} | {{status}} | {{points}} | {{notes}} |
{{/each}}

{{#if team_blockers}}
**Blockers:**
{{#each team_blockers}}
- {{story_key}} ({{assignee}}): {{reason}} {{#if eta}}— ETA: {{eta}}{{/if}}
{{/each}}
{{/if}}
    </output>
    <action>Return to Step 5 menu</action>
  </check>

  <check if="choice == 2">
    <ask>Which developer? (enter name or handle)</ask>
    <action>Display detailed view for selected developer:</action>
    <output>
### {{dev_name}} — Story Details

**Team:** {{team_name}}
**Role:** {{role}}
**Assigned:** {{assigned_count}} stories, {{assigned_points}} points
**Completed:** {{done_count}} stories, {{done_points}} points
**Active:** {{active_story}}

| Story | Status | Points | Description |
|-------|--------|--------|-------------|
{{#each dev_stories}}
| {{story_key}} | {{status}} | {{points}} | {{title}} |
{{/each}}
    </output>
    <action>Return to Step 5 menu</action>
  </check>

  <check if="choice == 3">
    <action>Display all blockers with dependency chain:</action>
    <output>
### All Blockers

{{#if all_blockers}}
| # | Blocked Story | Assignee | Team | Blocked By | Blocker Assignee | Blocker Status | Reason |
|---|--------------|----------|------|------------|-----------------|----------------|--------|
{{#each all_blockers}}
| {{@index}} | {{story_key}} | {{assignee}} | {{team}} | {{blocked_by_story}} | {{blocked_by_assignee}} | {{blocker_status}} | {{reason}} |
{{/each}}

**Dependency Chain:**
{{#each blocker_chains}}
{{chain_description}}
{{/each}}
{{else}}
No blockers at this time.
{{/if}}
    </output>
    <action>Return to Step 5 menu</action>
  </check>

  <check if="choice == 4">
    <check if="has_velocity_history == false">
      <output>
No velocity history available yet. Velocity tracking begins after the first completed sprint.
To start tracking, ensure sprint results are recorded in {team_dir}/velocity-history.yaml.
      </output>
      <action>Return to Step 5 menu</action>
    </check>
    <check if="has_velocity_history == true">
      <action>Display velocity chart using ASCII bars:</action>
      <output>
### Velocity Trend

{{#each sprint_velocities}}
Sprint {{sprint_number}}: {{bar}} {{points_completed}}/{{points_committed}} pts
{{/each}}
Sprint {{current_sprint}}: {{current_bar}} {{total_points_completed}}/{{total_points_committed}} pts (in progress)

**Average Velocity:** {{avg_velocity}} pts/sprint
**Current Projection:** {{projected_points}} pts
**Trend:** {{velocity_trend}}

{{#each team_velocities}}
  {{team_name}}: avg {{team_avg}} pts | current {{team_current}} pts | {{team_trend}}
{{/each}}
      </output>
    </check>
    <action>Return to Step 5 menu</action>
  </check>

  <check if="choice == 5">
    <action>Continue to Step 6</action>
  </check>

  <check if="choice == 6">
    <output>Dashboard session ended.</output>
    <action>Exit workflow</action>
  </check>
</step>

<step n="6" goal="Export as markdown report">
  <action>Load report template from {installed_path}/template-report.md</action>
  <action>Populate all template variables from computed metrics in Steps 2-4</action>

  <action>Ensure reports directory exists: {reports_dir}</action>
  <action>Generate filename: sprint-{{sprint_number}}-status-{{date}}.md</action>
  <action>Resolve date as ISO 8601 date (YYYY-MM-DD)</action>

  <action>Write the populated report to {reports_dir}/sprint-{{sprint_number}}-status-{{date}}.md</action>

  <output>
Report exported to: {reports_dir}/sprint-{{sprint_number}}-status-{{date}}.md

Contents:
- Sprint overview with story and point completion
- Team-by-team breakdowns with individual progress
- All blockers with dependency chains
- Velocity trend (if historical data available)
- Recommended actions
- AIDLC lifecycle state
  </output>

  <action>Return to Step 5 menu</action>
</step>

</workflow>

## Data File Schemas

### team-roster.yaml Expected Schema

```yaml
teams:
  - name: "Team Alpha"
    focus: "Auth & RBAC"
    lead: "alice"
    members:
      - name: "Alice"
        handle: "alice"
        role: "senior-dev"
      - name: "Bob"
        handle: "bob"
        role: "dev"
      - name: "Carol"
        handle: "carol"
        role: "dev"
  - name: "Team Beta"
    focus: "Finance"
    lead: "eve"
    members:
      - name: "Eve"
        handle: "eve"
        role: "senior-dev"
      - name: "Frank"
        handle: "frank"
        role: "dev"
      - name: "Grace"
        handle: "grace"
        role: "dev"
```

### sprint-assignments.yaml Expected Schema

```yaml
sprint_number: 3
start_date: "2026-02-10"
end_date: "2026-02-21"
sprint_days: 10

assignments:
  alice:
    - story_key: "1-1-auth-api"
      points: 8
    - story_key: "1-2-rbac-setup"
      points: 5
    - story_key: "1-3-session-mgmt"
      points: 8
  bob:
    - story_key: "1-4-password-reset"
      points: 5
    - story_key: "1-5-mfa-support"
      points: 3
  carol:
    - story_key: "1-6-audit-logging"
      points: 5
    - story_key: "1-7-auth-tests"
      points: 3
  eve:
    - story_key: "2-1-invoice-gen"
      points: 8
    - story_key: "2-2-payment-processing"
      points: 5
  frank:
    - story_key: "2-3-payment-api"
      points: 8
    - story_key: "2-4-tax-calc"
      points: 3
  grace:
    - story_key: "2-5-reporting"
      points: 5
    - story_key: "2-6-export-csv"
      points: 3
```

### velocity-history.yaml Expected Schema (Optional)

```yaml
sprints:
  - sprint_number: 1
    points_committed: 40
    points_completed: 32
  - sprint_number: 2
    points_committed: 42
    points_completed: 38
```

## Progress Bar Rendering

Use 10-character progress bars with Unicode block characters:

- Filled block: `#` (or Unicode full block if terminal supports it)
- Empty block: `-` (or Unicode light shade if terminal supports it)

Examples:
- 0%:   `[----------]`
- 30%:  `[###-------]`
- 60%:  `[######----]`
- 100%: `[##########]`

Round to nearest 10% for the bar. Show exact percentage in the label.

## Status Color Coding (for terminals that support it)

When rendering status text:
- done: green indicator
- in-progress: blue indicator
- ready-for-dev: yellow indicator
- blocked: red indicator
- backlog: dim/gray indicator
- review: cyan indicator
