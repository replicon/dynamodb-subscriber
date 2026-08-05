# Assign Stories - Sprint Story Assignment Workflow

<critical>The workflow execution engine is governed by: C:\Workspaces\dynamodb-subscriber\_daidlc/core/tasks/workflow.xml</critical>
<critical>You MUST have already loaded and processed: C:\Workspaces\dynamodb-subscriber\_daidlc/daidlc/workflows/team/assign-stories/workflow.yaml</critical>
<critical>Communicate all responses in {communication_language}</critical>
<critical>Generate all documents in {document_output_language}</critical>

<workflow>

<step n="1" goal="Load team roster and member capacity">
  <action>Load the team roster from {roster_file}</action>
  <action>For each team member, extract:
    - Name (display name and git username)
    - Role (frontend, backend, fullstack, QA, etc.)
    - Sprint capacity in story points (typical: 8-13 points per sprint)
    - Skills and module expertise
    - Availability notes (PTO, partial sprint, etc.)
  </action>

  <action>If {roster_file} does not exist:
    - STOP and inform the Scrum Master:
      "Team roster not found at {roster_file}. Run the Team Setup workflow first (/daidlc-agent-scrum-master → TS) to define your team."
    - Do NOT proceed without a roster.
  </action>

  <output>
    ## Team Roster Loaded

    | Member | Role | Capacity (pts) | Availability |
    |--------|------|----------------|--------------|
    {{roster_table}}

    **Total Team Capacity:** {{total_capacity}} points
  </output>
</step>

<step n="2" goal="Load current sprint stories and status">
  <action>Load sprint status from {sprint_status_file}</action>
  <action>Extract all stories and their current status</action>
  <action>Filter to stories that are eligible for assignment:
    - Status: backlog OR ready-for-dev
    - Exclude: in-progress, review, done (already being worked)
  </action>

  <action>If {sprint_status_file} does not exist:
    - STOP and inform the Scrum Master:
      "Sprint status not found at {sprint_status_file}. Run Sprint Planning first (/daidlc-agent-scrum-master → SP) to generate the sprint status."
    - Do NOT proceed without sprint status.
  </action>

  <action>For each eligible story, determine:
    - Story ID and title (from the sprint status key, e.g., 3-1-invoice-generation)
    - Epic membership (extract epic number from story ID prefix)
    - Story points (from the story file if it exists, otherwise estimate from epic description)
    - Dependencies (stories that must complete before this one can start)
    - Files likely touched (from story file tasks or epic description)
  </action>

  <output>
    ## Sprint Stories Loaded

    **Total stories:** {{total_stories}}
    **Eligible for assignment:** {{eligible_count}}
    **Already in-progress:** {{in_progress_count}}
    **Already done:** {{done_count}}
  </output>
</step>

<step n="3" goal="Load existing assignments if present">
  <action>Check if {assignments_file} exists</action>

  <check if="{assignments_file} exists">
    <action>Load existing assignments</action>
    <action>Preserve assignments for stories that are already in-progress or review status</action>
    <action>Note which stories have been previously assigned but not started</action>
    <output>
      ## Existing Assignments Loaded

      **Previously assigned (keeping):** {{preserved_count}}
      **Previously assigned (re-assignable):** {{reassignable_count}}
    </output>
  </check>

  <check if="{assignments_file} does NOT exist">
    <action>Starting fresh — all eligible stories are unassigned</action>
  </check>
</step>

<step n="4" goal="Present unassigned stories for assignment">
  <action>Display all unassigned stories grouped by epic</action>
  <action>For each story show:
    - Story ID and title
    - Epic name
    - Estimated story points
    - Dependencies (if any — which other stories must finish first)
    - Files likely to be modified (for conflict detection)
    - Recommended skills (frontend, backend, database, etc.)
  </action>

  <output>
    ## Unassigned Stories

    ### Epic {{epic_number}}: {{epic_title}}

    | # | Story ID | Title | Points | Dependencies | Skills Needed |
    |---|----------|-------|--------|-------------|---------------|
    {{unassigned_stories_table}}

    **Total unassigned points:** {{unassigned_points}}
    **Team capacity this sprint:** {{total_capacity}} points
  </output>
</step>

<step n="5" goal="Assign stories to team members interactively">
  <action>For each unassigned story, ask the Scrum Master to assign it to a team member</action>
  <action>When presenting the assignment choice, show:
    - The story details (ID, title, points)
    - Each available team member with their:
      - Current load (points already assigned this sprint)
      - Remaining capacity (capacity minus current load)
      - Skill match (highlight if member's skills align with story needs)
  </action>

  <action>Present assignment options:
    "**Assign {{story_id}}: {{story_title}}** ({{points}} pts)

    | Member | Current Load | Remaining | Skill Match |
    |--------|-------------|-----------|-------------|
    {{member_options_table}}

    Assign to: [member name] or SKIP to defer to next sprint"
  </action>

  <action>If SM types SKIP or "defer":
    - Add story to the unassigned/deferred list
    - Ask for a reason (optional but recommended)
    - Continue to next story
  </action>

  <action>If SM types BATCH:
    - Allow bulk assignment: "story-id → member, story-id → member, ..."
    - Validate all assignments after batch entry
  </action>

  <action>After each assignment, update running totals for the assigned member</action>
</step>

<step n="6" goal="Check for conflicts and warnings">
  <action>After all assignments are made, run conflict detection:</action>

  <action>**File Collision Check:**
    For each pair of stories assigned to DIFFERENT developers:
    - Compare the files each story is expected to modify
    - If overlap detected, emit warning:
      "WARNING: {{story_a}} ({{dev_a}}) and {{story_b}} ({{dev_b}}) both touch {{file_list}}.
       Recommend: Sequence these stories or coordinate merge strategy."
  </action>

  <action>**Capacity Overload Check:**
    For each developer:
    - Sum total assigned story points
    - Compare against their sprint capacity
    - If over capacity, emit warning:
      "WARNING: {{developer}} is assigned {{assigned_pts}} pts but has {{capacity}} pts capacity.
       Over by {{over_amount}} pts. Consider reassigning {{lowest_priority_story}}."
  </action>

  <action>**Dependency Sequencing Check:**
    For each story with dependencies:
    - Verify the dependency story is assigned to a same or earlier sprint
    - Verify the dependency is assigned to someone (not deferred)
    - If dependency is deferred or unassigned, emit warning:
      "WARNING: {{story_id}} depends on {{dependency_id}} which is {{status}}.
       {{story_id}} will be BLOCKED until {{dependency_id}} is completed."
    - If dependency is assigned to the same developer, note it (no conflict)
    - If dependency is assigned to a different developer, recommend coordination
  </action>

  <action>Present all warnings to the Scrum Master and ask:
    "Resolve warnings now, or proceed with current assignments?"
    - If resolve: re-enter assignment flow for flagged stories
    - If proceed: acknowledge warnings and continue
  </action>
</step>

<step n="7" goal="Generate git branch names for assigned stories">
  <action>For each assigned story, generate the git branch name using the convention:
    story/{story-id}

    Examples:
    - Story 3-1-invoice-generation → branch: story/3-1-invoice-generation
    - Story 3-2-payment-processing → branch: story/3-2-payment-processing
  </action>

  <action>Verify no branch name collisions (should not happen if story IDs are unique)</action>
</step>

<step n="8" goal="Write sprint assignments file">
  <action>Write the assignments to {assignments_file} with this structure:</action>

  <action>YAML structure:
    ```yaml
    # D-AIDLC Sprint Assignments
    # generated: {date}
    # project: {project_name}

    sprint: {{sprint_number}}
    started: "{date}"

    assignments:
      {{member_name}}:
        - story: {{story_id}}
          points: {{story_points}}
          branch: story/{{story_id}}
          status: ready-for-dev
          files:
            - {{file_1}}
            - {{file_2}}
          blocked_by: {{dependency_story_id_or_null}}
      # ... repeat for each member with assignments

    unassigned:
      - story: {{deferred_story_id}}
        points: {{story_points}}
        reason: "{{deferral_reason}}"
      # ... repeat for each deferred story

    warnings:
      - type: file-collision
        stories: [{{story_a}}, {{story_b}}]
        files: [{{shared_files}}]
        resolution: "{{agreed_resolution_or_pending}}"
      - type: over-capacity
        member: {{member_name}}
        assigned: {{assigned_pts}}
        capacity: {{capacity_pts}}
      - type: blocked-dependency
        story: {{story_id}}
        blocked_by: {{dependency_id}}
        dependency_status: {{status}}
    ```
  </action>

  <action>Write the file to {default_output_file}</action>
</step>

<step n="9" goal="Log assignments to audit trail">
  <action>Log the following to the audit trail:
    - Timestamp: {date}
    - Action: Sprint story assignment
    - Sprint number: {{sprint_number}}
    - Total stories assigned: {{assigned_count}}
    - Total points assigned: {{assigned_points}}
    - Stories deferred: {{deferred_count}}
    - Warnings issued: {{warning_count}}
    - Each assignment: {{member}} ← {{story_id}} ({{points}} pts)
  </action>
</step>

<step n="10" goal="Present assignment summary">
  <output>
    # D-AIDLC: Sprint Assignment Complete

    ## Assignment Summary

    | Developer | Stories | Points | Capacity | Utilization |
    |-----------|---------|--------|----------|-------------|
    {{developer_summary_table}}

    **Total Assigned:** {{total_assigned_points}} / {{total_capacity}} pts ({{utilization_pct}}%)

    ## Assignments by Developer

    {{per_developer_detail}}

    ## Deferred Stories
    {{deferred_stories_list}}

    ## Warnings
    {{warnings_summary}}

    **Artifacts Created:**
    - {default_output_file}

    > **REVIEW REQUIRED:** {default_output_file}
    >
    > **WHAT'S NEXT?**
    > 1. Request Changes - Reassign or adjust stories
    > 2. Approve and Continue - Developers can now run `/daidlc-my-work` to see their assignments
  </output>

  <action>Log completion to audit trail</action>
</step>

</workflow>

## Additional Notes

### Sprint Number Detection
- If {assignments_file} already exists, increment the sprint number
- If not, check {sprint_status_file} for sprint metadata
- Default to sprint 1 if no prior sprint data exists

### Story Points Estimation
If story files do not specify points, estimate from epic descriptions:
- Simple CRUD: 3 pts
- Moderate logic: 5 pts
- Complex integration: 8 pts
- Major feature: 13 pts

### Capacity Defaults
If roster does not specify capacity per member:
- Full-time developer: 10 pts/sprint
- Part-time / split role: 5 pts/sprint
- Tech lead (coding + review): 8 pts/sprint
