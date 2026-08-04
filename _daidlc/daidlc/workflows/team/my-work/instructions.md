# My Work - Developer Work Dashboard

<critical>The workflow execution engine is governed by: C:\Workspaces\dynamodb-subscriber\_daidlc/core/tasks/workflow.xml</critical>
<critical>You MUST have already loaded and processed: C:\Workspaces\dynamodb-subscriber\_daidlc/daidlc/workflows/team/my-work/workflow.yaml</critical>
<critical>Communicate all responses in {communication_language}</critical>

<workflow>

<step n="1" goal="Load sprint assignments">
  <action>Load the sprint assignments from {assignments_file}</action>

  <action>If {assignments_file} does not exist:
    - Inform the developer:
      "No sprint assignments found at {assignments_file}. The Scrum Master needs to run the Assign Stories workflow first (/daidlc-assign)."
    - STOP — do not proceed without assignments.
  </action>

  <action>Extract:
    - Sprint number
    - Sprint start date
    - All member assignments
    - Unassigned/deferred stories
    - Any recorded warnings
  </action>
</step>

<step n="2" goal="Identify the current developer">
  <action>Determine the current developer identity using this priority:
    1. Try to detect from git config: run `git config user.name` and `git config user.email`
    2. If git identity matches a member in the assignments file, use that match
    3. If no match or git not configured, ask: "What is your name? (as it appears in the team roster)"
  </action>

  <action>Match the developer name against the assignments file keys (case-insensitive, fuzzy match on first name)</action>

  <action>If no match found:
    - Show all team members from the assignments file
    - Ask: "I couldn't match your identity. Which team member are you?"
    - Present numbered list for selection
  </action>
</step>

<step n="3" goal="Display developer's assigned stories">
  <action>Filter the assignments to show only this developer's stories</action>

  <action>For each assigned story, display:
    - Story ID (e.g., 3-1-invoice-generation)
    - Title (derived from story ID: replace dashes with spaces, capitalize)
    - Story points
    - Current status (ready-for-dev, in-progress, review, done)
    - Git branch name
    - Blocked status:
      - If blocked_by is set AND that dependency story is not "done" → show "BLOCKED by {{dependency_id}} ({{dependency_status}})"
      - If blocked_by is null or dependency is "done" → show "Ready"
    - Story file path: {implementation_artifacts}/{{story_id}}.md
  </action>

  <output>
    ## Your Sprint {{sprint_number}} Assignments

    **Developer:** {{developer_name}}
    **Sprint started:** {{sprint_start_date}}

    | # | Story | Points | Status | Branch | Blocked? |
    |---|-------|--------|--------|--------|----------|
    {{developer_stories_table}}

    **Total points:** {{total_points}}
    **Completed:** {{done_points}} / {{total_points}} pts ({{completion_pct}}%)
  </output>
</step>

<step n="4" goal="Show recommended next action">
  <action>Analyze the developer's story statuses and recommend the next action:</action>

  <check if="any story has status 'in-progress'">
    <output>
      ### Recommended Action
      **Continue working on:** {{in_progress_story_id}} — {{in_progress_story_title}}
      **Branch:** `{{in_progress_branch}}`
      **Story file:** `{implementation_artifacts}/{{in_progress_story_id}}.md`

      Switch to branch: `git checkout {{in_progress_branch}}`
    </output>
  </check>

  <check if="no stories in-progress BUT stories are 'ready-for-dev' and not blocked">
    <action>Find the first ready-for-dev story that is NOT blocked</action>
    <output>
      ### Recommended Action
      **Start next story:** {{next_story_id}} — {{next_story_title}} ({{next_story_points}} pts)
      **Create branch:** `git checkout -b story/{{next_story_id}}`
      **Story file:** `{implementation_artifacts}/{{next_story_id}}.md`

      Read the story file first, then create the branch and begin implementation.
    </output>
  </check>

  <check if="no stories in-progress AND all ready-for-dev stories are blocked">
    <output>
      ### Recommended Action
      **All your stories are currently blocked.**

      Blocking dependencies:
      {{blocked_details}}

      Coordinate with the assigned developers or check with the Scrum Master for alternative work.
    </output>
  </check>

  <check if="all stories are 'review' or 'done'">
    <output>
      ### Recommended Action
      **All stories are complete or in review.** No pending development work.

      Options:
      - Check with the Scrum Master for additional stories
      - Help review other developers' stories
      - Work on technical debt or documentation
    </output>
  </check>
</step>

<step n="5" goal="Offer quick actions">
  <output>
    ## Quick Actions

    Select an action:
    1. **Start a story** — Mark a ready-for-dev story as in-progress (creates branch command)
    2. **Update status** — Mark a story as in-progress, ready-for-review, or done
    3. **View story details** — Load and display a story file
    4. **Refresh** — Reload assignments and show updated dashboard

    Type the number or action name, or type DONE to exit.
  </output>

  <action>Handle quick action selection:</action>

  <check if="user selects 1 (Start a story)">
    <action>Show ready-for-dev stories that are not blocked</action>
    <action>Ask which story to start</action>
    <action>Update {assignments_file}: set story status to "in-progress"</action>
    <action>Display: "Started {{story_id}}. Create your branch: `git checkout -b story/{{story_id}}`"</action>
    <action>Log status change to audit trail</action>
  </check>

  <check if="user selects 2 (Update status)">
    <action>Show the developer's current stories with status</action>
    <action>Ask which story to update and what the new status should be:
      - in-progress: Developer is actively working
      - review: Implementation complete, ready for code review
      - done: Story is fully complete (post-review)
    </action>
    <action>Update {assignments_file} with new status</action>
    <action>Log status change to audit trail</action>
  </check>

  <check if="user selects 3 (View story details)">
    <action>Show list of developer's assigned stories</action>
    <action>Ask which story to view</action>
    <action>Load and display the story file from {implementation_artifacts}/{{story_id}}.md</action>
    <action>If story file does not exist, inform: "Story file not yet created. The Scrum Master needs to run Create Story (CS) for this story first."</action>
  </check>

  <check if="user selects 4 (Refresh)">
    <action>Reload {assignments_file} and re-display the dashboard from step 3</action>
  </check>

  <check if="user types DONE">
    <action>Exit the workflow with a brief summary:
      "Sprint {{sprint_number}} dashboard closed. Run /daidlc-my-work anytime to check your assignments."
    </action>
  </check>

  <action>After handling a quick action, return to the quick actions menu (loop) unless user typed DONE</action>
</step>

</workflow>

## Additional Notes

### Developer Identity Matching
The workflow tries to match git user identity to roster names using these strategies:
- Exact match on git username or email prefix
- Case-insensitive first name match
- If the assignments file uses short names (e.g., "alice"), match against git user.name's first name

### Status Transitions Allowed
Developers can only move stories forward in the workflow:
- ready-for-dev -> in-progress
- in-progress -> review
- review -> done

Moving backward (e.g., review -> in-progress) should prompt a reason and log it to audit.

### Branch Naming Convention
All story branches follow: `story/{story-id}`
Example: `story/3-1-invoice-generation`

This convention is shared with the assign-stories workflow and must stay consistent.
