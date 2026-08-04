# D-AIDLC Content Validation

**Goal**: Validate artifact content quality before writing files.

## Validation Rules

### 1. Mermaid Diagram Syntax
For every Mermaid code block in the document:
- Verify diagram type is valid (graph, sequenceDiagram, classDiagram, flowchart, etc.)
- Check that all nodes referenced in connections are defined
- Verify arrow syntax is correct (-->, --->, -.->)
- Check for unclosed subgraphs
- Verify quote matching in labels

**Pass Criteria**: All Mermaid blocks parse without syntax errors.

### 2. Markdown Structure
- Heading hierarchy is sequential (no jumping from H1 to H3)
- All links have valid targets (internal anchors exist, external URLs are formatted)
- Code blocks have language specifiers
- Tables have matching column counts in header and rows
- Lists maintain consistent indentation
- No orphaned HTML tags

**Pass Criteria**: Document renders correctly as standard Markdown.

### 3. No Duplicate Content
- Scan for duplicated sections (same heading appearing twice)
- Check for copy-paste artifacts (repeated paragraphs)
- Verify no template placeholders remain ({{variable}}, {placeholder}, [TODO])

**Pass Criteria**: No duplicated sections, no unresolved placeholders.

### 4. Completeness Check
- All required sections per template are present
- No empty sections (heading with no content below it)
- All "TBD" or "TODO" markers are flagged

**Pass Criteria**: All sections populated, no placeholder text.

### 5. ASCII Diagram Alignment
If document contains ASCII/box diagrams:
- Verify box borders are aligned
- Check that connections don't overlap text
- Ensure diagram is readable in monospace font

**Pass Criteria**: ASCII diagrams render correctly in monospace.

## Validation Report Format

```markdown
## Content Validation Report

**File:** [path]
**Date:** [timestamp]
**Validator:** Grace (Governance Officer)

### Results

| Check | Status | Details |
|-------|--------|---------|
| Mermaid Syntax | PASS/FAIL | [details] |
| Markdown Structure | PASS/FAIL | [details] |
| No Duplicates | PASS/FAIL | [details] |
| Completeness | PASS/FAIL | [details] |
| ASCII Diagrams | PASS/FAIL/N/A | [details] |

**Overall: PASS/FAIL**

### Issues Found
[numbered list of issues with locations and fixes]
```

## Execution Steps

### Step 1: Identify Target
- Accept file path from user or from calling workflow
- Read the complete file content

### Step 2: Run All Checks
- Execute each validation rule in order
- Record results for each check

### Step 3: Report
- Generate validation report
- If FAIL: list all issues with line numbers and suggested fixes
- If PASS: confirm file is ready

### Step 4: Log
- Append validation result to audit trail
