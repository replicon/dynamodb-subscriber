# Tech Stack Validation

**Goal**: Verify all project technologies are on the approved list.

## Step 1: Load Approved Tech Stack

- If `approved_tech_stack` is 'none', skip validation
- Otherwise, load the approved technologies file
- Parse approved languages, frameworks, databases, and libraries

## Step 2: Scan Project Dependencies

Scan the project for all technologies in use:
- Languages (from file extensions and configs)
- Frameworks (from dependency files)
- Databases (from configs and connection strings)
- Libraries and packages (from package managers)
- Build tools and CI/CD tools
- Cloud services (from configs and IaC files)

## Step 3: Compare Against Approved List

For each technology found:
- **Approved**: On the approved list, version within range
- **Unapproved**: Not on the approved list → needs exception or replacement
- **Version Violation**: On approved list but version outside allowed range
- **Unlisted**: Category not covered by approved list (flag for review)

## Step 4: Generate Validation Report

Report includes:
- Technology inventory with approval status
- Unapproved technologies with recommended alternatives
- Version violations with required updates
- Exception requests for legitimate unapproved tech

Write to: {default_output_file}
Log to audit trail
