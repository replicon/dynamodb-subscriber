# Reverse Engineering Validation Checklist

## Artifact Completeness
- [ ] business-overview.md exists and is non-empty
- [ ] architecture.md exists and is non-empty
- [ ] code-structure.md exists and is non-empty
- [ ] api-documentation.md exists and is non-empty
- [ ] component-inventory.md exists and is non-empty
- [ ] technology-stack.md exists and is non-empty
- [ ] dependencies.md exists and is non-empty
- [ ] code-quality-assessment.md exists and is non-empty
- [ ] reverse-engineering-summary.md exists and is non-empty

## Project Type Detection
- [ ] Project type was correctly identified with detection signals cited
- [ ] Scale assessment matches actual file count
- [ ] Recommended depth level was appropriate for the project
- [ ] Framework detection matches actual frameworks found in config files

## Technique Application
- [ ] Analysis plan was presented and confirmed before execution
- [ ] Each artifact includes a "Techniques Applied" section listing techniques used
- [ ] Priority techniques (from project type's priority_categories) were applied first
- [ ] Technique selection matches the detected project type
- [ ] Framework-specific techniques were applied only for detected frameworks
- [ ] Deep techniques were only applied at deep depth level

## Quality
- [ ] All findings cite specific file paths and line numbers
- [ ] Architecture diagrams use valid Mermaid syntax
- [ ] Technology versions are specified (not just names)
- [ ] Dependencies include version numbers
- [ ] Code quality assessment includes specific examples with severity ratings
- [ ] No placeholder text remaining in any artifact
- [ ] Security findings include severity (Critical/High/Medium/Low)

## Accuracy
- [ ] Business domain description matches actual code behavior
- [ ] Architecture description matches actual folder structure
- [ ] API documentation matches actual route definitions
- [ ] Component relationships are verified through import analysis
- [ ] Confidence scores are honest (not all "High")
- [ ] Cross-validation was offered between artifacts

## Summary
- [ ] Executive summary is concise (one paragraph max)
- [ ] Top 5 findings are prioritized by impact
- [ ] Risk areas are actionable
- [ ] Confidence scores per artifact are provided with justification
- [ ] Analysis metadata is complete (project type, depth, technique count)
- [ ] Techniques applied vs. available count is documented
