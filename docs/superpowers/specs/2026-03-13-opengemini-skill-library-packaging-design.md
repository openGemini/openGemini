# openGemini Skill Library Packaging Design

## Goal

Move the openGemini skill set into the repository as the single source of truth under `.skill/`, and document how Claude Code, Codex, and OpenCode should consume that library.

## Scope

This change will:
- create a repository-local skill library under `.skill/skills/`
- migrate the nine existing `opengemini-*` skills and their references into the repository
- add repository docs for overview and platform usage
- add a setup script that links the repository skills into the platform-specific locations that need them
- add a short entry point in top-level docs

This change will not:
- add bi-directional sync between repo and home-directory skill folders
- add runtime validation harnesses for skill triggering
- remove the user's existing personal skill folders automatically

## Repository Layout

```text
.skill/
├── README.md
├── platforms.md
└── skills/
   └── <skill-name>/
      ├── SKILL.md
      └── references/
```

## Platform Strategy

### Claude Code

Claude Code supports project-local `.claude/skills/<name>/SKILL.md`. The repository will expose the library by linking `.claude/skills` to `.skill/skills`.

### OpenCode

OpenCode supports project-local `.opencode/skills/<name>/SKILL.md` and also understands Claude-compatible project skill folders. The repository will expose the library by linking `.opencode/skills` to `.skill/skills`.

### Codex

In this environment, Codex discovers skills from `~/.codex/skills`. The repository will remain the source of truth, and a setup script will link each repository skill into `~/.codex/skills/`.

## Setup Script

Add `scripts/setup-skills.sh`.

It should:
- create `.claude/skills -> ../.skill/skills` if absent or wrong
- create `.opencode/skills -> ../.skill/skills` if absent or wrong
- create/update per-skill symlinks in `~/.codex/skills`
- print what it changed

## Documentation

- `.skill/README.md`: what the library is, current skill list, source-of-truth rule
- `.skill/platforms.md`: platform-specific usage for Claude Code, Codex, and OpenCode
- `README.md`: short project-skills section pointing to `.skill/README.md` and `scripts/setup-skills.sh`
- `CONTRIBUTION.md`: contributor-facing note about editing the repository skill library instead of personal skill directories

## Verification Strategy

Static verification only:
- all nine skills exist under `.skill/skills/`
- `.claude/skills` and `.opencode/skills` resolve to `.skill/skills`
- `scripts/setup-skills.sh` exists and is executable
- docs reference real files and commands
