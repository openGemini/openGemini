# Platform Usage

The repository skill library lives in `.skill/skills/`. Platform-specific folders should only point at that directory.

## Claude Code

Claude Code reads project skills from `.claude/skills/<skill-name>/SKILL.md`. This repository exposes the library through:

```text
.claude/skills -> ../.skill/skills
```

Usage:
- open the repository in Claude Code
- start a new session after adding or renaming a skill
- mention the skill name directly, for example `opengemini-local-dev`

## Codex

In this environment, Codex reads from `~/.codex/skills`. Keep the repository as the source of truth and link the repo skills into that directory:

```bash
bash scripts/setup-skills.sh
```

The script creates or refreshes symlinks such as:

```text
~/.codex/skills/opengemini-local-dev -> /path/to/repo/.skill/skills/opengemini-local-dev
```

Usage:
- run `bash scripts/setup-skills.sh` once after clone and again after adding a new skill
- start a new Codex session if a skill was added or renamed
- mention the skill name directly or ask for the matching workflow

## OpenCode

OpenCode supports project-local skills from `.opencode/skills/<skill-name>/SKILL.md`. This repository exposes the library through:

```text
.opencode/skills -> ../.skill/skills
```

Usage:
- open the repository in OpenCode
- start a new session after adding or renaming a skill
- mention the skill name directly in the task

## Contributor workflow

1. Edit `.skill/skills/<skill-name>/SKILL.md`.
2. Add or update files under `.skill/skills/<skill-name>/references/` only when the main skill file would become too long.
3. Run `bash scripts/setup-skills.sh` if you use Codex locally.
4. Commit the repository files instead of home-directory copies.
