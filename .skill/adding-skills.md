# Adding or Updating Skills

Use this checklist when you add a new project skill or change an existing one.

## Repository rules

- Keep the repository copy under `.skill/skills/` as the only editable source.
- Use one directory per skill: `.skill/skills/<skill-name>/SKILL.md`.
- Use lowercase hyphenated names such as `opengemini-query-compat-testing`.
- Add `references/` only when the main skill file would otherwise become too long.

## SKILL.md rules

- Frontmatter supports only `name` and `description`.
- Keep `description` focused on when the skill should be used.
- Prefer concise sections with concrete triggers, commands, and file paths.
- Reuse repository terminology such as `ts-meta`, `ts-store`, `tests/server_test.go`, and `config/openGemini.conf`.

## Update process

1. Edit or create files under `.skill/skills/`.
2. Update `.skill/README.md` if the skill list changed.
3. Update `.skill/platforms.md` only if platform behavior changed.
4. Run `bash scripts/setup-skills.sh` if you use Codex locally.
5. Start a new Claude Code, Codex, or OpenCode session if you added or renamed a skill.

## Verification

Run:

```bash
find .skill/skills -maxdepth 2 -type f -name 'SKILL.md' | sort
bash scripts/setup-skills.sh
```

Check:
- the new skill appears under `.skill/skills/`
- `.claude/skills` and `.opencode/skills` still point to `../.skill/skills`
- `~/.codex/skills/<skill-name>` points back to the repository copy

## Commit scope

Commit repository files only. Do not commit home-directory backups or local platform caches.
