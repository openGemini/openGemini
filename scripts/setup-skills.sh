#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
SOURCE_DIR="$ROOT_DIR/.skill/skills"
CLAUDE_LINK="$ROOT_DIR/.claude/skills"
OPENCODE_LINK="$ROOT_DIR/.opencode/skills"
CODEX_DIR="$HOME/.codex/skills"
BACKUP_STAMP=$(date +%Y%m%d%H%M%S)

ensure_repo_link() {
  local link_path=$1
  local target=$2
  mkdir -p "$(dirname "$link_path")"
  if [ -L "$link_path" ]; then
    local current
    current=$(readlink "$link_path")
    if [ "$current" = "$target" ]; then
      echo "ok   $link_path -> $target"
      return
    fi
    rm -f "$link_path"
  elif [ -e "$link_path" ]; then
    echo "skip $link_path exists and is not a symlink"
    return
  fi
  ln -s "$target" "$link_path"
  echo "link $link_path -> $target"
}

if [ ! -d "$SOURCE_DIR" ]; then
  echo "missing source directory: $SOURCE_DIR" >&2
  exit 1
fi

ensure_repo_link "$CLAUDE_LINK" ../.skill/skills
ensure_repo_link "$OPENCODE_LINK" ../.skill/skills

mkdir -p "$CODEX_DIR"
for skill_dir in "$SOURCE_DIR"/*; do
  [ -d "$skill_dir" ] || continue
  skill_name=$(basename "$skill_dir")
  target="$CODEX_DIR/$skill_name"
  if [ -L "$target" ]; then
    rm -f "$target"
  elif [ -e "$target" ]; then
    backup_path="${target}.bak.${BACKUP_STAMP}"
    mv "$target" "$backup_path"
    echo "move $target -> $backup_path"
  fi
  ln -s "$skill_dir" "$target"
  echo "link $target -> $skill_dir"
done
