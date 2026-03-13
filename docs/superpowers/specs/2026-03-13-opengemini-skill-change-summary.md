# openGemini Skill Change Summary

## Scope

This branch adds and organizes the current openGemini skill set under `~/.codex/skills` and records the supporting design notes in `docs/superpowers/`.

## Skills Added

- `opengemini-local-dev`
- `opengemini-ci-gates`
- `opengemini-config-editing`
- `opengemini-query-compat-testing`
- `opengemini-distributed-debugging`
- `opengemini-python-udf`
- `opengemini-mcp-server`
- `opengemini-docker-k8s-deploy`
- `opengemini-go-performance-style`

## Supporting References Added

- `opengemini-local-dev/references/runtime-layout.md`
- `opengemini-config-editing/references/config-hotspots.md`
- `opengemini-distributed-debugging/references/log-map.md`
- `opengemini-python-udf/references/runtime-map.md`
- `opengemini-docker-k8s-deploy/references/env-matrix.md`
- `opengemini-go-performance-style/references/pattern-map.md`

## Documentation Added

- second-batch and third-batch skill design docs
- second-batch and third-batch implementation plans
- `2026-03-13-opengemini-go-performance-style-design.md`
- `2026-03-13-opengemini-go-performance-style.md`
- `2026-03-13-opengemini-skill-catalog.md`

## Refinement Pass

The current pass narrows all eight skill descriptions so triggering is more precise and less likely to load the wrong skill from broad repository keywords alone.

## Pending Integration Work

- decide whether to keep the worktree branch as-is or prepare a commit
- optionally add runtime validation for skill triggering behavior
- optionally add a top-level contributor note pointing to the skill catalog
