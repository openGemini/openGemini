# openGemini Skills PR Summary

## Summary

- add nine repository-specific Codex skills for openGemini development, testing, deployment, and performance-sensitive Go review
- add supporting reference files for runtime layout, config hotspots, debugging paths, Python UDF runtime, Docker env vars, and performance pattern mapping
- add design, plan, catalog, and change-summary documents under `docs/superpowers/`

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

## Why

These skills capture openGemini-specific workflows that are easy to get wrong without repository context:
- local standalone and pseudo-cluster startup
- CI gate ordering and failpoint-aware validation
- placeholder-driven config editing
- protocol compatibility test placement
- multi-process debugging paths
- Python UDF and MCP server workflows
- Docker and K8s env-var-driven deployment
- high-performance Go coding style for memory reuse, pooling, and bounded concurrency

## Verification

Static verification completed for all added skills:
- each skill file exists with valid frontmatter
- referenced repository anchors exist
- supporting reference files exist
- skill catalog and change summary include the full set

## Notes

- the work is isolated in worktree branch `opengemini-skill-batch-2`
- `.worktrees/` was added to `.gitignore` in the worktree branch to prevent future tracking mistakes
- runtime trigger validation for the skills has not been implemented; current verification is static
