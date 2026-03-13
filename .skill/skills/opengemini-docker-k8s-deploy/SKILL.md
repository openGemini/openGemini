---
name: opengemini-docker-k8s-deploy
description: Use when deploying openGemini with the repository's Docker images or K8s-style manifests, especially for OPEN_GEMINI_* env vars, domain substitution, or choosing between standalone and multi-role container flows.
---

# openGemini Docker and K8s Deploy

This repository has two distinct container stories: a simple server image and a multi-role Docker/K8s deployment flow. Keep them separate.

## When to Use

- You are editing `docker/README.md`, `docker/server`, or Docker entrypoints
- You need the env vars for multi-role container startup
- You need to understand why Docker requires `domain = "{{domain}}"`
- You need the quick `docker run` path for the standalone server image

## Deployment Split

- `docker/server/README.md`: simple standalone server image, `docker run`, port mapping, and volume mounts
- `docker/README.md`: multi-role Docker, K8s, and KubeEdge deployment with config substitution

## Key Environment Variables

Read [references/env-matrix.md](references/env-matrix.md) before changing the deployment flow.

## Core Rule

In container deployments, node IPs can change. The checked-in Docker docs require `domain = "{{domain}}"` and corresponding `OPEN_GEMINI_DOMAIN` substitution so cluster membership stays stable.

## Common Mistakes

- Mixing the standalone server image docs with the multi-role cluster deployment path
- Forgetting `OPEN_GEMINI_CONFIG` when mounting config into the container
- Updating `OPEN_GEMINI_DOMAIN` handling in one place but not the matching config template expectations
- Assuming container IPs are stable enough to replace domain-based discovery
