# Environment Matrix

## Multi-role Docker and K8s variables

- `OPEN_GEMINI_LAUNCH`: comma-separated roles such as `ts-meta,ts-sql,ts-store`
- `OPEN_GEMINI_DOMAIN`: stable domain used to fill `domain = "{{domain}}"`
- `OPEN_GEMINI_CONFIG`: mounted config file path inside the container

## Why domain substitution exists

`docker/README.md` explains that container IPs may change after restart, so Docker and K8s flows should use domain-based node discovery instead of fixed IPs.

## Simple standalone server image

`docker/server/README.md` uses the server image with commands like:
- `docker run -d --name opengemini-server-example opengeminidb/opengemini-server`
- `docker run -d -p 8086:8086 --name opengemini-server-example opengeminidb/opengemini-server`

The server entrypoint rewrites paths and replaces `127.0.0.1` with the container's local address before starting `ts-server`.
