---
name: opengemini-ci-gates
description: Use when validating openGemini changes for PRs, reproducing CI failures locally, or choosing the repository's required quality gates for a change.
---

# openGemini CI Gates

Use the repository's real gate order instead of ad hoc `go test ./...` guesses.

## When to Use

- You changed Go code and need the minimum correct local validation
- CI failed and you need to reproduce the failing stage locally
- You need to know whether a docs-only or config-only change needs full verification
- You are reviewing a PR and want the repo-native command order

## Gate Order

CI and `scripts/ut_test.sh` establish this command surface:

1. `make license-check`
2. `make static-check`
3. `make go-version-check`
4. `make go-generate`
5. `make style-check`
6. `make go-vet-check`
7. `make gotest`
8. `make build-check`
9. `make integration-test`

`make gotest` enables failpoints automatically, so prefer it over a raw `go test ./...` sweep when you want parity with repository expectations.

## Minimal Validation Matrix

- Docs-only or markdown-only changes: inspect the touched files and skip Go validation unless the change affects generated docs or checked-in command examples.
- Small Go logic changes without startup or protocol impact: run `make style-check`, `make go-vet-check`, and `make gotest`.
- Build, codegen, or dependency changes: run the full lint chain through `make go-generate`, then `make gotest`.
- Cluster startup, config-sensitive, or end-to-end behavior changes: add `make build-check` and `make integration-test`.

## Reproducing CI

For direct CI parity, follow `.github/workflows/ci.yml`:

```bash
make license-check
make static-check
make go-version-check
make go-generate
make style-check
make go-vet-check
go mod tidy
make gotest
make build-check
make go-build
make start-subscriber
bash scripts/install_cluster.sh
make integration-test
make stop-subscriber
```

## Common Pitfalls

- `make style-check` mutates imports; inspect `git diff` after running it.
- `make gotest` depends on `failpoint-ctl`; let the make target install and enable it.
- `make integration-test` assumes a running local cluster at `http://127.0.0.1:8086`.
- `scripts/ut_test.sh` mutates `config/openGemini.conf`; restore config changes before committing.

## Canonical Sources

- `Makefile`
- `Makefile.common`
- `scripts/ut_test.sh`
- `.github/workflows/ci.yml`
