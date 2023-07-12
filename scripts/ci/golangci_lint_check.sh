#!/usr/bin/env bash

set -e

golangci-lint run --disable=staticcheck --timeout=10m --tests=false --skip-dirs=open_src --skip-dirs=tests ./...
