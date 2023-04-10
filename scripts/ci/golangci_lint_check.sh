#!/usr/bin/env bash

set -e

golangci-lint run --timeout=10m --tests=false --skip-dirs=open_src ./...
