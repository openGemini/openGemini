#!/usr/bin/env bash

set -e

go list ./... | grep -vE "tests|open_src|ts-cli" | xargs go vet -tests=false