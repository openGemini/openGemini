#!/usr/bin/env bash

set -e

go list ./... | grep -vE "tests|lifted" | xargs go vet -tests=false