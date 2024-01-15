#!/usr/bin/env bash

set -e

go list ./... | grep -vE "tests|open_src|lifted" | xargs go vet -tests=false