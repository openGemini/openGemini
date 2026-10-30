#!/usr/bin/env bash

set -eo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
checker=${1:-"$script_dir/go_version_check.sh"}
checker=$(realpath "$checker")
test_dir=$(mktemp -d)
trap 'rm -rf "$test_dir"' EXIT
mkdir "$test_dir/bin"

cat > "$test_dir/bin/go" <<'GO'
#!/usr/bin/env bash
if [ "${MOCK_GO_FAILURE:-0}" = 1 ]; then
    echo "mock Go command failed" >&2
    exit 2
fi
case "$1" in
    version)
        printf 'go version go%s linux/amd64\n' "$MOCK_GO_VERSION"
        ;;
    mod)
        # Support the original checker's go mod edit -go=... -print call.
        printf 'module example.com/version-check\n\ngo %s\n' "${3#-go=}"
        ;;
    *) exit 2 ;;
esac
GO
chmod +x "$test_dir/bin/go"

failures=0
run_case() {
    local installed=$1 declared=$2 expected=$3 failure=${4:-0} format=${5:-lf}
    printf 'module example.com/version-check\n\ngo %s\n' "$declared" > "$test_dir/go.mod"
    if [ "$format" = crlf ]; then
        sed -i 's/$/\r/' "$test_dir/go.mod"
    fi
    cp "$test_dir/go.mod" "$test_dir/go.mod.before"
    local actual=0
    (cd "$test_dir" && PATH="$test_dir/bin:$PATH" MOCK_GO_VERSION="$installed" MOCK_GO_FAILURE="$failure" bash "$checker") > "$test_dir/output" 2>&1 || actual=$?
    if { [ "$expected" = pass ] && [ "$actual" -ne 0 ]; } || { [ "$expected" = fail ] && [ "$actual" -eq 0 ]; }; then
        printf 'FAIL: toolchain=%s go.mod=%s expected=%s format=%s exit=%s\n' "$installed" "$declared" "$expected" "$format" "$actual"
        cat "$test_dir/output"
        failures=$((failures + 1))
    else
        printf 'PASS: toolchain=%s go.mod=%s expected=%s format=%s\n' "$installed" "$declared" "$expected" "$format"
    fi
    if ! cmp -s "$test_dir/go.mod" "$test_dir/go.mod.before"; then
        echo 'FAIL: checker modified go.mod'
        failures=$((failures + 1))
    fi
}

run_case 1.25.0 1.25.0 pass
run_case 1.25.0 1.25 pass
run_case 1.25 1.25.0 pass
run_case 1.25.6 1.25.0 pass
run_case 1.25rc1 1.25.0 pass
run_case 1.24.9 1.25.0 fail
run_case 1.26.4 1.25.0 fail
run_case 2.25.0 1.25.0 fail
run_case invalid 1.25.0 fail
run_case 1.25.0 '' fail
run_case 1.25.0 1.25.0 fail 1
run_case 1.25.0 1.25 pass 0 crlf
run_case 1.25.0 1.25.0 pass 0 crlf

if [ "$failures" -ne 0 ]; then
    printf '%s regression checks failed\n' "$failures" >&2
    exit 1
fi

echo 'All Go version regression checks passed'
