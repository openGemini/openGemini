#!/usr/bin/env bash

function splittingLine()
{
  echo ""
  echo "$(date "+%Y-%m-%d %H:%M:%S") ======================== $1 =========================="
  echo ""
}

splittingLine "commit lint check start"
go install github.com/conventionalcommit/commitlint@v0.10.1

git log -1 --pretty=format:"%s%n%n%b" | tee COMMIT_EDITMSG

commitlint lint -m COMMIT_EDITMSG

if [[ $? -ne 0 ]]; then
    rm -f COMMIT_EDITMSG
    exit 1
fi
rm -f COMMIT_EDITMSG

########################### check licence header ###############################################
splittingLine "start: check licence header"

make licence-check

if [[ $? -ne 0 ]]; then
    exit 1
fi

splittingLine "end: check licence header"

########################### static check ###############################################
splittingLine "start: static check and other go lint"

[[ -s "$GVM_ROOT/scripts/gvm" ]] && source "$GVM_ROOT/scripts/gvm"
gvm use go1.19 -y --default

rm -f go.sum
go mod tidy

make static-check
golang_static_status=$?

make go-version-check
golang_version_status=$?

make go-generate
golang_generate_status=$?

make style-check
golang_style_status=$?

make go-vet-check
golang_vet_status=$?

make golangci-lint-check
golang_lint_status=$?

splittingLine "end: static check and other go lint"

if [[ $golang_static_status -ne 0 || $golang_version_status -ne 0
      || $golang_generate_status -ne 0 || $golang_style_status  -ne 0
      || $golang_vet_status  -ne 0 || $golang_lint_status -ne 0 ]]; then
    exit 1
fi

########################### unit test ###############################################

rm -f go.sum
go mod tidy

rm -rf /tmp/openGemini/*
mkdir /tmp/openGemini/logs -p

########################### test build for multi platform ###############################################
splittingLine "start: test build for multi platform"

make build-check
build_check_status=$?

splittingLine "end: test build for multi platform"

if [[ $build_check_status -ne 0 ]]; then
    exit 1
fi

########################### integration test ###############################################

sed -i 's/# time-range-limit = \["72h", "24h"\]/time-range-limit = \["0s", "24h"\]/g' config/openGemini.conf
sed -i "s/# ptnum-pernode = 2/ptnum-pernode = 2/g" config/openGemini.conf
sed -i "s/# pushers = \"\"/pushers = \"file\"/g" config/openGemini.conf
sed -i "s/# store-enabled = false/store-enabled = true/g" config/openGemini.conf
sed -i "s~# store-path = \"/tmp/openGemini/metric/{{id}}/metric.data\"~store-path = \"/tmp/openGemini/metric/{{id}}/metric.data\"~g" config/openGemini.conf
rm -rf /tmp/openGemini/metric/

splittingLine "start run AT-hot duration test"
for t in "normal"
do
    sed -i "s/# shard-tier = \"warm\"/shard-tier = \"hot\"/g" config/openGemini.conf

    sed -i "s/cache-table-data-block = false/cache-table-data-block = true/g" config/openGemini.conf
    sed -i "s/cache-table-meta-block = false/cache-table-meta-block = true/g" config/openGemini.conf

    if [[ $t == "HA" ]]; then
        sed -i "s/ha-enable = false/ha-enable = true/g" config/openGemini.conf
    else
        sed -i "s/ha-enable = true/ha-enable = false/g" config/openGemini.conf
    fi

    sh scripts/install_cluster.sh

    sleep 10
    splittingLine "start run test: $t"
    URL=http://127.0.0.1:8086 LOGDIR=/tmp/openGemini/logs CONFDIR=${GOPATH}/src/github.com/openGemini/openGemini/config go test -mod=mod -test.parallel 1 -timeout 10m ./tests -v GOCACHE=off -args $t | tee /tmp/openGemini/logs/test_at_hot_${t}.log
    splittingLine "end run test: $t"

    killall -9 -w ts-meta
    killall -9 -w ts-store
    killall -9 -w ts-sql

    FAIL_COUNT=`grep 'FAIL' /tmp/openGemini/logs/test_at_hot_*.log | wc -l`
    if [ $FAIL_COUNT -ne 0 ]; then
        git restore config/openGemini.conf
        exit 1
    fi
done


sed -i "s/DefaultInnerChunkSize = 1024/DefaultInnerChunkSize = 1/g" open_src/influx/httpd/handler.go

python build.py  --clean

splittingLine "start run AT-warm duration test, DefaultInnerChunkSize: 1"
for t in "normal"
do
    sed -i "s/shard-tier = \"hot\"/shard-tier = \"warm\"/g" config/openGemini.conf

    sed -i "s/cache-table-data-block = true/cache-table-data-block = false/g" config/openGemini.conf
    sed -i "s/cache-table-meta-block = true/cache-table-meta-block = false/g" config/openGemini.conf
    sed -i "s/enable-mmap-read = true/enable-mmap-read = false/g" config/openGemini.conf
    sed -i "s/read-cache-limit = 0/read-cache-limit = 5368709120/g" config/openGemini.conf
    sed -i "/subscriber/a\  enabled = true"	config/openGemini.conf

    if [[ $t == "HA" ]]; then
        sed -i "s/ha-enable = false/ha-enable = true/g" config/openGemini.conf
    else
        sed -i "s/ha-enable = true/ha-enable = false/g" config/openGemini.conf
    fi

    bash scripts/install_cluster.sh

    sleep 10
    splittingLine "start run test: $t"
    URL=http://127.0.0.1:8086 LOGDIR=/tmp/openGemini/logs CONFDIR=${GOPATH}/src/github.com/openGemini/openGemini/config go test -mod=mod -test.parallel 1 -timeout 10m ./tests -v GOCACHE=off -args $t | tee /tmp/openGemini/logs/test_at_warm_${t}.log
    splittingLine "end run test: $t"

    killall -9 -w ts-meta
    killall -9 -w ts-store
    killall -9 -w ts-sql

    FAIL_COUNT=`grep 'FAIL' /tmp/openGemini/logs/test_at_warm_*.log | wc -l`
    if [ $FAIL_COUNT -ne 0 ]; then
        git restore config/openGemini.conf
        exit 1
    fi
done

# restore the preceding changes.
sed -i 's/time-range-limit = \["0s", "24h"\]/# time-range-limit = \["72h", "24h"\]/g' config/openGemini.conf
sed -i "s/shard-tier = \"warm\"/# shard-tier = \"warm\"/g" config/openGemini.conf
sed -i "s/read-cache-limit = 5368709120/read-cache-limit = 0/g" config/openGemini.conf

sed -i "s/DefaultInnerChunkSize = 1/DefaultInnerChunkSize = 1024/g" open_src/influx/httpd/handler.go

sed -i "s/ptnum-pernode = 2/# ptnum-pernode = 2/g" config/openGemini.conf
sed -i "s/pushers = \"file\"/# pushers = \"\"/g" config/openGemini.conf
sed -i "s/store-enabled = true/# store-enabled = false/g" config/openGemini.conf
sed -i "s~store-path = \"/tmp/openGemini/metric/{{id}}/metric.data\"~# store-path = \"/tmp/openGemini/metric/{{id}}/metric.data\"~g" config/openGemini.conf
sed -i -e "/subscriber/{n;d}" config/openGemini.conf

git restore config/openGemini.conf
