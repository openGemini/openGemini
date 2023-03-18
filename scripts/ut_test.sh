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

rm -f go.sum
go mod tidy

PROJECT_DIR=$PWD

splittingLine "failpoint enable"
go install github.com/pingcap/failpoint/failpoint-ctl

# failpoint just for UT
# Converting gofail failpoints...
find $PROJECT_DIR -type d | grep -vE "(\.cid|\.build_config|tests)" | xargs failpoint-ctl enable

rm -rf /tmp/openGemini/*
mkdir /tmp/openGemini/logs -p

splittingLine "start run UT test "

for s in $(go list ./... | grep -v tests | grep -v github.com/hashicorp)
do
  go test -failfast -short -v -count 1 -p 1 -timeout 10m $s | tee -a /tmp/openGemini/logs/gotest.log
  if [ ${PIPESTATUS[0]} -ne 0 ]
  then
     find $PROJECT_DIR -type d | grep -vE "(\.cid|\.build_config|tests)" | xargs failpoint-ctl disable
     exit 1
  fi
done

splittingLine "end run ut test"

splittingLine "failpoint disable"
find $PROJECT_DIR -type d | grep -vE "(\.cid|\.build_config|tests)" | xargs failpoint-ctl disable

########################### test build for multi platform ###############################################
splittingLine "start: test build for multi platform"

python build.py  --version v1.0.0 --clean --platform darwin --arch amd64

python build.py  --version v1.0.0 --clean --platform darwin --arch arm64

python build.py  --version v1.0.0 --clean --platform linux --arch arm64

python build.py  --version v1.0.0 --clean --platform linux --arch amd64

splittingLine "end: test build for multi platform"


sed -i 's/# time-range-limit = \["72h", "24h"\]/time-range-limit = ["0s", "24h"]/g' config/openGemini.conf
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
        exit 1
    fi
done

# restore the preceding changes.
sed -i "s/shard-tier = \"warm\"/# shard-tier = \"warm\"/g" config/openGemini.conf
sed -i "s/read-cache-limit = 5368709120/read-cache-limit = 0/g" config/openGemini.conf

sed -i "s/DefaultInnerChunkSize = 1/DefaultInnerChunkSize = 1024/g" open_src/influx/httpd/handler.go

sed -i "s/ptnum-pernode = 2/# ptnum-pernode = 2/g" config/openGemini.conf
sed -i "s/pushers = \"file\"/# pushers = \"\"/g" config/openGemini.conf
sed -i "s/store-enabled = true/# store-enabled = false/g" config/openGemini.conf
sed -i "s~store-path = \"/tmp/openGemini/metric/{{id}}/metric.data\"~# store-path = \"/tmp/openGemini/metric/{{id}}/metric.data\"~g" config/openGemini.conf
