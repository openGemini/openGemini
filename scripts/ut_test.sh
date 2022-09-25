#!/bin/bash

function backup_log() {
    if [ $# -ge 2 ]
    then
    	SAVE_LOG_COUNT=$1
    	BUILD_ID=$2

    	if [ ${SAVE_LOG_COUNT} -gt 0 ]
    	then
    		BAK_LOG_NAME=test_log_${BUILD_ID}
    		if [ -d "test_log" ]
    		then
    			mv test_log ${BAK_LOG_NAME}

    			BAK_LOG_COUNT=`ls -t | grep test_log | grep -v -w test_log_tmp | grep -v -w test_log | wc -l`
    			if [ ${BAK_LOG_COUNT} -gt ${SAVE_LOG_COUNT} ]
    			then
    				REMOVE_COUNT=`expr ${BAK_LOG_COUNT} - ${SAVE_LOG_COUNT}`
    				#remove last "REMOVE_COUNT" files
    				rm -rf `ls -t | grep test_log | grep -v -w test_log_tmp | grep -v -w test_log | tail -n ${REMOVE_COUNT}`
    			fi
    		fi
    	fi
    fi

    if [ -d "/root/.jenkins/workspace/TSDB2.0/" ]; then
        BAK_LOG_NAME=test_log_${BUILD_ID}
        cp -R ${BAK_LOG_NAME} /root/.jenkins/workspace/TSDB2.0/
        cd /root/.jenkins/workspace/TSDB2.0/
        SAVE_LOG_COUNT=$1
        BAK_LOG_COUNT=`ls -t | grep test_log | grep -v -w test_log_tmp | grep -v -w test_log | wc -l`
        if [ ${BAK_LOG_COUNT} -gt ${SAVE_LOG_COUNT} ]
        then
            REMOVE_COUNT=`expr ${BAK_LOG_COUNT} - ${SAVE_LOG_COUNT}`
            rm -rf `ls -t | grep test_log | grep -v -w test_log_tmp | grep -v -w test_log | tail -n ${REMOVE_COUNT}`
        fi
    fi
}

PROJECT_DIR=$PWD

go install github.com/pingcap/failpoint/failpoint-ctl

# failpoint just for UT
# Converting gofail failpoints...
find $PROJECT_DIR -type d | grep -vE "(\.cid|\.build_config|tests)" | xargs failpoint-ctl enable

SAVE_LOG_COUNT=1
BUILD_ID=

killall -w influxd
killall -w ts-meta
killall -w ts-store
killall -w ts-sql
killall -w dlv

#
# step1: save last execute test_log and clear logs
#
if [ ! -d "test_log" ]
then
	mkdir -p test_log
else	
	#mv test_log test_log_tmp
	rm -rf test_log
	mkdir -p test_log
fi
rm -rf /opt/tsdb/data
rm -rf /opt/tsdb/logs
mkdir -p /opt/tsdb/logs
rm -rf go.sum
go mod download

#
# step2: execute ut test cases
#
echo "start run UT test "

go test -count=1 $(go list ./... | grep -v tests | grep -v github.com/hashicorp) -v | tee ./test_log/test_ut.log

echo "end run ut test "
find $PROJECT_DIR -type d | grep -vE "(\.cid|\.build_config|tests)" | xargs failpoint-ctl disable

FAIL_COUNT=`grep 'FAIL' test_log/test_ut.log | wc -l`
if [ $FAIL_COUNT -ne 0 ]
then
    backup_log $*
    exit 1
fi
# step2: end

python build.py  --clean

#
# step3: execute AT test cases: hot duration
#
echo "start run AT-hot duration test"
for t in "normal"
do
    mkdir -p /opt/tsdb/logs

    sed -i "s/shard-tier = \"warm\"/shard-tier = \"hot\"/g" config/openGemini-1.conf
    sed -i "s/cache-table-data-block = false/cache-table-data-block = true/g" config/openGemini-1.conf
    sed -i "s/cache-table-meta-block = false/cache-table-meta-block = true/g" config/openGemini-1.conf
    sed -i "s/cache-table-data-block = false/cache-table-data-block = true/g" config/openGemini-2.conf
    sed -i "s/cache-table-meta-block = false/cache-table-meta-block = true/g" config/openGemini-2.conf
    sed -i "s/cache-table-data-block = false/cache-table-data-block = true/g" config/openGemini-3.conf
    sed -i "s/cache-table-meta-block = false/cache-table-meta-block = true/g" config/openGemini-3.conf
    sh scripts/install_cluster.sh

    sleep 10
    echo "start run test: " $t
    URL=http://127.0.0.1:8086 LOGDIR=/opt/tsdb/logs CONFDIR=${GOPATH}/src/PyxisDB/config go test -mod=mod -test.parallel 1 -timeout 10m ./tests -v GOCACHE=off -args $t | tee test_log/test_at_hot_${t}.log
    echo "end run test: " $t

    killall -w influxd
    killall -w ts-meta
    killall -w ts-store
    killall -w ts-sql
    killall -w dlv

    mkdir -p test_log/${t}

    cp /opt/tsdb/logs/* test_log/${t}
done

FAIL_COUNT=`grep 'FAIL' test_log/test_at_hot_*.log | wc -l`
if [ $FAIL_COUNT -ne 0 ]
then
    backup_log $*
    exit 1
fi
# step3: end

sed -i "s/DefaultInnerChunkSize = 1024/DefaultInnerChunkSize = 1/g" open_src/influx/httpd/handler.go
python build.py  --clean
#
# step4: execute AT test cases: warm duration
#
echo "start run AT-warm duration test, DefaultInnerChunkSize: 1"
for t in "normal"
do
    mkdir -p /opt/tsdb/logs/

    sed -i "s/shard-tier = \"hot\"/shard-tier = \"warm\"/g" config/openGemini-1.conf
    for store in config/openGemini-1.conf config/openGemini-2.conf config/openGemini-3.conf
    do
    echo ${store}
    sed -i "s/cache-table-data-block = true/cache-table-data-block = false/g" ${store}
    sed -i "s/cache-table-meta-block = true/cache-table-meta-block = false/g" ${store}
    sed -i "s/enable-mmap-read = true/enable-mmap-read = false/g" ${store}
    sed -i "s/read-cache-limit = 0/read-cache-limit = 5368709120/g" ${store}
    done

    sh scripts/install_cluster.sh

    sleep 10
    echo "start run test: " $t
    URL=http://127.0.0.1:8086 LOGDIR=/opt/tsdb/logs CONFDIR=${GOPATH}/src/PyxisDB/config go test -mod=mod -test.parallel 1 -timeout 10m ./tests -v GOCACHE=off -args $t | tee test_log/test_at_warm_${t}.log
    echo "end run test: " $t

    killall -w influxd
    killall -w ts-meta
    killall -w ts-store
    killall -w ts-sql
    killall -w dlv

    mkdir -p test_log/${t}

    cp /opt/tsdb/logs/* test_log/${t}
done

FAIL_COUNT=`grep 'FAIL' test_log/test_at_warm_*.log | wc -l`
if [ $FAIL_COUNT -ne 0 ]
then
    backup_log $*
    exit 1
fi
# step4: end

#
# step5: backup test_logs
#
backup_log $*