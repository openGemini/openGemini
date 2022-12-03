#!/usr/bin/env bash

function checkStore(){
    count=`ps -ef |grep $1 |grep -v "grep" |wc -l`
    if [ 0 == $count ];then
        conf=`ls /opt/tsdb/tsstore/config/`
        extrafile=`ls /opt/tsdb/log/store*_extra.log`
        extraCount=0
        extrabak="/opt/tsdb/log/store_extra.log.bak1"
        if [ -f "$extrabak" ]; then
            extraCount=`ls /opt/tsdb/log/store_extra.log.bak* | wc -l`
        fi
        for((i=$extraCount;i>=1;i--))
        do
            mv /opt/tsdb/log/store_extra.log.bak$i /opt/tsdb/log/store_extra.log.bak$(($i+1))
        done
        cp $extrafile /opt/tsdb/log/store_extra.log.bak1
        nohup /opt/tsdb/tsstore/bin/ts-store -config /opt/tsdb/tsstore/config/$conf > $extrafile 2>&1 &
    fi
}

while true
do
    checkStore ts-store
    sleep 5
done
