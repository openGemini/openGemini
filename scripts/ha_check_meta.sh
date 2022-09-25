#!/bin/bash

function checkMeta(){
    count=`ps -ef |grep $1 |grep -v "grep" |wc -l`
    if [ 0 == $count ];then
	    conf=`ls /opt/tsdb/tsmeta/config/`
        extrafile=`ls /opt/tsdb/log/meta*_extra.log`
	    extraCount=0
        extrabak="/opt/tsdb/log/meta_extra.log.bak1"
        if [ -f "$extrabak" ]; then
            extraCount=`ls /opt/tsdb/log/meta_extra.log.bak* | wc -l`
        fi
        for((i=$extraCount;i>=1;i--))
        do
            mv /opt/tsdb/log/meta_extra.log.bak$i /opt/tsdb/log/meta_extra.log.bak$(($i+1))
        done
        cp $extrafile /opt/tsdb/log/meta_extra.log.bak1
        nohup /opt/tsdb/tsmeta/bin/ts-meta  -pidfile /opt/tsdb/tsmeta/pid/meta.pid -config /opt/tsdb/tsmeta/config/$conf > $extrafile 2>&1 &
    fi
}

while true
do
    checkMeta ts-meta
    sleep 5
done
