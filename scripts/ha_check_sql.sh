#!/bin/bash

function checkSql(){
    count=`ps -ef |grep $1 |grep -v "grep" |wc -l`
    if [ 0 == $count ];then
        conf=`ls /opt/tsdb/tssql/config/`
        extrafile=`ls /opt/tsdb/log/sql*_extra.log`
        extraCount=0
        extrabak="/opt/tsdb/log/sql_extra.log.bak1"
        if [ -f "$extrabak" ]; then
            extraCount=`ls /opt/tsdb/log/sql_extra.log.bak* | wc -l`
        fi
        for((i=$extraCount;i>=1;i--))
        do
            mv /opt/tsdb/log/sql_extra.log.bak$i /opt/tsdb/log/sql_extra.log.bak$(($i+1))
        done
        cp $extrafile /opt/tsdb/log/sql_extra.log.bak1
        nohup /opt/tsdb/tssql/bin/ts-sql  -pidfile /opt/tsdb/tssql/pid/sql.pid -config /opt/tsdb/tssql/config/$conf > $extrafile 2>&1 &
    fi
}

while true
do
    checkSql ts-sql
    sleep 5
done
