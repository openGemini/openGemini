#!/usr/bin/env bash

meta_conf="/opt/tsdb/tsmeta/config/meta.conf"
sql_conf="/opt/tsdb/tssql/config/sql.conf"
store_conf="/opt/tsdb/tsstore/config/storage.conf"

function checkMeta(){
  count=`ps -ef |grep $1 |grep -v "grep" |wc -l`
  if [ 0 == $count ];then
    pidfile=`ls /opt/tsdb/tsmeta/pid/meta.pid`
    if [ ! -f "$pidfile" ]; then
        touch /opt/tsdb/tsmeta/pid/meta.pid
    fi
	conf=`ls /opt/tsdb/tsmeta/config/`
    extrafile=`ls /opt/tsdb/log/meta*_extra.log`
	extraCount=0
    extrabak="/opt/tsdb/log/meta_extra.log.bak1"
    if [ -f "$extrabak" ]; then
        extraCount=`ls /opt/tsdb/log/meta_extra.log.bak* | wc -l`
    fi
    echo $extraCount
    for((i=$extraCount;i>=1;i--))
    do
        mv /opt/tsdb/log/meta_extra.log.bak$i /opt/tsdb/log/meta_extra.log.bak$(($i+1))
    done
    echo $extrafile
    cp $extrafile /opt/tsdb/log/meta_extra.log.bak1
    echo $pidfile
    echo $conf
    nohup /opt/tsdb/tsmeta/bin/ts-meta  -pidfile /opt/tsdb/tsmeta/pid/$pidfile -config /opt/tsdb/tsmeta/config/$conf > $extrafile 2>&1 &
  fi
}

function checkStore(){
  count=`ps -ef |grep $1 |grep -v "grep" |wc -l`
  if [ 0 == $count ];then
       conf=`ls /opt/tsdb/tsstore/config/`
       extrafile=`ls /opt/tsdb/log/storage*_extra.log`
       extraCount=0
       extrabak="/opt/tsdb/log/storage_extra.log.bak1"
       if [ -f "$extrabak" ]; then
           extraCount=`ls /opt/tsdb/log/storage_extra.log.bak* | wc -l`
       fi
        for((i=$extraCount;i>=1;i--))
        do
            mv /opt/tsdb/log/storage_extra.log.bak$i /opt/tsdb/log/storage_extra.log.bak$(($i+1))
        done
        cp $extrafile /opt/tsdb/log/storage_extra.log.bak1
        echo $extrafile
        nohup /opt/tsdb/tsstore/bin/ts-store -config /opt/tsdb/tsstore/config/$conf > $extrafile 2>&1 &
    fi
}

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
    if [ -f ${meta_conf} ]; then
        checkMeta ts-meta
    fi
    if [ -f ${sql_conf} ]; then
    	checkSql ts-sql
    fi
    if [ -f ${store_conf} ]; then
    	checkStore ts-store
    fi
    sleep 5
done
