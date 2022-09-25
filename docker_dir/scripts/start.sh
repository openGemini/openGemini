#!/bin/bash
DIR=$(cd `dirname $0`;pwd)

source $DIR/build_config.sh

BIN_PATH=/home/pyxisdb/bin
CONF_PATH=/home/pyxisdb/config
LOG_PATH=/opt/dbs/logs

mkdir -p $LOG_PATH
local_ip_addr=`/sbin/ifconfig -a|grep inet|grep -v 127.0.0.1|grep -v inet6|awk '{print $2}'|tr -d "addr:"`

function startApp() {
  app=$1

  conf="gemini"
  if [[ $app == "monitor" ]]; then
    conf="monitor"
  fi

  ok=`echo $PYXISDB_LAUNCH|grep "$app"`
  if [ -n "$ok" ]; then
    echo "start: ts-$app"
    nohup $BIN_PATH/ts-$app -config $CONF_PATH/${conf}.conf >> $LOG_PATH/${app}_extra.log 2>&1 &
  fi
}

startApp meta
startApp sql
startApp store

while true; do

  sleep 1s
  echo `date`

done