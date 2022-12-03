#!/usr/bin/env bash
HA_PID=$(ps -ef |grep ha_check.sh |grep -v grep |awk '{print $2}')
if [ "${HA_PID}" == "" ];then
  nohup bash /home/ha_check.sh >>/opt/tsdb/log/ha_check.log 2>&1 &
fi

