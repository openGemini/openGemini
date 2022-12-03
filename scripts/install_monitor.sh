#!/usr/bin/env bash

ps -ef | grep -v grep | grep -v init_monitor_influxdb.sh | grep ts-monitor | grep $USER > /dev/null
if [ $? == 0 ];then
	killall -9 -w ts-monitor
fi


mkdir -p /opt/tsdb/logs

rm -rf /opt/tsdb/logs/monitor*

cp config/monitor.conf config/monitor.tmp.conf
sed -i "s/{{addr}}/127.0.0.1/g" config/monitor.tmp.conf
sed -i "s/{{report_addr}}/127.0.0.1/g" config/monitor.tmp.conf
sed -i "s/{{query_addr}}/127.0.0.1/g" config/monitor.tmp.conf

nohup build/ts-monitor -config=config/monitor.tmp.conf  >/opt/tsdb/logs/monitor_extra.log 2>&1 &
