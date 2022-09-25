#!/bin/bash

ps -ef | grep -v grep | grep ts-server | grep $USER > /dev/null
if [ $? == 0 ];then
	killall -9 -w ts-server
fi

rm -rf /tmp/openGemini/logs/
mkdir -p /tmp/openGemini/logs/

nohup build/ts-server run -config=config/openGemini.singlenode.conf  > /tmp/openGemini/logs/server_extra.log 2>&1 &
