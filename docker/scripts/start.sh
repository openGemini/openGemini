#!/bin/bash/env bash
DIR=$(cd `dirname $0`;pwd)

source $DIR/build_config.sh

BIN_PATH=/opt/openGemini/bin
LOG_PATH=/opt/openGemini/logs

mkdir -p $LOG_PATH

function startApp() {
	app=$1

	ok=`echo $OPEN_GEMINI_LAUNCH|grep "$app"`
	if [ -n "$ok" ]; then
		echo "start: ts-$app"
		nohup $BIN_PATH/ts-$app -config ${config} >> $LOG_PATH/${app}_extra.log 2>&1 &
	fi
}

function checkApp() {
	app=$1

	ok=`echo $OPEN_GEMINI_LAUNCH|grep "$app"`
	if [ !-n "$ok" ]; then
		return
	fi

	n=`ps axu|grep "ts-$app"|grep -v grep|wc -l`
	if [[ $n -eq 0 ]]; then
		exit ""
	fi
}

startApp meta
startApp sql
startApp store
startApp server

while true; do

	sleep 1s
	startApp meta
	startApp sql
	startApp store
	startApp server

done
