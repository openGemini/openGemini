#!/bin/bash/env bash

config=/home/openGemini/config/gemini.conf
domain=`echo $OPEN_GEMINI_DOMAIN`

configMount=`echo $OPEN_GEMINI_CONFIG`
if [[ "$configMount" == "" ]]; then
	echo "Missing environment variable: OPEN_GEMINI_LAUNCH"
	exit 1
fi

if [[ ! -f $configMount ]]; then
	echo "Configuration file does not exist: $configMount"
	exit 1
fi

cp -f $configMount $config
localAddr=`/sbin/ifconfig -a|grep inet|grep -v 127.0.0.1|grep -v inet6|awk '{print $2;exit}'|tr -d "addr:"`
sed -i "s/{{addr}}/$localAddr/g" $config
sed -i "s/{{domain}}/$domain/g" $config

