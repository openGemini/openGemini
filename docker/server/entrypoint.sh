#!/bin/bash
set -e

sed -i 's#/tmp/openGemini/#/opt/openGemini/#g' $OPENGEMINI_CONFIG
sed -i 's#/opt/openGemini/logs/#/var/log/openGemini/#g' $OPENGEMINI_CONFIG

localAddr=`/sbin/ifconfig -a|grep inet|grep -v 127.0.0.1|grep -v inet6|awk '{print $2;exit}'|tr -d "addr:"`
sed -i "s/127.0.0.1/$localAddr/g" $OPENGEMINI_CONFIG

ts-server -config $OPENGEMINI_CONFIG | tee /var/log/openGemini/server_extra.log
