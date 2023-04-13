#!/bin/bash
set -e

sed -i 's#/tmp/openGemini/#/opt/openGemini/#g' $OPENGEMINI_CONFIG
sed -i 's#/opt/openGemini/logs/#/var/log/openGemini/#g' $OPENGEMINI_CONFIG


ts-server -config $OPENGEMINI_CONFIG | tee /var/log/openGemini/server_extra.log
