#!/usr/bin/env bash
#
#	Shell script to install openGemini as a cluster at one node.
#

if [ "$(uname -s)" == "Darwin" ]; then
  ps -ef | grep -v grep | grep ts-store | grep $USER > /dev/null
  if [ $? == 0 ];then
    killall -9 ts-store
  fi

  ps -ef | grep -v grep | grep ts-meta | grep $USER > /dev/null
  if [ $? == 0 ];then
    killall -9 ts-meta
  fi

  ps -ef | grep -v grep | grep ts-sql | grep $USER > /dev/null
  if [ $? == 0 ];then
    killall -9 ts-sql
  fi

  sleep 3

  echo "ref link:"
  echo "https://superuser.com/questions/458875/how-do-you-get-loopback-addresses-other-than-127-0-0-1-to-work-on-os-x"
  echo "temporarily open 127.0.0.2/127.0.0.3 for ts-store/ts-meta, please enter your power-on password"
  sudo ifconfig lo0 alias 127.0.0.2 up
  sudo ifconfig lo0 alias 127.0.0.3 up
  echo "\nif sed command occur error, please install gsed by: \nbrew install gnu-sed\nand then:\nexport PATH=\"/opt/homebrew/opt/gnu-sed/libexec/gnubin:\$PATH\""

elif [ "$(uname -s)" == "Linux" ]; then
  ps -ef | grep -v grep | grep ts-store | grep $USER > /dev/null
  if [ $? == 0 ];then
    killall -9 -w ts-store
  fi

  ps -ef | grep -v grep | grep ts-meta | grep $USER > /dev/null
  if [ $? == 0 ];then
    killall -9 -w ts-meta
  fi

  ps -ef | grep -v grep | grep ts-sql | grep $USER > /dev/null
  if [ $? == 0 ];then
    killall -9 -w ts-sql
  fi

else
  echo "not support the platform": $(uname)
  exit 1
fi

declare -a nodes[3]
for((i = 1; i < 4; i++))
do
    nodes[$i]=127.0.0.$i
done

# generate config
for((i = 1; i <= 3; i++))
do
    rm -rf config/openGemini-$i.conf
    cp config/openGemini.conf config/openGemini-$i.conf

    sed -i "s/{{meta_addr_1}}/${nodes[1]}/g" config/openGemini-$i.conf
    sed -i "s/{{meta_addr_2}}/${nodes[2]}/g" config/openGemini-$i.conf
    sed -i "s/{{meta_addr_3}}/${nodes[3]}/g" config/openGemini-$i.conf
    sed -i "s/{{addr}}/${nodes[$i]}/g" config/openGemini-$i.conf

    sed -i "s/{{id}}/$i/g" config/openGemini-$i.conf
done

rm -rf /tmp/openGemini/logs
mkdir -p /tmp/openGemini/logs/1
mkdir -p /tmp/openGemini/logs/2
mkdir -p /tmp/openGemini/logs/3

nohup build/ts-meta -config config/openGemini-1.conf -pidfile /tmp/openGemini/pid/meta1.pid > /tmp/openGemini/logs/1/meta_extra1.log 2>&1 &
nohup build/ts-meta -config config/openGemini-2.conf -pidfile /tmp/openGemini/pid/meta2.pid > /tmp/openGemini/logs/2/meta_extra2.log 2>&1 &
nohup build/ts-meta -config config/openGemini-3.conf -pidfile /tmp/openGemini/pid/meta3.pid > /tmp/openGemini/logs/3/meta_extra3.log 2>&1 &
sleep 5
nohup build/ts-store -config config/openGemini-1.conf -pidfile /tmp/openGemini/pid/store1.pid > /tmp/openGemini/logs/1/store_extra1.log 2>&1 &
sleep 0.1
nohup build/ts-store -config config/openGemini-2.conf -pidfile /tmp/openGemini/pid/store2.pid > /tmp/openGemini/logs/2/store_extra2.log 2>&1 &
sleep 0.1
nohup build/ts-store -config config/openGemini-3.conf -pidfile /tmp/openGemini/pid/store3.pid > /tmp/openGemini/logs/3/store_extra3.log 2>&1 &
sleep 0.1
nohup build/ts-sql -config config/openGemini-1.conf -pidfile /tmp/openGemini/pid/sql1.pid > /tmp/openGemini/logs/1/sql_extra1.log 2>&1 &
