#!/usr/bin/env bash
#
#	Shell script to install openGemini as a cluster at one node.
#

ps -ef | grep -v grep | grep ts-data | grep $USER > /dev/null
if [ $? == 0 ];then
  killall -9 -w ts-data
fi

ps -ef | grep -v grep | grep ts-meta | grep $USER > /dev/null
if [ $? == 0 ];then
  killall -9 -w ts-meta
fi

declare -a nodes[3]
for((i = 1; i < 4; i++))
do
    nodes[$i]=127.0.0.$i
done

if [ "$(uname)" == "Darwin" ]; then
  echo "ref link:"
  echo "https://superuser.com/questions/458875/how-do-you-get-loopback-addresses-other-than-127-0-0-1-to-work-on-os-x"
  echo "temporarily open 127.0.0.2/127.0.0.3 for ts-meta/ts-meta, please enter admin pwd:"
  sudo ifconfig lo0 alias 127.0.0.2 up
  sudo ifconfig lo0 alias 127.0.0.3 up

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
elif [ "$(expr substr $(uname -s) 1 5)" == "Linux" ]; then
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
else
  echo "not support the platform": $(uname)
  exit 1
fi

# generate config
rm -rf /tmp/openGemini/wal/logs
mkdir -p /tmp/openGemini/wal/logs/1
mkdir -p /tmp/openGemini/wal/logs/2
mkdir -p /tmp/openGemini/wal/logs/3

nohup build/ts-meta -config config/openGemini-1.conf -pidfile /tmp/openGemini/pid/meta1.pid > /tmp/openGemini/wal/logs/1/meta_extra1.log 2>&1 &
nohup build/ts-meta -config config/openGemini-2.conf -pidfile /tmp/openGemini/pid/meta2.pid > /tmp/openGemini/wal/logs/2/meta_extra2.log 2>&1 &
nohup build/ts-meta -config config/openGemini-3.conf -pidfile /tmp/openGemini/pid/meta3.pid > /tmp/openGemini/wal/logs/3/meta_extra3.log 2>&1 &
sleep 5
nohup build/ts-data -config config/openGemini-1.conf -pidfile /tmp/openGemini/pid/data1.pid > /tmp/openGemini/wal/logs/1/data_extra1.log 2>&1 &
sleep 0.1
nohup build/ts-data -config config/openGemini-2.conf -pidfile /tmp/openGemini/pid/data2.pid > /tmp/openGemini/wal/logs/2/data_extra2.log 2>&1 &
sleep 0.1
nohup build/ts-data -config config/openGemini-3.conf -pidfile /tmp/openGemini/pid/data3.pid > /tmp/openGemini/wal/logs/3/data_extra3.log 2>&1 &
