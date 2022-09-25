#!/bin/bash

configPath=/home/pyxisdb/config
configs=$configPath/*.conf

configMountPath=`echo $PYXISDB_CONFIG_MOUNT_PATH`
if [[ "$configMountPath" != "" ]]; then
  mkdir -p $configPath/backup
  mv $configs $configPath/backup/
  cp $configMountPath/*.conf $configPath
fi

metaJoin=`echo $PYXISDB_META_JOIN`
gossipMembers=`echo $PYXISDB_GOSSIP_MEMBERS`
httpsEnable=`echo $PYXISDB_HTTPS_ENABLE`
httpsCert=`echo $PYXISDB_HTTPS_CERT`
tlsEnable=`echo $PYXISDB_TLS_ENABLE`
tlsCert=`echo $PYXISDB_TLS_CERT`
executorMemorySizeLimit=`echo $PYXISDB_EXECUTOR_MEMORY_SIZE_LIMIT`

if [[ "$httpsEnable" != "true" || "$httpsCert" == "" ]]; then
  httpsEnable="false"
fi

if [[ "$tlsEnable" != "true" || "$tlsCert" == "" ]]; then
  tlsEnable="false"
fi

if [[ "$executorMemorySizeLimit" == "" ]]; then
  executorMemorySizeLimit="0"
fi

localAddr=`/sbin/ifconfig -a|grep inet|grep -v 127.0.0.1|grep -v inet6|awk '{print $2}'|tr -d "addr:"`

sed -i "s/{{meta_join}}/$metaJoin/g" $configs
sed -i "s/{{gossip_members}}/$gossipMembers/g" $configs
sed -i "s/{local_addr}/$localAddr/g" $configs
sed -i "s/{{monitor_database}}//g" $configs
sed -i "s/{{monitor_endpoint}}//g" $configs
sed -i "s/{{https_enable}}/$httpsEnable/g" $configs
sed -i "s/{{https_cert}}/$httpsCert/g" $configs
sed -i "s/{{tls_enable}}/$tlsEnable/g" $configs
sed -i "s/{{tls_cert}}/$tlsCert/g" $configs
sed -i "s/{{executor_memory_size_limit}}/$executorMemorySizeLimit/g" $configs
