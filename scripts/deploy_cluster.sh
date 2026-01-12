#!/usr/bin/env bash
#
#	Shell script to deploy openGemini cluster in different nodes.
# To run this script, you must finish password-free login.
#

# Node config for openGemini. Please change the ip if necessary.
# This is example for deploy an openGemini cluster.
# meta: 3 nodes
# sql: 3 nodes(At least one, the sql is entry for query and write)
# store: 3 nodes
metaNodes=(192.168.0.1 192.168.0.2 192.168.0.3)
sqlNodes=(192.168.0.1 192.168.0.2 192.168.0.3)
storeNodes=(192.168.0.1 192.168.0.2 192.168.0.3)

echo "meta node list is: ${metaNodes[*]}"
echo "sql node list: ${sqlNodes[*]}"
echo "store node list: ${storeNodes[*]}"


function splittingLine() {
  echo ""
  echo "======================== $1 =========================="
  echo ""
}

#==============================================================
splittingLine "kill ts-meta, ts-sql, ts-meta"
all_nodes=(${metaNodes[@]} ${sqlNodes[*]} ${storeNodes[*]})
all_nodes=($(echo ${all_nodes[*]} | sed 's/ /\n/g' | sort | uniq))
for node in ${all_nodes[*]}
do
  cmd="killall ts-meta;killall ts-sql;killall ts-store"
  # ssh ${node} "$cmd"
done
splittingLine "kill ts-meta, ts-sql, ts-meta finish"


#==============================================================
splittingLine "Generating config"
for((i=0; i<${#all_nodes[@]}; i++))
do
    cp config/openGemini.conf config/openGemini-$i.conf
    sed -i "s/{{meta_addr_1}}/${metaNodes[1]}/g" config/openGemini-$i.conf
    sed -i "s/{{meta_addr_2}}/${metaNodes[2]}/g" config/openGemini-$i.conf
    sed -i "s/{{meta_addr_3}}/${metaNodes[3]}/g" config/openGemini-$i.conf
    sed -i "s/{{addr}}/${nodes[$i]}/g" config/openGemini-$i.conf

    sed -i "s/{{id}}/$i/g" config/openGemini-$i.conf
done
ls -l config/openGemini-*
splittingLine "Generating config finish"


#==============================================================
splittingLine "deploy ts-meta service"
for((i=0; i<${#metaNodes[@]}; i++))
do
    ssh ${metaNodes[$i]} "mkdir /tmp/openGemini/bin/ -p;mkdir /tmp/openGemini/logs/ -p;mkdir /tmp/openGemini/config/ -p;mkdir /tmp/openGemini/pid -p;"
    scp sbin/ts-meta ${metaNodes[$i]}:/tmp/openGemini/bin/
    scp config/openGemini-$i.conf ${metaNodes[$i]}:/tmp/openGemini/config/
    ssh ${metaNodes[$i]} "mv /tmp/openGemini/config/openGemini-$i.conf /tmp/openGemini/config/openGemini.conf"
    ssh ${metaNodes[$i]} "chmod +x /tmp/openGemini/bin/ts-meta"
    cmd="nohup /tmp/openGemini/bin/ts-meta -pidfile /tmp/openGemini/pid/ts-meta.pid -config /tmp/openGemini/config/openGemini.conf > /tmp/openGemini/logs/meta_extra.log 2>&1 &"
    ssh ${metalist[$i]} "$cmd"
done
splittingLine "deploy ts-meta service finish"

#==============================================================
splittingLine "deploy ts-store service"
for((i=0; i<${#storeNodes[@]}; i++))
do
    ssh ${storeNodes[$i]} "mkdir /tmp/openGemini/bin/ -p;mkdir /tmp/openGemini/logs/ -p;mkdir /tmp/openGemini/config/ -p;mkdir /tmp/openGemini/pid -p;"
    scp sbin/ts-store ${storeNodes[$i]}:/tmp/openGemini/bin/
    scp config/openGemini-$i.conf ${storeNodes[$i]}:/tmp/openGemini/config/
    ssh ${storeNodes[$i]} "mv /tmp/openGemini/config/openGemini-$i.conf /tmp/openGemini/config/openGemini.conf"
    ssh ${storeNodes[$i]} "chmod +x /tmp/openGemini/bin/ts-store"
    cmd="nohup /tmp/openGemini/bin/ts-store -pidfile /tmp/openGemini/pid/ts-store.pid -config /tmp/openGemini/config/openGemini.conf > /tmp/openGemini/logs/store_extra.log 2>&1 &"
    ssh ${storeNodes[$i]} "$cmd"
done
splittingLine "deploy ts-store service finish"

#==============================================================
splittingLine "deploy ts-sql service"
for((i=0; i<${#sqlNodes[@]}; i++))
do
    ssh ${sqlNodes[$i]} "mkdir /tmp/openGemini/bin/ -p;mkdir /tmp/openGemini/logs/ -p;mkdir /tmp/openGemini/config/ -p;mkdir /tmp/openGemini/pid -p;"
    scp sbin/ts-sql ${sqlNodes[$i]}:/tmp/openGemini/bin/
    scp config/openGemini-$i.conf ${sqlNodes[$i]}:/tmp/openGemini/config/
    ssh ${sqlNodes[$i]} "mv /tmp/openGemini/config/openGemini-$i.conf /tmp/openGemini/config/openGemini.conf"
    ssh ${sqlNodes[$i]} "chmod +x /tmp/openGemini/bin/ts-sql"
    cmd="nohup /tmp/openGemini/bin/ts-sql -pidfile /tmp/openGemini/pid/ts-sql.pid -config /tmp/openGemini/config/openGemini.conf > /tmp/openGemini/logs/sql_extra.log 2>&1 &"
    ssh ${sqlNodes[$i]} "$cmd"
done
splittingLine "deploy ts-sql service finish"


#==============================================================
splittingLine "check service: show databases"
sqlIP=${sqlNodes[0]}
c_tmp=$(curl -s -XPOST "http://${sqlIP}:8086/query" --data-urlencode "q=show databases")
echo ${c_tmp}
splittingLine "check service finish"
