---
title: 备份恢复
order: 2
---
# 备份恢复

### 备份相关接口
```shell
# 备份状态查询
curl -i -XPOST 'http://127.0.0.1:8086/backup/status'
# 备份中断
curl -i -XPOST 'http://127.0.0.1:8086/backup/abort'
# 备份接口
curl -i -XPOST 'http://127.0.0.1:8086/backup/run?backupPath=%2Ftmp%2FopenGemini%2Fbackup_full&db=prom1,prom2&isInc=false'
# isNode           - 只备份当前节点数据
# backupPath       - 备份文件目录
# isInc            - 是否增量 true-增量 false-全量
# isRemote         - 是否全程 true-远程 false-本地 目前只支持本地
# dbs              - 需要进行备份的db，多个db通过","连接，不设置默认备份全部db
# backupMeta       - 强制备份meta数据(不设置该参数的情况下默认备份leader meta)

# 备份接口(旧)
curl -i -XPOST 'http://127.0.0.1:8086/debug/ctrl?mod=backup&backupPath=%2Ftmp%2FopenGemini%2Fbackup&isInc=false&isRemote=false'
```

### 恢复工具
恢复工具源码位于 app/ts-recover
```
[root@ecs-ff93-opengemini openGemini]# ./build/ts-recover -h
Usage of ts-recover:
  -force
    	force recover data file
  -fullBackupDataPath string
    	full backup file path
  -host string
    	leader meta node host (default "127.0.0.1:8901")
  -incBackupDataPath string
    	inc backup file path
  -insecure-tls 
    	ignore ssl verification when connecting openGemini by https.
  -recoverMode string
    	recover mode 1-full and inc recover, 2- full recover, 3- recover meta 4- rewrite path
  -ssl
    	use https for connecting to openGemini.
  --srcNode
        set with recoverMode=4,srouce datanode id
  --dstNode 
        set with recoverMode=4,target datanode id
```


### 备份使用
#### 约束
+ 通过isNode进行单节点备份，要求每个节点都要部署ts-sql/ts-data
+ 备份目录与openGemini数据存储目录要分开
+ 通过全库备份恢复前，需停服务；DB粒度备份恢复前，需删除对应DB，无需停服务。

#### 场景一
全量备份/增量备份结合的日常备份(可配置只备份部分db)
+ 增量备份的内容为当前数据与前一次全量备份数据的差异部分;多次增量备份，恢复时取最近一次全量备份和最近一次增量备份的内容即可

#### 场景二
节点故障且无法访问时补救恢复，将数据恢复到新节点(需多副本场景)
1. 在健康节点进行一次全量备份，注意需data，meta都备份下来。
2. 通过元数据接口查看元数据，获取备份节点(srcNode)和故障节点(dstNode)的dataID
3. 通过恢复工具设置recoverMode为4，同时设置srcNode和dstNode参数，建立路径映射
4. 将建立路径映射后的备份文件拷贝到新节点，再通过恢复工具进行恢复

### 域名配置
如有节点替换恢复等场景，需要提前进行域名配置，新老节点的域名需保持一致。
假设集群有3个节点，混合部署，IP 分别为: 192.168.1.100, 192.168.1.101, 192.168.1.102
在 /etc/hosts 中加入如下配置
```
192.168.1.100 host1
192.168.1.101 host2
192.168.1.102 host3
```

修改内核配置, 添加
```toml
[common]
# 配置meta 节点的域名
meta-join = ["host1:8092", "host2:8092", "host3:8092"]

[meta]
domain = "host1" #不同节点按需配置

[http]
domain = "host1" # 不同节点按需配置

[data]
domain = "host1" # 不同节点按需配置

[gossip]
# 配置meta 节点的域名
members = ["host1:8010", "host2:8010", "host3:8010"]
```
