该文档指导Docker环境部署openGemini，若有任何问题，请及时给社区提issue反馈

## 说明

openGemini 在配置文件中指定了 meta 节点的信息，用于节点发现。虚拟机部署时，我们直接使用 meta 节点的IP，但是在docker中，节点的IP可能在重启后变化，将导致节点不可用。因此，在docker环境中需要使用域名代替节点IP（虚拟机环境同样可以使用域名）

## KubeEdge ｜ K8s  容器配置

### openGemini集群部署说明

- openGemini的镜像需要自己制作，提供制作方法
- 部署openGemini集群至少需要两个节点，建议三个节点，**请一定提前查看**[**openGemini安装部署指南**](https://github.com/openGemini/community)
- 由于 openGemini 的每个节点都是有状态的，因此要为每个节点分配独立的域名
- 为方便修改，不建议将配置文件放在docker镜像内，配置文件通过挂载的方式加入



### 镜像制作说明

**当前Dockerfile的基础镜像系统为openEuler，需要根据实际系统进行更改**

镜像制作命令：

```shell
> docker build -t openGemini:0.2.0 .
```

保存镜像

```
> docker save -o openGemini_0.2.0.image.tar openGemini:0.2.0
```

至此，该镜像文件openGemini_0.2.0.image.tar便可以导入KubeEdge或者K8s了

### 环境变量说明

- **OPEN_GEMINI_LAUNCH**

  指定 docker 内启动程序，一个或多个，至少需要 3 个meta 节点

  有效值：ts-meta ｜ts-sql ｜ts-store，多个用逗号隔开

  示例：

  ```shell
  # 全部启动
  export OPEN_GEMINI_LAUNCH=ts-meta,ts-sql,ts-store
  
  # 仅启动 meta
  export OPEN_GEMINI_LAUNCH=ts-meta
  ```

- **OPEN_GEMINI_DOMAIN**

  配置指向 docker 节点的域名，在配置文件openGemini.conf的[data]和[meta]尾部添加如下配置：

  domain = "{{domain}}"

  示例：

  ```shell
  [meta]
    ...
    rpc-bind-address = "{{addr}}:8092"
    # ts-mete和ts-store节点需要在此配置域名，docker 启动时，从环境变量获取，并替换该配置项
    domain = "{{domain}}"
    
  [data]
    store-ingest-addr = "{{addr}}:8400"
    store-select-addr = "{{addr}}:8401"
    ...
    # ts-store节点需要在此配置域名，docker 启动时，从环境变量获取，并替换该配置项
    domain = "{{domain}}"
  ```

- **OPEN_GEMINI_CONFIG**

  docker 镜像内不包含启动程序所需的配置文件，需要通过挂载的方式，将配置文件传递到docker内通过该环境变量定义配置文件的挂载路径。

  建议对于该路径，分配只读权限，docker 内程序启动时，会从该路径 copy 配置文件到其他目录

### Docker Node的配置文件示例

```shell
apiVersion: apps/v1
kind: Deployment
metadata:
  # 【必填】根据实际情况填写
  name: opengemini-edge-001
  namespace: opengemini
  labels:
    app: opengemini-edge-001
spec:
  replicas: 1
  selector:
    matchLabels:
      app: opengemini-edge-001

  template:
    metadata:
      labels:
        app: opengemini-edge-001
    spec:
      containers:
      # 【必填】根据实际情况填写
      - name: opengemini
      # 【必填】将制作好的镜像文件导入docker后，使用命令：docker ps，填写该命令输出的openGemini的镜像名称
        image: xxx
        env:
        # 【必填】启动进程配置
        - name: OPEN_GEMINI_LAUNCH
          value: "ts-meta,ts-sql,ts-store"
        # 【必填】配置域名，一般由[servername].[namespace].svc.cluster.local几部分组成
        - name: OPEN_GEMINI_DOMAIN
          value: "svc-opengemini-001.opengemini.svc.cluster.local"
        # 【必填】配置文件挂载点，value是配置文件的完整路径（含配置文件名），与mountPath配置的目录需要保持一致
        - name: OPEN_GEMINI_CONFIG
          value: "/usr/share/config/openGemini.conf"
        volumeMounts:
          - name: vol-config
            mountPath: /usr/share/config
        # 【必填】端口矩阵，根据实际情况分配，可以保持默认值
        ports:
          - name: sql
            containerPort: 8086
          - name: store-write
            containerPort: 8400
          - name: store-query
            containerPort: 8401
          - name: meta-8092
            containerPort: 8092
          - name: meta-8091
            containerPort: 8091
          - name: store-8011
            containerPort: 8011
          - name: meta-8010
            containerPort: 8010
          - name: meta-8088
            containerPort: 8088
      volumes:
        - name:  vol-config
          # 【必填】宿主机上存放openGemini配置文件的目录，用于挂载到docker内部，启动时从该目录拷贝配置文件。存放的配置文件的命名
          #  需要和环境变量OPEN_GEMINI_CONFIG配置名称保持一致
          hostPath:
            path:  /home/openGemini/config
```

### Docker Server的配置文件示例

```shell
apiVersion: v1
kind: Service
metadata:
  #【必填】根据实际情况填写，需要同步修改上述配置文件【OPEN_GEMINI_DOMAIN】的配置，以及openGemini中的配置文件
  name: svc-opengemini-001
  namespace: opengemini
  labels:
    app: opengemini-svc-001
spec:
  selector:
    app: opengemini-edge-001
  #【必填】端口举证，根据实际情况配置，可以保持默认值  
  ports:
    - name: sql
      port: 8086
      protocol: TCP
      targetPort: 8086
    - name: store-write
      port: 8400
      protocol: TCP
      targetPort: 8400
    - name: store-query
      port: 8401
      protocol: TCP
      targetPort: 8401
    - name: meta-8092
      port: 8092
      protocol: TCP
      targetPort: 8092
    - name: meta-8091
      port: 8091
      protocol: TCP
      targetPort: 8091
    - name: store-8011
      port: 8011
      protocol: TCP
      targetPort: 8011
    - name: meta-8010
      port: 8010
      protocol: TCP
      targetPort: 8010
    - name: meta-8088
      port: 8088
      protocol: TCP
      targetPort: 8088
```

### openGemini配置文件说明

该示例配置文件并非全部配置项，仅用于说明需要特别关注的点

```shell
[common]
  #【必填】需要根据前面容器的域名来替换meta-join配置项的{{meta_addr_1}}，{{meta_addr_2}}，{{meta_addr_3}}
  meta-join =  ["{{meta_addr_1}}:9092", "{{meta_addr_2}}:9092", "{{meta_addr_3}}:9092"]
  #【必填】由于docker内，进程获取的CPU核数和内存大小可能不准确，这里进行配置，以便限制资源的使用
  cpu-num = 8
  memory-size = "16G"

[meta]
  #【不必修改】docker 启动时，自动替换 {{addr}} 变量为节点IP，通常不需要修改（下同）
  bind-address = "{{addr}}:8088"
  http-bind-address = "{{addr}}:8091"
  rpc-bind-address = "{{addr}}:8092"
  #【必填】ts-mete和ts-store节点需要在此配置域名，docker 启动时，从环境变量获取，并替换该配置项
  domain = "{{domain}}"

[http]
  bind-address = "{{addr}}:8086"

[data]
  store-ingest-addr = "{{addr}}:8400"
  store-select-addr = "{{addr}}:8401"
  #【必填】ts-store节点需要在此配置域名，docker 启动时，从环境变量获取，并替换该配置项
  domain = "{{domain}}"

[gossip]
  # enabled = true
  # log-enabled = true
  bind-address = "{{addr}}"
  store-bind-port = 8011
  meta-bind-port = 8010
  # prob-interval = '1s'
  # suspicion-mult = 4
  # 【必填】需要替换{{meta_addr_1}}，{{meta_addr_2}}，{{meta_addr_3}}为对应的域名
  members = ["{{meta_addr_1}}:8010", "{{meta_addr_2}}:8010", "{{meta_addr_3}}:8010"]

```

