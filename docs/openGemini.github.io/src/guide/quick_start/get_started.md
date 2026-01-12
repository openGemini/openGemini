---
title: Install & Deployment
order: 1
---

This section introduces how to quickly get started with the openGemini.

## Installation

::: tabs

@tab CPU platforms

| Platform | Support or not | Note                                                         |
| -------- | -------------- | ------------------------------------------------------------ |
| X86-64   | &#10004;       | -                                                            |
| X86-32   | &#10006;       | 32-bit is not currently supported, but since the openGemini kernel is developed by Golang.<br/>you can try to compile it on a 32-bit system. |
| ARM-64   | &#10004;       | -                                                            |
| ARM-32   | &#10006;       | 32-bit is not currently supported, but since the openGemini kernel is developed by Golang.<br/> you can try to compile it on a 32-bit system. |
| others   | &#10006;       | not support                                                  |

@tab OS

Since the vast majority of applications are developed on Windows and Mac, openGemini has started to support the [Mac](coco://sendMessage?ext={"s%24wiki_link"%3A"https%3A%2F%2Fm.baike.com%2Fwikiid%2F8792875024474215202"}&msg=Mac)OS and Windows operating systems since version v1.1.0 in order to facilitate the development and debugging of applications.

| OS      | Support or not | Note                                                         |
| ------- | -------------- | ------------------------------------------------------------ |
| Linux   | &#10004;       | Supports mainstream Linux OS such as openEuler, Ubuntu, [CentOS](coco://sendMessage?ext={"s%24wiki_link"%3A"https%3A%2F%2Fm.baike.com%2Fwikiid%2F5155123603410411334"}&msg=CentOS), [RedHat](coco://sendMessage?ext={"s%24wiki_link"%3A"https%3A%2F%2Fm.baike.com%2Fwikiid%2F10396951337429436"}&msg=RedHat), etc.<br>Validated version：<br>- CentOS v7.3 or later versions<br>- openEuler v22.03 LTS or later versions<br>- Red Hat v8.4 or later versions & v7.3 or later v7.x versions |
| Darwin  | &#10004;       | Supports MacOS, and it is recommended for project development and debugging. |
| Windows | &#10004;       | Supports Windows, and it is recommended for project development and debugging. <br>Validated version is win11 |

@tab fast installation

Use the Gemix tool for one-click installation and deployment, which can currently only be used for clusters, and supports deploying the openGemini cluster on one or more virtual machines or physical machines. For specific usage, refer to the [Gemix usage guide]().

Use the openGemini-operator for one-click containerized deployment, which can currently only be used for clusters, and supports K8s containerized deployment. For specific usage, refer to the [openGemini-operator usage guide]().

@tab install by binaries

1. Go to [GitHub Release](https://github.com/openGemini/openGemini/releases) to copy the link of the latest version.

    > Please replace **`<version>`** with the version of downloaded installation package.

    ```bash
    > wget https://github.com/openGemini/openGemini/releases/download/v<version>/openGemini-<version>-linux-amd64.tar.gz
    ```

     Manually downloading the corresponding installation package is also OK.

2. Move to the directory of the installation package, use `tar` to unzip.

   ```shell
   > mkdir openGemini
   > tar -zxvf openGemini-<version>-linux-amd64.tar.gz -C openGemini
   ```

   `ts-server` is the the standalone version of binary system.
   `openGemini.singlenode.conf` is the configuration file which adapts to `ts-server`.

3. run openGemini

   **single-node**

   Start openGemini on the local machine, which defaults to listening on 127.0.0.1:8086. Authentication and https are not enabled by default, and the default path for data and logs is /tmp/openGemini.

   ```
   > cd openGemini
   > ./usr/bin/ts-server
   ```

   If you need to modify the listening address, modify the configuration file openGemini.singlenode.conf and replace all 127.0.0.1

   ```tex
   [common]
     meta-join = ["127.0.0.1:8092"]
   	...

   [meta]
     bind-address = "127.0.0.1:8088"
     http-bind-address = "127.0.0.1:8091"
     rpc-bind-address = "127.0.0.1:8092"
     ...

   [http]
     bind-address = "127.0.0.1:8086"
     flight-address = "127.0.0.1:8087"
     ...

   [data]
     store-ingest-addr = "127.0.0.1:8400"
     store-select-addr = "127.0.0.1:8401"
     ...
   ```

   > Configuration file path: openGemini/etc/, where "openGemini" is the directory after decompressing the binary version.

   ```shell
   > ./usr/bin/ts-server --config ./etc/openGemini.singlenode.conf

   # run in the background
   > nohup ./usr/bin/ts-server --config ./etc/openGemini.singlenode.conf > server_extra.log 2>&1 &
   ```

   > **The openGemini.singlenode.conf configuration file is more concise than openGemini.conf, and some missing configuration items (such as authentication, https, etc.) can be found in openGemini.conf and copied to the corresponding location.**

   **cluster**

   Refer to the [cluster deployment]()

@tab source code compilation

**Information of compiling environments**

- [GO](https://go.dev/dl/) version v1.19+
- [Python](https://www.python.org/downloads/) version v3.7+
- [Git](https://git-scm.com/downloads)
- [Gcc (for windows)](https://www.cnblogs.com/kala00k/p/16364116.html)

**GO environmental variable settings**

Open `~/.profile` configuration file, add the following configuration at the end of the file:

```shell
# Set GOPATH (Need to customize directory)
export GOPATH=/path/to/dir
# Set domestic proxy
export GOPROXY=https://goproxy.cn,direct
# Open go mod mode
export GO111MODULE=on
export GONOSUMDB=*
export GOSUMDB=off
```

**Download source code**

```shell
> git clone https://github.com/openGemini/openGemini.git
```

**Move to the main directory**

```shell
> cd openGemini
```

**Compile**

```shell
> python3 build.py --clean
```

After successfully compiled, binary files are saved in the `build` directory。

**Run a standalone version**

```shell
> bash ./scripts/install.sh
```

**OR Move to the directory where ts-server is saved, and manual running**

```shell
./ts-server
```

To start in the background:

```shell
nohup ./ts-server > server_extra.log 2>&1 &
```

:::warning

If `v1.0.1` and version before, running `ts-server` equires specifying a configuration file to start:

```shell
./ts-server -config /path/to/openGemini.singlenode.conf
```



@tab specification selection

In terms of specification selection, it is difficult for the community to give accurate opinions. Specifications often relate to business volume. It is hoped that users can share in the community for reference by others in specification selection.

**Specific reference information:**

1. Compared with InfluxDB, openGemini has significantly optimized the occupation of resources, and has better memory control. Under the same specification, the openGemini ts-server can support a larger business volume.

2. After openGemini is started and the business is idling, the memory usage is about 100-200MB. Edge or embedded devices can refer to it.

3. Several query scenarios that use a large amount of memory:
   - When grouping and aggregating, the number of groups is large (such as hundreds of thousands or even millions)
   - When batch querying, the number of target time lines is large (such as hundreds of thousands)
   - When streaming computing, the single-node write traffic is relatively large
   - When the time-scale is large, the use of the `show series` command (such as millions or even larger)
   - When aggregating queries, the given time range is large, resulting in a large amount of target data to be computed (such as querying data within the past three months, with data volumes reaching billions or even tens of billions of rows)

If there are similar businesses as above, it is recommended to have more memory.

:::

## Command Line (ts-cli)

To facilitate the execution of various queries in the database, openGemini provides a command-line application (hereinafter referred to as openGemini CLI) ts cli. To enter the openGemini command line, simply enter the directory where `ts-cli` is located and execute `ts-cli` in the terminal.

```sh
> ./ts-cli
```

::: tip

Connect to 127.0.0.1:8086 in default. Connect to other host by the following command:

```shell
> ./ts-cli --host 192.168.0.1 --port 8086
```

For more usage, please use the following command to explore on your own:

```shell
> ./ts-cli -h
```

:::

## Basic Operations

**Create a database**

```sql
> create database db0
```

**Look up the database**

```sql
> show databases
```

Effects

```sql
> create database db0
Elapsed: 1.446074ms
> show databases
name: databases
+------+
| name |
+------+
| db0  |
+------+
1 columns, 1 rows in set

Elapsed: 2.178147ms
>>>
```

**Use the database**

```sql
> use db0
```

**Write in data**

```sql
> insert cpu_load,host=server-01,region=west_cn value=75.3
```

**Look up table**

```sql
> show measurements
```

**Look up data**

```sql
> select * from cpu_load
```

Effects

```sql
> use db0
Elapsed: 251ns
> insert cpu_load,host=server-01,region=west_cn value=75.3
Elapsed: 162.328339ms
> show measurements
name: measurements
+----------+
| name     |
+----------+
| cpu_load |
| mst      |
+----------+
1 columns, 2 rows in set

Elapsed: 13.374945ms
> select * from cpu_load
name: cpu_load
+---------------------+-------------+-----------+-------+
| time                | host        | region    | value |
+---------------------+-------------+-----------+-------+
| 1681483835745490423 | server-01   | west_cn   | 75.3  |
+---------------------+-------------+-----------+-------+
4 columns, 1 rows in set

Elapsed: 3.259995ms
```

## Attention

`ts-server` is a standalone binary file of OpenGemini, which can be simply understood as `ts-server` consisted of a`ts-sql`,a`ts-meta` and a `ts-store`. Attention:

1. If the default configuration cannot meet the requirements, configuration file `openGemini.singlenode.conf` is needed to start. For the complete configuration items and meanings, please refer to [Management - Configuration Items](../reference/configurations.md).
2. The data and logs in the default configuration file are saved in `/tmp/openGemini` by default. It is recommended to replace them with another directory to ensure sufficient storage space. If you use `scripts/install.sh` to start, you also need to modify `/tmp/` in the script accordingly.
3. If the port is found to be occupied during startup, the default port in the configuration file can be modified. Please refer to [Management Port Matrix ](../reference/ports.md)for all port purposes.
