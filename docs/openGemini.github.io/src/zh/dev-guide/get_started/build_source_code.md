---
order: 2
---

# 克隆-构建-运行

## 先决条件

### 编译环境信息

- [GO](https://go.dev/dl/)：版本 v1.18+
- [Python](https://www.python.org/downloads/)：版本 v3.7+
- [Git](https://git-scm.com/downloads) : openGemini 源代码作为 git 存储库托管在 GitHub 上。 要使用 git 存储库，请[安装 Git](https://git-scm.com/downloads)。

### 如何设置GO环境变量

打开~/.profile配置文件，在文件末尾添加如下配置：

```bash
# 设置 GOPATH（需要自定义目录）
export GOPATH=/path/to/dir
# 设置国内代理
export GOPROXY=https://goproxy.cn,direct
# 开启 go mod 模式
export GO111MODULE=on
export GONOSUMDB=*
export GOSUMDB=off
```

## 克隆

将源代码克隆到你的开发机器：

```bash
git clone https://github.com/openGemini/openGemini.git
```

## 构建

1. 进入主目录。

```bash
cd openGemini
```

2. 编译。

```bash
python3 build.py --clean
```

::: tip

编译后的二进制文件在build目录下

```bash
> ls build
> ts-cli ts-meta ts-monitor ts-server ts-sql ts-store
```

`ts-server` 是独立运行版本。

:::

## 运行

运行单机版本。

```bash
./build/ts-server
```

::: tip

对于 v1.0.1 及更早的版本，您需要指定启动 `ts-server` 的配置文件。

```bash
./build/ts-server -config config/openGemini.singlenode.conf
```

如果你想在后台启动它，你可以使用以下命令：

```bash
nohup ./build/ts-server -config config/openGemini.singlenode.conf > server_extra.log 2>&1 &
```

:::

## 连接

为了方便对数据库进行各种查询，openGemini提供了一个名为openGemini CLI（简称ts-cli）的命令行应用程序。 进入openGemini命令行，只需要进入`ts-cli`所在目录，在终端执行`ts-cli`即可。

使用客户端连接到openGemini你可以使用以下命令：

```狂欢
./build/ts-cli
```

::: tip

默认情况下，它连接到 `127.0.0.1:8086`，但您可以使用以下命令连接到其他主机：
```bash
./build/ts-cli -host 192.168.0.1 -port 8086
```


更多用法请使用以下命令探索：
```bash
./build/ts-cli -h
```

:::
