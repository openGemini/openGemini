---
order: 2
---

# Clone-Build-Run

## Prerequisites

### Compile environment information

- [GO](https://go.dev/dl/) : version v1.18+
- [Python](https://www.python.org/downloads/) : version v3.7+
- [Git](https://git-scm.com/downloads) : The openGemini source code is hosted on GitHub as a git repository. To work with the git repository, please [install Git](https://git-scm.com/downloads).

### How to set GO environment variables

Open ~/.profile configuration file and add the following configurations to the end of the file:

```bash
# Set GOPATH (requires custom directory)
export GOPATH=/path/to/dir
# Set up domestic proxy
export GOPROXY=https://goproxy.cn,direct
# Turn on go mod mode
export GO111MODULE=on
export GONOSUMDB=*
export GOSUMDB=off
```

## Clone

Clone the source code to your development machine:

```bash
git clone https://github.com/openGemini/openGemini.git
```

## Build

1. Enter the home directory.

```bash
cd openGemini
```

2. Compiling.

```bash
python3 build.py --clean
```

::: tips

The compiled binary file is in the build directory

```bash
> ls build
> ts-cli ts-meta ts-monitor ts-server ts-sql ts-store
```

`ts-server` is the standalone version.

:::

## Run

Run the stand-alone version.

```bash
./build/ts-server
```

::: tips

> For version v1.0.1 and earlier, you need to specify the configuration file to start the `ts-server`.
>
> ```bash
> ./build/ts-server -config config/openGemini.singlenode.conf
> ```
>
> If you want to start it in the background, you can use the following command.
>
> ```bash
> nohup ./build/ts-server -config config/openGemini.singlenode.conf > server_extra.log 2>&1 &
> ```

:::

## Connect

To facilitate various queries of the database, openGemini provides a command-line application called openGemini CLI (`ts-cli` for short). To enter the openGemini command line, you only need to go to the directory where `ts-cli` is located and execute `ts-cli` in the terminal.

Use the client to connect to openGemini you can use the following command:

```bash
./ts-cli
```

> **Tips:**
>
> By default, it connects to `127.0.0.1:8086`, but you can connect to other hosts using the following command:
>
> ```bash
> ./ts-cli -host 192.168.0.1 -port 8086
> ```
>
> For more usage, please use the following command to explore:
>
> ```bash
> ./ts-cli -h
> ```
