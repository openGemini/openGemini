# openGemini-cli

![License](https://img.shields.io/badge/license-Apache2.0-green)
[![OpenSSF Best Practices](https://www.bestpractices.dev/projects/11008/badge)](https://www.bestpractices.dev/projects/11008)
![Language](https://img.shields.io/badge/Language-Go-blue.svg)
[![version](https://img.shields.io/github/v/tag/opengemini/opengemini-cli?label=release&color=blue)](https://github.com/opengemini/opengemini-client-go/releases)
[![Go report](https://goreportcard.com/badge/github.com/opengemini/opengemini-cli)](https://goreportcard.com/report/github.com/opengemini/openGemini-cli)
[![Go Reference](https://pkg.go.dev/badge/github.com/opengemini/openGemini-cli.svg)](https://pkg.go.dev/github.com/opengemini/openGemini-cli)

`openGemini-cli` is client interactive command-line interface for CNCF openGemini.

## About OpenGemini

OpenGemini is a cloud-native distributed time series database, find more information [here](https://github.com/openGemini/openGemini)

## Install

**IMPORTANT**: It's highly recommended installing a specific version of `ts-cli` available on the [releases page](https://github.com/opengemini/openGemini-cli/releases).

```bash
go install github.com/openGemini/openGemini-cli/cmd/ts-cli@latest
```

## Usage

To run `ts-cli` execute:

```bash
root/Downloads> ./ts-cli -h
CNCF openGemini client interactive command-line interface.

Usage:
  ts-cli [flags]
  ts-cli [command]

Available Commands:
  help        Help about any command
  import      import data to openGemini
  version     version for openGemini CLI

Flags:
  -c, --cacert string       CA certificate to verify peer against when connecting openGemini by https.
  -C, --cert string         client certificate file when connecting openGemini by https.
  -k, --cert-key string     client certificate password.
  -d, --database string     database to connect to openGemini.
  -h, --help                help for ts-cli
  -H, --host string         ts-sql host to connect to. (default "localhost")
  -I, --insecure-hostname   ignore server certificate hostname verification when connecting openGemini by https.
  -i, --insecure-tls        ignore ssl verification when connecting openGemini by https.
  -P, --password string     password to connect to openGemini.
  -p, --port int            ts-sql tcp port to connect to. (default 8086)
  -S, --socket string       openGemini unix domain socket to connect to.
  -s, --ssl                 use https for connecting to openGemini.
  -t, --timeout int         request-timeout in mill-seconds. (default 5000)
  -u, --username string     username to connect to openGemini.

Use "ts-cli [command] --help" for more information about a command.
```

## Develop Requirements

- Go 1.24+

```bash
# 1. clone source codes from Github
git clone https://github.com/openGemini/openGemini-cli.git

# 2. enter the work directory
cd openGemini-cli

# 3. build binary to dist
go build -o dist/ ./cmd/...

# 4. check compiled binary file
ls dist/
> ts-cli

# 5. test function
ts-cli --host localhost --port 8086 # ...other params
```

## Code of Conduct

openGemini follows the [CNCF Code of Conduct](https://github.com/cncf/foundation/blob/master/code-of-conduct.md).

## Contact Us

1. [Slack](https://join.slack.com/t/opengemini/shared_invite/zt-2naig1675-x3bcwgXR_Rw5OwDU5X~dUQ)

2. [Twitter](https://twitter.com/openGemini)

3. [Email](mailto:community.ts@opengemini.org)

4. [mailing list](https://groups.google.com/g/openGemini)


## License

openGemini is licensed under the Apache License 2.0. Refer to [LICENSE](https://github.com/openGemini/openGemini/blob/main/LICENSE) for more details.

For third-party software usage notice, see [Open_Source_Software_Notice](Open_Source_Software_Notice.md)
