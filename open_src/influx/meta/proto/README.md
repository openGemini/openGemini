# How to Use Protobuf in Go Environment

This document is going to provide you with the step-by-step instructions on how to use protobuf in Go environment.

## Installing Protobuf

### Linux

Run the following commands:

```bash
# PROTOC_ZIP=protoc-3.14.0-linux-x86_64.zip
curl -OL https://github.com/protocolbuffers/protobuf/releases/download/v3.14.0/$PROTOC_ZIP
sudo unzip -o $PROTOC_ZIP -d /usr/local bin/protoc
sudo unzip -o $PROTOC_ZIP -d /usr/local 'include/*'
rm -f $PROTOC_ZIP
```

## Installing Gogo Plugin

Run the following commands:

```bash
go install github.com/gogo/protobuf/protoc-gen-gogo@latest
```

## Executing Command

With the installation of Protobuf and Gogo plugin, you can now use Protobuf in your Go project. Here are the commands for generating Protobuf Go code:

```bash
# Generate Go code from .proto file
protoc --gogo_out=. open_src/influx/meta/proto/meta.proto
```