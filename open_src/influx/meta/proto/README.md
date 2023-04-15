# install protoc

https://google.github.io/proto-lens/installing-protoc.html

## check protoc

```shell
> protoc --version
libprotoc 3.21.12
```

# install protoc-gen-gogo

go install github.com/gogo/protobuf/gogoproto

## check protoc-gen-gogo

```shell
> ls $(go env GOPATH)/bin/
... protoc-gen-gogo ...
```

# generate meta.pb.go

```shell
cd /path/to/openGemini
protoc --gogo_out=. open_src/influx/meta/proto/meta.proto
```

# please format your meta.pb.go !
