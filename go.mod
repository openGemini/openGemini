module github.com/openGemini/openGemini

go 1.26.10

require (
	dario.cat/mergo v1.0.2
	github.com/BurntSushi/toml v1.5.0
	github.com/RoaringBitmap/roaring v1.9.4
	github.com/VictoriaMetrics/fastcache v1.13.3
	github.com/agiledragon/gomonkey/v2 v2.11.0
	github.com/apache/arrow-go/v18 v18.8.0
	github.com/bits-and-blooms/bloom/v3 v3.5.0
	github.com/bytedance/sonic v1.15.2
	github.com/cespare/xxhash/v2 v2.3.0
	github.com/deckarep/golang-set/v2 v2.6.0
	github.com/dgraph-io/ristretto/v2 v2.2.0
	github.com/go-playground/validator/v10 v10.27.0
	github.com/goccy/go-yaml v1.18.0
	github.com/golang-jwt/jwt/v5 v5.3.1
	github.com/golang/geo v0.0.0-20250813021530-247f39904721
	github.com/google/uuid v1.6.0
	github.com/gorilla/mux v1.8.1
	github.com/hashicorp/go-metrics v0.5.4
	github.com/hashicorp/go-msgpack/v2 v2.1.2
	github.com/hashicorp/golang-lru/v2 v2.0.7
	github.com/hashicorp/memberlist v0.5.3
	github.com/hashicorp/raft v1.7.2
	github.com/huaweicloud/huaweicloud-sdk-go-obs v3.25.4+incompatible
	github.com/indirect v0.0.0
	github.com/influxdata/influxdb v1.12.4
	github.com/influxdata/influxdb-observability/common v0.5.6
	github.com/influxdata/influxdb-observability/otel2influx v0.5.6
	github.com/influxdata/influxql v1.3.0
	github.com/klauspost/compress v1.19.2
	github.com/nxadm/tail v1.4.11
	github.com/openGemini/opengemini-client-go v0.9.1
	github.com/panjf2000/ants/v2 v2.11.0
	github.com/pierrec/lz4/v4 v4.1.29
	github.com/prometheus/client_golang v1.23.2
	github.com/prometheus/common v0.68.1
	github.com/prometheus/prometheus v0.312.0
	github.com/segmentio/kafka-go v0.4.48
	github.com/shirou/gopsutil/v3 v3.24.5
	github.com/smartystreets/goconvey v1.8.1
	github.com/spf13/cast v1.10.0
	github.com/spf13/cobra v1.9.1
	github.com/stretchr/testify v1.12.1
	github.com/tinylib/msgp v1.3.0
	github.com/xlab/treeprint v1.2.0
	go.etcd.io/bbolt v1.4.2
	go.etcd.io/etcd/raft/v3 v3.5.21
	go.opentelemetry.io/collector/consumer/consumererror v0.154.0
	go.opentelemetry.io/otel v1.47.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.44.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.44.0
	go.opentelemetry.io/otel/sdk v1.47.0
	go.uber.org/zap v1.28.0
	golang.org/x/crypto v0.55.0
	golang.org/x/exp v0.0.0-20260611194520-c48552f49976
	golang.org/x/sys v0.48.0
	golang.org/x/text v0.41.0
	golang.org/x/time v0.15.0
	google.golang.org/grpc v1.83.2
	google.golang.org/protobuf v1.36.12
	gopkg.in/natefinch/lumberjack.v2 v2.2.1
	modernc.org/sqlite v1.57.0
)

require (
	github.com/VictoriaMetrics/VictoriaMetrics v1.122.25 // indirect
	github.com/VictoriaMetrics/easyproto v1.2.0 // indirect
	github.com/VictoriaMetrics/metrics v1.44.0 // indirect
	github.com/VictoriaMetrics/metricsql v0.87.1 // indirect
	github.com/andybalholm/brotli v1.2.3 // indirect
	github.com/apache/thrift v0.24.0 // indirect
	github.com/armon/go-metrics v0.4.1 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bits-and-blooms/bitset v1.12.0 // indirect
	github.com/bytedance/gopkg v0.1.3 // indirect
	github.com/bytedance/sonic/loader v0.5.1 // indirect
	github.com/cenkalti/backoff/v5 v5.0.3 // indirect
	github.com/cloudwego/base64x v0.1.6 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/dennwc/varint v1.0.0 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/edsrzf/mmap-go v1.2.1-0.20241212181136-fad1cd13edbd // indirect
	github.com/facette/natsort v0.0.0-20181210072756-2cd4dd1e2dcb // indirect
	github.com/fatih/color v1.19.0 // indirect
	github.com/fsnotify/fsnotify v1.10.1 // indirect
	github.com/fxamacker/cbor/v2 v2.9.2 // indirect
	github.com/gabriel-vasile/mimetype v1.4.8 // indirect
	github.com/go-logr/logr v1.4.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/go-playground/locales v0.14.1 // indirect
	github.com/go-playground/universal-translator v0.18.1 // indirect
	github.com/goccy/go-json v0.10.6 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/golang/snappy v1.0.0 // indirect
	github.com/google/btree v1.1.3 // indirect
	github.com/google/flatbuffers v25.12.19+incompatible // indirect
	github.com/gopherjs/gopherjs v1.17.2 // indirect
	github.com/grafana/regexp v0.0.0-20250905093917-f7b3be9d1853 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.29.0 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-hclog v1.6.3 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.1 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/go-sockaddr v1.0.2 // indirect
	github.com/hashicorp/go-version v1.9.0 // indirect
	github.com/hashicorp/golang-lru v0.6.0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/json-iterator/go v1.1.13-0.20220915233716-71ac16282d12 // indirect
	github.com/jsternberg/zap-logfmt v1.2.0 // indirect
	github.com/jtolds/gls v4.20.0+incompatible // indirect
	github.com/klauspost/cpuid/v2 v2.4.0 // indirect
	github.com/leodido/go-urn v1.4.0 // indirect
	github.com/lufia/plan9stats v0.0.0-20211012122336-39d0f177ccd0 // indirect
	github.com/mattn/go-colorable v0.1.15 // indirect
	github.com/mattn/go-isatty v0.0.24 // indirect
	github.com/miekg/dns v1.1.72 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.3-0.20250322232337-35a7c28c31ee // indirect
	github.com/mschoch/smat v0.2.0 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/ncruces/go-strftime v1.0.0 // indirect
	github.com/philhofer/fwd v1.1.3-0.20240916144458-20a13a1f6b7c // indirect
	github.com/power-devops/perfstat v0.0.0-20210106213030-5aafc221ea8c // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/procfs v0.20.1 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	github.com/sean-/seed v0.0.0-20170313163322-e2103e2c3529 // indirect
	github.com/shoenig/go-m1cpu v0.1.6 // indirect
	github.com/smarty/assertions v1.15.0 // indirect
	github.com/spf13/pflag v1.0.10 // indirect
	github.com/tklauser/go-sysconf v0.3.12 // indirect
	github.com/tklauser/numcpus v0.6.1 // indirect
	github.com/twitchyliquid64/golang-asm v0.15.1 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	github.com/valyala/fastjson v1.6.10 // indirect
	github.com/valyala/fastrand v1.1.0 // indirect
	github.com/valyala/gozstd v1.24.0 // indirect
	github.com/valyala/histogram v1.2.0 // indirect
	github.com/valyala/quicktemplate v1.8.0 // indirect
	github.com/x448/float16 v0.8.4 // indirect
	github.com/yusufpapurcu/wmi v1.2.4 // indirect
	github.com/zeebo/xxh3 v1.1.0 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/collector/featuregate v1.60.0 // indirect
	go.opentelemetry.io/collector/pdata v1.60.0 // indirect
	go.opentelemetry.io/collector/pdata/pprofile v0.154.0 // indirect
	go.opentelemetry.io/collector/semconv v0.110.0 // indirect
	go.opentelemetry.io/otel/log v1.47.0 // indirect
	go.opentelemetry.io/otel/metric v1.47.0 // indirect
	go.opentelemetry.io/otel/trace v1.47.0 // indirect
	go.opentelemetry.io/proto/otlp v1.10.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.yaml.in/yaml/v2 v2.4.4 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
	golang.org/x/arch v0.0.0-20210923205945-b76863e36670 // indirect
	golang.org/x/mod v0.38.0 // indirect
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/oauth2 v0.36.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/term v0.45.0 // indirect
	golang.org/x/tools v0.48.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260615183401-62b3387ff324 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260615183401-62b3387ff324 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/tomb.v1 v1.0.0-20141024135613-dd632973f1e7 // indirect
	k8s.io/apimachinery v0.36.2 // indirect
	k8s.io/client-go v0.36.2 // indirect
	k8s.io/klog/v2 v2.140.0 // indirect
	k8s.io/kube-openapi v0.0.0-20260603220949-865597e52e25 // indirect
	k8s.io/utils v0.0.0-20260507154919-ff6756f316d2 // indirect
	modernc.org/libc v1.74.4 // indirect
	modernc.org/mathutil v1.7.1 // indirect
	modernc.org/memory v1.11.0 // indirect
	sigs.k8s.io/json v0.0.0-20250730193827-2d320260d730 // indirect
	sigs.k8s.io/randfill v1.0.0 // indirect
	sigs.k8s.io/structured-merge-diff/v6 v6.4.0 // indirect
	sigs.k8s.io/yaml v1.6.0 // indirect
)

replace (
	github.com/indirect => ./lib/util/lifted/indirect
	github.com/influxdata/influxdb => ./lib/util/lifted/influxdb
	github.com/openGemini/openGemini/lib/util/lifted/influx/influxql => ./protocol/influxql
)
