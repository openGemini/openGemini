module github.com/openGemini/openGemini

go 1.22

require (
	github.com/BurntSushi/toml v0.4.1
	github.com/RoaringBitmap/roaring v0.9.1
	github.com/VictoriaMetrics/VictoriaMetrics v1.67.0
	github.com/VictoriaMetrics/fastcache v1.7.0
	github.com/agiledragon/gomonkey/v2 v2.10.1
	github.com/apache/arrow/go/v13 v13.0.0-20230630125530-5a06b2ec2a8e
	github.com/armon/go-metrics v0.4.1
	github.com/bits-and-blooms/bloom/v3 v3.5.0
	github.com/bytedance/sonic v1.12.2
	github.com/cespare/xxhash/v2 v2.2.0
	github.com/cockroachdb/errors v1.9.1
	github.com/deckarep/golang-set v1.8.0
	github.com/docker/go-units v0.5.0
	github.com/golang-jwt/jwt/v5 v5.0.0
	github.com/golang/snappy v0.0.5-0.20231225225746-43d5d4cd4e0e
	github.com/gorilla/mux v1.8.1
	github.com/hashicorp/golang-lru/v2 v2.0.7
	github.com/hashicorp/memberlist v0.3.1
	github.com/hashicorp/raft v1.6.1
	github.com/hashicorp/serf v0.9.6
	github.com/huaweicloud/huaweicloud-sdk-go-obs v3.23.3+incompatible
	github.com/influxdata/influxdb v1.9.5
	github.com/influxdata/influxdb-observability/common v0.2.19
	github.com/influxdata/influxdb-observability/otel2influx v0.2.19
	github.com/influxdata/influxql v1.1.1-0.20211004132434-7e7d61973256
	github.com/json-iterator/go v1.1.12
	github.com/klauspost/compress v1.17.2
	github.com/mattn/go-sqlite3 v1.14.19
	github.com/mitchellh/cli v1.1.5
	github.com/nxadm/tail v1.4.11
	github.com/olekukonko/tablewriter v0.0.0-20170122224234-a0225b3f23b5
	github.com/openGemini/go-prompt v0.0.0-20240906095849-29653678978f
	github.com/panjf2000/ants/v2 v2.5.0
	github.com/pierrec/lz4/v4 v4.1.17
	github.com/pingcap/failpoint v0.0.0-20220801062533-2eaa32854a6c
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.12.1
	github.com/prometheus/common v0.32.1
	github.com/prometheus/prometheus v1.8.2-0.20210914090109-37468d88dce8
	github.com/savsgio/dictpool v0.0.0-20221023140959-7bf2e61cea94
	github.com/shirou/gopsutil/v3 v3.22.1
	github.com/smartystreets/goconvey v1.7.2
	github.com/spf13/cobra v1.3.0
	github.com/stretchr/testify v1.9.0
	github.com/tinylib/msgp v1.1.8
	github.com/valyala/fastjson v1.6.4
	github.com/xlab/treeprint v1.2.0
	go.etcd.io/bbolt v1.3.8
	go.etcd.io/etcd/raft/v3 v3.5.10
	go.opentelemetry.io/collector/pdata v0.50.0
	go.opentelemetry.io/collector/semconv v0.99.0
	go.uber.org/zap v1.19.1
	golang.org/x/crypto v0.26.0
	golang.org/x/sys v0.23.0
	golang.org/x/term v0.23.0
	golang.org/x/text v0.17.0
	golang.org/x/time v0.5.0
	google.golang.org/grpc v1.62.1
	google.golang.org/protobuf v1.33.0
	gopkg.in/natefinch/lumberjack.v2 v2.2.1
)

require (
	cloud.google.com/go v0.112.1 // indirect
	cloud.google.com/go/bigquery v1.53.0 // indirect
	cloud.google.com/go/iam v1.1.7 // indirect
	cloud.google.com/go/longrunning v0.5.6 // indirect
	github.com/JohnCGriffin/overflow v0.0.0-20211019200055-46fa312c352c // indirect
	github.com/Masterminds/goutils v1.1.1 // indirect
	github.com/Masterminds/semver v1.4.2 // indirect
	github.com/Masterminds/semver/v3 v3.1.1 // indirect
	github.com/Masterminds/sprig v2.16.0+incompatible // indirect
	github.com/Masterminds/sprig/v3 v3.2.1 // indirect
	github.com/VictoriaMetrics/metrics v1.18.0 // indirect
	github.com/VictoriaMetrics/metricsql v0.26.0 // indirect
	github.com/alecthomas/units v0.0.0-20210208195552-ff826a37aa15 // indirect
	github.com/andreyvit/diff v0.0.0-20170406064948-c7f18ee00883 // indirect
	github.com/andybalholm/brotli v1.0.4 // indirect
	github.com/aokoli/goutils v1.0.1 // indirect
	github.com/apache/arrow/go/arrow v0.0.0-20200923215132-ac86123a3f01 // indirect
	github.com/apache/thrift v0.16.0 // indirect
	github.com/armon/circbuf v0.0.0-20150827004946-bbbad097214e // indirect
	github.com/armon/go-radix v1.0.0 // indirect
	github.com/aws/aws-sdk-go v1.40.58 // indirect
	github.com/benbjohnson/immutable v0.2.1 // indirect
	github.com/benbjohnson/tmpl v1.0.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bgentry/speakeasy v0.1.0 // indirect
	github.com/bits-and-blooms/bitset v1.8.0 // indirect
	github.com/bmizerany/pat v0.0.0-20170815010413-6226ea591a40 // indirect
	github.com/bytedance/sonic/loader v0.2.0 // indirect
	github.com/c-bata/go-prompt v0.2.6 // indirect
	github.com/cespare/xxhash v1.1.0 // indirect
	github.com/cloudwego/base64x v0.1.4 // indirect
	github.com/cloudwego/iasm v0.2.0 // indirect
	github.com/cockroachdb/logtags v0.0.0-20211118104740-dabe8e521a4f // indirect
	github.com/cockroachdb/redact v1.1.3 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dennwc/varint v1.0.0 // indirect
	github.com/dgryski/go-bitstream v0.0.0-20180413035011-3522498ce2c8 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/edsrzf/mmap-go v1.0.0 // indirect
	github.com/fatih/color v1.13.0 // indirect
	github.com/fsnotify/fsnotify v1.6.0 // indirect
	github.com/getsentry/sentry-go v0.12.0 // indirect
	github.com/glycerine/go-unsnap-stream v0.0.0-20180323001048-9f0cb55181dd // indirect
	github.com/go-chi/chi v4.1.0+incompatible // indirect
	github.com/go-kit/log v0.2.0 // indirect
	github.com/go-logfmt/logfmt v0.5.1 // indirect
	github.com/go-logr/logr v1.4.1 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/goccy/go-json v0.10.0 // indirect
	github.com/gofrs/uuid v4.0.0+incompatible // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang-jwt/jwt v3.2.2+incompatible // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/google/btree v1.0.1 // indirect
	github.com/google/flatbuffers v23.1.21+incompatible // indirect
	github.com/google/go-cmp v0.6.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/gopherjs/gopherjs v0.0.0-20181017120253-0766667cb4d1 // indirect
	github.com/hashicorp/errwrap v1.0.0 // indirect
	github.com/hashicorp/go-hclog v1.6.2 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.1 // indirect
	github.com/hashicorp/go-msgpack v0.5.3 // indirect
	github.com/hashicorp/go-msgpack/v2 v2.1.1 // indirect
	github.com/hashicorp/go-multierror v1.1.0 // indirect
	github.com/hashicorp/go-sockaddr v1.0.2 // indirect
	github.com/hashicorp/go-syslog v1.0.0 // indirect
	github.com/hashicorp/golang-lru v0.5.4 // indirect
	github.com/hashicorp/logutils v1.0.0 // indirect
	github.com/hashicorp/mdns v1.0.4 // indirect
	github.com/huandu/xstrings v1.3.2 // indirect
	github.com/imdario/mergo v0.3.11 // indirect
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/influxdata/flux v0.131.0 // indirect
	github.com/influxdata/httprouter v1.3.1-0.20191122104820-ee83e2772f69 // indirect
	github.com/influxdata/pkg-config v0.2.8 // indirect
	github.com/influxdata/roaring v0.4.13-0.20180809181101-fc520f41fab6 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/jpillora/backoff v1.0.0 // indirect
	github.com/jsternberg/zap-logfmt v1.2.0 // indirect
	github.com/jtolds/gls v4.20.0+incompatible // indirect
	github.com/jwilder/encoding v0.0.0-20170811194829-b4e1701a28ef // indirect
	github.com/klauspost/asmfmt v1.3.2 // indirect
	github.com/klauspost/cpuid/v2 v2.2.3 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/lufia/plan9stats v0.0.0-20211012122336-39d0f177ccd0 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.19 // indirect
	github.com/mattn/go-runewidth v0.0.13 // indirect
	github.com/mattn/go-tty v0.0.3 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.2-0.20181231171920-c182affec369 // indirect
	github.com/miekg/dns v1.1.43 // indirect
	github.com/minio/asm2plan9s v0.0.0-20200509001527-cdd76441f9d8 // indirect
	github.com/minio/c2goasm v0.0.0-20190812172519-36a3d3bbc4f3 // indirect
	github.com/mitchellh/copystructure v1.0.0 // indirect
	github.com/mitchellh/mapstructure v1.4.3 // indirect
	github.com/mitchellh/reflectwalk v1.0.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/mschoch/smat v0.2.0 // indirect
	github.com/mwitkow/go-conntrack v0.0.0-20190716064945-2f068394615f // indirect
	github.com/oklog/ulid v1.3.1 // indirect
	github.com/opentracing/opentracing-go v1.2.0 // indirect
	github.com/philhofer/fwd v1.1.2 // indirect
	github.com/pingcap/errors v0.11.4 // indirect
	github.com/pkg/term v1.2.0-beta.2 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/posener/complete v1.2.3 // indirect
	github.com/power-devops/perfstat v0.0.0-20210106213030-5aafc221ea8c // indirect
	github.com/prometheus/client_model v0.5.0 // indirect
	github.com/prometheus/common/sigv4 v0.1.0 // indirect
	github.com/prometheus/procfs v0.7.3 // indirect
	github.com/rivo/uniseg v0.2.0 // indirect
	github.com/rogpeppe/go-internal v1.10.0 // indirect
	github.com/ryanuber/columnize v2.1.0+incompatible // indirect
	github.com/savsgio/gotils v0.0.0-20220530130905-52f3993e8d6d // indirect
	github.com/sean-/seed v0.0.0-20170313163322-e2103e2c3529 // indirect
	github.com/sergi/go-diff v1.1.0 // indirect
	github.com/shopspring/decimal v1.2.0 // indirect
	github.com/smartystreets/assertions v1.2.0 // indirect
	github.com/spf13/cast v1.4.1 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/tklauser/go-sysconf v0.3.9 // indirect
	github.com/tklauser/numcpus v0.3.0 // indirect
	github.com/twitchyliquid64/golang-asm v0.15.1 // indirect
	github.com/uber/jaeger-client-go v2.29.1+incompatible // indirect
	github.com/uber/jaeger-lib v2.4.1+incompatible // indirect
	github.com/valyala/fastrand v1.1.0 // indirect
	github.com/valyala/gozstd v1.13.0 // indirect
	github.com/valyala/histogram v1.2.0 // indirect
	github.com/willf/bitset v1.1.11 // indirect
	github.com/yusufpapurcu/wmi v1.2.2 // indirect
	github.com/zeebo/xxh3 v1.0.2 // indirect
	go.opentelemetry.io/collector/model v0.50.0 // indirect
	go.uber.org/atomic v1.9.0 // indirect
	go.uber.org/goleak v1.3.0 // indirect
	go.uber.org/multierr v1.7.0 // indirect
	golang.org/x/arch v0.0.0-20210923205945-b76863e36670 // indirect
	golang.org/x/exp v0.0.0-20230206171751-46f607a40771 // indirect
	golang.org/x/mod v0.17.0 // indirect
	golang.org/x/net v0.25.0 // indirect
	golang.org/x/oauth2 v0.17.0 // indirect
	golang.org/x/sync v0.8.0 // indirect
	golang.org/x/tools v0.21.1-0.20240508182429-e35e4ccd0d2d // indirect
	golang.org/x/xerrors v0.0.0-20220907171357-04be3eba64a2 // indirect
	google.golang.org/api v0.169.0 // indirect
	google.golang.org/appengine v1.6.8 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20240311132316-a219d84964c2 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240311132316-a219d84964c2 // indirect
	gopkg.in/tomb.v1 v1.0.0-20141024135613-dd632973f1e7 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/openGemini/openGemini/lib/util/lifted/influx/influxql => ./protocol/influxql
