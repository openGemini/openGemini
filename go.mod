module github.com/openGemini/openGemini

go 1.16

require (
	github.com/BurntSushi/toml v0.4.1
	github.com/RoaringBitmap/roaring v0.9.1
	github.com/VictoriaMetrics/VictoriaMetrics v1.67.0
	github.com/VictoriaMetrics/fastcache v1.7.0
	github.com/armon/go-metrics v0.3.10
	github.com/c-bata/go-prompt v0.2.2
	github.com/cockroachdb/pebble v0.0.0-20211013210608-e95e73745ce8
	github.com/gogo/protobuf v1.3.2
	github.com/golang-jwt/jwt v3.2.1+incompatible
	github.com/golang/snappy v0.0.4
	github.com/hashicorp/memberlist v0.3.1
	github.com/hashicorp/raft v1.3.1
	github.com/hashicorp/serf v0.9.6
	github.com/hpcloud/tail v1.0.1-0.20170707194310-a927b6857fc7
	github.com/influxdata/influxdb v1.9.5
	github.com/influxdata/influxql v1.1.1-0.20210223160523-b6ab99450c93
	github.com/jedib0t/go-pretty/v6 v6.4.0
	github.com/json-iterator/go v1.1.12
	github.com/klauspost/compress v1.13.6
	github.com/mitchellh/cli v1.1.0
	github.com/mitchellh/copystructure v1.2.0
	github.com/pingcap/failpoint v0.0.0-20200702092429-9f69995143ce
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.12.1
	github.com/prometheus/common v0.32.1
	github.com/prometheus/prometheus v1.8.2-0.20201119142752-3ad25a6dc3d9
	github.com/ryanuber/columnize v2.1.2+incompatible
	github.com/shirou/gopsutil/v3 v3.22.1
	github.com/spf13/cobra v1.3.0
	github.com/stretchr/testify v1.7.4
	github.com/tinylib/msgp v1.1.2
	github.com/valyala/fastjson v1.6.3
	github.com/xlab/treeprint v1.1.0
	go.etcd.io/bbolt v1.3.5
	go.uber.org/zap v1.19.1
	golang.org/x/sync v0.0.0-20220722155255-886fb9371eb4
	golang.org/x/sys v0.0.0-20220722155257-8c9f86f7a55f
	golang.org/x/text v0.3.7
	golang.org/x/time v0.0.0-20210723032227-1f47c861a9ac
	golang.org/x/tools v0.1.12 // indirect
	google.golang.org/protobuf v1.27.1
	gopkg.in/natefinch/lumberjack.v2 v2.0.1-0.20190411184413-94d9e492cc53
)

replace github.com/openGemini/openGemini/open_src/influx/influxql => ./protocol/influxql
