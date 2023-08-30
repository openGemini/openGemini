/*
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package syscontrol

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/logger"
	meta "github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/netstorage"
	meta2 "github.com/openGemini/openGemini/open_src/influx/meta"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"go.uber.org/zap"
)

var (
	ErrNoSuchParam = fmt.Errorf("no parameter find")
)

type SysControl struct {
	MetaClient meta.MetaClient
	NetStore   netstorage.Storage
}

func NewSysControl() *SysControl {
	return &SysControl{}
}

var SysCtrl *SysControl

func init() {
	SysCtrl = NewSysControl()

	handlerOnQueryRequest[QueryShardStatus] = handleQueryShardStatus
}

/*
Store cmd:
curl -i -XPOST 'http://127.0.0.1:8086/debug/ctrl?mod=flush'
curl -i -XPOST 'https://127.0.0.1:8086/debug/ctrl?mod=snapshot&flushduration=5m' -k --insecure -u admin:aBeGhKO0Qr2V9YZ~
curl -i -XPOST 'http://127.0.0.1:8086/debug/ctrl?mod=compen&switchon=true&allshards=true&shid=4'
curl -i -XPOST 'http://127.0.0.1:8086/debug/ctrl?mod=merge&switchon=true&allshards=true&shid=4'
curl -i -XPOST 'http://127.0.0.1:8086/debug/ctrl?mod=snapshot&duration=30m'
curl -i -XPOST 'http://127.0.0.1:8086/debug/ctrl?mod=downsample_in_order&order=true'

curl -i -XPOST 'http://127.0.0.1:8086/debug/ctrl?mod=readonly&switchon=true&allnodes=y'
curl -i -XPOST 'http://127.0.0.1:8086/debug/ctrl?mod=readonly&switchon=true&host=127.0.0.1'
curl -i -XPOST 'http://127.0.0.1:8086/debug/ctrl?mod=verifynode&switchon=false'
curl -i -XPOST 'http://127.0.0.1:8086/debug/ctrl?mod=memusagelimit&limit=85'
curl -i -XPOST 'http://127.0.0.1:8086/debug/ctrl?mod=backgroundReadLimiter&limit=100m'

curl -i -XPOST 'http://127.0.0.1:8086/debug/ctrl?mod=interruptquery&switchon=true&allnodes=y'
curl -i -XPOST 'http://127.0.0.1:8086/debug/ctrl?mod=uppermemusepct&limit=99&allnodes=y'

Sql cmd:
curl -i -XPOST 'http://127.0.0.1:8086/debug/ctrl?mod=chunk_reader_parallel&limit=4'
curl -i -XPOST 'http://127.0.0.1:8086/debug/ctrl?mod=binary_tree_merge&enabled=1'
curl -i -XPOST 'http://127.0.0.1:8086/debug/ctrl?mod=print_logical_plan&enabled=1'
curl -i -XPOST 'http://127.0.0.1:8086/debug/ctrl?mod=sliding_window_push_up&enabled=1'
curl -i -XPOST 'http://127.0.0.1:8086/debug/ctrl?mod=log_rows&switchon=true&rules=mst,tk1=tv1'
curl -i -XPOST 'http://127.0.0.1:8086/debug/ctrl?mod=force_broadcast_query&enabled=1'
curl -i -XPOST 'http://127.0.0.1:8086/debug/ctrl?mod=time_filter_protection&enabled=true'
curl -i -XPOST 'http://127.0.0.1:8086/debug/ctrl?mod=disablewrite&switchon=true'
curl -i -XPOST 'http://127.0.0.1:8086/debug/ctrl?mod=disableread&switchon=true'
*/

const (
	DataFlush             = "flush"
	DownSampleInOrder     = "downsample_in_order"
	compactionEn          = "compen"
	compmerge             = "merge"
	snapshot              = "snapshot"
	ChunkReaderParallel   = "chunk_reader_parallel"
	BinaryTreeMerge       = "binary_tree_merge"
	PrintLogicalPlan      = "print_logical_plan"
	SlidingWindowPushUp   = "sliding_window_push_up"
	ForceBroadcastQuery   = "force_broadcast_query"
	Failpoint             = "failpoint"
	Readonly              = "readonly"
	LogRows               = "log_rows"
	verifyNode            = "verifynode"
	memUsageLimit         = "memusagelimit"
	TimeFilterProtection  = "time_filter_protection"
	disableWrite          = "disablewrite"
	disableRead           = "disableread"
	BackgroundReadLimiter = "backgroundReadLimiter"

	NodeInterruptQuery = "interruptquery"
	UpperMemUsePct     = "uppermemusepct"
)

var (
	QueryParallel int32 = -1

	querySeriesLimit = 0 // query series upper bound in one shard. See also query-series-limit in config

	queryEnabledWhenExceedSeries = true // this determines whether to return value when select series exceed the limit number

	DisableReads  = false
	DisableWrites = false

	InterruptQuery       = false
	UpperMemPct    int64 = 0
)

func UpdateInterruptQuery(switchOn bool) {
	InterruptQuery = switchOn
}

func SetUpperMemUsePct(d int64) {
	logger.GetLogger().Info("SetUpperMemPct:", zap.Int64("UpperMemPct", d))
	if d <= 0 || d > 100 {
		return
	}
	atomic.StoreInt64(&UpperMemPct, d)
}

func SetDisableWrite(en bool) {
	DisableWrites = en
	logger.GetLogger().Info("DisableWrites", zap.Bool("switch", en))
}

func SetDisableRead(en bool) {
	DisableReads = en
	logger.GetLogger().Info("DisableReads", zap.Bool("switch", en))
}

func SetQueryParallel(limit int64) {
	atomic.StoreInt32(&QueryParallel, int32(limit))
}

func SetQuerySeriesLimit(limit int) {
	querySeriesLimit = limit
}

func GetQuerySeriesLimit() int {
	return querySeriesLimit
}

func SetQuerySchemaLimit(limit int) {
	executor.SetQuerySchemaLimit(limit)
}

func SetQueryEnabledWhenExceedSeries(enabled bool) {
	queryEnabledWhenExceedSeries = enabled
}

func GetQueryEnabledWhenExceedSeries() bool {
	return queryEnabledWhenExceedSeries
}

func SetTimeFilterProtection(enabled bool) {
	query.TimeFilterProtection = enabled
}

func GetTimeFilterProtection() bool {
	return query.TimeFilterProtection
}

type LogRowsRule struct {
	Mst  string
	Tags map[string]string
}

var (
	LogRowsRuleSwitch int32 // 0: disable, 1: enable
	MuLogRowsRule     sync.RWMutex
	MyLogRowsRule     = &LogRowsRule{
		Mst: "",
	}
)

func SetLogRowsRuleSwitch(switchon bool, rules string) error {
	MuLogRowsRule.Lock()
	if switchon {
		// set rules
		rr := strings.Split(rules, ",")
		MyLogRowsRule.Mst = rr[0]
		MyLogRowsRule.Tags = make(map[string]string)
		for _, r := range rr[1:] {
			tkvs := strings.Split(r, "=")
			if len(tkvs) != 2 {
				continue
			}
			MyLogRowsRule.Tags[tkvs[0]] = tkvs[1]
		}
		atomic.StoreInt32(&LogRowsRuleSwitch, 1)
	} else {
		// clean rules
		MyLogRowsRule.Mst = ""
		MyLogRowsRule.Tags = make(map[string]string)
		atomic.StoreInt32(&LogRowsRuleSwitch, 0)
	}
	MuLogRowsRule.Unlock()
	fmt.Println("SetLogRowsRuleSwitch:", switchon)
	return nil
}

var handlerOnQueryRequest = make(map[queryRequestMod]func(req netstorage.SysCtrlRequest) (string, error), 1)

type queryRequestMod string

const (
	QueryShardStatus queryRequestMod = "queryShardStatus"
)

func handleQueryShardStatus(req netstorage.SysCtrlRequest) (string, error) {
	dataNodes, err := SysCtrl.MetaClient.DataNodes()
	if err != nil {
		return "", err
	}
	var lock sync.Mutex
	result := make(map[string]interface{})

	var wg sync.WaitGroup
	for _, d := range dataNodes {
		wg.Add(1)
		go func(d meta2.DataNode) {
			defer wg.Done()
			nodeRes, err := SysCtrl.NetStore.SendQueryRequestOnNode(d.ID, req)
			if err != nil {
				return
			}

			for k, v := range nodeRes {
				var vd interface{}
				err = json.Unmarshal([]byte(v), &vd)
				if err != nil {
					continue
				}
				lock.Lock()
				result[k] = vd
				lock.Unlock()
			}
		}(d)
	}
	wg.Wait()

	data, err := json.Marshal(result)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// ProcessQueryRequest only process get method
func ProcessQueryRequest(mod queryRequestMod, param map[string]string) (string, error) {
	handler, ok := handlerOnQueryRequest[mod]
	if !ok {
		return "", fmt.Errorf("not support query mod %s", mod)
	}

	var req netstorage.SysCtrlRequest
	req.SetParam(param)
	req.SetMod(string(mod))
	return handler(req)
}

func ProcessRequest(req netstorage.SysCtrlRequest, resp *strings.Builder) (err error) {
	switch req.Mod() {
	case DataFlush, compactionEn, compmerge, snapshot, Failpoint, DownSampleInOrder, verifyNode, memUsageLimit, BackgroundReadLimiter:
		// store SysCtrl cmd
		dataNodes, err := SysCtrl.MetaClient.DataNodes()
		if err != nil {
			return err
		}
		var lock sync.Mutex
		var wg sync.WaitGroup
		for _, d := range dataNodes {
			wg.Add(1)
			go sendCmdToStoreAsync(req, resp, d.ID, d.Host, &lock, &wg)
		}
		wg.Wait()
	case Readonly:
		return handleSelectedStoreCmd(req, resp)
	case ChunkReaderParallel:
		// sql SysCtrl cmd
		limit, err := GetIntValue(req.Param(), "limit")
		if err != nil {
			return err
		}
		if limit < 0 {
			return fmt.Errorf("invalid limit:%v", limit)
		}
		SetQueryParallel(limit)
		res := "\n\tsuccess"
		resp.WriteString(res)
	case BinaryTreeMerge:
		enabled, err := GetIntValue(req.Param(), "enabled")
		if err != nil {
			return err
		}
		if enabled != 0 && enabled != 1 {
			return fmt.Errorf("invalid enabled:%v", enabled)
		}
		executor.SetEnableBinaryTreeMerge(enabled)
		res := "\n\tsuccess"
		resp.WriteString(res)
	case PrintLogicalPlan:
		enabled, err := GetIntValue(req.Param(), "enabled")
		if err != nil {
			return err
		}
		if enabled != 0 && enabled != 1 {
			return fmt.Errorf("invalid enabled:%v", enabled)
		}
		executor.SetEnablePrintLogicalPlan(enabled)
		res := "\n\tsuccess"
		resp.WriteString(res)
	case SlidingWindowPushUp:
		enabled, err := GetIntValue(req.Param(), "enabled")
		if err != nil {
			return err
		}
		if enabled != 0 && enabled != 1 {
			return fmt.Errorf("invalid enabled:%v", enabled)
		}
		executor.SetEnableSlidingWindowPushUp(enabled)
		res := "\n\tsuccess"
		resp.WriteString(res)
	case LogRows:
		err := handleLogRowsCmd(req)
		if err != nil {
			return err
		}
		res := "\n\tsuccess"
		resp.WriteString(res)
	case ForceBroadcastQuery:
		enabled, err := GetIntValue(req.Param(), "enabled")
		if err != nil {
			return err
		}
		if enabled != 0 && enabled != 1 {
			return fmt.Errorf("invalid enabled:%v", enabled)
		}
		executor.SetEnableForceBroadcastQuery(enabled)
		res := "\n\tsuccess"
		resp.WriteString(res)
	case TimeFilterProtection:
		enabled, err := GetBoolValue(req.Param(), "enabled")
		if err != nil {
			return err
		}
		SetTimeFilterProtection(enabled)
		res := "\n\tsuccess"
		resp.WriteString(res)
	case disableWrite:
		en, err := GetBoolValue(req.Param(), "switchon")
		if err != nil {
			return err
		}
		SetDisableWrite(en)
		res := "\n\tsuccess"
		resp.WriteString(res)
	case disableRead:
		en, err := GetBoolValue(req.Param(), "switchon")
		if err != nil {
			return err
		}
		SetDisableRead(en)
		res := "\n\tsuccess"
		resp.WriteString(res)
	case NodeInterruptQuery:
		if err != nil {
			return err
		}
		return broadcastCmdToStore(req, resp)
	case UpperMemUsePct:
		if err != nil {
			return err
		}
		return broadcastCmdToStore(req, resp)
	default:
		return fmt.Errorf("unknown sysctrl mod: %v", req.Mod())
	}
	return nil
}

func broadcastCmdToStore(req netstorage.SysCtrlRequest, resp *strings.Builder) error {
	// store SysCtrl cmd
	if SysCtrl.MetaClient == nil {
		return fmt.Errorf("broadcastCmdToStore fail: metaClient nil")
	}
	dataNodes, err := SysCtrl.MetaClient.DataNodes()
	if err != nil {
		return err
	}
	var lock sync.Mutex
	var wg sync.WaitGroup
	for _, d := range dataNodes {
		wg.Add(1)
		go sendCmdToStoreAsync(req, resp, d.ID, d.Host, &lock, &wg)
	}
	wg.Wait()
	return nil
}

func sendCmdToStoreAsync(req netstorage.SysCtrlRequest, resp *strings.Builder, nid uint64, host string, lock *sync.Mutex, wg *sync.WaitGroup) {
	res := sendCmdToStore(req, nid, host)
	lock.Lock()
	resp.WriteString(res)
	lock.Unlock()
	wg.Done()
}

func handleSelectedStoreCmd(req netstorage.SysCtrlRequest, resp *strings.Builder) error {
	// selected store SysCtrl cmd
	dataNodes, err := SysCtrl.MetaClient.DataNodes()
	if err != nil {
		return err
	}
	_, ok := req.Param()["allnodes"]
	target, nodeOk := req.Param()["host"]
	if !ok {
		if !nodeOk {
			return fmt.Errorf("lack of allnodes or host param")
		}
	}

	var lock sync.Mutex
	var wg sync.WaitGroup
	for _, d := range dataNodes {
		if target != "" {
			ip := strings.Split(d.Host, ":")[0]
			if ip != target {
				continue
			}
		}
		wg.Add(1)
		go sendCmdToStoreAsync(req, resp, d.ID, d.Host, &lock, &wg)
	}
	wg.Wait()
	return nil
}

func handleLogRowsCmd(req netstorage.SysCtrlRequest) error {
	switchon, err := GetBoolValue(req.Param(), "switchon")
	if err != nil {
		return err
	}
	var rules string
	if switchon {
		rules = req.Param()["rules"]
		if len(rules) == 0 {
			return fmt.Errorf("rules can not be empty")
		} else if strings.Contains(rules, " ") {
			return fmt.Errorf("rules can not contains space")
		}
		fmt.Println("LogRowsRules", rules)
	}
	return SetLogRowsRuleSwitch(switchon, rules)
}

func sendCmdToStore(req netstorage.SysCtrlRequest, nid uint64, host string) string {
	var res string
	_, err := SysCtrl.NetStore.SendSysCtrlOnNode(nid, req)
	if err != nil {
		res = fmt.Sprintf("\n\t%v: failed,%v,", host, err)
	} else {
		res = fmt.Sprintf("\n\t%v: success,", host)
	}
	return res
}

func GetIntValue(param map[string]string, key string) (int64, error) {
	str, ok := param[key]
	if !ok {
		return 0, ErrNoSuchParam
	}
	n, err := strconv.ParseUint(str, 10, strconv.IntSize)
	if err != nil {
		return 0, err
	}
	return int64(n), nil
}

func GetBoolValue(param map[string]string, key string) (bool, error) {
	switchStr, ok := param[key]
	if !ok {
		return false, ErrNoSuchParam
	}
	return strconv.ParseBool(switchStr)
}

func GetDurationValue(param map[string]string, key string) (time.Duration, error) {
	text, ok := param[key]
	if !ok || len(text) == 0 {
		return 0, ErrNoSuchParam
	}
	text = strings.Trim(text, " ")
	text = strings.ToLower(text)
	if len(text) < 1 {
		return 0, fmt.Errorf("unknow low query time")
	}

	d, err := time.ParseDuration(text)
	return d, err
}

func GetBytesValue(param map[string]string, key string) (int64, error) {
	str, ok := param[key]
	if !ok {
		return 0, ErrNoSuchParam
	}
	str = strings.Trim(str, " ")
	if len(str) == 0 {
		return 0, fmt.Errorf("error unit")
	}

	unit := str[len(str)-1:]
	size, err := strconv.ParseInt(str[0:len(str)-1], 10, strconv.IntSize)
	if err != nil {
		return 0, err
	}

	switch strings.ToLower(unit) {
	case "k":
		size *= config.KB
	case "m":
		size *= config.MB
	case "g":
		size *= config.GB
	default:
		return 0, fmt.Errorf("error unit")
	}

	return size, nil
}
