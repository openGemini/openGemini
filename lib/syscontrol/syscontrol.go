// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package syscontrol

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/openGemini/openGemini/lib/backup"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	meta "github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/msgservice"
	"github.com/openGemini/openGemini/lib/sysconfig"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"go.uber.org/zap"
)

var (
	ErrNoSuchParam = fmt.Errorf("no parameter found")
)

type SysControl struct {
	MetaClient meta.MetaClient
}

func NewSysControl() *SysControl {
	return &SysControl{}
}

func (s *SysControl) SendSysCtrlOnNode(nodeID uint64, req msgservice.SysCtrlRequest) (map[string]string, error) {
	r := msgservice.NewRequester(0, nil, s.MetaClient)
	err := r.InitWithNodeID(nodeID)
	if err != nil {
		return nil, err
	}

	v, err := r.SysCtrl(&req)
	if err != nil {
		return nil, err
	}

	resp, ok := v.(*msgservice.SysCtrlResponse)
	if !ok {
		return nil, errno.NewInvalidTypeError("*msgservice.SysCtrlResponse", v)
	}

	if resp.Error() != nil {
		return nil, resp.Error()
	}

	return resp.Result(), nil
}

func (s *SysControl) SendQueryRequestOnNode(nodeID uint64, req msgservice.SysCtrlRequest) (map[string]string, error) {
	r := msgservice.NewRequester(0, nil, s.MetaClient)
	err := r.InitWithNodeID(nodeID)
	if err != nil {
		return nil, err
	}

	v, err := r.SysCtrl(&req)
	if err != nil {
		return nil, err
	}

	resp, ok := v.(*msgservice.SysCtrlResponse)
	if !ok {
		return nil, errno.NewInvalidTypeError("*msgservice.SysCtrlResponse", v)
	}
	return resp.Result(), nil
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
curl -i -XPOST 'http://127.0.0.1:8086/debug/ctrl?mod=backup'
curl -i -XPOST 'http://127.0.0.1:8086/debug/ctrl?mod=compactmemusagelimit&limit=90&allnodes=y'
curl -i -XPOST 'http://127.0.0.1:8635/debug/ctrl?mod=updatestreamconf&key=xxx&val=xxx&allnodes=y'
curl -i -XPOST 'http://127.0.0.1:8635/debug/ctrl?mod=tsspunreftimeout&duration=0s&allnodes=y'
curl -i -XPOST 'http://127.0.0.1:8635/debug/ctrl?mod=dbptunreftimeout&duration=0s&allnodes=y'
curl -i -XPOST 'http://127.0.0.1:8635/debug/ctrl?mod=offloadwaitquery&switchon=true&allnodes=y'

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
curl -i -XPOST 'http://127.0.0.1:8635/debug/ctrl?mod=db_num_limit&limit=3'
curl -i -XPOST 'http://127.0.0.1:8635/debug/ctrl?mod=obsaksk&ak="xx"&sk="xx"'
curl -i -XPOST 'http://127.0.0.1:8635/debug/ctrl?mod=obshostname&hostname="xxx"'
curl -i -XPOST 'http://127.0.0.1:8635/debug/ctrl?mod=slowquery&limit=10s'
curl -i -XPOST 'http://127.0.0.1:8635/debug/ctrl?mod=querytimeout&limit=10s'
curl -i -XPOST 'http://127.0.0.1:8635/debug/ctrl?mod=shard_group_timezone&timezone=Asia/Shanghai'
curl -i -XPOST 'http://127.0.0.1:8086/debug/ctrl?mod=enable_readonly_write&switchon=true'
curl -i -XPOST 'http://127.0.0.1:8635/debug/ctrl?mod=merge_self_conf&max_merge_self_level=3&max_num_of_file_to_merge_self=8,8,4&allnodes=y'
curl -i -XPOST 'http://127.0.0.1:8086/debug/ctrl?mod=walenabled&switchon=true'
curl -i -XPOST 'http://127.0.0.1:8086/debug/ctrl?mod=datacacheenable&enabled=1'
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
	LogRows               = "log_rows"
	verifyNode            = "verifynode"
	memUsageLimit         = "memusagelimit"
	CompactMemUsageLimit  = "compactmemusagelimit"
	TimeFilterProtection  = "time_filter_protection"
	disableWrite          = "disablewrite"
	disableRead           = "disableread"
	BackgroundReadLimiter = "backgroundReadLimiter"

	NodeInterruptQuery = "interruptquery"
	UpperMemUsePct     = "uppermemusepct"
	ParallelQuery      = "parallelbatch"
	Backup             = "backup"
	AbortBackup        = "abort_backup"
	BackupStatus       = "backup_status"

	WriteStreamPointsEnable = "write_stream_points_enable"
	DBLimit                 = "db_num_limit"

	UpdateStreamConf   = "updatestreamconf"
	ObsAkSk            = "obsaksk"
	ObsHostName        = "obshostname"
	SlowQuery          = "slowquery"
	QueryTimeOut       = "querytimeout"
	ShardGroupTimeZone = "shard_group_timezone"

	LastRowCache = "last_row_cache"

	NodeReadonly            = "readonly"
	NodeEnableReadonlyWrite = "enable_readonly_write"
	MergeSelfConf           = "merge_self_conf"

	TsspUnrefTimeout = "tsspunreftimeout"
	DbptUnrefTimeout = "dbptunreftimeout"
	OffloadWaitQuery = "offloadwaitquery"
)

const (
	LastRowCacheEnable        = "last-row-cache-enabled"
	LastRowCacheMetricsEnable = "last-row-cache-metrics-enabled"
	LastRowCacheMaxCost       = "last-row-cache-max-cost"
	ForceFlushDuration        = "force_flush_duration"
	WalEnabled                = "walenabled"
	DataCacheEnable           = "datacacheenable"
)

const (
	ParquetTask               = "parquet_task"
	ParquetLevel              = "tssp-to-parquet-level"
	ParquetGroupLen           = "max-group-len"
	ParquetPageSize           = "page-size"
	ParquetBatchSize          = "write-batch-size"
	ParquetItrSize            = "itr-batch-size"
	ParquetDictCompressEnable = "dict-compress-enable"
	ParquetCompressAlg        = "compress-alg"
	ParquetEnableMst          = "enable-mst"
	ParquetMaxStatsSize       = "max-stats-size"
)

var (
	QueryParallel int32 = -1

	querySeriesLimit = 0 // query series upper bound in one shard. See also query-series-limit in config

	queryEnabledWhenExceedSeries = true // this determines whether to return value when select series exceed the limit number

	DisableReads  = false
	DisableWrites = false

	ParallelQueryInBatch int32 = 0 // this determines whether to use parallel query when a query is combined with multi queries

	Readonly            = false
	EnableReadonlyWrite = false

	HierarchicalStorageEnabled int32 = 0

	HierarchicalIndexStorageEnabled int32 = 0

	WriteColdShardEnable int32 = 0

	indexReadCachePersistent bool  = true
	DatabaseNumLimit         int32 = 0
)

func SetDisableWrite(en bool) {
	DisableWrites = en
	logger.GetLogger().Info("DisableWrites", zap.Bool("switch", en))
}

func SetDatabaselimit(limit int32) {
	atomic.StoreInt32(&DatabaseNumLimit, limit)
	logger.GetLogger().Info("SetDatabaselimit", zap.Int32("limit", DatabaseNumLimit))
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
	sysconfig.SetQuerySchemaLimit(limit)
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

func SetParallelQueryInBatch(en bool) {
	if en {
		atomic.StoreInt32(&ParallelQueryInBatch, 1)
	} else {
		atomic.StoreInt32(&ParallelQueryInBatch, 0)
	}
	fmt.Println("ParallelQueryInBatch:", ParallelQueryInBatch)
}

func SetIndexReadCachePersistent(persist bool) {
	indexReadCachePersistent = persist
	fmt.Println("indexReadCachePersistent:", persist)
}

func IsIndexReadCachePersistent() bool {
	return indexReadCachePersistent
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

func UpdateNodeReadonly(switchOn bool) {
	Readonly = switchOn
}

func IsReadonly() bool {
	return Readonly
}

func UpdateNodeEnableReadonlyWrite(switchOn bool) {
	EnableReadonlyWrite = switchOn
	log.Printf("set EnableReadonlyWrite: %v", switchOn)
}

func IsEnableReadonlyWrite() bool {
	return EnableReadonlyWrite
}

func IsHierarchicalStorageEnabled() bool {
	enabled := atomic.LoadInt32(&HierarchicalStorageEnabled)
	return enabled == 1
}

func IsHierarchicalIndexStorageEnabled() bool {
	enabled := atomic.LoadInt32(&HierarchicalIndexStorageEnabled)
	return enabled == 1
}

func SetHierarchicalStorageEnabled(en bool) {
	if en {
		atomic.StoreInt32(&HierarchicalStorageEnabled, 1)
	} else {
		atomic.StoreInt32(&HierarchicalStorageEnabled, -1)
	}
	logger.GetLogger().Info("set shard hierarchical storage", zap.String("timestamp", time.Now().Format(time.RFC3339Nano)), zap.Bool("HierarchicalStorageEnabled", en))
}

func SetIndexHierarchicalStorageEnabled(en bool) {
	if en {
		atomic.StoreInt32(&HierarchicalIndexStorageEnabled, 1)
	} else {
		atomic.StoreInt32(&HierarchicalIndexStorageEnabled, -1)
	}
	logger.GetLogger().Info("set index hierarchical storage", zap.String("timestamp", time.Now().Format(time.RFC3339Nano)), zap.Bool("HierarchicalIndexStorageEnabled", en))
}

func IsWriteColdShardEnabled() bool {
	enabled := atomic.LoadInt32(&WriteColdShardEnable)
	return enabled == 1
}

func SetWriteColdShardEnabled(en bool) {
	if en {
		atomic.StoreInt32(&WriteColdShardEnable, 1)
	} else {
		atomic.StoreInt32(&WriteColdShardEnable, -1)
	}
	fmt.Println(time.Now().Format(time.RFC3339Nano), "WriteColdShardEnable:", en)
}

var handlerOnQueryRequest = make(map[queryRequestMod]func(req msgservice.SysCtrlRequest) (string, error), 1)

type queryRequestMod string

const (
	QueryShardStatus queryRequestMod = "queryShardStatus"
)

func handleQueryShardStatus(req msgservice.SysCtrlRequest) (string, error) {
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
			nodeRes, err := SysCtrl.SendQueryRequestOnNode(d.ID, req)
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

	var req msgservice.SysCtrlRequest
	req.SetParam(param)
	req.SetMod(string(mod))
	return handler(req)
}

func ProcessRequest(req msgservice.SysCtrlRequest, resp *bufio.Writer) (err error) {
	switch req.Mod() {
	case Failpoint:
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
		// meta SysCtrl cmd
		metaRes, err := SysCtrl.MetaClient.SendSysCtrlToMeta(req.Mod(), req.Param())
		if err != nil {
			WriteString(resp, fmt.Sprintf("\n\t%v,", err))
		}
		for n, s := range metaRes {
			WriteString(resp, fmt.Sprintf("\n\t%v: %s,", n, s))
		}
	case DataFlush, compactionEn, compmerge, snapshot, DownSampleInOrder, verifyNode, memUsageLimit, BackgroundReadLimiter, CompactMemUsageLimit:
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
	case AbortBackup, BackupStatus:
		dataNodes, err := SysCtrl.MetaClient.DataNodes()
		if err != nil {
			return err
		}
		var lock sync.Mutex
		var wg sync.WaitGroup
		for _, d := range dataNodes {
			wg.Add(1)
			go sendBackupCmdToStoreAsync(req, resp, d.ID, d.Host, &lock, &wg)
		}
		wg.Wait()
	case NodeReadonly:
		switchOn, err := GetBoolValue(req.Param(), "switchon")
		if err != nil {
			return err
		}
		UpdateNodeReadonly(switchOn)
		res := "\n\tsuccess"
		WriteString(resp, res)
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
		WriteString(resp, res)
	case BinaryTreeMerge:
		enabled, err := GetIntValue(req.Param(), "enabled")
		if err != nil {
			return err
		}
		if enabled != 0 && enabled != 1 {
			return fmt.Errorf("invalid enabled:%v", enabled)
		}
		sysconfig.SetEnableBinaryTreeMerge(enabled)
		res := "\n\tsuccess"
		WriteString(resp, res)
	case PrintLogicalPlan:
		enabled, err := GetIntValue(req.Param(), "enabled")
		if err != nil {
			return err
		}
		if enabled != 0 && enabled != 1 {
			return fmt.Errorf("invalid enabled:%v", enabled)
		}
		sysconfig.SetEnablePrintLogicalPlan(enabled)
		res := "\n\tsuccess"
		WriteString(resp, res)
	case SlidingWindowPushUp:
		enabled, err := GetIntValue(req.Param(), "enabled")
		if err != nil {
			return err
		}
		if enabled != 0 && enabled != 1 {
			return fmt.Errorf("invalid enabled:%v", enabled)
		}
		sysconfig.SetEnableSlidingWindowPushUp(enabled)
		res := "\n\tsuccess"
		WriteString(resp, res)
	case LogRows:
		err := handleLogRowsCmd(req)
		if err != nil {
			return err
		}
		res := "\n\tsuccess"
		WriteString(resp, res)
	case ForceBroadcastQuery:
		enabled, err := GetIntValue(req.Param(), "enabled")
		if err != nil {
			return err
		}
		if enabled != 0 && enabled != 1 {
			return fmt.Errorf("invalid enabled:%v", enabled)
		}
		sysconfig.SetEnableForceBroadcastQuery(enabled)
		res := "\n\tsuccess"
		WriteString(resp, res)
	case TimeFilterProtection:
		enabled, err := GetBoolValue(req.Param(), "enabled")
		if err != nil {
			return err
		}
		SetTimeFilterProtection(enabled)
		res := "\n\tsuccess"
		WriteString(resp, res)
	case disableWrite:
		en, err := GetBoolValue(req.Param(), "switchon")
		if err != nil {
			return err
		}
		SetDisableWrite(en)
		res := "\n\tsuccess"
		WriteString(resp, res)
	case disableRead:
		en, err := GetBoolValue(req.Param(), "switchon")
		if err != nil {
			return err
		}
		SetDisableRead(en)
		res := "\n\tsuccess"
		WriteString(resp, res)
	case NodeInterruptQuery:
		// update InterruptQuery status for ts-sql
		switchOn, err := GetBoolValue(req.Param(), "switchon")
		if err != nil {
			return err
		}
		sysconfig.SetInterruptQuery(switchOn)
		// update InterruptQuery status for all of ts-store
		return broadcastCmdToStore(req, resp)
	case UpperMemUsePct:
		// update UpperMemUsePct for ts-sql
		upper, err := GetIntValue(req.Param(), "limit")
		if err != nil {
			return err
		}
		sysconfig.SetUpperMemPct(upper)
		// update UpperMemUsePct  for all of ts-store
		return broadcastCmdToStore(req, resp)
	case ParallelQuery:
		enabled, err := GetBoolValue(req.Param(), "enabled")
		if err != nil {
			return err
		}
		SetParallelQueryInBatch(enabled)
		res := "\n\tsuccess"
		WriteString(resp, res)
	case WriteStreamPointsEnable:
		_, err := GetBoolValue(req.Param(), "switchon")
		if err != nil {
			return err
		}
		return sendCmdToLocalOrAll(req, resp)
	case ShardGroupTimeZone:
		timezone, ok := req.Param()["timezone"]
		if !ok {
			return ErrNoSuchParam
		}
		err := config.UpdateTimeZoneLoc(timezone)
		if err != nil {
			return err
		}
		// update ShardGroupTimeZone for all of ts-store
		return broadcastCmdToStore(req, resp)
	case ObsAkSk, ObsHostName, UpdateStreamConf:
		return broadcastCmdToStore(req, resp)
	case DBLimit:
		// update db_num_limit for ts-sql
		dblimit, err := GetIntValue(req.Param(), "limit")
		if err != nil {
			logger.GetLogger().Error("parse DBLimit value error", zap.Error(err))
			return err
		}
		if dblimit < 0 {
			err = fmt.Errorf("dblimit value should be greater than 0")
			logger.GetLogger().Error("parse dblimit value error", zap.Error(err))
			return err
		}
		SetDatabaselimit(int32(dblimit))
		res := "\n\tsuccess"
		WriteString(resp, res)
	case SlowQuery:
		slowQueryLimit, err := GetDurationValue(req.Param(), "limit")
		if err != nil {
			logger.GetLogger().Error("parse SlowQuery value error", zap.Error(err))
			return err
		}
		if slowQueryLimit < 0 {
			err = fmt.Errorf("slowQueryLimit value should be greater than 0")
			logger.GetLogger().Error("parse slowQueryLimit value error", zap.Error(err))
			return err
		}
		sysconfig.SetSlowQuery(slowQueryLimit)
		res := "\n\tsuccess"
		WriteString(resp, res)
	case QueryTimeOut:
		queryTimeOutLimit, err := GetDurationValue(req.Param(), "limit")
		if err != nil {
			logger.GetLogger().Error("parse QueryTimeOut value error", zap.Error(err))
			return err
		}
		if queryTimeOutLimit < 0 {
			err = fmt.Errorf("QueryTimeOut value should be greater than 0")
			logger.GetLogger().Error("parse QueryTimeOut value error", zap.Error(err))
			return err
		}
		sysconfig.SetQueryTimeOut(queryTimeOutLimit)
		res := "\n\tsuccess"
		WriteString(resp, res)
	case LastRowCache:
		return sendCmdToLocalOrAll(req, resp)
	case ParquetTask:
		if err := CheckUpdateParquetTaskParamValid(req.Param()); err != nil {
			return err
		}
		return sendCmdToLocalOrAll(req, resp)
	case NodeEnableReadonlyWrite:
		switchOn, err := GetBoolValue(req.Param(), "switchon")
		if err != nil {
			return err
		}
		UpdateNodeEnableReadonlyWrite(switchOn)
		WriteString(resp, "\n\tsuccess")
	case ForceFlushDuration:
		_, err := GetDurationValue(req.Param(), "duration")
		if err != nil {
			return err
		}
		return sendCmdToLocalOrAll(req, resp)
	case MergeSelfConf:
		return sendCmdToLocalOrAll(req, resp)
	case WalEnabled:
		if _, err := CheckSwitchOnParamValid(req.Param()); err != nil {
			return err
		}
		return broadcastCmdToStore(req, resp)
	case DataCacheEnable:
		if _, err := CheckEnabledParamValid(req.Param()); err != nil {
			return err
		}
		return broadcastCmdToStore(req, resp)
	case TsspUnrefTimeout, DbptUnrefTimeout, OffloadWaitQuery:
		return sendCmdToLocalOrAll(req, resp)
	default:
		return fmt.Errorf("unknown sysctrl mod: %v", req.Mod())
	}
	return nil
}

func ProcessBackup(req msgservice.SysCtrlRequest, resp *bufio.Writer, sqlHost []string) (err error) {
	params := req.Param()
	isNode := params["isNode"] == "true"
	// store SysCtrl cmd
	dataNodes, err := SysCtrl.MetaClient.DataNodes()
	if err != nil {
		return err
	}
	var lock sync.Mutex
	var wg sync.WaitGroup
	for _, d := range dataNodes {
		if !isNode {
			wg.Add(1)
			go sendCmdToStoreAsync(req, resp, d.ID, d.Host, &lock, &wg)
			continue
		}
		for _, host := range sqlHost {
			if strings.HasPrefix(d.Host, host+":") {
				wg.Add(1)
				go sendCmdToStoreAsync(req, resp, d.ID, d.Host, &lock, &wg)
				break
			}
		}
	}
	wg.Wait()

	backupMeta := params[backup.BackupMeta] == "true"
	if params[backup.DataBases] == "" {
		backupMeta = true
	}
	if !backupMeta {
		return nil
	}
	metaServers := SysCtrl.MetaClient.MetaServers()
	metaRes := make(map[string]string, len(metaServers))
	for i, m := range metaServers {
		if !isNode {
			res := SysCtrl.MetaClient.SendBackupToMeta(i, req.Mod(), req.Param())
			metaRes[m] = res
			continue
		}
		for _, host := range sqlHost {
			if strings.HasPrefix(m, host+":") {
				res := SysCtrl.MetaClient.SendBackupToMeta(i, req.Mod(), req.Param())
				metaRes[m] = res
				break
			}
		}
	}
	for n, s := range metaRes {
		WriteString(resp, fmt.Sprintf("\n\t%v: %s,", n, s))
	}

	return nil
}

func sendCmdToLocalOrAll(req msgservice.SysCtrlRequest, resp *bufio.Writer) error {
	if allNodes, ok := req.Param()["allnodes"]; ok {
		if strings.EqualFold(allNodes, "y") || strings.EqualFold(allNodes, "yes") {
			return broadcastCmdToStore(req, resp)
		}
	}

	if req.LocalBindAddress() == "" && req.LocalDomain() == "" {
		return broadcastCmdToStore(req, resp)
	}

	// Optimized for cloud instances. Only send to the store node of the same container.
	storeHosts, storeDomain := getLocalStoreNodeInfo(req.LocalDomain(), req.LocalBindAddress())
	dataNodes, err := SysCtrl.MetaClient.DataNodes()
	if err != nil {
		return err
	}
	for _, node := range dataNodes {
		if _, ok := storeHosts[node.Host]; ok || node.Host == storeDomain {
			res := sendCmdToStore(req, node.ID, node.Host)
			_, err = resp.WriteString(res)
			return err
		}
	}

	logger.GetLogger().Info("local store node not found, broadcast to all nodes")
	return broadcastCmdToStore(req, resp)
}

var sqlBindIpList []string
var localStoreHosts map[string]struct{}
var localStoreDomain string

func getLocalStoreNodeInfo(sqlDomain, sqlBindAddr string) (map[string]struct{}, string) {
	// Initialized, return directly
	if len(localStoreHosts) > 0 && len(localStoreDomain) > 0 {
		return localStoreHosts, localStoreDomain
	}

	// parse sql node ip
	if len(sqlBindIpList) == 0 {
		addrs := strings.Split(sqlBindAddr, ",")
		sqlBindIpList = make([]string, len(addrs))
		for i, addr := range addrs {
			sqlBindIpList[i] = strings.Split(addr, ":")[0]
		}
	}

	// `sqlstore_sql_node_N` to `sqlstore_store_node_N`
	localStoreDomain = fmt.Sprintf("%s:8400", strings.ReplaceAll(sqlDomain, "_sql_node_", "_store_node_"))
	localStoreHosts = make(map[string]struct{}, len(sqlBindIpList))
	for _, ip := range sqlBindIpList {
		localStoreHosts[fmt.Sprintf("%s:8400", strings.Split(ip, ":")[0])] = struct{}{}
	}
	return localStoreHosts, localStoreDomain
}

func broadcastCmdToStore(req msgservice.SysCtrlRequest, resp *bufio.Writer) error {
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

func sendCmdToStoreAsync(req msgservice.SysCtrlRequest, resp *bufio.Writer, nid uint64, host string, lock *sync.Mutex, wg *sync.WaitGroup) {
	res := sendCmdToStore(req, nid, host)
	lock.Lock()
	WriteString(resp, res)
	lock.Unlock()
	wg.Done()
}

func sendBackupCmdToStoreAsync(req msgservice.SysCtrlRequest, resp *bufio.Writer, nid uint64, host string, lock *sync.Mutex, wg *sync.WaitGroup) {
	res := sendBackupCmdToStore(req, nid, host)
	lock.Lock()
	WriteString(resp, res)
	lock.Unlock()
	wg.Done()
}
func handleLogRowsCmd(req msgservice.SysCtrlRequest) error {
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

func sendCmdToStore(req msgservice.SysCtrlRequest, nid uint64, host string) string {
	var res string
	_, err := SysCtrl.SendSysCtrlOnNode(nid, req)
	if err != nil {
		res = fmt.Sprintf("\n\t%v: failed,%v,", host, err)
	} else {
		res = fmt.Sprintf("\n\t%v: success,", host)
	}
	return res
}

func sendBackupCmdToStore(req msgservice.SysCtrlRequest, nid uint64, host string) string {
	var res string
	result, err := SysCtrl.SendSysCtrlOnNode(nid, req)
	if err != nil {
		res = fmt.Sprintf("\n\t%v: failed,%v,", host, err)
		return res
	}
	if r, err := json.Marshal(result); err == nil {
		res = fmt.Sprintf("\n\t%v: %s", host, r)
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

func WriteString(w *bufio.Writer, content string) {
	_, err := w.WriteString(content)
	if err != nil {
		logger.GetLogger().Error("WriteString error", zap.Error(err))
	}
}

func CheckUpdateAsSkParamValid(params map[string]string) error {
	if _, ok := params["ak"]; !ok {
		return errors.New("missing ak")
	}

	if _, ok := params["sk"]; !ok {
		return errors.New("missing sk")
	}
	return nil
}

func CheckUpdateObsHostNameParamValid(params map[string]string) error {
	if _, ok := params["hostname"]; !ok {
		return errors.New("missing hostName")
	}
	return nil
}

func CheckUpdateStreamConfParamValid(params map[string]string) error {
	if key, ok := params["key"]; !ok || key == "" {
		return errors.New("missing key")
	}

	if val, ok := params["val"]; !ok || val == "" {
		return errors.New("missing val")
	}
	return nil
}

func CheckUpdateParquetTaskParamValid(params map[string]string) error {
	delete(params, "mod")
	validKeys := []string{ParquetLevel, ParquetPageSize, ParquetBatchSize, ParquetGroupLen, ParquetItrSize,
		ParquetDictCompressEnable, ParquetCompressAlg, ParquetEnableMst, "allnodes", ParquetMaxStatsSize}
	checkValid := func(v string) bool {
		for _, validKey := range validKeys {
			if validKey == v {
				return true
			}
		}
		return false
	}

	for k := range params {
		if !checkValid(k) {
			return fmt.Errorf("invalid parquet task param %s", k)
		}
	}
	return nil
}

func CheckSwitchOnParamValid(params map[string]string) (bool, error) {
	switchOn, err := GetBoolValue(params, "switchon")
	if err != nil {
		return false, err
	}
	return switchOn, nil
}

func CheckEnabledParamValid(params map[string]string) (int64, error) {
	enabled, err := GetIntValue(params, "enabled")
	if err != nil {
		return -1, err
	}
	if enabled != 0 && enabled != 1 {
		return -1, fmt.Errorf("invalid enabled:%v", enabled)
	}
	return enabled, nil
}
