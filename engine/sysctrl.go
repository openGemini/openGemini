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

package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/openGemini/openGemini/lib/backup"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/memory"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/msgservice"
	"github.com/openGemini/openGemini/lib/sysconfig"
	"github.com/openGemini/openGemini/lib/syscontrol"
	"github.com/openGemini/openGemini/services/stream"
	"github.com/pingcap/failpoint"
	"go.uber.org/zap"
)

/*
 curl -i -XPOST 'http://127.0.0.1:8086/debug/ctrl?mod=flush'
 curl -i -XPOST 'https://127.0.0.1:8086/debug/ctrl?mod=snapshot&flushduration=5m' -k --insecure -u admin:aBeGhKO0Qr2V9YZ~
 curl -i -XPOST 'http://127.0.0.1:8086/debug/ctrl?mod=compen&switchon=true&allshards=true&shid=4'
 curl -i -XPOST 'http://127.0.0.1:8086/debug/ctrl?mod=merge&switchon=true&allshards=true&shid=4'
 curl -i -XPOST 'http://127.0.0.1:8086/debug/ctrl?mod=snapshot&duration=30m'
 curl -i -XPOST 'http://127.0.0.1:8086/debug/ctrl?mod=downsample_in_order&order=true'
 curl -i -XPOST 'http://127.0.0.1:8086/debug/ctrl?mod=verifynode&switchon=false'
 curl -i -XPOST 'http://127.0.0.1:8086/debug/ctrl?mod=memusagelimit&limit=85'
*/

const (
	queryShardStatus = "queryShardStatus"

	dataFlush             = "flush"
	compactionEn          = "compen"
	compmerge             = "merge"
	snapshot              = "snapshot"
	downSampleInOrder     = "downsample_in_order"
	Failpoint             = "failpoint"
	verifyNode            = "verifynode"
	memUsageLimit         = "memusagelimit"
	BackgroundReadLimiter = "backgroundReadLimiter"
)

var (
	memUsageLimitSize int32 = 100
)

func getReqParam(req *msgservice.SysCtrlRequest) (int64, bool, error) {
	en, err := syscontrol.GetBoolValue(req.Param(), "switchon")
	if err != nil {
		log.Error("get switch from param fail", zap.Error(err))
		return 0, false, err
	}
	shardId, err := syscontrol.GetIntValue(req.Param(), "shid")
	if err != nil {
		log.Error("get shard id  from param fail", zap.Error(err))
		return 0, false, err
	}
	return shardId, en, nil
}

func (e *EngineImpl) processReq(req *msgservice.SysCtrlRequest) (map[string]string, error) {
	if req.Mod() == queryShardStatus {
		return e.getShardStatus(req.Param())
	}

	switch req.Mod() {
	case dataFlush:
		e.ForceFlush()
		return nil, nil
	case compactionEn:
		allEn, err := syscontrol.GetBoolValue(req.Param(), "allshards")
		if err != nil && err != syscontrol.ErrNoSuchParam {
			log.Error("get compaction switchon from param fail", zap.Error(err))
			return nil, err
		}

		if err == nil {
			compWorker.SetAllShardsCompactionSwitch(allEn)
			log.Info("set all shard compaction switch", zap.Bool("switch", allEn))
			return nil, nil
		}

		shardId, en, err := getReqParam(req)
		if err != nil {
			return nil, err
		}

		compWorker.ShardCompactionSwitch(uint64(shardId), en)
		log.Info("set shard compaction switch", zap.Bool("switch", allEn), zap.Int64("shardId", shardId))
		return nil, nil
	case compmerge:
		allEn, err := syscontrol.GetBoolValue(req.Param(), "allshards")
		if err != nil && err != syscontrol.ErrNoSuchParam {
			log.Error("get merge switchon from param fail", zap.Error(err))
			return nil, err

		}
		if err == nil {
			compWorker.SetAllOutOfOrderMergeSwitch(allEn)
			log.Info("set all shard merge switch", zap.Bool("switch", allEn))
			return nil, nil
		}

		shardId, en, err := getReqParam(req)
		if err != nil {
			return nil, err
		}

		compWorker.ShardOutOfOrderMergeSwitch(uint64(shardId), en)
		log.Info("set shard merge switch", zap.Bool("switch", allEn), zap.Int64("shid", shardId))
		return nil, nil
	case snapshot:
		d, err := syscontrol.GetDurationValue(req.Param(), "duration")
		if err != nil {
			log.Error("get shard snapshot duration from param fail", zap.Error(err))
			return nil, err
		}
		compWorker.SetSnapshotColdDuration(d)
		log.Info("set shard snapshot duration", zap.Duration("duration", d))
		return nil, nil
	case Failpoint:
		err := handleFailpoint(req)
		if err != nil {
			return nil, err
		}
		log.Info("failpoint switch ok", zap.String("switchon", req.Param()["switchon"]))
		return nil, nil
	case downSampleInOrder:
		order, err := syscontrol.GetBoolValue(req.Param(), "order")
		if err != nil {
			log.Error("get downsample order from param fail", zap.Error(err))
			return nil, err
		}
		downSampleInorder = order
		return nil, nil
	case verifyNode:
		en, err := syscontrol.GetBoolValue(req.Param(), "switchon")
		if err != nil {
			log.Error("get verify switch from param fail", zap.Error(err))
			return nil, err
		}
		metaclient.VerifyNodeEn = en
		log.Info("set verify switch ok", zap.String("switchon", req.Param()["switchon"]))
		return nil, nil
	case memUsageLimit:
		limit, err := syscontrol.GetIntValue(req.Param(), "limit")
		if err != nil {
			return nil, err
		}
		setMemUsageLimit(int32(limit))
		return nil, nil
	case BackgroundReadLimiter:
		limit, err := syscontrol.GetBytesValue(req.Param(), "limit")
		if err != nil {
			return nil, err
		}
		if limit <= 0 {
			log.Error("set background read limiter failed, limit<=0", zap.Int64("limit", limit))
		}
		fileops.SetBackgroundReadLimiter(int(limit))
		return nil, nil
	case syscontrol.NodeInterruptQuery:
		switchOn, err := syscontrol.GetBoolValue(req.Param(), "switchon")
		if err != nil {
			return nil, err
		}
		sysconfig.SetInterruptQuery(switchOn)
		return nil, nil
	case syscontrol.UpperMemUsePct:
		upper, err := syscontrol.GetIntValue(req.Param(), "limit")
		if err != nil {
			return nil, err
		}
		sysconfig.SetUpperMemPct(upper)
		return nil, nil
	case syscontrol.Backup:
		e.mu.Lock()
		if e.backup != nil && e.backup.Status == InProgress {
			err := fmt.Errorf("node is backing up")
			log.Error("run backup error", zap.Error(err))
			e.mu.Unlock()
			return nil, err
		}
		e.backup = &Backup{}
		e.mu.Unlock()
		go e.processBackup(req)
		return nil, nil

	case syscontrol.AbortBackup:
		if e.backup == nil {
			return nil, nil
		}
		e.backup.IsAborted = true
		return nil, nil
	case syscontrol.BackupStatus:
		res := map[string]string{"status": string(NotBackedUp)}
		if e.backup != nil {
			res["status"] = string(e.backup.Status)
		}

		return res, nil
	case syscontrol.WriteStreamPointsEnable:
		switchOn, err := syscontrol.GetBoolValue(req.Param(), "switchon")
		if err != nil {
			return nil, err
		}
		stream.SetWriteStreamPointsEnabled(switchOn)
		return nil, nil
	default:
		return nil, fmt.Errorf("unknown sys cmd %v", req.Mod())
	}
}

func (e *EngineImpl) processBackup(req *msgservice.SysCtrlRequest) {
	params := req.Param()
	backupPath := params[backup.BackupPath]
	if backupPath == "" {
		log.Error("backup: invalid parameter", zap.String("backupPath", backupPath))
	}
	if _, err := os.Stat(filepath.Join(backupPath, backup.DataBackupDir)); err == nil {
		return
	}
	DB := make([]string, 0)
	isRemote := params[backup.IsRemote] == "true"
	isInc := params[backup.IsInc] == "true"
	onlyBackupMater := params[backup.OnlyBackupMaster] == "true"
	if params[backup.DataBases] != "" {
		DB = strings.Split(params[backup.DataBases], ",")
	}

	e.backup = &Backup{
		IsInc:           isInc,
		IsRemote:        isRemote,
		BackupPath:      backupPath,
		OnlyBackupMater: onlyBackupMater,
		Engine:          e,
		Status:          InProgress,
		DataBases:       DB,
	}

	if err := e.backup.RunBackupData(); err != nil {
		log.Error("run backup error", zap.Error(err))
	}
}

func handleFailpoint(req *msgservice.SysCtrlRequest) error {
	switchon, err := syscontrol.GetBoolValue(req.Param(), "switchon")
	if err != nil {
		log.Error("get switchon from param fail", zap.Error(err))
		return err
	}

	point, ok := req.Param()["point"]
	if !ok {
		log.Error("get point from param fail", zap.Error(err))
		return err
	}
	if !switchon {
		err = failpoint.Disable(point)
		if err != nil {
			log.Error("disable failpoint fail", zap.Error(err))
			return err
		}
		return nil
	}
	term, ok := req.Param()["term"]
	if !ok {
		log.Error("get term from param fail", zap.Error(err))
		return err
	}
	err = failpoint.Enable(point, term)
	if err != nil {
		log.Error("enable failpoint fail", zap.Error(err))
		return err
	}
	return nil
}

func setMemUsageLimit(limit int32) {
	atomic.StoreInt32(&memUsageLimitSize, limit)
	fmt.Println(time.Now().Format(time.RFC3339Nano), "memUsageLimit:", limit)
}

func GetMemUsageLimit() int32 {
	return atomic.LoadInt32(&memUsageLimitSize)
}

func IsMemUsageExceeded() bool {
	memLimitPct := GetMemUsageLimit()
	if memLimitPct < 1 || memLimitPct >= 100 {
		return false
	}
	memUsedPct := memory.GetMemMonitor().MemUsedPct()
	exceeded := memUsedPct > float64(memLimitPct)
	log.Info("system mem usage", zap.Float64("memUsedPct", memUsedPct), zap.Float64("memLimitPct", float64(memLimitPct)), zap.Bool("exceeded", exceeded))
	return exceeded
}
