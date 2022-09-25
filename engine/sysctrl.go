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

package engine

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/pingcap/failpoint"
	"go.uber.org/zap"
)

/*
 curl -i -XPOST 'http://127.0.0.1:8086/debug/ctrl?mod=flush'
 curl -i -XPOST 'https://127.0.0.1:8086/debug/ctrl?mod=snapshot&flushduration=5m' -k --insecure -u admin:aBeGhKO0Qr2V9YZ~
 curl -i -XPOST 'http://127.0.0.1:8086/debug/ctrl?mod=compen&switchon=true&allshards=true&shid=4'
 curl -i -XPOST 'http://127.0.0.1:8086/debug/ctrl?mod=merge&switchon=true&allshards=true&shid=4'
 curl -i -XPOST 'http://127.0.0.1:8086/debug/ctrl?mod=snapshot&duration=30m'
*/

const (
	dataFlush    = "flush"
	compactionEn = "compen"
	compmerge    = "merge"
	snapshot     = "snapshot"
	Failpoint    = "failpoint"
	Readonly     = "readonly"
)

var (
	ErrNoSuchParam = fmt.Errorf("no parameter find")
)

func (e *Engine) processReq(req *netstorage.SysCtrlRequest) error {
	switch req.Mod() {
	case dataFlush:
		e.ForceFlush()
		return nil
	case compactionEn:
		allEn, err := boolValue(req.Param(), "allshards")
		if err != nil {
			if err != ErrNoSuchParam {
				log.Error("get compaction switchon from param fail", zap.Error(err))
				return err
			}

			en, err := boolValue(req.Param(), "switchon")
			if err != nil {
				log.Error("get compaction switchon from param fail", zap.Error(err))
				return err
			}
			shid, err := intValue(req.Param(), "shid")
			if err != nil {
				log.Error("get shard id  from param fail", zap.Error(err))
				return err
			}
			compWorker.ShardCompactionSwitch(uint64(shid), en)
			log.Info("set shard compaction switch", zap.Bool("switch", allEn), zap.Int64("shid", shid))
		} else {
			compWorker.SetAllShardsCompactionSwitch(allEn)
			log.Info("set all shard compaction switch", zap.Bool("switch", allEn))
		}
		return nil
	case compmerge:
		allEn, err := boolValue(req.Param(), "allshards")
		if err != nil {
			if err != ErrNoSuchParam {
				log.Error("get compaction switchon from param fail", zap.Error(err))
				return err
			}
			en, err := boolValue(req.Param(), "switchon")
			if err != nil {
				log.Error("get compaction switchon from param fail", zap.Error(err))
				return err
			}
			shid, err := intValue(req.Param(), "shid")
			if err != nil {
				log.Error("get shard id  from param fail", zap.Error(err))
				return err
			}
			compWorker.ShardOutOfOrderMergeSwitch(uint64(shid), en)
			log.Info("set shard merge switch", zap.Bool("switch", allEn), zap.Int64("shid", shid))
		} else {
			compWorker.SetAllOutOfOrderMergeSwitch(allEn)
			log.Info("set all shard merge switch", zap.Bool("switch", allEn))
		}
		return nil
	case snapshot:
		d, err := durationToInt(req.Param(), "duration")
		if err != nil {
			log.Error("get shard snapshot duration from param fail", zap.Error(err))
			return err
		}
		compWorker.SetSnapshotColdDuration(d)
		log.Info("set shard snapshot duration", zap.Duration("duration", d))
		return nil
	case Failpoint:
		err := handleFailpoint(req)
		if err != nil {
			return err
		}
		log.Info("failpoint switch ok", zap.String("switchon", req.Param()["switchon"]))
		return nil
	case Readonly:
		return e.handleReadonly(req)
	default:
		return fmt.Errorf("unknown sys cmd %v", req.Mod())
	}
}

func handleFailpoint(req *netstorage.SysCtrlRequest) error {
	switchon, err := boolValue(req.Param(), "switchon")
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

func (e *Engine) handleReadonly(req *netstorage.SysCtrlRequest) error {
	switchon, err := boolValue(req.Param(), "switchon")
	if err != nil {
		log.Error("get switchon from param fail", zap.Error(err))
		return err
	}
	e.mu.Lock()
	e.ReadOnly = switchon
	e.mu.Unlock()
	log.Info("readonly status switch ok", zap.String("switchon", req.Param()["switchon"]))
	return nil
}

func intValue(param map[string]string, key string) (int64, error) {
	str, ok := param[key]
	if !ok {
		return 0, fmt.Errorf("no %v in parameter", key)
	}

	n, err := strconv.ParseUint(str, 10, strconv.IntSize)
	if err != nil {
		return 0, err
	}
	return int64(n), nil
}

func boolValue(param map[string]string, key string) (bool, error) {
	switchStr, ok := param[key]
	if !ok {
		return false, ErrNoSuchParam
	}
	return strconv.ParseBool(switchStr)
}

func durationToInt(param map[string]string, key string) (time.Duration, error) {
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
