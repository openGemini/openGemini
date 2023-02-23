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

	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/syscontrol"
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
*/

const (
	dataFlush         = "flush"
	compactionEn      = "compen"
	compmerge         = "merge"
	snapshot          = "snapshot"
	downSampleInOrder = "downsample_in_order"
	Failpoint         = "failpoint"
	Readonly          = "readonly"
	verifyNode        = "verifynode"
)

func getReqParam(req *netstorage.SysCtrlRequest) (error, int64, bool) {
	en, err := syscontrol.GetBoolValue(req.Param(), "switchon")
	if err != nil {
		log.Error("get switch from param fail", zap.Error(err))
		return err, 0, false
	}
	shardId, err := syscontrol.GetIntValue(req.Param(), "shid")
	if err != nil {
		log.Error("get shard id  from param fail", zap.Error(err))
		return err, 0, false
	}
	return nil, shardId, en
}

func (e *Engine) processReq(req *netstorage.SysCtrlRequest) error {
	switch req.Mod() {
	case dataFlush:
		e.ForceFlush()
		return nil
	case compactionEn:
		allEn, err := syscontrol.GetBoolValue(req.Param(), "allshards")
		if err != nil && err != syscontrol.ErrNoSuchParam {
			log.Error("get compaction switchon from param fail", zap.Error(err))
			return err
		}

		if err == nil {
			compWorker.SetAllShardsCompactionSwitch(allEn)
			log.Info("set all shard compaction switch", zap.Bool("switch", allEn))
			return nil
		}

		err, shardId, en := getReqParam(req)
		if err != nil {
			return err
		}

		compWorker.ShardCompactionSwitch(uint64(shardId), en)
		log.Info("set shard compaction switch", zap.Bool("switch", allEn), zap.Int64("shardId", shardId))
		return nil
	case compmerge:
		allEn, err := syscontrol.GetBoolValue(req.Param(), "allshards")
		if err != nil && err != syscontrol.ErrNoSuchParam {
			log.Error("get merge switchon from param fail", zap.Error(err))
			return err

		}
		if err == nil {
			compWorker.SetAllOutOfOrderMergeSwitch(allEn)
			log.Info("set all shard merge switch", zap.Bool("switch", allEn))
			return nil
		}

		err, shardId, en := getReqParam(req)
		if err != nil {
			return err
		}

		compWorker.ShardOutOfOrderMergeSwitch(uint64(shardId), en)
		log.Info("set shard merge switch", zap.Bool("switch", allEn), zap.Int64("shid", shardId))
		return nil
	case snapshot:
		d, err := syscontrol.GetDurationValue(req.Param(), "duration")
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
	case downSampleInOrder:
		order, err := syscontrol.GetBoolValue(req.Param(), "order")
		if err != nil {
			log.Error("get downsample order from param fail", zap.Error(err))
			return err
		}
		downSampleInorder = order
		return nil
	case verifyNode:
		en, err := syscontrol.GetBoolValue(req.Param(), "switchon")
		if err != nil {
			log.Error("get verify switch from param fail", zap.Error(err))
			return err
		}
		metaclient.VerifyNodeEn = en
		log.Info("set verify switch ok", zap.String("switchon", req.Param()["switchon"]))
		return nil
	default:
		return fmt.Errorf("unknown sys cmd %v", req.Mod())
	}
}

func handleFailpoint(req *netstorage.SysCtrlRequest) error {
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

func (e *Engine) handleReadonly(req *netstorage.SysCtrlRequest) error {
	switchon, err := syscontrol.GetBoolValue(req.Param(), "switchon")
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
