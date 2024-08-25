/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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
package app_test

import (
	"testing"
	"time"

	"github.com/openGemini/openGemini/app"
	handler2 "github.com/openGemini/openGemini/app/ts-store/transport/handler"
	"github.com/openGemini/openGemini/app/ts-store/transport/query"
	"github.com/openGemini/openGemini/engine/executor"
	query2 "github.com/openGemini/openGemini/lib/util/lifted/influx/query"
)

func TestProactiveManager_ServiceStartAndClose(t *testing.T) {
	obj := app.NewProactiveManager()
	if obj.Open() != nil {
		t.Error("TestProactiveManager_ServiceStartAndClose error")
	}
	if obj.Open() == nil {
		t.Error("TestProactiveManager_ServiceStartAndClose error")
	}
	obj.SetInspectInterval(3 * time.Second)
	time.Sleep(1 * time.Second)
	if obj.Close() != nil {
		t.Error("TestProactiveManager_ServiceStartAndClose error")
	}
	if obj.Close() == nil {
		t.Error("TestProactiveManager_ServiceStartAndClose error")
	}
}

func TestProactiveManager_ServiceKillQuery(t *testing.T) {
	qm := query.NewManager(1)
	qm.SetAbortedExpire(time.Second * 2)
	opt := query2.ProcessorOptions{
		QueryId: 1,
		Query:   "select * from mst",
	}
	req := executor.RemoteQuery{
		Opt:      opt,
		Database: "db0",
	}
	s := handler2.NewSelect(nil, nil, &req)
	qm.Add(1, s)
	obj := app.NewProactiveManager()
	qryLst := obj.GetQueryList(1)
	for _, id := range qryLst {
		obj.KillQuery(id)
	}
	time.Sleep(3 * time.Second)
	qm.Finish(1, s)
	val := qm.Get(1)
	if len(val) != 0 {
		t.Error("TestProactiveManager_ServiceKillQuery fail")
	}
}
