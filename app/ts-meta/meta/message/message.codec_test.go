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

package message_test

import (
	"path/filepath"
	"runtime"
	"testing"

	"github.com/openGemini/openGemini/app/ts-meta/meta/message"
	"github.com/openGemini/openGemini/lib/codec/gen"
)

func __TestMetaMessage(t *testing.T) {
	var objs []interface{}
	pingRequest := message.PingRequest{
		All: 1,
	}
	pingResponse := message.PingResponse{
		Leader: []byte("127.0.0.1"),
		Err:    "err",
	}
	objs = append(objs, pingRequest, pingResponse)

	peersResponse := message.PeersResponse{
		Peers:      []string{"a", "b"},
		Err:        "err",
		StatusCode: 500,
	}
	objs = append(objs, peersResponse)

	createNodeRequest := message.CreateNodeRequest{
		WriteHost: "127.0.0.1",
		QueryHost: "127.0.0.1",
	}
	createNodeResponse := message.CreateNodeResponse{
		Data: []byte{1, 2, 3},
		Err:  "err",
	}
	objs = append(objs, createNodeRequest, createNodeResponse)

	userSnapshotRequest := message.UserSnapshotRequest{
		Index: 1,
	}
	userSnapshotResponse := message.UserSnapshotResponse{
		Err: "err",
	}
	objs = append(objs, userSnapshotRequest, userSnapshotResponse)

	snapshotRequest := message.SnapshotRequest{
		Index: 1,
	}
	snapshotResponse := message.SnapshotResponse{
		Data: []byte{1, 2, 3},
		Err:  "err",
	}
	objs = append(objs, snapshotRequest, snapshotResponse)

	updateRequest := message.UpdateRequest{
		Body: []byte{1, 2, 3},
	}
	updateResponse := message.UpdateResponse{
		Data:       []byte{1, 2, 3},
		Location:   "127.0.0.1",
		Err:        "err",
		StatusCode: 500,
	}
	objs = append(objs, updateRequest, updateResponse)

	executeRequest := message.ExecuteRequest{
		Body: []byte{1, 2, 3},
	}
	executeResponse := message.ExecuteResponse{
		Index:      11,
		Err:        "err",
		ErrCommand: "400 error",
	}
	objs = append(objs, executeRequest, executeResponse)

	reportRequest := message.ReportRequest{
		Body: []byte{1, 2, 3},
	}
	reportResponse := message.ReportResponse{
		Index:      11,
		Err:        "err",
		ErrCommand: "400 error",
	}
	objs = append(objs, reportRequest, reportResponse)

	getShardInfoRequest := message.GetShardInfoRequest{
		Body: []byte{1, 2, 3},
	}
	getShardInfoResponse := message.GetShardInfoResponse{
		Data: []byte{1, 2, 3},
		Err:  "err",
	}
	objs = append(objs, getShardInfoRequest, getShardInfoResponse)
	g := gen.NewCodecGen("message")
	for _, obj := range objs {
		g.Gen(obj)

		_, f, _, _ := runtime.Caller(0)
		g.SaveTo(filepath.Dir(f) + "/message.codec.gen.go")
	}
}
