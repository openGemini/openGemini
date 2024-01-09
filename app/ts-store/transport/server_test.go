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

package transport

import (
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	numenc "github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/openGemini/openGemini/app/ts-store/storage"
	"github.com/openGemini/openGemini/app/ts-store/stream"
	"github.com/openGemini/openGemini/engine/executor/spdy"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/open_src/influx/meta"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServer(t *testing.T) {
	s := NewServer("127.0.0.2:18100", "127.0.0.2:18101")
	if !assert.NoError(t, s.Open()) {
		return
	}
	defer s.MustClose()

	s2 := NewServer("127.0.0.2:18100", "127.0.0.21:18101")
	err := s2.Open()
	if !assert.NotEmpty(t, err) || !assert.Regexp(t, regexp.MustCompile("^cannot create a server"), err.Error()) {
		return
	}

	s3 := NewServer("127.0.0.21:18100", "127.0.0.2:18101")
	err = s3.Open()
	if !assert.NotEmpty(t, err) || !assert.Regexp(t, regexp.MustCompile("^cannot create a server"), err.Error()) {
		return
	}

	s.Run(nil, nil)
	time.Sleep(time.Second)
}

func mockRows() []influx.Row {
	keys := []string{
		"mst0,tk1=value1,tk2=value2,tk3=value3 f1=value1,f2=value2",
		"mst0,tk1=value11,tk2=value22,tk3=value33 f1=value1,f2=value2",
	}
	pts := make([]influx.Row, 0, len(keys))
	for _, key := range keys {
		pt := influx.Row{}
		splited := strings.Split(key, " ")
		strs := strings.Split(splited[0], ",")
		pt.Name = strs[0]
		pt.Tags = make(influx.PointTags, len(strs)-1)
		for i, str := range strs[1:] {
			kv := strings.Split(str, "=")
			pt.Tags[i].Key = kv[0]
			pt.Tags[i].Value = kv[1]
		}
		sort.Sort(&pt.Tags)
		fields := strings.Split(splited[1], ",")
		pt.Fields = make(influx.Fields, len(fields))
		for i, str := range fields {
			kv := strings.Split(str, "=")
			pt.Fields[i].Key = kv[0]
			pt.Fields[i].Type = influx.Field_Type_String
			pt.Fields[i].StrValue = kv[1]
		}
		sort.Sort(&pt.Fields)
		pt.Timestamp = time.Now().UnixNano()
		pt.UnmarshalIndexKeys(nil)
		pt.ShardKey = pt.IndexKey
		pts = append(pts, pt)
	}
	return pts
}

func mockMarshaledPoint(cut int) []byte {
	if cut == 0 {
		return nil
	}
	pBuf := make([]byte, 0)
	pBuf = append(pBuf[:0], netstorage.PackageTypeFast)
	if cut == 1 {
		pBuf = append(pBuf[:0], 9)
	}
	// db
	db := "db0"
	pBuf = append(pBuf, uint8(len(db)))
	if cut == 2 {
		return pBuf
	}
	pBuf = append(pBuf, db...)
	// rp
	rp := "rp0"
	pBuf = append(pBuf, uint8(len(rp)))
	if cut == 3 {
		return pBuf
	}
	pBuf = append(pBuf, rp...)
	// ptid
	pt := uint32(0)
	pBuf = numenc.MarshalUint32(pBuf, pt)
	shard := uint64(0)
	pBuf = numenc.MarshalUint64(pBuf, shard)

	rows := mockRows()
	if cut == 4 {
		return pBuf
	}
	pBuf, err := influx.FastMarshalMultiRows(pBuf, rows)
	if err != nil {
		panic(err)
	}
	return pBuf
}

func TestWritePointsWork_decodePoints(t *testing.T) {
	type TestCase struct {
		Name      string
		reqBuf    []byte
		expectMsg string
	}
	var testCases = []TestCase{
		{
			Name:      "loss of points data",
			reqBuf:    mockMarshaledPoint(0),
			expectMsg: "invalid points buffer",
		},
		{
			Name:      "fast marshal header error",
			reqBuf:    mockMarshaledPoint(1),
			expectMsg: "not a fast marshal points package",
		},
		{
			Name:      "buff no db error",
			reqBuf:    mockMarshaledPoint(2),
			expectMsg: "no data for db name",
		},
		{
			Name:      "buff no rp error",
			reqBuf:    mockMarshaledPoint(3),
			expectMsg: "no data for rp name",
		},
		{
			Name:      "buff no point error",
			reqBuf:    mockMarshaledPoint(4),
			expectMsg: "no data for points data",
		},
	}

	for _, tt := range testCases {
		t.Run(tt.Name, func(t *testing.T) {
			var err error
			ww := GetWritePointsWork()
			ww.reqBuf = tt.reqBuf
			_, _, _, _, _, _, err = ww.decodePoints()
			require.Equal(t, err.Error(), tt.expectMsg)
		})
	}
}

var storageDataPath = "/tmp/data/"
var metaPath = "/tmp/meta"

func mockStorage() *storage.Storage {
	node := metaclient.NewNode(metaPath)
	storeConfig := config.NewStore()
	config.SetHaPolicy(config.SSPolicy)
	monitorConfig := config.Monitor{
		Pushers:      "http",
		StoreEnabled: true,
	}
	conf := &config.TSStore{
		Data:    storeConfig,
		Monitor: monitorConfig,
		Common:  config.NewCommon(),
		Meta:    config.NewMeta(),
	}

	store, err := storage.OpenStorage(storageDataPath, node, nil, conf)
	if err != nil {
		return nil
	}
	return store
}

type MockStream struct{}

func (m MockStream) WriteRows(db, rp string, ptId uint32, shardID uint64, streamIdDstShardIdMap map[uint64]uint64, ww stream.WritePointsWorkIF) {
	panic("implement me")
}

func (m MockStream) RegisterTask(info *meta.StreamInfo, fieldCalls []*stream.FieldCall) error {
	panic("implement me")
}

func (m MockStream) Drain() {
	panic("implement me")
}

func (m MockStream) DeleteTask(id uint64) {
	panic("implement me")
}

func (m MockStream) Run() {
	panic("implement me")
}

func (m MockStream) Close() {
	panic("implement me")
}

func mockStream() stream.Engine {
	return &MockStream{}
}

type MockNewResponser struct {
}

func (m MockNewResponser) Encode(bytes []byte, i interface{}) ([]byte, error) {
	panic("implement me")
}

func (m MockNewResponser) Decode(bytes []byte) (interface{}, error) {
	panic("implement me")
}

func (m MockNewResponser) Response(i interface{}, b bool) error {
	return nil
}

func (m MockNewResponser) Callback(i interface{}) error {
	panic("implement me")
}

func (m MockNewResponser) Apply() error {
	panic("implement me")
}

func (m MockNewResponser) Type() uint8 {
	panic("implement me")
}

func (m MockNewResponser) Session() *spdy.MultiplexedSession {
	panic("implement me")
}

func (m MockNewResponser) Sequence() uint64 {
	panic("implement me")
}

func (m MockNewResponser) StartAnalyze(span *tracing.Span) {
	panic("implement me")
}

func (m MockNewResponser) FinishAnalyze() {
	panic("implement me")
}

func TestInsertProcessor(t *testing.T) {
	store := mockStorage()
	defer store.MustClose()
	stream := mockStream()
	processor := NewInsertProcessor(store, stream)
	w := &MockNewResponser{}
	req1 := netstorage.NewWritePointsRequest([]byte{1, 2, 3, 4, 5, 6, 7})
	if err := processor.Handle(w, req1); err != nil {
		t.Fatal("WritePointsRequest failed")
	}

	req2 := netstorage.NewWriteStreamPointsRequest(mockMarshaledStreamPoint(true, true),
		[]*netstorage.StreamVar{{Only: false, Id: []uint64{1}}, {Only: false, Id: []uint64{2}}})
	if err := processor.Handle(w, req2); err != nil {
		t.Fatal("WritePointsRequest failed")
	}

	req3 := netstorage.NewWriteStreamPointsRequest(mockMarshaledStreamPoint(true, false),
		[]*netstorage.StreamVar{{Only: false, Id: []uint64{1}}, {Only: false, Id: []uint64{2}}})
	if err := processor.Handle(w, req3); err != nil {
		t.Fatal("WritePointsRequest failed")
	}

	req4 := netstorage.NewWriteStreamPointsRequest(mockMarshaledStreamPoint(true, false),
		[]*netstorage.StreamVar{{Only: false, Id: []uint64{1}}, {Only: false, Id: []uint64{2}}})
	if err := processor.Handle(w, req4); err != nil {
		t.Fatal("WritePointsRequest failed")
	}
}

func mockMarshaledStreamPoint(haveStreamShardList bool, validStreamShardList bool) []byte {
	pBuf := make([]byte, 0)
	pBuf = append(pBuf[:0], netstorage.PackageTypeFast)
	// db
	db := "db0"
	pBuf = append(pBuf, uint8(len(db)))
	pBuf = append(pBuf, db...)
	// rp
	rp := "rp0"
	pBuf = append(pBuf, uint8(len(rp)))
	pBuf = append(pBuf, rp...)
	// ptid
	pt := uint32(0)
	pBuf = numenc.MarshalUint32(pBuf, pt)
	// shard
	shard := uint64(0)
	pBuf = numenc.MarshalUint64(pBuf, shard)
	if haveStreamShardList {
		// streamShardIdList
		var streamShardIdList []uint64
		if validStreamShardList {
			streamShardIdList = []uint64{0}
		} else {
			streamShardIdList = []uint64{0, 1}
		}
		pBuf = numenc.MarshalUint32(pBuf, uint32(len(streamShardIdList)))
		pBuf = numenc.MarshalVarUint64s(pBuf, streamShardIdList)
	}

	// rows
	rows := mockRows()
	pBuf, err := influx.FastMarshalMultiRows(pBuf, rows)
	if err != nil {
		panic(err)
	}
	return pBuf
}

func TestDecodePoints(t *testing.T) {
	ww := GetWritePointsWork()
	ww.reqBuf = mockMarshaledStreamPoint(true, true)
	ww.streamVars = []*netstorage.StreamVar{{Only: false, Id: []uint64{0}}, {Only: true, Id: []uint64{1}}}
	_, _, _, _, _, _, err := ww.decodePoints()
	if err != nil {
		t.Fatal("DecodePoints failed")
	}

	ww.streamVars = []*netstorage.StreamVar{{Only: false, Id: []uint64{0}}}
	_, _, _, _, _, _, err = ww.decodePoints()
	if err == nil {
		t.Fatal("DecodePoints failed")
	}
	if !strings.Contains(err.Error(), "unmarshal rows failed, the num of the rows is not equal to the stream vars") {
		t.Fatal("DecodePoints failed")
	}

	rows := ww.GetRows()
	ww.SetRows(rows)
	ww.PutWritePointsWork()
}
