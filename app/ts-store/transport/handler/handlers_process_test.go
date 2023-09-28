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

package handler

import (
	"fmt"
	"path"
	"testing"

	"github.com/openGemini/openGemini/app/ts-store/storage"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/netstorage"
	internal "github.com/openGemini/openGemini/lib/netstorage/data"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/stretchr/testify/assert"
)

func TestProcessDDL(t *testing.T) {
	condition := "tk1=tv1"
	errStr := processDDL(&condition, func(expr influxql.Expr, tr influxql.TimeRange) error {
		return errno.NewError(errno.PtNotFound)
	})

	err := netstorage.NormalizeError(errStr)
	assert.Equal(t, true, errno.Equal(err, errno.PtNotFound))

	errStr = processDDL(&condition, func(expr influxql.Expr, tr influxql.TimeRange) error {
		return fmt.Errorf("shard not found")
	})

	err = netstorage.NormalizeError(errStr)
	assert.Equal(t, true, err.Error() == "shard not found")

	errStr = processDDL(&condition, func(expr influxql.Expr, tr influxql.TimeRange) error {
		return nil
	})

	err = netstorage.NormalizeError(errStr)
	assert.Equal(t, true, err == nil)
}

func TestProcessShowTagValues(t *testing.T) {
	db := path.Join(dataPath, "db0")
	pts := []uint32{1}

	h := newHandler(netstorage.ShowTagValuesRequestMessage)
	if err := h.SetMessage(&netstorage.ShowTagValuesRequest{
		ShowTagValuesRequest: internal.ShowTagValuesRequest{
			Db:    &db,
			PtIDs: pts,
		},
	}); err != nil {
		t.Fatal(err)
	}

	s := &storage.Storage{}
	s.SetEngine(&MockEngine{})
	h.SetStore(s)
	rsp, _ := h.Process()
	response, ok := rsp.(*netstorage.ShowTagValuesResponse)
	if !ok {
		t.Fatal("response type is invalid")
	}
	assert.NotNil(t, response.GetErr())
	response.Err = nil
}

type MockEngine struct {
	netstorage.Engine
}

func (e *MockEngine) SeriesKeys(_ string, _ []uint32, _ [][]byte, _ influxql.Expr, _ influxql.TimeRange) ([]string, error) {
	return nil, nil
}

func (e *MockEngine) GetShardSplitPoints(_ string, _ uint32, _ uint64, _ []int64) ([]string, error) {
	return nil, nil
}

func TestProcessGetShardSplitPoints(t *testing.T) {
	db := path.Join(dataPath, "db0")
	pt := uint32(1)

	h := newHandler(netstorage.GetShardSplitPointsRequestMessage)
	if err := h.SetMessage(&netstorage.GetShardSplitPointsRequest{
		GetShardSplitPointsRequest: internal.GetShardSplitPointsRequest{
			DB:   &db,
			PtID: &pt,
		},
	}); err != nil {
		t.Fatal(err)
	}

	s := &storage.Storage{}
	s.SetEngine(&MockEngine{})
	h.SetStore(s)

	rsp, _ := h.Process()
	response, ok := rsp.(*netstorage.GetShardSplitPointsResponse)
	if !ok {
		t.Fatal("response type is invalid")
	}
	assert.NotNil(t, response.GetErr())
	response.Err = nil
}

func TestProcessSeriesKeys(t *testing.T) {
	db := path.Join(dataPath, "db0")
	pts := []uint32{1}
	ms := []string{"cpu"}

	h := newHandler(netstorage.SeriesKeysRequestMessage)
	if err := h.SetMessage(&netstorage.SeriesKeysRequest{
		SeriesKeysRequest: internal.SeriesKeysRequest{
			Db:           &db,
			PtIDs:        pts,
			Measurements: ms,
		},
	}); err != nil {
		t.Fatal(err)
	}

	s := &storage.Storage{}
	s.SetEngine(&MockEngine{})
	h.SetStore(s)

	rsp, _ := h.Process()
	response, ok := rsp.(*netstorage.SeriesKeysResponse)
	if !ok {
		t.Fatal("response type is invalid")
	}
	assert.NotNil(t, response.GetErr())
	response.Err = nil
}
