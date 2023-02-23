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
	"path"
	"testing"

	"github.com/openGemini/openGemini/app/ts-store/storage"
	"github.com/openGemini/openGemini/lib/codec"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/netstorage"
	internal "github.com/openGemini/openGemini/lib/netstorage/data"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const dataPath = "/tmp/test_data"

type handlerItem struct {
	typ uint8
	msg codec.BinaryCodec
}

type mockCodec struct {
}

func (r *mockCodec) MarshalBinary() ([]byte, error) {
	return []byte{1}, nil
}

func (r *mockCodec) UnmarshalBinary(buf []byte) error {
	return nil
}

func TestBaseHandler_SetMessage(t *testing.T) {
	var items = []handlerItem{
		{
			typ: netstorage.SeriesKeysRequestMessage,
			msg: &netstorage.SeriesKeysRequest{},
		},
		{
			typ: netstorage.SeriesExactCardinalityRequestMessage,
			msg: &netstorage.SeriesExactCardinalityRequest{},
		},
		{
			typ: netstorage.SeriesCardinalityRequestMessage,
			msg: &netstorage.SeriesCardinalityRequest{},
		},
		{
			typ: netstorage.ShowTagValuesRequestMessage,
			msg: &netstorage.ShowTagValuesRequest{},
		},
		{
			typ: netstorage.ShowTagValuesCardinalityRequestMessage,
			msg: &netstorage.ShowTagValuesCardinalityRequest{},
		},
		{
			typ: netstorage.GetShardSplitPointsRequestMessage,
			msg: &netstorage.GetShardSplitPointsRequest{},
		},
		{
			typ: netstorage.DeleteRequestMessage,
			msg: &netstorage.DeleteRequest{},
		},
		{
			typ: netstorage.CreateDataBaseRequestMessage,
			msg: &netstorage.CreateDataBaseRequest{},
		},
	}

	for _, item := range items {
		h := newHandler(item.typ)
		require.NoError(t, h.SetMessage(item.msg))
		require.NotNil(t, h.SetMessage(&mockCodec{}))
	}
}

func TestCreateDataBase_Process(t *testing.T) {
	db := path.Join(dataPath, "db0")
	pt := uint32(1)
	rp := "rp0"

	h := newHandler(netstorage.CreateDataBaseRequestMessage)
	if err := h.SetMessage(&netstorage.CreateDataBaseRequest{
		CreateDataBaseRequest: internal.CreateDataBaseRequest{
			Db: &db,
			Pt: &pt,
			Rp: &rp,
		},
	}); err != nil {
		t.Fatal(err)
	}

	h.SetStore(&storage.Storage{})

	rsp, _ := h.Process()
	response, ok := rsp.(*netstorage.CreateDataBaseResponse)
	if !ok {
		t.Fatal("response type is invalid")
	}
	assert.NotNil(t, response.GetErr())
	response.Err = nil

	fileops.MkdirAll(dataPath, 0750)
	defer fileops.RemoveAll(dataPath)
	rsp, _ = h.Process()
	response, ok = rsp.(*netstorage.CreateDataBaseResponse)
	if !ok {
		t.Fatal("response type is invalid")
	}
	assert.Nil(t, response.Error())
}
