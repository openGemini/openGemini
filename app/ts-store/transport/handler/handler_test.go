package handler

import (
	"path"
	"testing"

	"github.com/openGemini/openGemini/app/ts-store/storage"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/netstorage"
	internal "github.com/openGemini/openGemini/lib/netstorage/data"
	"github.com/stretchr/testify/assert"
)

const dataPath = "/tmp/test_data"

func TestBaseHandler_SetMessage(t *testing.T) {
	h := &CreateDataBase{}

	req := netstorage.CreateDataBaseRequest{}
	if err := h.SetMessage(&req); err != nil {
		t.Fatalf("SetMessage failed: %+v", err)
	}

	err := h.SetMessage(&netstorage.SeriesKeysRequest{})
	assert.NotNil(t, err)
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
