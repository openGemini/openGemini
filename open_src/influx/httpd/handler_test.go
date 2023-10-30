package httpd

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/influxdata/influxdb/services/httpd"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/syscontrol"
	"github.com/openGemini/openGemini/open_src/influx/meta"
	"github.com/stretchr/testify/assert"
)

func TestDebugCtrl(t *testing.T) {
	h := Handler{}

	t.Run(" unknown sysctrl mod", func(t *testing.T) {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/debug/ctrl?mod=invalid", nil)
		h.serveDebug(w, req)
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})
}

type mockMetaClient struct {
	metaclient.MetaClient
}

func (mockMetaClient) DataNodes() ([]meta.DataNode, error) {
	return []meta.DataNode{
		{
			NodeInfo: meta.NodeInfo{
				ID:   0,
				Host: "127.0.0.1:8400",
			},
		},
		{
			NodeInfo: meta.NodeInfo{
				ID:   1,
				Host: "127.0.0.2:8400",
			},
		},
	}, nil
}

type mockStorage struct {
	netstorage.Storage
}

func (mockStorage) SendQueryRequestOnNode(nodID uint64, req netstorage.SysCtrlRequest) (map[string]string, error) {
	return map[string]string{
		"db: db0, rp: autogen, pt: 1": `[{"Opened":true,"ReadOnly":false,"ShardId":1}]`,
	}, nil
}

func TestHandler_ServeDebugQuery(t *testing.T) {
	h := Handler{}

	t.Run("invalid method", func(t *testing.T) {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/debug/query?mod=shards&db=mydb&rp=myrp", nil)
		h.serveDebugQuery(w, req)
		assert.Equal(t, http.StatusMethodNotAllowed, w.Code)
	})

	t.Run("empty mod", func(t *testing.T) {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/debug/query?mod=&db=mydb&rp=myrp", nil)
		h.serveDebugQuery(w, req)
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("unsupported mod", func(t *testing.T) {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/debug/query?mod=shard&db=mydb&rp=myrp", nil)
		h.serveDebugQuery(w, req)
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("query shards success", func(t *testing.T) {
		syscontrol.SysCtrl.MetaClient = &mockMetaClient{}
		syscontrol.SysCtrl.NetStore = &mockStorage{}

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/debug/query?mod=shards&db=mydb&rp=myrp", nil)
		h.serveDebugQuery(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
	})
}

func TestHandler_Disabled_Prom_Write_Read(t *testing.T) {
	h := Handler{
		requestTracker: httpd.NewRequestTracker(),
		Logger:         logger.NewLogger(errno.ModuleHTTP),
	}

	var user meta.User
	t.Run("disable prom read", func(t *testing.T) {
		syscontrol.SysCtrl.MetaClient = &mockMetaClient{}
		syscontrol.SysCtrl.NetStore = &mockStorage{}

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/debug/ctrl?mod=disableread&switchon=true", nil)
		defer func() {
			req := httptest.NewRequest(http.MethodPost, "/debug/ctrl?mod=disableread&switchon=false", nil)
			h.serveDebug(w, req)
		}()

		h.serveDebug(w, req)

		w = httptest.NewRecorder()
		req = httptest.NewRequest(http.MethodPost, "/api/v1/prom/read", nil)
		h.servePromRead(w, req, user)
		assert.Equal(t, http.StatusForbidden, w.Code)
	})

	t.Run("disable prom write", func(t *testing.T) {
		syscontrol.SysCtrl.MetaClient = &mockMetaClient{}
		syscontrol.SysCtrl.NetStore = &mockStorage{}

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/debug/ctrl?mod=disablewrite&switchon=true", nil)
		h.serveDebug(w, req)
		defer func() {
			req := httptest.NewRequest(http.MethodPost, "/debug/ctrl?mod=disablewrite&switchon=false", nil)
			h.serveDebug(w, req)
		}()

		w = httptest.NewRecorder()
		req = httptest.NewRequest(http.MethodPost, "/api/v1/prom/write", nil)
		h.servePromWrite(w, req, user)
		assert.Equal(t, http.StatusForbidden, w.Code)
	})
}

func TestHandler_Disabled_Write_Read(t *testing.T) {
	h := Handler{
		requestTracker: httpd.NewRequestTracker(),
		Logger:         logger.NewLogger(errno.ModuleHTTP),
	}

	var user meta.User
	t.Run("disable read", func(t *testing.T) {
		syscontrol.SysCtrl.MetaClient = &mockMetaClient{}
		syscontrol.SysCtrl.NetStore = &mockStorage{}

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/debug/ctrl?mod=disableread&switchon=true", nil)
		h.serveDebug(w, req)
		defer func() {
			req := httptest.NewRequest(http.MethodPost, "/debug/ctrl?mod=disableread&switchon=false", nil)
			h.serveDebug(w, req)
		}()

		w = httptest.NewRecorder()
		req = httptest.NewRequest(http.MethodPost, "/api/v1/read", nil)
		h.serveQuery(w, req, user)
		assert.Equal(t, http.StatusForbidden, w.Code)
	})

	t.Run("disable write", func(t *testing.T) {
		syscontrol.SysCtrl.MetaClient = &mockMetaClient{}
		syscontrol.SysCtrl.NetStore = &mockStorage{}

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/debug/ctrl?mod=disablewrite&switchon=true", nil)
		h.serveDebug(w, req)
		defer func() {
			req := httptest.NewRequest(http.MethodPost, "/debug/ctrl?mod=disablewrite&switchon=false", nil)
			h.serveDebug(w, req)
		}()

		w = httptest.NewRecorder()
		req = httptest.NewRequest(http.MethodPost, "/api/v1/write", nil)
		h.serveWrite(w, req, user)
		assert.Equal(t, http.StatusForbidden, w.Code)
	})
}

func TestHandler_Flux_Disabled_Read(t *testing.T) {
	h := Handler{
		requestTracker: httpd.NewRequestTracker(),
		Logger:         logger.NewLogger(errno.ModuleHTTP),
	}

	var user meta.User
	t.Run("flux disable read", func(t *testing.T) {
		syscontrol.SysCtrl.MetaClient = &mockMetaClient{}
		syscontrol.SysCtrl.NetStore = &mockStorage{}

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/debug/ctrl?mod=disableread&switchon=true", nil)
		h.serveDebug(w, req)
		defer func() {
			req := httptest.NewRequest(http.MethodPost, "/debug/ctrl?mod=disableread&switchon=false", nil)
			h.serveDebug(w, req)
		}()

		w = httptest.NewRecorder()
		req = httptest.NewRequest(http.MethodPost, "/api/v2/query", nil)
		h.serveFluxQuery(w, req, user)
		assert.Equal(t, http.StatusForbidden, w.Code)
	})
}

func TestHandler_Invalid_Disabled_Write_Read(t *testing.T) {
	h := Handler{
		requestTracker: httpd.NewRequestTracker(),
		Logger:         logger.NewLogger(errno.ModuleHTTP),
	}

	t.Run("disable read", func(t *testing.T) {
		syscontrol.SysCtrl.MetaClient = &mockMetaClient{}
		syscontrol.SysCtrl.NetStore = &mockStorage{}

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/debug/ctrl?mod=disableread&switchond=true", nil)
		h.serveDebug(w, req)
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("disable write", func(t *testing.T) {
		syscontrol.SysCtrl.MetaClient = &mockMetaClient{}
		syscontrol.SysCtrl.NetStore = &mockStorage{}

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/debug/ctrl?mod=disablewrite&switchone=true", nil)
		h.serveDebug(w, req)
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})
}

func TestHandler_SysCtrl(t *testing.T) {
	h := Handler{
		requestTracker: httpd.NewRequestTracker(),
		Logger:         logger.NewLogger(errno.ModuleHTTP),
	}
	t.Run("ChunkReaderParallel", func(t *testing.T) {
		syscontrol.SysCtrl.MetaClient = &mockMetaClient{}
		syscontrol.SysCtrl.NetStore = &mockStorage{}

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/debug/ctrl?mod=chunk_reader_parallel&lmit=4", nil)
		h.serveDebug(w, req)
		assert.Equal(t, http.StatusBadRequest, w.Code)

		w = httptest.NewRecorder()
		req = httptest.NewRequest(http.MethodPost, "/debug/ctrl?mod=chunk_reader_parallel&limit=4", nil)
		h.serveDebug(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("PrintLogicalPlan", func(t *testing.T) {
		syscontrol.SysCtrl.MetaClient = &mockMetaClient{}
		syscontrol.SysCtrl.NetStore = &mockStorage{}

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/debug/ctrl?mod=print_logical_plan&enabled=0", nil)
		h.serveDebug(w, req)
		assert.Equal(t, http.StatusOK, w.Code)

		w = httptest.NewRecorder()
		req = httptest.NewRequest(http.MethodPost, "/debug/ctrl?mod=print_logical_plan&enabld=0", nil)
		h.serveDebug(w, req)
		assert.Equal(t, http.StatusBadRequest, w.Code)

		w = httptest.NewRecorder()
		req = httptest.NewRequest(http.MethodPost, "/debug/ctrl?mod=print_logical_plan&enabled=2", nil)
		h.serveDebug(w, req)
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("BinaryTreeMerge", func(t *testing.T) {
		syscontrol.SysCtrl.MetaClient = &mockMetaClient{}
		syscontrol.SysCtrl.NetStore = &mockStorage{}

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/debug/ctrl?mod=binary_tree_merge&enabled=0", nil)
		h.serveDebug(w, req)
		assert.Equal(t, http.StatusOK, w.Code)

		w = httptest.NewRecorder()
		req = httptest.NewRequest(http.MethodPost, "/debug/ctrl?mod=binary_tree_merge&enabe=0", nil)
		h.serveDebug(w, req)
		assert.Equal(t, http.StatusBadRequest, w.Code)

		w = httptest.NewRecorder()
		req = httptest.NewRequest(http.MethodPost, "/debug/ctrl?mod=binary_tree_merge&enabled=2", nil)
		h.serveDebug(w, req)
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("sliding_window_push_up", func(t *testing.T) {
		syscontrol.SysCtrl.MetaClient = &mockMetaClient{}
		syscontrol.SysCtrl.NetStore = &mockStorage{}

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/debug/ctrl?mod=sliding_window_push_up&enabled=1", nil)
		h.serveDebug(w, req)
		assert.Equal(t, http.StatusOK, w.Code)

		w = httptest.NewRecorder()
		req = httptest.NewRequest(http.MethodPost, "/debug/ctrl?mod=sliding_window_push_up&enable=2", nil)
		h.serveDebug(w, req)
		assert.Equal(t, http.StatusBadRequest, w.Code)

		w = httptest.NewRecorder()
		req = httptest.NewRequest(http.MethodPost, "/debug/ctrl?mod=sliding_window_push_up&enabled=2", nil)
		h.serveDebug(w, req)
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

}

func TestHandler_ServeWrite_ReadOnly(t *testing.T) {
	h := Handler{
		requestTracker: httpd.NewRequestTracker(),
		Logger:         logger.NewLogger(errno.ModuleHTTP),
	}

	var user meta.User

	t.Run("read only", func(t *testing.T) {
		syscontrol.UpdateNodeReadonly(true)
		defer func() {
			syscontrol.UpdateNodeReadonly(false)
		}()

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/write", nil)
		h.serveWrite(w, req, user)
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})
}
