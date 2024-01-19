package httpd

import (
	"bufio"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bmizerany/pat"
	"github.com/influxdata/influxdb/services/httpd"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/syscontrol"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/httpd/config"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
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

func TestTransYaccSyntaxErr(t *testing.T) {
	testStr := [][2]string{
		{"unexpected COMMA", "unexpected COMMA"},
		{"unexpected COLON, expecting IDENT or STRING", "unexpected COLON, expecting IDENTIFIER or STRING"},
		{"unexpected $end, expecting RPAREN or OR or AND", "unexpected END, expecting RIGHT PARENTHESIS or OR or AND"},
		{"unexpected COLON, expecting LPAREN", "unexpected COLON, expecting LEFT PARENTHESIS"},
		{"unexpected $unk", "unexpected UNKNOWN TOKEN"},
		{"unexpected BITWISE_OR", "unexpected PIPE OPERATOR"},
	}

	for i := 0; i < len(testStr); i++ {
		dstStr := TransYaccSyntaxErr(testStr[i][0])
		if dstStr != testStr[i][1] {
			t.Fatalf("TestTransYaccSyntaxErr failed, expect:%s, real:%s", testStr[i][1], dstStr)
		}
	}
}

func TestGetSqlAndPplQuery(t *testing.T) {
	h := Handler{
		requestTracker: httpd.NewRequestTracker(),
		Logger:         logger.NewLogger(errno.ModuleHTTP),
	}
	req := httptest.NewRequest(http.MethodGet, "/repo/repo0/logstreams/log0/logs", nil)
	req.URL.RawQuery = "%3Arepository=repo0&%3AlogStream=log0&"
	testStr := [][3]string{
		{"content: abc and efg | select count(*) from log0",
			"SELECT count(*) FROM repo0.log0.log0 WHERE content MATCHPHRASE 'abc' AND content MATCHPHRASE 'efg' AND time::time >= 1 AND time::time < 100 GROUP BY * ORDER BY time DESC",
			"content MATCHPHRASE 'abc' AND content MATCHPHRASE 'efg'"},
		{"content: abc or content: efg | select count(*) from log0 where status > 200",
			"SELECT count(*) FROM repo0.log0.log0 WHERE status > 200 AND content MATCHPHRASE 'abc' OR content MATCHPHRASE 'efg' AND time::time >= 1 AND time::time < 100 GROUP BY * ORDER BY time DESC",
			"content MATCHPHRASE 'abc' OR content MATCHPHRASE 'efg'"},
		{"* | select count(*) from log0",
			"SELECT count(*) FROM repo0.log0.log0 WHERE time::time >= 1 AND time::time < 100 GROUP BY * ORDER BY time DESC",
			""},
		{"* | content: * | select count(*) from log0",
			"SELECT count(*) FROM repo0.log0.log0 WHERE time::time >= 1 AND time::time < 100 GROUP BY * ORDER BY time DESC",
			""},
		{"* | content: * | tag: host123 | select count(*) from log0",
			"SELECT count(*) FROM repo0.log0.log0 WHERE \"tag\" MATCHPHRASE 'host123' AND time::time >= 1 AND time::time < 100 GROUP BY * ORDER BY time DESC",
			"\"tag\" MATCHPHRASE 'host123'"},
		{"content: abc | tag: host123 | tag: host456",
			"SELECT * FROM repo0.log0.log0 WHERE content MATCHPHRASE 'abc' AND \"tag\" MATCHPHRASE 'host123' AND \"tag\" MATCHPHRASE 'host456' AND time::time >= 1 AND time::time < 100 GROUP BY * ORDER BY time DESC",
			"content MATCHPHRASE 'abc' AND \"tag\" MATCHPHRASE 'host123' AND \"tag\" MATCHPHRASE 'host456'"},
	}

	var user meta.User
	for i := 0; i < len(testStr); i++ {
		param := &QueryParam{
			Query:     testStr[i][0],
			TimeRange: TimeRange{1, 100},
		}
		sqlQuery, pplQuery, err, status := h.getSqlAndPplQuery(req, param, user)
		if err != nil {
			t.Fatalf("%d-parser pipe syntax: %v, %d", i, err, status)
		}

		sql := sqlQuery.String()
		if sql != testStr[i][1] {
			t.Fatalf("%d-expect ppl:%s,\n  real Ppl:%s", i, testStr[i][1], sql)
		}

		if pplQuery != nil {
			ppl := pplQuery.String()
			if ppl != testStr[i][2] {
				t.Fatalf("%d-expect ppl:%s,\n  real Ppl:%s", i, testStr[i][2], ppl)
			}
		}
	}
}

func TestParseJsonVV2(t *testing.T) {
	h := &Handler{
		mux: pat.New(),
		Config: &config.Config{
			AuthEnabled: false,
		},
		requestTracker: httpd.NewRequestTracker(),
		Logger:         logger.NewLogger(errno.ModuleLogStore),
	}
	// {"time":%v, "http":"127.0.0.1", "cnt":4}
	now := time.Now().UnixMilli()
	s := fmt.Sprintf("{\"time\":%v, \"http\":\"127.0.0.1\", \"cnt\":4}", now)
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(s))
	q := req.URL.Query()
	q.Add(":repository", "test")
	q.Add(":logStream", "test")
	q.Add("type", "json")
	q.Add("mapping", `{"timestamp":"time", "default_type":"tags","content": ["http", "cnt"]}`)
	req.URL.RawQuery = q.Encode()

	req2, err := h.getLogWriteRequest(req)
	if err != nil {
		t.Fatal(req2, err)
	}
	scanner := bufio.NewScanner(strings.NewReader(s))
	scanBuf := byteBufferPool.Get()
	defer byteBufferPool.Put(scanBuf)
	scanner.Buffer(scanBuf, ScannerBufferSize)
	scanner.Split(bufio.ScanLines)

	ld := 7 * 24 * time.Hour
	expiredEarliestTime := time.Now().UnixMilli() - ld.Milliseconds()
	rows := &record.Record{}
	failRows := record.NewRecord(schema, false)
	_ = h.parseJsonV2(scanner, req2, rows, failRows, expiredEarliestTime)
	expect := fmt.Sprintf("field(tags):[]string(nil)\nfield(cnt):[]float64{4}\nfield(http):[]string{\"127.0.0.1\"}\nfield(time):[]int64{%v}\n", now*1e6)
	res := rows.String()
	if expect != res {
		t.Fatal("unexpect", res)
	}
}
