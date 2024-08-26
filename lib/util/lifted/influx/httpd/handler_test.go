package httpd

import (
	"bufio"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	prompb2 "github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
	"github.com/gorilla/mux"
	"github.com/influxdata/influxdb/services/httpd"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/pool"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/syscontrol"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/httpd/config"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/promql2influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
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

func TestHandler_Disabled_Prom_Query(t *testing.T) {
	h := Handler{
		requestTracker: httpd.NewRequestTracker(),
		Logger:         logger.NewLogger(errno.ModuleHTTP),
		Config:         &config.Config{},
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
		req = httptest.NewRequest(http.MethodPost, "/api/v1/query?db=db1&query=up", nil)
		h.servePromQuery(w, req, user)
		assert.Equal(t, http.StatusForbidden, w.Code)
	})

	t.Run("prom query api params check", func(t *testing.T) {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/query?db=db1", nil)
		h.servePromQuery(w, req, user)
		assert.Equal(t, http.StatusBadRequest, w.Code)

		w = httptest.NewRecorder()
		req = httptest.NewRequest(http.MethodGet, "/api/v1/query?db=db1&query=up&timeout=xx", nil)
		h.servePromQuery(w, req, user)
		assert.Equal(t, http.StatusBadRequest, w.Code)

		w = httptest.NewRecorder()
		req = httptest.NewRequest(http.MethodGet, "/api/v1/query?db=db1&query=up&time=xx", nil)
		h.servePromQuery(w, req, user)
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("prom query_range api params check", func(t *testing.T) {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/query_range?db=db1&query=up", nil)
		h.servePromQueryRange(w, req, user)
		assert.Equal(t, http.StatusBadRequest, w.Code)

		w = httptest.NewRecorder()
		req = httptest.NewRequest(http.MethodGet, "/api/v1/query_range?db=db1&query=up&start=1708572257", nil)
		h.servePromQueryRange(w, req, user)
		assert.Equal(t, http.StatusBadRequest, w.Code)

		w = httptest.NewRecorder()
		req = httptest.NewRequest(http.MethodGet, "/api/v1/query_range?db=db1&query=up&start=1708572257&end=1708585337", nil)
		h.servePromQueryRange(w, req, user)
		assert.Equal(t, http.StatusBadRequest, w.Code)

		w = httptest.NewRecorder()
		req = httptest.NewRequest(http.MethodGet, "/api/v1/query_range?db=db1&query=up&start=2708572257&end=1708585337", nil)
		h.servePromQueryRange(w, req, user)
		assert.Equal(t, http.StatusBadRequest, w.Code)

		w = httptest.NewRecorder()
		req = httptest.NewRequest(http.MethodGet, "/api/v1/query_range?db=db1&query=up&start=1708572257&end=2708585337&step=15s", nil)
		h.servePromQueryRange(w, req, user)
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})
}

func TestHandler_Prom_Metadata_Query(t *testing.T) {
	h := Handler{
		requestTracker: httpd.NewRequestTracker(),
		Logger:         logger.NewLogger(errno.ModuleHTTP),
		Config:         &config.Config{},
	}
	var user meta.User

	t.Run("prom meta query", func(t *testing.T) {
		// check db
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/labels", nil)
		h.servePromQuery(w, req, user)
		assert.Equal(t, http.StatusBadRequest, w.Code)

		// check start
		w = httptest.NewRecorder()
		req = httptest.NewRequest(http.MethodGet, "/api/v1/labels?db=prometheus&start=a", nil)
		h.servePromQuery(w, req, user)
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("prom label-values query", func(t *testing.T) {
		// check label name
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/label/up./values?db=prometheus", nil)
		h.servePromQuery(w, req, user)
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("prom series query", func(t *testing.T) {
		// check match[] exits
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/series?db=prometheus", nil)
		h.servePromQuery(w, req, user)
		assert.Equal(t, http.StatusBadRequest, w.Code)
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
			"SELECT count(*) FROM repo0.log0.log0 WHERE content MATCHPHRASE 'abc' AND __log___::string MATCHPHRASE 'efg' AND time::time >= 1 AND time::time < 100 GROUP BY * ORDER BY time DESC",
			"content MATCHPHRASE 'abc' AND __log___::string MATCHPHRASE 'efg'"},
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
		sqlQuery, pplQuery, err, status := h.getSqlAndPplQuery(req, param, user, &measurementInfo{
			name:            "log0",
			database:        "repo0",
			retentionPolicy: "log0",
		})
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

func TestTimeSeries2Rows(t *testing.T) {
	type args struct {
		dst []influx.Row
		tss []prompb2.TimeSeries
	}
	tests := []struct {
		name    string
		args    args
		want    []influx.Row
		wantErr bool
	}{
		{
			name: "Test with valid time series",
			args: args{
				dst: []influx.Row{},
				tss: []prompb2.TimeSeries{
					{
						Labels: []prompb2.Label{
							{Name: []byte("label1"), Value: []byte("value1")},
						},
						Samples: []prompb2.Sample{
							{Value: 1.0, Timestamp: 1000},
						},
					},
				},
			},
			want: []influx.Row{
				{
					Tags: influx.PointTags{
						influx.Tag{
							Key:   "label1",
							Value: "value1",
						},
					},
					Name:      promql2influxql.DefaultMeasurementName,
					Timestamp: time.Unix(0, 1000*int64(time.Millisecond)).UnixNano(),
					Fields: []influx.Field{
						{
							Type:     influx.Field_Type_Float,
							Key:      promql2influxql.DefaultFieldKey,
							NumValue: 1.0,
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "Test with invalid time series",
			args: args{
				dst: []influx.Row{},
				tss: []prompb2.TimeSeries{
					{
						Labels: []prompb2.Label{
							{Name: []byte("label1"), Value: []byte("value1")},
						},
						Samples: []prompb2.Sample{
							{Value: math.Inf(1), Timestamp: 1000},
						},
					},
				},
			},
			want: []influx.Row{
				{
					Tags: influx.PointTags{
						influx.Tag{
							Key:   "label1",
							Value: "value1",
						},
					},
					Name:      promql2influxql.DefaultMeasurementName,
					Timestamp: time.Unix(0, 1000*int64(time.Millisecond)).UnixNano(),
					Fields: []influx.Field{
						{
							Type:     influx.Field_Type_Float,
							Key:      promql2influxql.DefaultFieldKey,
							NumValue: math.Inf(1),
						},
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := timeSeries2Rows(EmptyPromMst, tt.args.dst, tt.args.tss)
			if (err != nil) != tt.wantErr {
				t.Errorf("timeSeries2Rows() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("timeSeries2Rows() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseJson(t *testing.T) {
	h := &Handler{
		mux: mux.NewRouter(),
		Config: &config.Config{
			AuthEnabled: false,
		},
		requestTracker: httpd.NewRequestTracker(),
		Logger:         logger.NewLogger(errno.ModuleLogStore),
	}
	// {"time":%v, "http":"127.0.0.1", "cnt":4}
	now := time.Now().UnixMilli()
	s := fmt.Sprintf("{\"time\":%v, \"http\":\"127.0.0.1\", \"cnt\":4}", now)
	req := mockLogWriteRequest(`{"timestamp":"time", "default_type":"tags","content": ["http", "cnt"]}`)
	scanner := bufio.NewScanner(strings.NewReader(s))
	scanBuf := byteBufferPool.Get()
	defer byteBufferPool.Put(scanBuf)
	scanner.Buffer(scanBuf, ScannerBufferSize)
	scanner.Split(bufio.ScanLines)

	ld := 7 * 24 * time.Hour
	req.expiredTime = time.Now().UnixNano() - ld.Nanoseconds()
	rows := record.NewRecord(logSchema, false)
	failRows := record.NewRecord(failLogSchema, false)
	req.logSchema = logSchema
	_ = h.parseJson(scanner, req, rows, failRows)
	expect := fmt.Sprintf("field(__retry_tag__):[]bool{false}\nfield(cnt):[]float64{4}\nfield(http):[]string{\"127.0.0.1\"}\nfield(time):[]int64{%v}\n", now*1e6)
	res := rows.String()
	if expect != res {
		t.Fatal("unexpect", res)
	}
}

func newTimeSeries(num int) []prompb2.TimeSeries {
	ts := prompb2.TimeSeries{
		Labels: []prompb2.Label{
			{Name: []byte("__name__"), Value: []byte("test_metric")},
			{Name: []byte("label1"), Value: []byte("value1")},
		},
		Samples: []prompb2.Sample{
			{Value: 1.0, Timestamp: 1000},
			{Value: 2.0, Timestamp: 2000},
		},
	}

	tss := make([]prompb2.TimeSeries, 0, num)
	for i := 0; i < num; i++ {
		for j := range ts.Samples {
			ts.Samples[j].Value += 1.0
			ts.Samples[j].Timestamp += 1000
		}
		tss = append(tss, ts)
	}
	return tss
}

func Benchmark_TimeSeries2Rows(t *testing.B) {
	num := 100000
	tss := newTimeSeries(num)
	rs := pool.GetRows(num)
	defer pool.PutRows(rs)
	t.N = 10
	t.ReportAllocs()
	t.ResetTimer()
	var err error
	for i := 0; i < t.N; i++ {
		t.StartTimer()
		for j := 0; j < 1000000; j++ {
			*rs, err = timeSeries2Rows(EmptyPromMst, *rs, tss)
			if err != nil {
				if err != nil {
					t.Fatal("timeSeries2Rows fail")
				}
			}
			t.StopTimer()
		}
	}
}

func TestMergeFragments(t *testing.T) {
	// A test of multiple highlighted section coverage
	fragments := []Fragment{
		{1, 10},
		{5, 15},
	}
	fragments = mergeFragments(fragments)
	assert.Equal(t, 1, len(fragments))
	assert.Equal(t, 1, fragments[0].Offset)
	assert.Equal(t, 19, fragments[0].Length)

	// A test for one highlight containing another
	fragments = []Fragment{
		{1, 10},
		{5, 3},
	}
	fragments = mergeFragments(fragments)
	assert.Equal(t, 1, len(fragments))
	assert.Equal(t, 1, fragments[0].Offset)
	assert.Equal(t, 10, fragments[0].Length)

	// A test of multiple highlights independent of each other
	fragments = []Fragment{
		{1, 10},
		{15, 3},
	}
	fragments = mergeFragments(fragments)
	assert.Equal(t, 2, len(fragments))

	// A test of same highlights
	fragments = []Fragment{
		{1, 10},
		{1, 10},
	}
	fragments = mergeFragments(fragments)
	assert.Equal(t, 1, len(fragments))
	assert.Equal(t, 1, fragments[0].Offset)
	assert.Equal(t, 10, fragments[0].Length)
}
