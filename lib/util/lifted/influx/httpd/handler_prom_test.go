// Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package httpd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/golang/snappy"
	"github.com/openGemini/openGemini/coordinator"
	config2 "github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/httpd/config"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/protobuf/proto"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/openGemini/openGemini/lib/validation"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func NewTestHandle() Handler {
	limits := config2.NewLimits()
	limits.PromLimitEnabled = true
	limits.RejectOldSamples = true
	limits.MaxLabelNamesPerSeries = 2
	limits.MaxMetadataLength = 20
	limits.MaxQueryLength = model.Duration(30 * 24 * time.Hour)

	validation.InitOverrides(limits, nil)
	cfg := config.NewConfig()
	return Handler{
		Config:       &cfg,
		Logger:       logger.NewLogger(errno.ModuleHTTP),
		MetaClient:   metaclient.NewClient("", false, 20),
		PointsWriter: coordinator.NewPointsWriter(1),
	}
}

func MockValidDB() func() {
	var cli *metaclient.Client
	mockMeta := gomonkey.ApplyMethod(reflect.TypeOf(cli), "Database", func(_ *metaclient.Client, name string) (*meta2.DatabaseInfo, error) {
		dbi := &meta2.DatabaseInfo{
			DefaultRetentionPolicy: "prom",
		}
		return dbi, nil
	})
	var pw *coordinator.PointsWriter
	mockPw := gomonkey.ApplyMethod(reflect.TypeOf(pw), "RetryWritePointRows", func(_ *coordinator.PointsWriter, database, retentionPolicy string, points []influx.Row) error {
		return nil
	})
	return func() {
		mockMeta.Reset()
		mockPw.Reset()
	}
}

func MockInvalidDB() func() {
	var cli *metaclient.Client
	mockMeta := gomonkey.ApplyMethod(reflect.TypeOf(cli), "Database", func(_ *metaclient.Client, name string) (*meta2.DatabaseInfo, error) {
		return nil, fmt.Errorf("database %s doesn't exist", name)
	})
	var pw *coordinator.PointsWriter
	mockPw := gomonkey.ApplyMethod(reflect.TypeOf(pw), "RetryWritePointRows", func(_ *coordinator.PointsWriter, database, retentionPolicy string, points []influx.Row) error {
		return nil
	})
	return func() {
		mockMeta.Reset()
		mockPw.Reset()
	}
}

func TestHandlerPromWriteTimeSeries(t *testing.T) {
	var user meta.User

	h := NewTestHandle()

	cancel := MockValidDB()
	defer cancel()

	now := model.Now()
	past := now.Add(-15 * 24 * time.Hour)

	t.Run("multiple validation fails return the first one", func(t *testing.T) {
		timeseries := []prompb.TimeSeries{
			{
				Labels: []prompb.Label{ // too many labels
					{Name: model.MetricNameLabel, Value: "testmetric"},
					{Name: "foo", Value: "bar"},
					{Name: "foo2", Value: "bar2"},
				},
				Samples: []prompb.Sample{
					{Value: 2, Timestamp: int64(now)},
				},
			},
			{
				Labels: []prompb.Label{ // old sample
					{Name: model.MetricNameLabel, Value: "testmetric"},
					{Name: "foo", Value: "bar"},
				},
				Samples: []prompb.Sample{
					{Value: 2, Timestamp: int64(past)},
				},
			},
		}
		// Create write request
		data, err := proto.Marshal(&prompb.WriteRequest{Timeseries: timeseries})
		require.NoError(t, err)
		compressed := snappy.Encode(nil, data)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/write", bytes.NewReader(compressed))
		h.servePromWrite(w, req, user)
		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Equal(t, `{"error":"error when processing imported data: series has too many labels (actual: 3, limit: 2) series: 'testmetric{foo2=\"bar2\", foo=\"bar\"}'"}`, w.Body.String())
	})

	t.Run("partial write succcess", func(t *testing.T) {
		timeseries := []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: model.MetricNameLabel, Value: "testmetric"},
					{Name: "foo", Value: "bar"},
				},
				Samples: []prompb.Sample{
					{Value: 2, Timestamp: int64(now)},
				},
			},
			{
				Labels: []prompb.Label{ // no sample
					{Name: model.MetricNameLabel, Value: "testmetric"},
					{Name: "foo", Value: "bar"},
				},
				Samples: []prompb.Sample{},
			},
		}
		// Create write request
		data, err := proto.Marshal(&prompb.WriteRequest{Timeseries: timeseries})
		require.NoError(t, err)
		compressed := snappy.Encode(nil, data)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/write", bytes.NewReader(compressed))
		h.servePromWrite(w, req, user)
		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Equal(t, `{"error":"timeseries have no sample"}`, w.Body.String())
	})

	t.Run("input empty", func(t *testing.T) {
		timeseries := []prompb.TimeSeries{}
		// Create write request
		data, err := proto.Marshal(&prompb.WriteRequest{Timeseries: timeseries})
		require.NoError(t, err)
		compressed := snappy.Encode(nil, data)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/write", bytes.NewReader(compressed))
		h.servePromWrite(w, req, user)
		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Equal(t, `{"error":"error when processing imported data: timeseries have no sample"}`, w.Body.String())
	})
}

func TestHandlerPromWriteMetadata(t *testing.T) {
	var user meta.User

	h := NewTestHandle()

	cancel := MockValidDB()
	defer cancel()

	t.Run("multiple validation fails return the first one", func(t *testing.T) {
		metadata := []prompb.MetricMetadata{
			{MetricFamilyName: "", Type: prompb.MetricMetadata_COUNTER, Help: "Number of goroutines.", Unit: ""},                                        // no metric name
			{MetricFamilyName: "go_goroutines_and_routines_and_routines", Type: prompb.MetricMetadata_COUNTER, Help: "Number of goroutines.", Unit: ""}, // long metric name
		}
		// Create write request
		data, err := proto.Marshal(&prompb.WriteRequest{Metadata: metadata})
		require.NoError(t, err)
		compressed := snappy.Encode(nil, data)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/write?metadata=true", bytes.NewReader(compressed))
		h.servePromWrite(w, req, user)
		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Equal(t, `{"error":"metadata missing metric name"}`, w.Body.String())
	})

	t.Run("partial write succcess", func(t *testing.T) {
		metadata := []prompb.MetricMetadata{
			{MetricFamilyName: "go_goroutines", Type: prompb.MetricMetadata_COUNTER, Help: "Number_goroutines", Unit: ""},                           // valid
			{MetricFamilyName: "go_goroutines_and_routines_and_routines", Type: prompb.MetricMetadata_COUNTER, Help: "Number_goroutines", Unit: ""}, // long metric name
		}
		// Create write request
		data, err := proto.Marshal(&prompb.WriteRequest{Metadata: metadata})
		require.NoError(t, err)
		compressed := snappy.Encode(nil, data)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/write?metadata=true", bytes.NewReader(compressed))
		h.servePromWrite(w, req, user)
		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Equal(t, `{"error":"metadata 'METRIC_NAME' value too long: \"go_goroutines_and_routines_and_routines\" metric \"go_goroutines_and_routines_and_routines\""}`, w.Body.String())
	})

	t.Run("input empty", func(t *testing.T) {
		metadata := []prompb.MetricMetadata{}
		// Create write request
		data, err := proto.Marshal(&prompb.WriteRequest{Metadata: metadata})
		require.NoError(t, err)
		compressed := snappy.Encode(nil, data)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/write?metadata=true", bytes.NewReader(compressed))
		h.servePromWrite(w, req, user)
		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Equal(t, `{"error":"request have no metadata"}`, w.Body.String())
	})
}

func TestDefaultRetentionPolicy(t *testing.T) {
	mockDispose := MockValidDB()
	defer mockDispose()

	h := NewTestHandle()
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/query?query=1%2b1&rp=none", bytes.NewReader([]byte{}))
	h.servePromQuery(w, req, nil)
	assert.Equal(t, http.StatusOK, w.Code)

	var data map[string]any
	err := json.Unmarshal(w.Body.Bytes(), &data)
	assert.Nil(t, err)
	assert.Equal(t, data["status"], "success")
}

func TestNonExistRetentionPolicy(t *testing.T) {
	mockDispose := MockInvalidDB()
	defer mockDispose()

	h := NewTestHandle()
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/query?query=1%2b1&rp=none", bytes.NewReader([]byte{}))
	h.servePromQuery(w, req, nil)
	assert.Equal(t, http.StatusOK, w.Code)

	var data map[string]any
	err := json.Unmarshal(w.Body.Bytes(), &data)
	assert.Nil(t, err)
	assert.Equal(t, data["status"], "success")
}

func TestHandlerPromRead(t *testing.T) {
	var user meta.User

	h := NewTestHandle()

	cancel := MockValidDB()
	defer cancel()

	t.Run("invalid query time range", func(t *testing.T) {
		queries := []*prompb.Query{
			{ // valid time range
				StartTimestampMs: 1734601310000, // 2024-12-19 17:41:50
				EndTimestampMs:   1734774110000, // 2024-12-21 17:41:50
			},
			{ // invalid
				StartTimestampMs: 1729503710000, // 2024-10-21 17:41:50
				EndTimestampMs:   1734774110000, // 2024-12-21 17:41:50
			},
		}
		// Create read request
		data, err := proto.Marshal(&prompb.ReadRequest{Queries: queries})
		require.NoError(t, err)
		compressed := snappy.Encode(nil, data)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/read", bytes.NewReader(compressed))
		h.servePromRead(w, req, user)
		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Equal(t, `{"error":"the query time range exceeds the limit (start: 2024-10-21 09:41:50 +0800 UTC, end 2024-12-21 09:41:50 +0800 UTC, query_len: 1464h0m0s, limit: 720h0m0s)"}`, w.Body.String())
	})

	t.Run("input empty", func(t *testing.T) {
		queries := []*prompb.Query{}
		// Create read request
		data, err := proto.Marshal(&prompb.ReadRequest{Queries: queries})
		require.NoError(t, err)
		compressed := snappy.Encode(nil, data)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/read", bytes.NewReader(compressed))
		h.servePromRead(w, req, user)
		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Equal(t, `{"error":"syntax error: unexpected $end"}`, w.Body.String())
	})
}

func TestHandlerPromQueryRange(t *testing.T) {
	var user meta.User

	h := NewTestHandle()

	cancel := MockValidDB()
	defer cancel()

	t.Run("invalid query time range", func(t *testing.T) {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/query_range", nil)
		req.Form = make(url.Values)
		req.Form.Add("query", "cpu{tpye=\"test\"}")
		req.Form.Add("start", "1729503710000") // 2024-10-21 17:41:50
		req.Form.Add("end", "1734774110000")   // 2024-12-21 17:41:50

		h.servePromQueryRange(w, req, user)
		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Equal(t, `{"status":"error","errorType":"bad_data","error":"invalid parameter \"start_end\": the query time range exceeds the limit (start: 56775-10-17 01:13:20 +0000 UTC, end 56942-10-21 01:13:20 +0000 UTC, query_len: 1464000h0m0s, limit: 720h0m0s)"}`, w.Body.String())
	})
}

func TestHandlerPromQuerySeries(t *testing.T) {
	var user meta.User

	h := NewTestHandle()

	cancel := MockValidDB()
	defer cancel()

	t.Run("invalid query time range", func(t *testing.T) {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/series", nil)
		req.Form = make(url.Values)
		req.Form.Add("match[]", "cpu{tpye=\"test\"}")
		req.Form.Add("start", "1729503710000") // 2024-10-21 17:41:50
		req.Form.Add("end", "1734774110000")   // 2024-12-21 17:41:50

		h.servePromQuerySeries(w, req, user)
		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Equal(t, `{"status":"error","errorType":"bad_data","error":"invalid parameter \"start_end\": the query time range exceeds the limit (start: 56775-10-17 01:13:20 +0000 UTC, end 56942-10-21 01:13:20 +0000 UTC, query_len: 1464000h0m0s, limit: 720h0m0s)"}`, w.Body.String())
	})
}
