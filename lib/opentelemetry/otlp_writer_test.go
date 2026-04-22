// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package opentelemetry_test

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/influxdata/influxdb-observability/common"
	"github.com/openGemini/openGemini/lib/opentelemetry"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	semconv "go.opentelemetry.io/collector/semconv/v1.16.0"
)

type writer struct{}

func (writer) RetryWritePointRows(database, retentionPolicy string, points []influx.Row) error {
	return nil
}

func TestOtelContext_WriteTraces_OK(t *testing.T) {
	var genTraceData = func() ptraceotlp.ExportRequest {
		var mockTraceData = `{"resourceSpans":[{"resource":{},"scopeSpans":[{"scope":{},"spans":[{"traceId":"01000000000000000000000000000000","spanId":"0200000000000000","parentSpanId":"","name":"test_span","startTimeUnixNano":"1678898440000000000","endTimeUnixNano":"1678898440000001000","attributes":[{"key":"cpu","value":{"intValue":"1"}},{"key":"event","value":{"stringValue":"test"}}],"status":{}}]}]}]}`

		stringTime := "2023-03-15 16:40:40"
		loc, _ := time.LoadLocation("UTC")
		theTime, _ := time.ParseInLocation("2006-01-02 15:04:05", stringTime, loc)

		req := ptraceotlp.NewExportRequest()
		span1 := req.Traces().ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
		span1.SetName("test_span")
		span1.SetTraceID([16]byte{1})
		span1.SetSpanID([8]byte{2})
		span1.SetParentSpanID([8]byte{0})
		span1.SetStartTimestamp(pcommon.Timestamp(theTime.UnixNano()))
		span1.SetEndTimestamp(pcommon.Timestamp(theTime.UnixNano() + 1000))
		span1.Attributes().PutInt("cpu", 1)
		span1.Attributes().PutStr("event", "test")

		var mockReq = ptraceotlp.NewExportRequest()
		err := mockReq.UnmarshalJSON([]byte(mockTraceData))
		require.NoError(t, err)

		mockReqProto, err := mockReq.MarshalProto()
		require.NoError(t, err)
		reqProto, err := req.MarshalProto()
		require.NoError(t, err)

		require.Equal(t, mockReqProto, reqProto)
		return req
	}

	req := genTraceData()
	buf, _ := req.MarshalProto()

	reader := bufio.NewReader(bytes.NewReader(buf))
	octx, _ := opentelemetry.GetOtelContext(reader)
	// do not put back to pool

	require.True(t, octx.Read())

	ctx := context.Background()
	var w = &writer{}
	octx.Writer = w
	octx.Database = "db0"
	octx.RetentionPolicy = "rp0"
	err := octx.WriteTraces(ctx)
	require.NoError(t, err)
}

type mockTraceWriterError struct{}

func (mockTraceWriterError) WriteTraces(ctx context.Context, md ptrace.Traces) error {
	return errors.New("failed to convert OTLP span to line protocol")
}

type mockTraceWriterError2 struct{}

func (mockTraceWriterError2) WriteTraces(ctx context.Context, md ptrace.Traces) error {
	return errors.New("test error")
}

func TestOtelContext_WriteTraces_WriteError(t *testing.T) {
	reader := bufio.NewReader(bytes.NewReader([]byte("xxxxx")))
	octx, err := opentelemetry.GetOtelContext(reader)
	require.NoError(t, err)
	var w = &writer{}
	octx.Writer = w
	ctx := context.Background()
	require.True(t, octx.Read())
	err = octx.WriteTraces(ctx)
	require.NoError(t, err)
	require.EqualError(t, octx.Error(), "unexpected EOF")

	reader = bufio.NewReader(bytes.NewReader([]byte{}))
	octx, err = opentelemetry.GetOtelContext(reader)
	require.NoError(t, err)
	// do not put back to pool

	// mock write error
	writeError := &mockTraceWriterError{}
	octx.PtraceWriter = writeError

	require.True(t, octx.Read())

	octx.Writer = w
	octx.Database = "db0"
	octx.RetentionPolicy = "rp0"
	err = octx.WriteTraces(ctx)
	require.NoError(t, err)
	require.EqualError(t, octx.Error(), "failed to convert OTLP span to line protocol")

	// mock write error
	writeError2 := &mockTraceWriterError2{}
	octx.PtraceWriter = writeError2
	require.EqualError(t, octx.WriteTraces(ctx), "test error")
}

func TestOtelContext_WriteMetrics_OK(t *testing.T) {
	var genMetricData = func() pmetricotlp.ExportRequest {
		var mockMetricData = `{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{},"metrics":[{"name":"test_metric","description":"this is description","unit":"ms","gauge":{"dataPoints":[{"startTimeUnixNano":"1678812040000000000","timeUnixNano":"1678898440000000000","asInt":"1314520"}]}}]}]}]}`
		var mockReq = pmetricotlp.NewExportRequest()
		err := mockReq.UnmarshalJSON([]byte(mockMetricData))
		require.NoError(t, err)

		return mockReq
	}

	req := genMetricData()
	buf, _ := req.MarshalProto()

	reader := bufio.NewReader(bytes.NewReader(buf))
	octx, _ := opentelemetry.GetOtelContext(reader)
	// do not put back to pool
	require.True(t, octx.Read())

	ctx := context.Background()
	var w = &writer{}
	octx.Writer = w
	octx.Database = "db0"
	octx.RetentionPolicy = "rp0"
	err := octx.WriteMetrics(ctx)
	require.NoError(t, err)
}

type mockMetricWriterError struct{}

func (mockMetricWriterError) WriteMetrics(ctx context.Context, md pmetric.Metrics) error {
	return errors.New("failed to convert OTLP metric to line protocol")
}

type mockMetricWriterError2 struct{}

func (mockMetricWriterError2) WriteMetrics(ctx context.Context, md pmetric.Metrics) error {
	return errors.New("test error")
}

func TestOtelContext_WriteMetrics_WriteError(t *testing.T) {
	reader := bufio.NewReader(bytes.NewReader([]byte("xxxxx")))
	octx, err := opentelemetry.GetOtelContext(reader)
	require.NoError(t, err)
	var w = &writer{}
	octx.Writer = w
	ctx := context.Background()
	require.True(t, octx.Read())
	err = octx.WriteMetrics(ctx)
	require.NoError(t, err)
	require.EqualError(t, octx.Error(), "unexpected EOF")

	reader = bufio.NewReader(bytes.NewReader([]byte{}))
	octx, err = opentelemetry.GetOtelContext(reader)
	// do not put back to pool

	// mock write error
	writeError := &mockMetricWriterError{}
	octx.PmetricWriter = writeError

	require.True(t, octx.Read())
	octx.Writer = w
	octx.Database = "db0"
	octx.RetentionPolicy = "rp0"
	err = octx.WriteMetrics(ctx)
	require.NoError(t, err)
	require.EqualError(t, octx.Error(), "failed to convert OTLP metric to line protocol")

	// mock write error
	writeError2 := &mockMetricWriterError2{}
	octx.PmetricWriter = writeError2
	require.EqualError(t, octx.WriteMetrics(ctx), "test error")
}

type mockReaderError struct {
}

func (m *mockReaderError) Read(p []byte) (n int, err error) {
	return 0, errors.New("test err")
}

func Test_otelContext_ReadError(t *testing.T) {
	otelContext, err := opentelemetry.GetOtelContext(&mockReaderError{})
	assert.NoError(t, err)
	otelContext.Read()
	assert.Error(t, otelContext.Error())

	otelContext.Read()
	assert.Error(t, otelContext.Error())
}

type mockLogWriterError struct{}

func (mockLogWriterError) WriteLogs(ctx context.Context, md plog.Logs) error {
	return errors.New("failed to convert OTLP log to line protocol")
}

type mockLogWriterError2 struct{}

func (mockLogWriterError2) WriteLogs(ctx context.Context, md plog.Logs) error {
	return errors.New("test error")
}

func TestOtelContext_WriteLogs_WriteError(t *testing.T) {
	reader := bufio.NewReader(bytes.NewReader([]byte("xxxxx")))
	octx, err := opentelemetry.GetOtelContext(reader)
	require.NoError(t, err)
	var w = &writer{}
	octx.Writer = w
	ctx := context.Background()
	require.True(t, octx.Read())
	err = octx.WriteLogs(ctx)
	require.NoError(t, err)
	require.EqualError(t, octx.Error(), "unexpected EOF")

	reader = bufio.NewReader(bytes.NewReader([]byte{}))
	octx, err = opentelemetry.GetOtelContext(reader)
	require.NoError(t, err)
	// do not put back to pool

	// mock write error
	writeError := &mockLogWriterError{}
	octx.PlogWriter = writeError

	require.True(t, octx.Read())

	octx.Writer = w
	octx.Database = "db0"
	octx.RetentionPolicy = "rp0"
	err = octx.WriteLogs(ctx)
	require.NoError(t, err)
	require.EqualError(t, octx.Error(), "failed to convert OTLP log to line protocol")

	// mock write error
	writeError2 := &mockLogWriterError2{}
	octx.PlogWriter = writeError2
	require.EqualError(t, octx.WriteLogs(ctx), "test error")
}

func TestOtelContext_WriteLogs_OK(t *testing.T) {
	var genMetricData = func() plogotlp.ExportRequest {
		var mockReq = plogotlp.NewExportRequest()
		mockLog := mockReq.Logs().ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
		mockLog.Body().SetStr("test log")
		return mockReq
	}

	req := genMetricData()
	buf, _ := req.MarshalProto()

	reader := bufio.NewReader(bytes.NewReader(buf))
	octx, _ := opentelemetry.GetOtelContext(reader)
	// do not put back to pool
	require.True(t, octx.Read())

	ctx := context.Background()
	var w = &writer{}
	octx.Writer = w
	octx.Database = "db0"
	octx.RetentionPolicy = "rp0"
	err := octx.WriteLogs(ctx)
	require.NoError(t, err)
}

func Test_otelContext_EnqueuePoint(t *testing.T) {
	reader := bufio.NewReader(bytes.NewReader([]byte{}))
	octx, err := opentelemetry.GetOtelContext(reader)
	require.NoError(t, err)
	var w = &writer{}
	octx.Writer = w
	ctx := context.Background()
	tags := map[string]string{"k1": "v1"}
	fields := map[string]interface{}{semconv.AttributeTelemetrySDKName: "v1", semconv.AttributeTelemetrySDKVersion: int64(12), "f3": 1.2, "f4": true, "f5": uint64(12), "f6": false}
	err = octx.EnqueuePoint(ctx, "mst", tags, fields, time.Now(), common.InfluxMetricValueTypeGauge)
	assert.NoError(t, err)

	err = octx.EnqueuePoint(ctx, "mst", tags, map[string]interface{}{"f": nil}, time.Now(), common.InfluxMetricValueTypeGauge)
	assert.Error(t, err)
}

func Test_otelContext_EnqueuePoint_gauge(t *testing.T) {
	var genMetricData = func() pmetricotlp.ExportRequest {
		var mockMetricData = `{"resourceMetrics":[{"resource":{"attributes":[{"key":"container.name","value":{"stringValue":"42"}}]},"scopeMetrics":[{"scope":{"name":"My Library","version":"latest"},"metrics":[{"name":"cache_age_seconds","description":"Age in seconds of the current cache","gauge":{"dataPoints":[{"attributes":[{"key":"engine_id","value":{"intValue":"0"}}],"startTimeUnixNano":"1395066363000000001","timeUnixNano":"1395066363000000123","asDouble":13.9},{"attributes":[{"key":"engine_id","value":{"intValue":"0"}}],"startTimeUnixNano":"1395066363000000001","timeUnixNano":"1395066363000000123","asDouble":23.9},{"attributes":[{"key":"engine_id","value":{"intValue":"1"}}],"startTimeUnixNano":"1395066363000000001","timeUnixNano":"1395066363000000123","asDouble":11.9}]}},{"name":"cpu_percentage","description":"cpu percentage","gauge":{"dataPoints":[{"attributes":[{"key":"engine_id","value":{"intValue":"0"}}],"startTimeUnixNano":"1395066363000000001","timeUnixNano":"1395066363000000123","asDouble":13.9}]}}]}]}]}`
		var mockReq = pmetricotlp.NewExportRequest()
		err := mockReq.UnmarshalJSON([]byte(mockMetricData))
		require.NoError(t, err)

		return mockReq
	}

	req := genMetricData()
	buf, _ := req.MarshalProto()

	reader := bufio.NewReader(bytes.NewReader(buf))
	octx, err := opentelemetry.GetOtelContext(reader)
	require.NoError(t, err)
	var w = &writer{}
	octx.Writer = w
	ctx := context.Background()
	require.True(t, octx.Read())
	err = octx.WriteMetrics(ctx)
	assert.NoError(t, err)
	assert.NoError(t, octx.Error())
}

func Test_otelContext_EnqueuePoint_sum(t *testing.T) {
	var genMetricData = func() pmetricotlp.ExportRequest {
		var mockMetricData = `{"resourceMetrics":[{"resource":{"attributes":[{"key":"container.name","value":{"stringValue":"42"}}]},"scopeMetrics":[{"scope":{"name":"My Library","version":"latest"},"metrics":[{"name":"http_requests_total","description":"The total number of HTTP requests","sum":{"dataPoints":[{"attributes":[{"key":"code","value":{"intValue":"200"}},{"key":"method","value":{"stringValue":"post"}}],"startTimeUnixNano":"1395066363000000001","timeUnixNano":"1395066363000000123","asDouble":1025},{"attributes":[{"key":"code","value":{"intValue":"200"}},{"key":"method","value":{"stringValue":"post"}}],"startTimeUnixNano":"1395066363000000001","timeUnixNano":"1395066363000000123","asDouble":1027},{"attributes":[{"key":"code","value":{"intValue":"400"}},{"key":"method","value":{"stringValue":"post"}}],"startTimeUnixNano":"1395066363000000001","timeUnixNano":"1395066363000000123","asDouble":3}],"aggregationTemporality":2,"isMonotonic":true}},{"name":"http_requests_bytes","description":"The total bytes of HTTP requests","sum":{"dataPoints":[{"attributes":[{"key":"code","value":{"intValue":"200"}},{"key":"method","value":{"stringValue":"post"}}],"startTimeUnixNano":"1395066363000000001","timeUnixNano":"1395066363000000123","asDouble":1025}],"aggregationTemporality":2,"isMonotonic":true}}]}]}]}`
		var mockReq = pmetricotlp.NewExportRequest()
		err := mockReq.UnmarshalJSON([]byte(mockMetricData))
		require.NoError(t, err)

		return mockReq
	}

	req := genMetricData()
	buf, _ := req.MarshalProto()

	reader := bufio.NewReader(bytes.NewReader(buf))
	octx, err := opentelemetry.GetOtelContext(reader)
	require.NoError(t, err)
	var w = &writer{}
	octx.Writer = w
	ctx := context.Background()
	require.True(t, octx.Read())
	err = octx.WriteMetrics(ctx)
	assert.NoError(t, err)
	assert.NoError(t, octx.Error())
}

func Test_otelContext_EnqueuePoint_sum2(t *testing.T) {
	var genMetricData = func() pmetricotlp.ExportRequest {
		var mockMetricData = `{"resourceMetrics":[{"resource":{"attributes":[{"key":"container.name","value":{"stringValue":"42"}}]},"scopeMetrics":[{"scope":{"name":"My Library","version":"latest"},"metrics":[{"name":"http_requests_total","description":"The total number of HTTP requests","sum":{"dataPoints":[{"attributes":[{"key":"code","value":{"intValue":"200"}},{"key":"method","value":{"stringValue":"post"}}],"startTimeUnixNano":"1395066363000000001","timeUnixNano":"1395066363000000123","asDouble":1025},{"attributes":[{"key":"code","value":{"intValue":"200"}},{"key":"method","value":{"stringValue":"post"}}],"startTimeUnixNano":"1395066363000000001","timeUnixNano":"1395066363000000123","asDouble":1027},{"attributes":[{"key":"code","value":{"intValue":"400"}},{"key":"method","value":{"stringValue":"post"}}],"startTimeUnixNano":"1395066363000000001","timeUnixNano":"1395066363000000123","asDouble":3}],"aggregationTemporality":1,"isMonotonic":true}},{"name":"http_requests_bytes","description":"The total bytes of HTTP requests","sum":{"dataPoints":[{"attributes":[{"key":"code","value":{"intValue":"200"}},{"key":"method","value":{"stringValue":"post"}}],"startTimeUnixNano":"1395066363000000001","timeUnixNano":"1395066363000000123","asDouble":1025}],"aggregationTemporality":1,"isMonotonic":true}}]}]}]}`
		var mockReq = pmetricotlp.NewExportRequest()
		err := mockReq.UnmarshalJSON([]byte(mockMetricData))
		require.NoError(t, err)

		return mockReq
	}

	req := genMetricData()
	buf, _ := req.MarshalProto()

	reader := bufio.NewReader(bytes.NewReader(buf))
	octx, err := opentelemetry.GetOtelContext(reader)
	require.NoError(t, err)
	var w = &writer{}
	octx.Writer = w
	ctx := context.Background()
	require.True(t, octx.Read())
	err = octx.WriteMetrics(ctx)
	assert.NoError(t, err)
	assert.NoError(t, octx.Error())
}

func Test_otelContext_EnqueuePoint_histogram(t *testing.T) {
	var genMetricData = func() pmetricotlp.ExportRequest {
		var mockMetricData = `{"resourceMetrics":[{"resource":{"attributes":[{"key":"container.name","value":{"stringValue":"42"}}]},"scopeMetrics":[{"scope":{"name":"My Library","version":"latest"},"metrics":[{"name":"http_request_duration_seconds","description":"A histogram of the request duration","histogram":{"dataPoints":[{"attributes":[{"key":"code","value":{"intValue":"200"}},{"key":"method","value":{"stringValue":"post"}}],"startTimeUnixNano":"1395066363000000001","timeUnixNano":"1395066363000000123","count":"144320","sum":53423,"bucketCounts":["24054","9390","66948","28997","4599","10332"],"explicitBounds":[0.05,0.1,0.2,0.5,1],"min":0,"max":100}],"aggregationTemporality":2}}]}]}]}`
		var mockReq = pmetricotlp.NewExportRequest()
		err := mockReq.UnmarshalJSON([]byte(mockMetricData))
		require.NoError(t, err)

		return mockReq
	}

	req := genMetricData()
	buf, _ := req.MarshalProto()

	reader := bufio.NewReader(bytes.NewReader(buf))
	octx, err := opentelemetry.GetOtelContext(reader)
	require.NoError(t, err)
	var w = &writer{}
	octx.Writer = w
	ctx := context.Background()
	require.True(t, octx.Read())
	err = octx.WriteMetrics(ctx)
	assert.NoError(t, err)
	assert.NoError(t, octx.Error())
}

func Test_otelContext_EnqueuePoint_histogram2(t *testing.T) {
	var genMetricData = func() pmetricotlp.ExportRequest {
		var mockMetricData = `{"resourceMetrics":[{"resource":{"attributes":[{"key":"container.name","value":{"stringValue":"42"}}]},"scopeMetrics":[{"scope":{"name":"My Library","version":"latest"},"metrics":[{"name":"http_request_duration_seconds","description":"A histogram of the request duration","histogram":{"dataPoints":[{"attributes":[{"key":"code","value":{"intValue":"200"}},{"key":"method","value":{"stringValue":"post"}}],"startTimeUnixNano":"1395066363000000001","timeUnixNano":"1395066363000000123","count":"144320","sum":53423,"bucketCounts":["24054","9390","66948","28997","4599","10332"],"explicitBounds":[0.05,0.1,0.2,0.5,1],"min":0,"max":100}],"aggregationTemporality":1}}]}]}]}`
		var mockReq = pmetricotlp.NewExportRequest()
		err := mockReq.UnmarshalJSON([]byte(mockMetricData))
		require.NoError(t, err)

		return mockReq
	}

	req := genMetricData()
	buf, _ := req.MarshalProto()

	reader := bufio.NewReader(bytes.NewReader(buf))
	octx, err := opentelemetry.GetOtelContext(reader)
	require.NoError(t, err)
	var w = &writer{}
	octx.Writer = w
	ctx := context.Background()
	require.True(t, octx.Read())
	err = octx.WriteMetrics(ctx)
	assert.NoError(t, err)
	assert.NoError(t, octx.Error())
}

func Test_otelContext_EnqueuePoint_histogram3(t *testing.T) {
	var genMetricData = func() pmetricotlp.ExportRequest {
		var mockMetricData = `{"resourceMetrics":[{"resource":{"attributes":[{"key":"container.name","value":{"stringValue":"42"}}]},"scopeMetrics":[{"scope":{"name":"My Library","version":"latest"},"metrics":[{"name":"http_request_duration_seconds","description":"A histogram of the request duration","histogram":{"dataPoints":[{"attributes":[{"key":"code","value":{"intValue":"200"}},{"key":"method","value":{"stringValue":"post"}}],"startTimeUnixNano":"1395066363000000001","timeUnixNano":"1395066363000000123","count":"144320","sum":53423,"bucketCounts":["24054","9390","66948","28997","4599"],"explicitBounds":[0.05,0.1,0.2,0.5,1]}],"aggregationTemporality":2}}]}]}]}`
		var mockReq = pmetricotlp.NewExportRequest()
		err := mockReq.UnmarshalJSON([]byte(mockMetricData))
		require.NoError(t, err)

		return mockReq
	}

	req := genMetricData()
	buf, _ := req.MarshalProto()

	reader := bufio.NewReader(bytes.NewReader(buf))
	octx, err := opentelemetry.GetOtelContext(reader)
	require.NoError(t, err)
	var w = &writer{}
	octx.Writer = w
	ctx := context.Background()
	require.True(t, octx.Read())
	err = octx.WriteMetrics(ctx)
	assert.NoError(t, err)
	assert.NoError(t, octx.Error())
}

func Test_otelContext_EnqueuePoint_histogram4(t *testing.T) {
	var genMetricData = func() pmetricotlp.ExportRequest {
		var mockMetricData = `{"resourceMetrics":[{"resource":{"attributes":[{"key":"container.name","value":{"stringValue":"42"}}]},"scopeMetrics":[{"scope":{"name":"My Library","version":"latest"},"metrics":[{"name":"http_request_duration_seconds","description":"A histogram of the request duration","histogram":{"dataPoints":[{"attributes":[{"key":"code","value":{"intValue":"200"}},{"key":"method","value":{"stringValue":"post"}}],"startTimeUnixNano":"1395066363000000001","timeUnixNano":"1395066363000000123","count":"144320","sum":53423,"bucketCounts":["24054","9390","66948","28997","4599"],"explicitBounds":[0.05,0.1,0.2,0.5,1]}],"aggregationTemporality":1}}]}]}]}`
		var mockReq = pmetricotlp.NewExportRequest()
		err := mockReq.UnmarshalJSON([]byte(mockMetricData))
		require.NoError(t, err)

		return mockReq
	}

	req := genMetricData()
	buf, _ := req.MarshalProto()

	reader := bufio.NewReader(bytes.NewReader(buf))
	octx, err := opentelemetry.GetOtelContext(reader)
	require.NoError(t, err)
	var w = &writer{}
	octx.Writer = w
	ctx := context.Background()
	require.True(t, octx.Read())
	err = octx.WriteMetrics(ctx)
	assert.NoError(t, err)
	assert.NoError(t, octx.Error())
}

func Test_otelContext_EnqueuePoint_summary(t *testing.T) {
	var genMetricData = func() pmetricotlp.ExportRequest {
		var mockMetricData = `{"resourceMetrics":[{"resource":{"attributes":[{"key":"container.name","value":{"stringValue":"42"}}]},"scopeMetrics":[{"scope":{"name":"My Library","version":"latest"},"metrics":[{"name":"rpc_duration_seconds","description":"A summary of the RPC duration in seconds","summary":{"dataPoints":[{"attributes":[{"key":"code","value":{"intValue":"200"}},{"key":"method","value":{"stringValue":"post"}}],"startTimeUnixNano":"1395066363000000001","timeUnixNano":"1395066363000000123","count":"2693","sum":17560473,"quantileValues":[{"quantile":0.01,"value":3102},{"quantile":0.05,"value":3272},{"quantile":0.5,"value":4773},{"quantile":0.9,"value":9001},{"quantile":0.99,"value":76656}]}]}}]}]}]}`
		var mockReq = pmetricotlp.NewExportRequest()
		err := mockReq.UnmarshalJSON([]byte(mockMetricData))
		require.NoError(t, err)

		return mockReq
	}

	req := genMetricData()
	buf, _ := req.MarshalProto()

	reader := bufio.NewReader(bytes.NewReader(buf))
	octx, err := opentelemetry.GetOtelContext(reader)
	require.NoError(t, err)
	var w = &writer{}
	octx.Writer = w
	ctx := context.Background()
	require.True(t, octx.Read())
	err = octx.WriteMetrics(ctx)
	assert.NoError(t, err)
	assert.NoError(t, octx.Error())
}

func Test_otelContext_EnqueuePoint_ErrWithInValidAttributes(t *testing.T) {
	reader := bufio.NewReader(bytes.NewReader([]byte{}))
	octx, err := opentelemetry.GetOtelContext(reader)
	require.NoError(t, err)
	var w = &writer{}
	octx.Writer = w
	ctx := context.Background()
	tags := map[string]string{"k1": "v1"}
	fields := map[string]interface{}{"attributes": "xxx"}
	err = octx.EnqueuePoint(ctx, "mst", tags, fields, time.Now(), common.InfluxMetricValueTypeGauge)
	assert.Error(t, err)
}
