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

package opentelemetry_test

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/influxdata/influxdb-observability/otel2influx"
	"github.com/openGemini/openGemini/lib/opentelemetry"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
)

type writer struct{}

func (writer) RetryWritePointRows(database, retentionPolicy string, points []influx.Row) error {
	// fmt.Println(database, retentionPolicy, points)
	return nil
}

func TestOtelConext_WriteTraces_OK(t *testing.T) {
	var genTraceData = func() ptraceotlp.Request {
		var mockTraceData = `{"resourceSpans":[{"resource":{},"scopeSpans":[{"scope":{},"spans":[{"traceId":"01000000000000000000000000000000","spanId":"0200000000000000","parentSpanId":"","name":"test_span","startTimeUnixNano":"1678898440000000000","endTimeUnixNano":"1678898440000001000","attributes":[{"key":"cpu","value":{"intValue":"1"}},{"key":"event","value":{"stringValue":"test"}}],"status":{}}]}]}]}`

		stringTime := "2023-03-15 16:40:40"
		loc, _ := time.LoadLocation("UTC")
		theTime, _ := time.ParseInLocation("2006-01-02 15:04:05", stringTime, loc)

		req := ptraceotlp.NewRequest()
		span1 := req.Traces().ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
		span1.SetName("test_span")
		span1.SetTraceID(pcommon.NewTraceID([16]byte{1}))
		span1.SetSpanID(pcommon.NewSpanID([8]byte{2}))
		span1.SetParentSpanID(pcommon.NewSpanID([8]byte{0}))
		span1.SetStartTimestamp(pcommon.Timestamp(theTime.UnixNano()))
		span1.SetEndTimestamp(pcommon.Timestamp(theTime.UnixNano() + 1000))
		span1.Attributes().InsertInt("cpu", 1)
		span1.Attributes().InsertString("event", "test")

		//	got, _ := req.MarshalJSON()
		//	fmt.Println(string(got))

		var mockReq = ptraceotlp.NewRequest()
		err := mockReq.UnmarshalJSON([]byte(mockTraceData))
		require.NoError(t, err)

		require.Equal(t, req, mockReq)
		return req
	}

	req := genTraceData()
	buf, _ := req.MarshalProto()

	reader := bufio.NewReader(bytes.NewReader(buf))
	octx := opentelemetry.GetOtelContext(reader)
	// do not put back to pool

	require.True(t, octx.Read(len(buf)))

	ctx := context.Background()
	var w = &writer{}
	octx.Writer = w
	octx.Database = "db0"
	octx.RetentionPolicy = "rp0"
	err := octx.WriteTraces(ctx)
	require.NoError(t, err)
}

type mockTraceWriterError struct{}

func (mockTraceWriterError) WriteTraces(ctx context.Context, md ptrace.Traces, w otel2influx.InfluxWriter) error {
	return errors.New("failed to convert OTLP span to line protocol")
}

func TestOtelConext_WriteTraces_WriteError(t *testing.T) {
	reader := bufio.NewReader(bytes.NewReader([]byte{}))
	octx := opentelemetry.GetOtelContext(reader)
	// do not put back to pool

	// mock write error
	writeError := &mockTraceWriterError{}
	octx.PtraceWriter = writeError

	require.True(t, octx.Read(0))

	ctx := context.Background()
	var w = &writer{}
	octx.Writer = w
	octx.Database = "db0"
	octx.RetentionPolicy = "rp0"
	err := octx.WriteTraces(ctx)
	require.NoError(t, err)
	require.EqualError(t, octx.Error(), "failed to convert OTLP span to line protocol")
}

func TestOtelConext_WriteMetrics_OK(t *testing.T) {
	var genMetricData = func() pmetricotlp.Request {
		var mockMetricData = `{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{},"metrics":[{"name":"test_metric","description":"this is description","unit":"ms","sum":{"dataPoints":[{"startTimeUnixNano":"1678812040000000000","timeUnixNano":"1678898440000000000","asInt":"1314520"}]}}]}]}]}`

		//		stringTime := "2023-03-15 16:40:40"
		//		loc, _ := time.LoadLocation("UTC")
		//		theTime, _ := time.ParseInLocation("2006-01-02 15:04:05", stringTime, loc)
		//
		//		req := pmetricotlp.NewRequest()
		//		metric1 := req.Metrics().ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
		//		metric1.SetName("test_metric")
		//		metric1.SetDescription("this is description")
		//		metric1.SetUnit("ms")
		//		data1 := metric1.Sum().DataPoints().AppendEmpty()
		//		data1.SetIntVal(1314520)
		//		data1.SetStartTimestamp(pcommon.Timestamp(theTime.Add(-24 * time.Hour).UnixNano()))
		//		data1.SetTimestamp(pcommon.Timestamp(theTime.UnixNano()))

		//	got, _ := req.MarshalJSON()
		//	fmt.Println(string(got))

		var mockReq = pmetricotlp.NewRequest()
		err := mockReq.UnmarshalJSON([]byte(mockMetricData))
		require.NoError(t, err)

		return mockReq
	}

	req := genMetricData()
	buf, _ := req.MarshalProto()

	reader := bufio.NewReader(bytes.NewReader(buf))
	octx := opentelemetry.GetOtelContext(reader)
	// do not put back to pool
	require.True(t, octx.Read(len(buf)))

	ctx := context.Background()
	var w = &writer{}
	octx.Writer = w
	octx.Database = "db0"
	octx.RetentionPolicy = "rp0"
	err := octx.WriteMetrics(ctx)
	require.NoError(t, err)
}

type mockMetricWriterError struct{}

func (mockMetricWriterError) WriteMetrics(ctx context.Context, md pmetric.Metrics, w otel2influx.InfluxWriter) error {
	return errors.New("failed to convert OTLP metric to line protocol")
}

func TestOtelConext_WriteMetrics_WriteError(t *testing.T) {
	reader := bufio.NewReader(bytes.NewReader([]byte{}))
	octx := opentelemetry.GetOtelContext(reader)
	// do not put back to pool

	// mock write error
	writeError := &mockMetricWriterError{}
	octx.PmetricWriter = writeError

	require.True(t, octx.Read(0))

	ctx := context.Background()
	var w = &writer{}
	octx.Writer = w
	octx.Database = "db0"
	octx.RetentionPolicy = "rp0"
	err := octx.WriteMetrics(ctx)
	require.NoError(t, err)
	require.EqualError(t, octx.Error(), "failed to convert OTLP metric to line protocol")
}
