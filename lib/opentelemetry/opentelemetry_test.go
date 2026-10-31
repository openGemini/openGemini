/*
Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.

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
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/openGemini/openGemini/lib/opentelemetry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

func TestInitTrace(t *testing.T) {
	wg := sync.WaitGroup{}
	wg.Add(1)
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer wg.Done()
		assert.Equal(t, "/api/v1/otlp/trace/traces", r.URL.Path)
	}))
	defer mockServer.Close()

	url := strings.TrimPrefix(mockServer.URL, "http://")
	_, err := opentelemetry.InitTrace(url, false, "trace", 1.0, "service")
	require.NoError(t, err)

	tracer := otel.Tracer("castor_sql")
	_, span := tracer.Start(context.Background(), "castor_reduce")
	span.End()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for trace to finish")
	}
}

func TestInitTrace_WithHttps(t *testing.T) {
	_, err := opentelemetry.InitTrace("test", true, "trace", 1.0, "service")
	require.NoError(t, err)
}

func TestInitTrace_TracingRatioError(t *testing.T) {
	_, err := opentelemetry.InitTrace("test", true, "trace", 1.1, "service")
	assert.Error(t, err)
}

func TestInjectContextToRecord(t *testing.T) {
	_, err := opentelemetry.InitTrace("test", false, "trace", 1.0, "service")
	require.NoError(t, err)

	record := buildTestRecord()
	tracer := otel.Tracer("castor_sql")
	ctx, span := tracer.Start(context.Background(), "castor_reduce")
	toRecord := opentelemetry.InjectContextToRecord(record, ctx)
	defer span.End()
	traceId := span.SpanContext().TraceID().String()

	h, b := toRecord.Schema().Metadata().GetValue(opentelemetry.TraceContextHeader)
	assert.True(t, b)
	fmt.Println(h)

	propagator := otel.GetTextMapPropagator()
	carrier := make(map[string]string)
	err = json.Unmarshal([]byte(h), &carrier)
	require.NoError(t, err)
	extract := propagator.Extract(context.Background(), propagation.MapCarrier(carrier))

	ctx, subSpan := tracer.Start(extract, "test")

	assert.Equal(t, traceId, subSpan.SpanContext().TraceID().String())
}

func buildTestRecord() arrow.Record {
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "name", Type: arrow.BinaryTypes.String},
			{Name: "age", Type: arrow.PrimitiveTypes.Int32},
			{Name: "active", Type: arrow.FixedWidthTypes.Boolean},
		},
		nil,
	)

	// 创建数组构建器
	nameBuilder := array.NewStringBuilder(pool)
	defer nameBuilder.Release()

	ageBuilder := array.NewInt32Builder(pool)
	defer ageBuilder.Release()

	activeBuilder := array.NewBooleanBuilder(pool)
	defer activeBuilder.Release()

	nameBuilder.AppendValues([]string{"Alice", "Bob", "Charlie", "Diana"}, nil)
	ageBuilder.AppendValues([]int32{30, 25, 35, 28}, nil)
	activeBuilder.AppendValues([]bool{true, false, true, true}, nil)

	nameArray := nameBuilder.NewStringArray()

	ageArray := ageBuilder.NewInt32Array()

	activeArray := activeBuilder.NewBooleanArray()

	record := array.NewRecord(schema, []arrow.Array{nameArray, ageArray, activeArray}, 4)
	return record
}
