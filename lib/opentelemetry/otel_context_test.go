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

package opentelemetry

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/influxdata/influxdb-observability/common"
	"github.com/influxdata/influxdb-observability/otel2influx"
	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestOtelConext(t *testing.T) {
	var buf = []byte{1, 2, 3}
	var o []*OtelContext

	for i := 0; i <= cpu.GetCpuNum()*2; i++ {
		reader := bufio.NewReader(bytes.NewReader(buf))
		octx, _ := GetOtelContext(reader)
		o = append(o, octx)
	}

	for i := 0; i <= cpu.GetCpuNum()*2; i++ {
		PutOtelContext(o[i])
	}

	// get from chan
	for i := 0; i <= cpu.GetCpuNum(); i++ {
		reader := bufio.NewReader(bytes.NewReader(buf))
		ctx, err := GetOtelContext(reader)
		assert.NoError(t, err)
		ctx.Read()
		assert.Equal(t, ctx.ReqBuf.B, buf)
	}
	// get from pool
	for i := 0; i <= cpu.GetCpuNum(); i++ {
		reader := bufio.NewReader(bytes.NewReader(buf))
		ctx, err := GetOtelContext(reader)
		assert.NoError(t, err)
		ctx.Read()
		assert.Equal(t, ctx.ReqBuf.B, buf)
	}
}

func TestOtelLogger_Debug(t *testing.T) {
	observedZapCore, observedLog := observer.New(zap.DebugLevel)
	observedLogger := zap.New(observedZapCore)
	l := logger.NewLogger(errno.ModuleUnknown)
	l.SetZapLogger(observedLogger)
	log := otelLogger{
		logger: l,
	}
	log.Debug("test")
	assert.Equal(t, 0, observedLog.Len())
}

func TestOtelConext_Error(t *testing.T) {
	octx := OtelContext{err: io.EOF}
	assert.Nil(t, octx.Error())

	err := errors.New("test error")
	octx.err = err
	assert.Equal(t, err, octx.Error())
}

type mockInfluxWriter struct {
}

func (w *mockInfluxWriter) NewBatch() otel2influx.InfluxWriterBatch {
	return w
}

func (w *mockInfluxWriter) EnqueuePoint(ctx context.Context, measurement string, tags map[string]string, fields map[string]interface{}, ts time.Time, vType common.InfluxMetricValueType) error {
	return errors.New("test error")
}

func (w *mockInfluxWriter) WriteBatch(ctx context.Context) error {
	return nil
}

func TestOtlpMetricWriter_WriteHistogram_Err(t *testing.T) {
	log := otelLogger{
		logger: logger.NewLogger(errno.ModuleUnknown),
	}
	writer := OtlpMetricWriter{
		Writer: &mockInfluxWriter{},
		Logger: &log,
	}
	metrics := pmetric.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()
	ilMetrics := rm.ScopeMetrics().AppendEmpty()
	m := ilMetrics.Metrics().AppendEmpty()
	m.SetName("test_metric2")
	m.SetEmptyHistogram()
	dpHis := m.Histogram().DataPoints().AppendEmpty()
	dpHis.Attributes().PutInt("", 0)
	dpHis.BucketCounts().FromRaw([]uint64{24054, 9390, 66948, 28997, 4599})
	dpHis.ExplicitBounds().FromRaw([]float64{0.05, 0.1, 0.2})

	err := writer.WriteMetrics(context.Background(), metrics)
	assert.Error(t, err)
}

func TestOtlpMetricWriter_WriteGauge_Err(t *testing.T) {
	log := otelLogger{
		logger: logger.NewLogger(errno.ModuleUnknown),
	}
	writer := OtlpMetricWriter{
		Writer: &mockInfluxWriter{},
		Logger: &log,
	}
	metrics := pmetric.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()
	ilMetrics := rm.ScopeMetrics().AppendEmpty()
	m := ilMetrics.Metrics().AppendEmpty()
	m.SetName("test_metric")
	m.SetEmptyGauge()
	dp := m.Gauge().DataPoints().AppendEmpty()
	dp.Attributes().PutInt("", 0)

	err := writer.WriteMetrics(context.Background(), metrics)
	assert.Error(t, err)
}

func TestOtlpMetricWriter_WriteSum_Err(t *testing.T) {
	log := otelLogger{
		logger: logger.NewLogger(errno.ModuleUnknown),
	}
	writer := OtlpMetricWriter{
		Writer: &mockInfluxWriter{},
		Logger: &log,
	}
	metrics := pmetric.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()
	ilMetrics := rm.ScopeMetrics().AppendEmpty()
	m := ilMetrics.Metrics().AppendEmpty()
	m.SetName("test_metric")
	m.SetEmptySum()
	dp := m.Sum().DataPoints().AppendEmpty()
	dp.Attributes().PutInt("", 0)
	dp.SetIntValue(1)
	dp = m.Sum().DataPoints().AppendEmpty()
	dp.Attributes().PutInt("", 0)

	err := writer.WriteMetrics(context.Background(), metrics)
	assert.Error(t, err)
}

func TestOtlpMetricWriter_WriteSummary_Err(t *testing.T) {
	log := otelLogger{
		logger: logger.NewLogger(errno.ModuleUnknown),
	}
	writer := OtlpMetricWriter{
		Writer: &mockInfluxWriter{},
		Logger: &log,
	}
	metrics := pmetric.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()
	ilMetrics := rm.ScopeMetrics().AppendEmpty()
	m := ilMetrics.Metrics().AppendEmpty()
	m.SetName("test_metric")
	m.SetEmptySummary()
	dp := m.Summary().DataPoints().AppendEmpty()
	dp.Attributes().PutInt("", 0)
	q := dp.QuantileValues().AppendEmpty()
	q.SetQuantile(1)
	q.SetValue(1)

	err := writer.WriteMetrics(context.Background(), metrics)
	assert.Error(t, err)
}
