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
	"io"

	"github.com/influxdata/influxdb-observability/common"
	"github.com/influxdata/influxdb-observability/otel2influx"
	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/pool"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	semconv "go.opentelemetry.io/collector/semconv/v1.16.0"
)

var otelContextPool = pool.NewUnionPool[OtelContext](cpu.GetCpuNum(), pool.DefaultMaxEleMemSize, pool.DefaultMaxLocalEleMemSize, NewOtelContext)

func GetOtelContext(r io.Reader) (*OtelContext, error) {
	octx := otelContextPool.Get()
	err := octx.Init(r)
	return octx, err
}

func PutOtelContext(octx *OtelContext) {
	octx.reset()
	otelContextPool.Put(octx)
}

func (octx *OtelContext) MemSize() int {
	return cap(octx.ReqBuf.B) + cap(octx.rowsBuff)*util.Int64SizeBytes
}

func (octx *OtelContext) Instance() *OtelContext {
	return octx
}

func (octx *OtelContext) Init(r io.Reader) error {
	octx.br.Reset(r)
	log := &otelLogger{
		logger: logger.NewLogger(errno.ModuleHTTP),
	}
	var err error
	traceConfig := &otel2influx.OtelTracesToLineProtocolConfig{
		Logger: log,
		Writer: octx,
		SpanDimensions: []string{
			semconv.AttributeServiceName,
			common.AttributeSpanName,
		},
	}
	if octx.PtraceWriter, err = otel2influx.NewOtelTracesToLineProtocol(traceConfig); err != nil {
		return err
	}
	octx.PmetricWriter = &OtlpMetricWriter{Writer: octx, Logger: log}
	logConfig := &otel2influx.OtelLogsToLineProtocolConfig{
		Logger: log,
		Writer: octx,
		LogRecordDimensions: []string{
			semconv.AttributeServiceName,
			common.AttributeSpanName,
		},
	}
	if octx.PlogWriter, err = otel2influx.NewOtelLogsToLineProtocol(logConfig); err != nil {
		return err
	}
	return nil
}

func NewOtelContext() *OtelContext {
	return &OtelContext{
		rowsBuff: make(influx.Rows, 0, 20),
		logger:   logger.NewLogger(errno.ModuleHTTP),
	}
}

type otelLogger struct {
	logger *logger.Logger
}

func (l *otelLogger) Debug(msg string, kv ...interface{}) {
	l.logger.Debug(msg)
}
