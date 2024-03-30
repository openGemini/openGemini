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

package opentelemetry

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/influxdata/influxdb-observability/common"
	"github.com/influxdata/influxdb-observability/otel2influx"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	semconv "go.opentelemetry.io/collector/semconv/v1.16.0"
	"go.uber.org/zap"
)

type TracesWriter interface {
	WriteTraces(ctx context.Context, md ptrace.Traces, w otel2influx.InfluxWriter) error
}

type MetricsWriter interface {
	WriteMetrics(ctx context.Context, md pmetric.Metrics, w otel2influx.InfluxWriter) error
}

type LogsWriter interface {
	WriteLogs(ctx context.Context, md plog.Logs, w otel2influx.InfluxWriter) error
}

type InfluxRowsWriter interface {
	RetryWritePointRows(database, retentionPolicy string, points []influx.Row) error
}

type otelConext struct {
	// read from request
	br      bufio.Reader
	ReqBuf  []byte
	ptrace  ptraceotlp.Request
	pmetric pmetricotlp.Request
	plog    plogotlp.Request
	err     error // this is parse error, http code: 400

	// write to rows
	PtraceWriter    TracesWriter
	PmetricWriter   MetricsWriter
	PlogWriter      LogsWriter
	Writer          InfluxRowsWriter
	Database        string
	RetentionPolicy string
	rowsBuff        influx.Rows // reuse influx.Rows every write request

	logger *logger.Logger
}

func (octx *otelConext) Read(contentLength int) bool {
	if octx.err != nil {
		return false
	}
	if contentLength > cap(octx.ReqBuf) {
		octx.ReqBuf = append(octx.ReqBuf, make([]byte, contentLength)...)
	}
	octx.ReqBuf = octx.ReqBuf[:contentLength]
	n, err := io.ReadFull(&octx.br, octx.ReqBuf)
	octx.ReqBuf = octx.ReqBuf[:n]
	if err != nil {
		if err != io.EOF {
			octx.err = fmt.Errorf("cannot read influx line protocol data: %w", octx.err)
			return false
		}
		return true
	}
	return true
}

// Error returns the read or paser error, http code: 400
func (octx *otelConext) Error() error {
	if octx.err == io.EOF {
		return nil
	}
	return octx.err
}

func (octx *otelConext) reset() {
	octx.ReqBuf = octx.ReqBuf[:0]
	octx.err = nil

	octx.Database = ""
	octx.RetentionPolicy = ""
	octx.rowsBuff = octx.rowsBuff[:]
}

func (octx *otelConext) WriteTraces(ctx context.Context) error {
	// TODO: only support proto, next step is json
	octx.ptrace = ptraceotlp.NewRequest() // new every time
	if err := octx.ptrace.UnmarshalProto(octx.ReqBuf); err != nil {
		octx.err = err
		return nil
	}

	err := octx.PtraceWriter.WriteTraces(ctx, octx.ptrace.Traces(), octx)
	if err != nil {
		if strings.Contains(err.Error(), "failed to convert OTLP span to line protocol") {
			octx.err = err
			return nil
		}
		return err
	}
	return nil
}

func (octx *otelConext) WriteMetrics(ctx context.Context) error {
	// TODO: only support proto, next step is json
	octx.pmetric = pmetricotlp.NewRequest() // new every time
	if err := octx.pmetric.UnmarshalProto(octx.ReqBuf); err != nil {
		octx.err = err
		return nil
	}

	err := octx.PmetricWriter.WriteMetrics(ctx, octx.pmetric.Metrics(), octx)
	if err != nil {
		if strings.Contains(err.Error(), "failed to convert OTLP metric to line protocol") {
			octx.err = err
			return nil
		}
		return err
	}
	return nil
}

func (octx *otelConext) WriteLogs(ctx context.Context) error {
	// TODO: only support proto, next step is json
	octx.plog = plogotlp.NewRequest() // new every time
	if err := octx.plog.UnmarshalProto(octx.ReqBuf); err != nil {
		octx.err = err
		return nil
	}

	err := octx.PlogWriter.WriteLogs(ctx, octx.plog.Logs(), octx)
	if err != nil {
		if strings.Contains(err.Error(), "failed to convert OTLP log record to line protocol") {
			octx.err = err
			return nil
		}
		return err
	}
	return nil
}

// NewBatch impl otel2influx.InfluxWriter
func (octx *otelConext) NewBatch() otel2influx.InfluxWriter {
	return octx
}

// WritePoint impl otel2influx.InfluxWriterBatch
func (octx *otelConext) WritePoint(ctx context.Context, measurement string, tags map[string]string, fields map[string]interface{}, ts time.Time, vType common.InfluxMetricValueType) error {
	r := influx.Row{
		Timestamp: ts.UnixNano(),
		Name:      strings.ReplaceAll(measurement, "/", "-"),
	}
	for k, v := range tags {
		r.Tags = append(r.Tags, influx.Tag{
			Key:   k,
			Value: v,
		})
	}
	for k, v := range fields {
		k = getAttributeName(k)
		switch value := v.(type) {
		case float64:
			r.Fields = append(r.Fields,
				influx.Field{
					Type:     influx.Field_Type_Float,
					Key:      k,
					NumValue: value,
				})
		case int64:
			r.Fields = append(r.Fields,
				influx.Field{
					Type:     influx.Field_Type_Int,
					Key:      k,
					NumValue: float64(value),
				},
			)
		case uint64:
			r.Fields = append(r.Fields,
				influx.Field{
					Type:     influx.Field_Type_Int,
					Key:      k,
					NumValue: float64(value),
				},
			)

		case string:
			r.Fields = append(r.Fields,
				influx.Field{
					Type:     influx.Field_Type_String,
					Key:      k,
					StrValue: value,
				},
			)
		case bool:
			vb := float64(1)
			if !value {
				vb = float64(0)
			}
			r.Fields = append(r.Fields,
				influx.Field{
					Type:     influx.Field_Type_Boolean,
					Key:      k,
					NumValue: vb,
				},
			)
		case []interface{}:
			val := fmt.Sprintf("%s", v)
			r.Fields = append(r.Fields,
				influx.Field{
					Type:     influx.Field_Type_String,
					Key:      k,
					StrValue: val,
				},
			)
		default:
			octx.logger.Error("field type is not supported", zap.String("type", fmt.Sprintf("%T", v)), zap.Any("value", v))
			return fmt.Errorf("field type is not supported, type: %T, value: %v", v, v)
		}
	}

	return octx.Writer.RetryWritePointRows(octx.Database, octx.RetentionPolicy, influx.Rows{r})
}

// FlushBatch impl otel2influx.InfluxWriterBatch
// TODO: new version
func (octx *otelConext) FlushBatch(ctx context.Context) error {
	defer func() {
		octx.rowsBuff = octx.rowsBuff[:0]
	}()
	return octx.Writer.RetryWritePointRows(octx.Database, octx.RetentionPolicy, octx.rowsBuff)
}

var AttributeTelemetryKeys = map[string]struct{}{
	semconv.AttributeTelemetrySDKName:     {},
	semconv.AttributeTelemetrySDKLanguage: {},
	semconv.AttributeTelemetrySDKVersion:  {},
	semconv.AttributeTelemetryAutoVersion: {},
}

func getAttributeName(name string) string {
	if _, ok := AttributeTelemetryKeys[name]; ok {
		return name + "_1"
	}
	return name
}
