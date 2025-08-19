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
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
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
	WriteTraces(ctx context.Context, md ptrace.Traces) error
}

type MetricsWriter interface {
	WriteMetrics(ctx context.Context, md pmetric.Metrics) error
}

type LogsWriter interface {
	WriteLogs(ctx context.Context, md plog.Logs) error
}

type InfluxRowsWriter interface {
	RetryWritePointRows(database, retentionPolicy string, points []influx.Row) error
}

type OtelContext struct {
	// read from request
	br      bufio.Reader
	ReqBuf  bytesutil.ByteBuffer
	ptrace  ptraceotlp.ExportRequest
	pmetric pmetricotlp.ExportRequest
	plog    plogotlp.ExportRequest
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

func (octx *OtelContext) Read() bool {
	if octx.err != nil {
		return false
	}
	reqLen, err := octx.ReqBuf.ReadFrom(&octx.br)
	octx.ReqBuf.B = octx.ReqBuf.B[:reqLen]
	if err != nil {
		if err != io.EOF {
			octx.err = fmt.Errorf("cannot read influx line protocol data: %w", err)
		}
		return false
	}
	return true
}

// Error returns the read or parser error, http code: 400
func (octx *OtelContext) Error() error {
	if octx.err == io.EOF {
		return nil
	}
	return octx.err
}

func (octx *OtelContext) reset() {
	octx.ReqBuf.Reset()
	octx.err = nil

	octx.Database = ""
	octx.RetentionPolicy = ""
	octx.rowsBuff = octx.rowsBuff[:]
}

func (octx *OtelContext) WriteTraces(ctx context.Context) error {
	octx.ptrace = ptraceotlp.NewExportRequest() // new every time
	if err := octx.ptrace.UnmarshalProto(octx.ReqBuf.B); err != nil {
		octx.err = err
		return nil
	}

	err := octx.PtraceWriter.WriteTraces(ctx, octx.ptrace.Traces())
	if err != nil {
		if strings.Contains(err.Error(), "failed to convert OTLP span to line protocol") {
			octx.err = err
			return nil
		}
		return err
	}
	return nil
}

func (octx *OtelContext) WriteMetrics(ctx context.Context) error {
	octx.pmetric = pmetricotlp.NewExportRequest() // new every time
	if err := octx.pmetric.UnmarshalProto(octx.ReqBuf.B); err != nil {
		octx.err = err
		return nil
	}

	err := octx.PmetricWriter.WriteMetrics(ctx, octx.pmetric.Metrics())
	if err != nil {
		if strings.Contains(err.Error(), "failed to convert OTLP metric to line protocol") {
			octx.err = err
			return nil
		}
		return err
	}
	return nil
}

func (octx *OtelContext) WriteLogs(ctx context.Context) error {
	octx.plog = plogotlp.NewExportRequest() // new every time
	if err := octx.plog.UnmarshalProto(octx.ReqBuf.B); err != nil {
		octx.err = err
		return nil
	}

	err := octx.PlogWriter.WriteLogs(ctx, octx.plog.Logs())
	if err != nil {
		if strings.Contains(err.Error(), "failed to convert OTLP log to line protocol") {
			octx.err = err
			return nil
		}
		return err
	}
	return nil
}

// NewBatch impl otel2influx.InfluxWriterBatch
func (octx *OtelContext) NewBatch() otel2influx.InfluxWriterBatch {
	return octx
}

// EnqueuePoint impl otel2influx.InfluxWriterBatch
func (octx *OtelContext) EnqueuePoint(ctx context.Context, measurement string, tags map[string]string, fields map[string]interface{}, ts time.Time, vType common.InfluxMetricValueType) error {
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
	sort.Sort(&r.Tags)
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
			// trace and log may contain field key, which is in form of [{"key":"a","value":"b"}], but is converted to
			// {"a":"b"} by otel2influx, therefore we transform it back here.
			if k == common.AttributeAttributes {
				if attributes, err := convertAttributesToKeyValuesPairs(value); err == nil {
					r.Fields = append(r.Fields,
						influx.Field{
							Type:     influx.Field_Type_String,
							Key:      k,
							StrValue: attributes,
						},
					)
					continue
				} else {
					return err
				}
			}
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
		default:
			octx.logger.Error("field type is not supported", zap.String("type", fmt.Sprintf("%T", v)), zap.Any("value", v))
			return fmt.Errorf("field type is not supported, type: %T, value: %v", v, v)
		}
	}
	octx.rowsBuff = append(octx.rowsBuff, r)
	return nil
}

type keyValue struct {
	Key   string      `json:"key"`
	Value interface{} `json:"value"`
}

func convertAttributesToKeyValuesPairs(attributeJson string) (string, error) {
	attributes := make(map[string]interface{})
	err := json.Unmarshal([]byte(attributeJson), &attributes)
	if err != nil {
		return "", err
	}
	var result []keyValue
	for k, v := range attributes {
		result = append(result, keyValue{Key: k, Value: v})
	}
	output, err := json.Marshal(result)
	if err != nil {
		return "", err
	}
	return string(output), nil
}

// WriteBatch impl otel2influx.InfluxWriterBatch
func (octx *OtelContext) WriteBatch(ctx context.Context) error {
	defer func() {
		octx.rowsBuff = octx.rowsBuff[:0]
	}()
	return octx.Writer.RetryWritePointRows(octx.Database, octx.RetentionPolicy, octx.rowsBuff)
}

// getAttributeName when use MetricsSchemaOtelV1 schme to write metrics, otel scope name/version is written to sdk name/version,
// it will collide with real sdk name/version, this method rewrites it back
func getAttributeName(name string) string {
	switch name {
	case semconv.AttributeTelemetrySDKName:
		name = semconv.AttributeOtelScopeName
	case semconv.AttributeTelemetrySDKVersion:
		name = semconv.OtelLibraryVersion
	default:
	}
	return name
}
