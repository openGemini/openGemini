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

package opentelemetry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.16.0"
)

const (
	TraceContextHeader = "x-trace-data"
	TracePathFormat    = "/api/v1/otlp/%s/traces"
)

func InitTrace(endPoint string, httpsEnabled bool, storePath string, tracingRatio float64, serviceName string) (*tracesdk.TracerProvider, error) {
	if tracingRatio < 0 || tracingRatio > 1 {
		return nil, errors.New("tracing ratio must be between 0 and 1")
	}

	ctx := context.Background()

	// set global propagator as W3C standard
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	// create otlp exporter
	exp, err := newOtlpTraceExporter(endPoint, httpsEnabled, storePath, ctx)
	if err != nil {
		return nil, err
	}
	res, err := resource.New(ctx, resource.WithAttributes(semconv.ServiceNameKey.String(serviceName)))
	if err != nil {
		return nil, err
	}
	sampler := tracesdk.ParentBased(tracesdk.TraceIDRatioBased(tracingRatio))
	tp := tracesdk.NewTracerProvider(
		tracesdk.WithBatcher(exp),
		tracesdk.WithResource(res),
		tracesdk.WithSampler(sampler),
	)
	otel.SetTracerProvider(tp)
	return tp, nil
}

func newOtlpTraceExporter(endPoint string, httpsEnabled bool, storePath string, ctx context.Context) (*otlptrace.Exporter, error) {
	var exp *otlptrace.Exporter
	var err error
	if httpsEnabled {
		exp, err = otlptracehttp.New(
			ctx,
			otlptracehttp.WithEndpoint(endPoint),
			otlptracehttp.WithURLPath(fmt.Sprintf(TracePathFormat, storePath)),
		)
	} else {
		exp, err = otlptracehttp.New(
			ctx,
			otlptracehttp.WithInsecure(),
			otlptracehttp.WithEndpoint(endPoint),
			otlptracehttp.WithURLPath(fmt.Sprintf(TracePathFormat, storePath)),
		)
	}
	return exp, err
}

func InjectContextToRecord(r arrow.Record, ctx context.Context) arrow.Record {
	propagator := otel.GetTextMapPropagator()
	carrier := make(map[string]string)
	propagator.Inject(ctx, propagation.MapCarrier(carrier))
	existingMeta := r.Schema().Metadata()
	existingKeys := existingMeta.Keys()
	existingValues := existingMeta.Values()
	newKeys := append([]string{}, existingKeys...)
	newValues := append([]string{}, existingValues...)
	newKeys = append(newKeys, TraceContextHeader)
	marshal, err := json.Marshal(carrier)
	if err != nil {
		// err occurs, cancel inject trace and return original record
		return r
	}
	newValues = append(newValues, string(marshal))
	newMeta := arrow.NewMetadata(newKeys, newValues)
	newSchema := arrow.NewSchema(
		r.Schema().Fields(),
		&newMeta,
	)
	newRec := array.NewRecord(newSchema, r.Columns(), r.NumRows())
	return newRec
}
