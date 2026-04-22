// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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

package tracing

import (
	"context"
)

type ContextKey string

const (
	spanKey  ContextKey = "tracing-span-key"
	traceKey ContextKey = "tracing-trace-key"
)

func NewContextWithSpan(ctx context.Context, c *Span) context.Context {
	return context.WithValue(ctx, spanKey, c)
}

func SpanFromContext(ctx context.Context) *Span {
	c, ok := ctx.Value(spanKey).(*Span)
	if !ok {
		return nil
	}
	return c
}

func NewContextWithTrace(ctx context.Context, t *Trace) context.Context {
	return context.WithValue(ctx, traceKey, t)
}

func TraceFromContext(ctx context.Context) *Trace {
	c, ok := ctx.Value(traceKey).(*Trace)
	if !ok {
		return nil
	}
	return c
}
