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

package tracing_test

import (
	"context"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/stretchr/testify/assert"
)

func TestTracing(t *testing.T) {
	trace, span := makeTrace()
	other, _ := makeTrace()
	trace.AddSub(other, span)

	exp := regexp.MustCompile(`.
└── root
    ├── labels
    │   └── node_id: 10
    └── remote_iterator:pp=[\d\\.]+(ns|µs|ms)
        ├── pp: 123.45ms
        └── root
            ├── labels
            │   └── node_id: 10
            └── remote_iterator:pp=[\d\\.]+(ns|µs|ms)
                └── pp: 123.45ms`)

	got := trace.String()
	if !exp.MatchString(got) {
		t.Fatalf("test span faield.\nexp: %s\ngot: %s\n",
			exp.String(), got)
	}
}

func TestTracingCodec(t *testing.T) {
	trace, span := makeTrace()

	sub := span.StartSpan("sub")
	sub.AppendNameValue("duration", time.Second)
	sub.AddStringField("host", "127.0.0.1")
	sub.AddIntField("total", 10)
	sub.Finish()

	buf, err := trace.MarshalBinary()
	assert.NoError(t, err)

	other, _ := tracing.NewTrace("")
	assert.NoError(t, other.UnmarshalBinary(buf))

	exp := trace.String()
	got := other.String()
	assert.Equal(t, exp, got)
}

func TestNewTrace(t *testing.T) {
	ctx := context.Background()
	assert.Empty(t, tracing.TraceFromContext(ctx))
	assert.Empty(t, tracing.SpanFromContext(ctx))

	trace, span := tracing.NewTrace("root")
	ctx = tracing.NewContextWithTrace(context.Background(), trace)
	ctx = tracing.NewContextWithSpan(ctx, span)

	assert.NotEmpty(t, tracing.TraceFromContext(ctx))
	assert.NotEmpty(t, tracing.SpanFromContext(ctx))
}

func makeTrace() (*tracing.Trace, *tracing.Span) {
	ctx := context.Background()
	trace, spanRoot := tracing.NewTrace("root")

	ctx = tracing.NewContextWithTrace(ctx, trace)
	labels := []string{"node_id", strconv.Itoa(10)}
	spanRoot.SetLabels(labels...)
	span := spanRoot.StartSpan("remote_iterator").StartPP()

	span.AddStringField("pp", "123.45ms")

	defer func() {
		span.Finish()
	}()

	ctx = tracing.NewContextWithSpan(ctx, span)
	defer spanRoot.Finish()

	time.Sleep(time.Microsecond * 7)

	return trace, span
}
