// Copyright Huawei Cloud Computing Technologies Co., Ltd.
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
	"regexp"
	"testing"
	"time"

	"github.com/influxdata/influxdb/pkg/tracing/fields"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/stretchr/testify/assert"
)

func TestSpan(t *testing.T) {
	exp := regexp.MustCompile(`.
└── root:pp=[\d\\.]+(ns|µs|ms)
    ├── sub:duration=1s:pp=[\d\\.]+(ns|µs|ms)
    │   ├── labels
    │   │   ├── label_a: label_b
    │   │   └── label_c: label_a
    │   ├── age: 32
    │   ├── counter: 7 s
    │   ├── host: 127.0.0.2
    │   ├── pp: 10ms
    │   └── total: 10
    └── sub2:pp=[\d\\.]+(ns|µs|ms)`)

	//ctx := context.Background()
	trace, span := tracing.NewTrace("root")
	span.StartPP()

	sub := span.StartSpan("sub")
	sub.SetFields(fields.Fields{fields.String("pp", "10ms")})
	tracing.StartPP(sub)

	sub.CreateCounter("counter", "s")
	sub.CreateCounter("counter", "ss")
	sub.Count("counter", 7)
	sub.Count("counter_not_exists", 9)

	sub.AppendNameValue("duration", time.Second)
	sub.AddStringField("host", "127.0.0.1")
	sub.AddIntField("total", 10)
	sub.AddIntFields([]string{"age"}, []int{32})
	sub.AddIntFields([]string{"age"}, []int{33, 44, 55})
	sub.SetLabels("label_a", "label_b")
	sub.MergeLabels("label_c", "label_a")
	sub.MergeFields(fields.String("host", "127.0.0.2"))

	tracing.EndPP(sub)
	tracing.Finish(sub, nil)

	sub2 := tracing.Start(span, "sub2", true)
	tracing.SpanElapsed(sub2, func() {

	})
	tracing.AddPP(sub2, time.Now().Add(time.Duration(-10)))
	sub2.Finish()
	span.Finish()

	var nilSpan *tracing.Span
	tracing.AddPP(nilSpan, time.Now())
	assert.Equal(t, nilSpan, tracing.Start(nil, "nil", true))

	traceStr := trace.String()
	if !exp.MatchString(traceStr) {
		t.Fatalf("test span faield.\nexp: %s\ngot: %s\n",
			exp.String(), traceStr)
	}
}
