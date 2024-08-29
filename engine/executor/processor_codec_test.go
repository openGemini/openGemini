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

package executor_test

import (
	"reflect"
	"regexp"
	"testing"
	"time"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
)

func TestProcessorCodec(t *testing.T) {
	cond, err := influxql.ParseExpr("a=b AND c=1")
	if err != nil {
		t.Fatalf("%v", err)
	}

	opt := &query.ProcessorOptions{
		Name:  "name",
		Expr:  nil,
		Exprs: nil,
		Aux: influxql.VarRefs{
			{
				Val:  "name",
				Type: influxql.String,
			},
			{
				Val:  "age",
				Type: influxql.Integer,
			},
		},
		FieldAux:    nil,
		TagAux:      nil,
		Sources:     nil,
		Interval:    hybridqp.Interval{Duration: 5, Offset: 100},
		Dimensions:  []string{"id", "tid"},
		GroupBy:     map[string]struct{}{"id": {}, "tid": {}},
		Location:    time.FixedZone("Asia/Shanghai", 0),
		Fill:        1,
		FillValue:   3.3,
		Condition:   cond,
		StartTime:   time.Now().Unix() - 3600*10,
		EndTime:     time.Now().Unix(),
		Ascending:   false,
		Limit:       10,
		Offset:      10,
		SLimit:      10,
		SOffset:     10,
		StripName:   false,
		Dedupe:      false,
		Ordered:     false,
		MaxSeriesN:  0,
		InterruptCh: nil,
		Authorizer:  nil,
		Parallel:    false,
		ChunkSize:   7,
		MaxParallel: 0,
		QueryId:     0,

		HintType: hybridqp.ExactStatisticQuery,
	}
	reg, err := regexp.Compile("/table_*/")
	if err != nil {
		t.Fatalf("%v", err)
	}

	opt.Sources = append(opt.Sources, &influxql.Measurement{
		Database:          "db0",
		RetentionPolicy:   "default",
		Name:              "taxi",
		Regex:             &influxql.RegexLiteral{Val: reg},
		IsTarget:          false,
		SystemIterator:    "",
		IsSystemStatement: false,
	})

	buf, err := opt.MarshalBinary()
	if err != nil {
		t.Fatalf("ProcessorOptions marshal failed: %v", err)
		return
	}

	other := &query.ProcessorOptions{}
	if err := other.UnmarshalBinary(buf); err != nil {
		t.Fatalf("failed to unmarshal ProcessorOptions: %v", err)
		return
	}

	var deepEquals = [][3]interface{}{
		{"Aux", opt.Aux, other.Aux},
		{"Interval", opt.Interval, other.Interval},
		{"Dimensions", opt.Dimensions, other.Dimensions},
		{"GroupBy", opt.GroupBy, other.GroupBy},
		{"Sources", opt.Sources[0], other.Sources[0]},
	}

	for _, item := range deepEquals {
		if !reflect.DeepEqual(item[1], item[2]) {
			t.Fatalf("failed to marshal %s. exp: %+v; got: %+v", item[0], item[1], item[2])
		}
	}

	if opt.Location.String() != other.Location.String() {
		t.Fatalf("failed to marshal Location. exp: %s; got: %s", opt.Location, other.Location)
	}

	if opt.Condition.String() != other.Condition.String() {
		t.Fatalf("failed to marshal Condition. exp: %s; got: %s", opt.Condition, other.Condition)
	}

	if opt.ChunkSize != other.ChunkSize {
		t.Fatalf("failed to marshal ChunkSize. exp: %d; got: %d", opt.ChunkSize, other.ChunkSize)
	}

	if opt.HintType != other.HintType {
		t.Fatalf("failed to marshal HintType. exp: %d; got: %d", opt.HintType, other.HintType)
	}

}

func compareSchema(s1, s2 *executor.QuerySchema) error {
	fn := func() error {
		return CompareFields(s1.Fields(), s2.Fields(), "QuerySchema")
	}

	return CompareNil(s1, s2, "QuerySchema", fn)
}
