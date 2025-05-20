// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package httpd

import (
	"fmt"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/util/lifted/prometheus/model/labels"
	"github.com/openGemini/openGemini/lib/util/lifted/prometheus/promql"
	"github.com/openGemini/openGemini/lib/util/lifted/promql2influxql"
	"github.com/prometheus/prometheus/promql/parser"
	"go.uber.org/zap"
)

type TestLogger interface {
	Log(args ...any)
}

type MockLogger struct {
	t TestLogger
}

func (m MockLogger) Info(msg string, fields ...zap.Field) {
	m.t.Log(msg)
}

func (m MockLogger) Debug(msg string, fields ...zap.Field) {
	m.t.Log(msg)
}

func (m MockLogger) Error(msg string, fields ...zap.Field) {
	m.t.Log(msg)
}

func NewPromMatrixResponse(matrix *promql.Matrix) *promql2influxql.PromQueryResponse {
	res := &promql2influxql.PromQueryResponse{
		Status: "success",
		Data: &promql2influxql.PromData{
			ResultType: string(parser.ValueTypeMatrix),
			Result: &promql2influxql.PromDataMatrix{
				Matrix: matrix,
			},
		},
	}
	return res
}

func isResponseEqual(t *testing.T, cur, exp *promql2influxql.PromQueryResponse) {
	if cur.Data.ResultType != exp.Data.ResultType {
		t.Fatalf("data result type not equal,curr: %s,exp: %s", cur.Data.ResultType, exp.Data.ResultType)
	}
	curMatrix := *cur.Data.Result.(*promql2influxql.PromDataMatrix).Matrix
	expMatrix := *exp.Data.Result.(*promql2influxql.PromDataMatrix).Matrix
	if len(curMatrix) != len(expMatrix) {
		t.Fatalf("data results metrix not equal,curr: %v,exp: %v", curMatrix, expMatrix)
	}
	for i := range curMatrix {
		curFloats := curMatrix[i].Floats
		expFloats := expMatrix[i].Floats
		if len(curFloats) != len(expFloats) {
			t.Fatalf("data results floats not equal,curr: %v,exp: %v", curMatrix[i].Floats, expMatrix[i].Floats)
		}
		for j := range curFloats {
			if curFloats[j].T != expFloats[j].T || curFloats[j].F != expFloats[j].F {
				t.Fatalf("data results floats not equal,curr: %v,exp: %v", curMatrix[i].Floats, expMatrix[i].Floats)
			}
		}
	}
}

func Test_GenerateCacheKey(t *testing.T) {
	start := time.Unix(0, 0).Add(8 * time.Minute)
	step := 10 * time.Second
	mst := "mst1"
	cmd := &promql2influxql.PromCommand{
		Start:           &start,
		Step:            step,
		Cmd:             "up",
		Database:        "db",
		RetentionPolicy: "rp",
	}
	exp := fmt.Sprintf("%s:%s:%s:%s:%s:%d", mst, "db", "rp", "up", step.String(), 1)
	key := generateCacheKey(cmd, "mst1", 5*time.Minute)
	if exp != key {
		t.Fatalf("key not euql,exp: %s,got: %s", exp, key)
	}

	start = start.Add(1 * time.Minute)
	exp = fmt.Sprintf("%s:%s:%s:%s:%s:%d", mst, "db", "rp", "up", step.String(), 1)
	key = generateCacheKey(cmd, "mst1", 5*time.Minute)
	if exp != key {
		t.Fatalf("key not euql,exp: %s,got: %s", exp, key)
	}

	start = start.Add(1 * time.Minute)
	exp = fmt.Sprintf("%s:%s:%s:%s:%s:%d", mst, "db", "rp", "up", step.String(), 2)
	key = generateCacheKey(cmd, "mst1", 5*time.Minute)
	if exp != key {
		t.Fatalf("key not euql,exp: %s,got: %s", exp, key)
	}
}

func Test_ExtractResponse(t *testing.T) {
	matrix := &promql.Matrix{
		{
			Floats: []promql.FPoint{{T: 0, F: 1}, {T: 10, F: 2}, {T: 20, F: 3}},
			Metric: labels.Labels{},
		},
	}
	res := NewPromMatrixResponse(matrix)
	t.Run("1", func(t *testing.T) {
		expRes := NewPromMatrixResponse(&promql.Matrix{
			{
				Floats: []promql.FPoint{{T: 10, F: 2}, {T: 20, F: 3}},
				Metric: labels.Labels{},
			},
		})
		results := ExtractResponse(res, 10, 20)

		isResponseEqual(t, results, expRes)
	})

	t.Run("2", func(t *testing.T) {
		expRes := NewPromMatrixResponse(&promql.Matrix{
			{
				Floats: []promql.FPoint{{T: 20, F: 3}},
				Metric: labels.Labels{},
			},
		})
		results := ExtractResponse(res, 20, 20)

		isResponseEqual(t, results, expRes)
	})

	t.Run("3", func(t *testing.T) {
		expRes := NewPromMatrixResponse(&promql.Matrix{})
		results := ExtractResponse(res, 30, 40)

		isResponseEqual(t, results, expRes)
	})

}

func Test_MergeResponse(t *testing.T) {
	res1 := NewPromMatrixResponse(&promql.Matrix{
		{
			Floats: []promql.FPoint{{T: 0, F: 1}, {T: 10, F: 2}, {T: 20, F: 3}},
			Metric: labels.Labels{{Name: "a", Value: "a"}, {Name: "b", Value: "b"}},
		},
		{
			Floats: []promql.FPoint{{T: 0, F: 10}, {T: 10, F: 20}, {T: 20, F: 30}},
			Metric: labels.Labels{{Name: "a", Value: "a2"}, {Name: "b", Value: "b"}},
		},
	})
	t.Run("1", func(t *testing.T) {
		res2 := NewPromMatrixResponse(&promql.Matrix{
			{
				Floats: []promql.FPoint{{T: 20, F: 1}, {T: 30, F: 2}, {T: 40, F: 3}},
				Metric: labels.Labels{{Name: "a", Value: "a"}, {Name: "b", Value: "b"}},
			},
			{
				Floats: []promql.FPoint{{T: 20, F: 10}, {T: 30, F: 20}, {T: 40, F: 30}},
				Metric: labels.Labels{{Name: "a", Value: "a2"}, {Name: "b", Value: "b"}},
			},
		})
		expRes := NewPromMatrixResponse(&promql.Matrix{
			{
				Floats: []promql.FPoint{{T: 0, F: 1}, {T: 10, F: 2}, {T: 20, F: 3}, {T: 30, F: 2}, {T: 40, F: 3}},
				Metric: labels.Labels{{Name: "a", Value: "a"}, {Name: "b", Value: "b"}},
			},
			{
				Floats: []promql.FPoint{{T: 0, F: 10}, {T: 10, F: 20}, {T: 20, F: 30}, {T: 30, F: 20}, {T: 40, F: 30}},
				Metric: labels.Labels{{Name: "a", Value: "a2"}, {Name: "b", Value: "b"}},
			},
		})
		res, err := MergeResponse(nil, res1, res2)
		if err != nil {
			t.Fatal(err)
		}
		isResponseEqual(t, res, expRes)
	})

	t.Run("2", func(t *testing.T) {
		res2 := NewPromMatrixResponse(&promql.Matrix{
			{
				Floats: []promql.FPoint{{T: 20, F: 1}, {T: 30, F: 2}, {T: 40, F: 3}},
				Metric: labels.Labels{{Name: "a", Value: "a"}, {Name: "b", Value: "b"}},
			},
			{
				Floats: []promql.FPoint{{T: 20, F: 10}, {T: 30, F: 20}, {T: 40, F: 30}},
				Metric: labels.Labels{{Name: "a", Value: "a2"}, {Name: "b", Value: "b2"}},
			},
		})
		expRes := NewPromMatrixResponse(&promql.Matrix{
			{
				Floats: []promql.FPoint{{T: 0, F: 1}, {T: 10, F: 2}, {T: 20, F: 3}, {T: 30, F: 2}, {T: 40, F: 3}},
				Metric: labels.Labels{{Name: "a", Value: "a"}, {Name: "b", Value: "b"}},
			},
			{
				Floats: []promql.FPoint{{T: 0, F: 10}, {T: 10, F: 20}, {T: 20, F: 30}},
				Metric: labels.Labels{{Name: "a", Value: "a2"}, {Name: "b", Value: "b"}},
			},
			{
				Floats: []promql.FPoint{{T: 20, F: 10}, {T: 30, F: 20}, {T: 40, F: 30}},
				Metric: labels.Labels{{Name: "a", Value: "a2"}, {Name: "b", Value: "b2"}},
			},
		})
		res, err := MergeResponse(nil, res1, res2)
		if err != nil {
			t.Fatal(err)
		}
		isResponseEqual(t, res, expRes)
	})
}

func Test_IsAtModifierCacheable(t *testing.T) {
	rc := &ResultsCache{
		Logger: &logger.Logger{},
	}
	t.Run("1", func(t *testing.T) {
		start := time.Unix(0, 0)
		end := time.Unix(2000, 0)
		cmd := &promql2influxql.PromCommand{
			Cmd:   "up @ 1",
			Start: &start,
			End:   &end,
		}
		ok := rc.shouldCacheResponse(2000, cmd)
		if !ok {
			t.Fatalf("should be ok")
		}
	})

	t.Run("2", func(t *testing.T) {
		start := time.Unix(0, 0)
		end := time.Unix(2000, 0)
		cmd := &promql2influxql.PromCommand{
			Cmd:   "up @ 1",
			Start: &start,
			End:   &end,
		}
		ok := rc.shouldCacheResponse(20, cmd)
		if ok {
			t.Fatalf("should be not ok")
		}
	})

	t.Run("3", func(t *testing.T) {
		start := time.Unix(0, 0)
		end := time.Unix(2000, 0)
		cmd := &promql2influxql.PromCommand{
			Cmd:   "up[5m] @ 1",
			Start: &start,
			End:   &end,
		}
		ok := rc.shouldCacheResponse(20, cmd)
		if ok {
			t.Fatalf("should be not ok")
		}
	})

	t.Run("4", func(t *testing.T) {
		start := time.Unix(0, 0)
		end := time.Unix(2000, 0)
		cmd := &promql2influxql.PromCommand{
			Cmd:   "up[5m:1m] @ 1000",
			Start: &start,
			End:   &end,
		}
		ok := rc.shouldCacheResponse(20, cmd)
		if ok {
			t.Fatalf("should be not ok")
		}
	})

	t.Run("5", func(t *testing.T) {
		start := time.Unix(0, 0)
		end := time.Unix(2000, 0)
		cmd := &promql2influxql.PromCommand{
			Cmd:   "up",
			Start: &start,
			End:   &end,
		}
		ok := rc.shouldCacheResponse(20, cmd)
		if !ok {
			t.Fatalf("should be ok")
		}
	})
}

func Test_IsOffsetCacheable(t *testing.T) {
	rc := &ResultsCache{
		Logger: &logger.Logger{},
	}
	t.Run("1", func(t *testing.T) {
		start := time.Unix(0, 0)
		end := time.Unix(2000, 0)
		cmd := &promql2influxql.PromCommand{
			Cmd:   "up offset 1m",
			Start: &start,
			End:   &end,
		}
		ok := rc.shouldCacheResponse(200, cmd)
		if !ok {
			t.Fatalf("should be ok")
		}
	})

	t.Run("2", func(t *testing.T) {
		start := time.Unix(0, 0)
		end := time.Unix(2000, 0)
		cmd := &promql2influxql.PromCommand{
			Cmd:   "up offset -1m",
			Start: &start,
			End:   &end,
		}
		ok := rc.shouldCacheResponse(200, cmd)
		if ok {
			t.Fatalf("should be not ok")
		}
	})

	t.Run("3", func(t *testing.T) {
		start := time.Unix(0, 0)
		end := time.Unix(2000, 0)
		cmd := &promql2influxql.PromCommand{
			Cmd:   "up[5m] offset -1m",
			Start: &start,
			End:   &end,
		}
		ok := rc.shouldCacheResponse(200, cmd)
		if ok {
			t.Fatalf("should be not ok")
		}
	})

	t.Run("4", func(t *testing.T) {
		start := time.Unix(0, 0)
		end := time.Unix(2000, 0)
		cmd := &promql2influxql.PromCommand{
			Cmd:   "up[5m:1m] offset -1m",
			Start: &start,
			End:   &end,
		}
		ok := rc.shouldCacheResponse(200, cmd)
		if ok {
			t.Fatalf("should be not ok")
		}
	})

	t.Run("5", func(t *testing.T) {
		start := time.Unix(0, 0)
		end := time.Unix(2000, 0)
		cmd := &promql2influxql.PromCommand{
			Cmd:   "up",
			Start: &start,
			End:   &end,
		}
		ok := rc.shouldCacheResponse(200, cmd)
		if !ok {
			t.Fatalf("should be ok")
		}
	})
}

func TestFilterRecentExtents(t *testing.T) {
	t.Run("1", func(t *testing.T) {
		rc := &ResultsCache{}
		exs := []promql2influxql.Extent{
			{Start: time.Now().Add(-time.Minute * 2).UnixNano(), Response: &promql2influxql.PromQueryResponse{Data: nil}},
		}
		step := time.Minute.Nanoseconds()
		extents := rc.filterRecentExtents(exs, step, time.Duration(0))
		if len(extents) != 0 {
			t.Fatalf("should have no extents")
		}

	})
	t.Run("2", func(t *testing.T) {
		rc := &ResultsCache{}
		exs := []promql2influxql.Extent{
			{
				Start: time.Now().Add(-time.Minute * 2).UnixNano(),
				Response: &promql2influxql.PromQueryResponse{
					Data: &promql2influxql.PromData{Result: &promql2influxql.PromDataMatrix{}},
				}},
		}
		step := time.Minute.Nanoseconds()
		extents := rc.filterRecentExtents(exs, step, time.Duration(0))
		if len(extents) != 0 {
			t.Fatalf("should have no extents")
		}
	})
	t.Run("3", func(t *testing.T) {
		rc := &ResultsCache{}
		exs := []promql2influxql.Extent{
			{
				Start: time.Now().Add(-time.Minute * 2).UnixNano(),
				Response: &promql2influxql.PromQueryResponse{
					Data: &promql2influxql.PromData{Result: &promql2influxql.PromDataVector{}},
				}},
		}
		step := time.Minute.Nanoseconds()
		extents := rc.filterRecentExtents(exs, step, time.Duration(0))
		if len(extents) != 0 {
			t.Fatalf("should have no extents")
		}
	})
}
