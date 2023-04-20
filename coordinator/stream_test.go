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

package coordinator_test

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/openGemini/openGemini/coordinator"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/stringinterner"
	strings2 "github.com/openGemini/openGemini/lib/strings"
	"github.com/openGemini/openGemini/open_src/github.com/savsgio/dictpool"
	meta2 "github.com/openGemini/openGemini/open_src/influx/meta"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	assert2 "github.com/stretchr/testify/assert"
)

func TestStreamGenerateGroupKey(t *testing.T) {
	timeout := time.Second * 10
	pw := coordinator.NewPointsWriter(timeout)
	pw.MetaClient = coordinator.NewMockMetaClient()
	pw.TSDBStore = coordinator.NewMockNetStore()
	s := coordinator.NewStream(pw.TSDBStore, pw.MetaClient, logger.NewLogger(errno.ModuleCoordinator), timeout)

	rows := make([]influx.Row, 1)
	rows = generateRows(1, rows)
	rows[0].IndexOptions = make([]influx.IndexOption, 1)

	ctx := coordinator.GetStreamCtx()
	defer coordinator.PutStreamCtx(ctx)

	ctx.SetBP(coordinator.NewBuilderPool())
	keys := []string{"tk1", "fk1"}
	value, _ := s.GenerateGroupKey(ctx, keys, &rows[0])
	assert2.Equal(t, value, "value1 1")

	rows[0].IndexOptions = make([]influx.IndexOption, 1)
	keys = []string{"tk3"}
	value, _ = s.GenerateGroupKey(ctx, keys, &rows[0])
	assert2.Equal(t, value, "value3")

	rows[0].IndexOptions = make([]influx.IndexOption, 1)
	keys = []string{"tk1", "fk3"}
	_, err := s.GenerateGroupKey(ctx, keys, &rows[0])
	if err == nil {
		t.Fatal("StreamGenerateGroupKey failed")
	}
	if !strings.Contains(err.Error(), "the group key is incomplite") {
		t.Fatal("StreamGenerateGroupKey failed")
	}

	rows[0].IndexOptions = rows[0].IndexOptions[:0]
	keys = []string{"tk4"}
	_, err = s.GenerateGroupKey(ctx, keys, &rows[0])
	if err == nil {
		t.Fatal("StreamGenerateGroupKey failed")
	}
	if !strings.Contains(err.Error(), "the group key is incomplite") {
		t.Fatal("StreamGenerateGroupKey failed")
	}
}

func TestStreamBuildFieldCall(t *testing.T) {
	infos := map[string]*meta2.StreamInfo{}
	info := &meta2.StreamInfo{}
	info.ID = 1
	src := meta2.StreamMeasurementInfo{
		Name:            "mst0",
		Database:        "db0",
		RetentionPolicy: "rp0",
	}

	dst := meta2.StreamMeasurementInfo{
		Name:            "mst2",
		Database:        "db0",
		RetentionPolicy: "rp0",
	}

	info.DesMst = &dst
	info.SrcMst = &src
	var groupKeys []string
	groupKeys = append(groupKeys, "tk1")
	info.Dims = groupKeys
	info.Name = "t"
	info.Interval = time.Duration(5)
	info.Calls = []*meta2.StreamCall{
		{
			Call:  "sum",
			Field: "fk1",
			Alias: "sum_fk1",
		},
		{
			Call:  "max",
			Field: "fk2",
			Alias: "max_fk2",
		},
		{
			Call:  "min",
			Field: "fk3",
			Alias: "min_fk3",
		},
		{
			Call:  "count",
			Field: "fk4",
			Alias: "count_fk2",
		},
	}
	infos[info.Name] = info
	srcSchema := map[string]int32{
		"fk1": 3,
		"fk2": 3,
		"fk3": 3,
		"fk4": 3,
	}
	dstSchema := map[string]int32{
		"sum_fk1":   3,
		"max_fk2":   3,
		"min_fk3":   3,
		"count_fk2": 1,
	}
	_, err := coordinator.BuildFieldCall(info, srcSchema, dstSchema)
	if err != nil {
		t.Fatal("StreamBuildFieldCall failed")
	}

}

// Comparing Benchmark_Map_Write and Benchmark_Dict_Write to get the conclusion,
// keys more than 50, map quicker, mem bigger, 150% than dictpool.Dict
func Benchmark_Map_Write(t *testing.B) {
	t.N = 10
	size := 100
	repeat := 10000
	m := make(map[string]*influx.Row)
	t.ReportAllocs()
	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		t.StartTimer()
		for k := 0; k < repeat; k++ {
			for j := 0; j < size; j++ {
				m[fmt.Sprintf("name%v", j)] = &influx.Row{}
			}
			m = make(map[string]*influx.Row)
		}
		t.StopTimer()
	}
}

func Benchmark_Dict_Write(t *testing.B) {
	t.N = 10
	size := 100
	repeat := 10000
	m := &dictpool.Dict{}
	t.ReportAllocs()
	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		t.StartTimer()
		for k := 0; k < repeat; k++ {
			for j := 0; j < size; j++ {
				m.Set(fmt.Sprintf("name%v", j), &influx.Row{})
			}
			m.Reset()
		}
		t.StopTimer()
	}
}

// Comparing Benchmark_Map_Read and Benchmark_Dict_Read to get the conclusion,
// map speed 50% than dict
func Benchmark_Map_Read(t *testing.B) {
	t.N = 10
	size := 100
	repeat := 10000
	m := make(map[string]*influx.Row)
	for j := 0; j < size; j++ {
		m[fmt.Sprintf("name%v", j)] = &influx.Row{}
	}
	t.ReportAllocs()
	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		t.StartTimer()
		for k := 0; k < repeat; k++ {
			for j := 0; j < size; j++ {
				_ = m[fmt.Sprintf("name%v", j)]
			}
		}
		t.StopTimer()
	}
}

func Benchmark_Dict_Read(t *testing.B) {
	t.N = 10
	size := 100
	repeat := 10000
	m := &dictpool.Dict{}
	for j := 0; j < size; j++ {
		m.Set(fmt.Sprintf("name%v", j), &influx.Row{})
	}
	t.ReportAllocs()
	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		t.StartTimer()
		for k := 0; k < repeat; k++ {
			for j := 0; j < size; j++ {
				_ = m.Get(fmt.Sprintf("name%v", j))
			}
		}
		t.StopTimer()
	}
}

// Comparing Benchmark_Map_Reuse and Benchmark_Dict_Reuse to get the conclusion,
// reuse dict, speed up 15% than new dict, mem reduce 30% than new
func Benchmark_Dict_Reuse(t *testing.B) {
	t.N = 10
	size := 100
	repeat := 10000
	m := &dictpool.Dict{}
	t.ReportAllocs()
	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		t.StartTimer()
		for k := 0; k < repeat; k++ {
			for j := 0; j < size; j++ {
				m.Set(fmt.Sprintf("name%v", j), &influx.Row{})
			}
			m.Reset()
		}
		t.StopTimer()
	}
}

func Benchmark_Dict_No_Reuse(t *testing.B) {
	t.N = 10
	size := 100
	repeat := 10000

	t.ReportAllocs()
	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		t.StartTimer()
		for k := 0; k < repeat; k++ {
			m := &dictpool.Dict{}
			for j := 0; j < size; j++ {
				m.Set(fmt.Sprintf("name%v", j), &influx.Row{})
			}
		}
		t.StopTimer()
	}
}

// Comparing Benchmark_Map_Unclean_Reuse and Benchmark_Map_Unclean_No_Reuse to get the conclusion,
// unclean reuse map, speed 80% than dict
func Benchmark_Map_Unclean_Reuse(t *testing.B) {
	t.N = 10
	size := 1000
	repeat := 1000
	m := make(map[string]*influx.Row)
	t.ReportAllocs()
	t.ResetTimer()

	for i := 0; i < t.N; i++ {
		t.StartTimer()
		for k := 0; k < repeat; k++ {
			for j := 0; j < size; j++ {
				m[fmt.Sprintf("name%v", j)] = &influx.Row{}
			}
		}
		t.StopTimer()
	}
}

func Benchmark_Map_Unclean_Reuse_Key(t *testing.B) {
	t.N = 10
	size := 1000
	repeat := 1000
	m := make(map[string]*influx.Row)
	t.ReportAllocs()
	t.ResetTimer()

	for i := 0; i < t.N; i++ {
		t.StartTimer()
		for k := 0; k < repeat; k++ {
			for j := 0; j < size; j++ {
				m[stringinterner.InternSafe(fmt.Sprintf("name%v", j))] = &influx.Row{}
			}
		}
		t.StopTimer()
	}
}

func Benchmark_Map_Unclean_Reuse_Key_Concurrency(t *testing.B) {
	size := 1000
	repeat := 10000
	m := make(map[string]*influx.Row)
	t.ReportAllocs()
	t.ResetTimer()
	concurrency := 4

	for i := 0; i < t.N; i++ {
		t.StartTimer()
		var wg sync.WaitGroup
		for c := 0; c < concurrency; c++ {
			wg.Add(1)
			go func() {
				for k := 0; k < repeat; k++ {
					for j := 0; j < size; j++ {
						m[stringinterner.InternSafe(fmt.Sprintf("name%v", j))] = &influx.Row{}
					}
				}
				wg.Done()
			}()
			wg.Wait()
		}
		t.StopTimer()
	}
}

func Benchmark_Map_Unclean_No_Reuse(t *testing.B) {
	t.N = 10
	size := 1000
	repeat := 1000
	t.ReportAllocs()
	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		t.StartTimer()
		for k := 0; k < repeat; k++ {
			m := make(map[string]*influx.Row)
			for j := 0; j < size; j++ {
				m[fmt.Sprintf("name%v", j)] = &influx.Row{}
			}
		}
		t.StopTimer()
	}
}

func Benchmark_Map_Unclean_No_Reuse_Concurrency(t *testing.B) {
	size := 1000
	repeat := 10000
	t.ReportAllocs()
	t.ResetTimer()
	concurrency := 4

	for i := 0; i < t.N; i++ {
		t.StartTimer()
		var wg sync.WaitGroup
		for c := 0; c < concurrency; c++ {
			wg.Add(1)
			go func() {
				for k := 0; k < repeat; k++ {
					m := make(map[string]*influx.Row, repeat)
					for j := 0; j < size; j++ {
						m[fmt.Sprintf("name%v", j)] = &influx.Row{}
					}
				}
				wg.Done()
			}()
			wg.Wait()
		}
		t.StopTimer()
	}
}

// Comparing Benchmark_Map_String and Benchmark_Map_Int to get the conclusion,
// unclean reuse map, int key 8x than string key
func Benchmark_Map_String(t *testing.B) {
	t.N = 10
	size := 100
	repeat := 100000
	keyTemp := "aaaaaaaaaaaaaaaaaaaaaaaaa_"
	m := make(map[string]int)
	t.ReportAllocs()
	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		t.StartTimer()
		for k := 0; k < repeat; k++ {
			for j := 0; j < size; j++ {
				m[keyTemp+strconv.Itoa(j)] = j
			}
		}
		t.StopTimer()
	}
}

func Benchmark_Map_String_Clone(t *testing.B) {
	t.N = 10
	size := 100
	repeat := 100000
	keyTemp := "aaaaaaaaaaaaaaaaaaaaaaaaa_"
	m := make(map[string]int)
	t.ReportAllocs()
	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		t.StartTimer()
		for k := 0; k < repeat; k++ {
			for j := 0; j < size; j++ {
				m[strings2.Clone(keyTemp+strconv.Itoa(j))] = j
			}
		}
		t.StopTimer()
	}
}

func Benchmark_Map_String_Sync(t *testing.B) {
	t.N = 10
	size := 100
	repeat := 100000
	keyTemp := "aaaaaaaaaaaaaaaaaaaaaaaaa_"
	m := make(map[string]int)
	t.ReportAllocs()
	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		t.StartTimer()
		for k := 0; k < repeat; k++ {
			for j := 0; j < size; j++ {
				m[stringinterner.InternSafe(keyTemp+strconv.Itoa(j))] = j
			}
		}
		t.StopTimer()
	}
}

func Benchmark_Map_Int(t *testing.B) {
	t.N = 10
	size := 100
	repeat := 100000
	m := make(map[int]int)
	t.ReportAllocs()
	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		t.StartTimer()
		for k := 0; k < repeat; k++ {
			for j := 0; j < size; j++ {
				m[i] = j
			}
		}
		t.StopTimer()
	}
}

// Comparing Benchmark_Reuse_Row and Benchmark_New_Row to get the conclusion,
// reuse r is 8x than new influx.Field, if use field array, 10 fields, 5x than new influx.Field
func Benchmark_Reuse_Row(t *testing.B) {
	t.N = 10
	size := 10
	repeat := 10000000
	rs := make([]influx.Field, size)
	t.ReportAllocs()
	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		t.StartTimer()
		for k := 0; k < repeat; k++ {
			for j := 0; j < size; j++ {
				rs[j].Key = "xxx"
				rs[j].NumValue = 0
				rs[j].StrValue = "xxx"
				rs[j].Type = 0
			}
			rs = rs[:0]
		}
		t.StopTimer()
	}
}

func Benchmark_New_Row(t *testing.B) {
	t.N = 10
	size := 10
	repeat := 10000000
	rs := make([]influx.Field, 0, size)
	r := influx.Field{}
	t.ReportAllocs()
	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		t.StartTimer()
		for k := 0; k < repeat; k++ {
			for j := 0; j < size; j++ {
				r = influx.Field{
					Key:      "xxx",
					NumValue: 0,
					StrValue: "xxx",
					Type:     0,
				}
				rs = append(rs, r)
			}
			rs = rs[:0]
		}
		t.StopTimer()
	}
}

func generateRows(num int, rows []influx.Row) []influx.Row {
	tmpKeys := []string{
		"mst0,tk1=value1,tk2=value2,tk3=value3",
		"mst0,tk1=value11,tk2=value22,tk3=value33",
		"mst0,tk1=value12,tk2=value23",
		"mst0,tk1=value12,tk2=value23",
		"mst0,tk1=value12,tk2=value23",
	}
	keys := make([]string, num)
	for i := 0; i < num; i++ {
		keys[i] = tmpKeys[i%len(tmpKeys)]
	}
	rows = rows[:cap(rows)]
	for j, key := range keys {
		if cap(rows) <= j {
			rows = append(rows, influx.Row{})
		}
		pt := &rows[j]
		strs := strings.Split(key, ",")
		pt.Name = strs[0]
		pt.Tags = pt.Tags[:cap(pt.Tags)]
		for i, str := range strs[1:] {
			if cap(pt.Tags) <= i {
				pt.Tags = append(pt.Tags, influx.Tag{})
			}
			kv := strings.Split(str, "=")
			pt.Tags[i].Key = kv[0]
			pt.Tags[i].Value = kv[1]
		}
		pt.Tags = pt.Tags[:len(strs[1:])]
		sort.Sort(&pt.Tags)
		pt.Timestamp = time.Now().UnixNano()
		pt.UnmarshalIndexKeys(nil)
		pt.ShardKey = pt.IndexKey
		pt.Fields = pt.Fields[:cap(pt.Fields)]
		if cap(pt.Fields) < 1 {
			pt.Fields = append(pt.Fields, influx.Field{}, influx.Field{})
		}
		pt.Fields[0].NumValue = 1
		pt.Fields[0].StrValue = ""
		pt.Fields[0].Type = influx.Field_Type_Float
		pt.Fields[0].Key = "fk1"
		pt.Fields[1].NumValue = 1
		pt.Fields[1].StrValue = ""
		pt.Fields[1].Type = influx.Field_Type_Int
		pt.Fields[1].Key = "fk2"
	}
	return rows[:num]
}
