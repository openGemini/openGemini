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

package tests

import (
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestServer_Query_ColumnStore(t *testing.T) {
	t.Skipf("TODO: column store engine is being improved and will be adapted to this use case in a later MR")
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	// set infinite retention policy as we are inserting data in the past and don't want retention policy enforcement to make this test racy
	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	if err := s.CreateMeasurement("CREATE MEASUREMENT db0.rp0.cpu (region tag,  az tag, v1 int64,  v2 float64,  v3 bool, v4 string) WITH  ENGINETYPE = columnstore  SHARDKEY az,region PRIMARYKEY az,region,time"); err != nil {
		t.Fatal(err)
	}

	test := NewTest("db0", "rp0")

	writes := []string{}
	for i := 0; i < 10; i++ {
		for j := 0; j < 2048; j++ {
			data := fmt.Sprintf(`cpu,region=region_%d,az=az_%d v1=%di,v2=%f,v3=%t,v4="%s" %d`,
				i, i, i*2048+j, generateFloat(i*2048+j), generateBool(i*2048+j), generateString(i*2048+j), time.Unix(int64(i*2048+j), int64(0)).UTC().UnixNano())
			writes = append(writes, data)
		}
	}
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		// 1. multi-column *
		{
			name:    "count(time)",
			params:  url.Values{"inner_chunk_size": []string{"2"}},
			command: `select count(time) from db0.rp0.cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","count"],"values":[["1970-01-01T00:00:00Z",20480]]}]}]}`,
			skip:    true,
		},
		{
			name:    "exact count(time)",
			params:  url.Values{"inner_chunk_size": []string{"2"}},
			command: `select /*+ Exact_Statistic_Query */ count(time) from db0.rp0.cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","count"],"values":[["1970-01-01T00:00:00Z",20480]]}]}]}`,
		},
		{
			name:    "count(*)",
			params:  url.Values{"inner_chunk_size": []string{"2"}},
			command: `select count(*) from db0.rp0.cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","count_v1","count_v2","count_v3","count_v4"],"values":[["1970-01-01T00:00:00Z",20480,20480,20480,20480]]}]}]}`,
		},
		{
			name:    "mean(*)",
			params:  url.Values{"inner_chunk_size": []string{"2"}},
			command: `select mean(*) from db0.rp0.cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","mean_v1","mean_v2"],"values":[["1970-01-01T00:00:00Z",10239.5,10239.5]]}]}]}`,
		},
		{
			name:    "sum(*)",
			params:  url.Values{"inner_chunk_size": []string{"2"}},
			command: `select sum(*) from db0.rp0.cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","sum_v1","sum_v2"],"values":[["1970-01-01T00:00:00Z",209704960,209704960]]}]}]}`,
		},
		{
			name:    "min(v1),min(v2)",
			params:  url.Values{"inner_chunk_size": []string{"2"}},
			command: `select min(v1),min(v2) from db0.rp0.cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","min","min_1"],"values":[["1970-01-01T00:00:00Z",0,0]]}]}]}`,
		},
		{
			name:    "max(v1),max(v2)",
			params:  url.Values{"inner_chunk_size": []string{"2"}},
			command: `select max(v1),max(v2) from db0.rp0.cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","max","max_1"],"values":[["1970-01-01T00:00:00Z",20479,20479]]}]}]}`,
		},
		{
			name:    "first(*)",
			params:  url.Values{"inner_chunk_size": []string{"2"}},
			command: `select first(*) from db0.rp0.cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","first_v1","first_v2","first_v3","first_v4"],"values":[["1970-01-01T00:00:00Z",0,0,true,"abc0"]]}]}]}`,
		},
		{
			name:    "last(*)",
			params:  url.Values{"inner_chunk_size": []string{"2"}},
			command: `select last(*) from db0.rp0.cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","last_v1","last_v2","last_v3","last_v4"],"values":[["1970-01-01T00:00:00Z",20479,20479,false,"abc20479"]]}]}]}`,
		},
		{
			name:    "percentile(*, 50)",
			params:  url.Values{"inner_chunk_size": []string{"2"}},
			command: `select percentile(*, 50) from db0.rp0.cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","percentile_v1","percentile_v2"],"values":[["1970-01-01T00:00:00Z",10239,10239]]}]}]}`,
		},

		// 2. multi-column
		{
			name:    "count(time) group by time",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select count(time) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h) fill(0) order by time asc`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","count"],"values":[["1970-01-01T00:00:00Z",3600],["1970-01-01T01:00:00Z",3600],["1970-01-01T02:00:00Z",3600],["1970-01-01T03:00:00Z",3600],["1970-01-01T04:00:00Z",3600],["1970-01-01T05:00:00Z",2480]]}]}]}`,
		},
		{
			name:    "count(time) group by *",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select count(time) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by * order by az asc, region asc`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",2048]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",2048]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",2048]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",2048]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",2048]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",2048]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",2048]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",2048]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",2048]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",2048]]}]}]}`,
		},
		{
			name:    "count(time) group by time, *",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select count(time) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h),* fill(0) order by az asc, region asc, time asc`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",2048],["1970-01-01T01:00:00Z",0],["1970-01-01T02:00:00Z",0],["1970-01-01T03:00:00Z",0],["1970-01-01T04:00:00Z",0],["1970-01-01T05:00:00Z",0]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",1552],["1970-01-01T01:00:00Z",496],["1970-01-01T02:00:00Z",0],["1970-01-01T03:00:00Z",0],["1970-01-01T04:00:00Z",0],["1970-01-01T05:00:00Z",0]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",0],["1970-01-01T01:00:00Z",2048],["1970-01-01T02:00:00Z",0],["1970-01-01T03:00:00Z",0],["1970-01-01T04:00:00Z",0],["1970-01-01T05:00:00Z",0]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",0],["1970-01-01T01:00:00Z",1056],["1970-01-01T02:00:00Z",992],["1970-01-01T03:00:00Z",0],["1970-01-01T04:00:00Z",0],["1970-01-01T05:00:00Z",0]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",0],["1970-01-01T01:00:00Z",0],["1970-01-01T02:00:00Z",2048],["1970-01-01T03:00:00Z",0],["1970-01-01T04:00:00Z",0],["1970-01-01T05:00:00Z",0]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",0],["1970-01-01T01:00:00Z",0],["1970-01-01T02:00:00Z",560],["1970-01-01T03:00:00Z",1488],["1970-01-01T04:00:00Z",0],["1970-01-01T05:00:00Z",0]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",0],["1970-01-01T01:00:00Z",0],["1970-01-01T02:00:00Z",0],["1970-01-01T03:00:00Z",2048],["1970-01-01T04:00:00Z",0],["1970-01-01T05:00:00Z",0]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",0],["1970-01-01T01:00:00Z",0],["1970-01-01T02:00:00Z",0],["1970-01-01T03:00:00Z",64],["1970-01-01T04:00:00Z",1984],["1970-01-01T05:00:00Z",0]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",0],["1970-01-01T01:00:00Z",0],["1970-01-01T02:00:00Z",0],["1970-01-01T03:00:00Z",0],["1970-01-01T04:00:00Z",1616],["1970-01-01T05:00:00Z",432]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",0],["1970-01-01T01:00:00Z",0],["1970-01-01T02:00:00Z",0],["1970-01-01T03:00:00Z",0],["1970-01-01T04:00:00Z",0],["1970-01-01T05:00:00Z",2048]]}]}]}`,
		},
		{
			name:    "count(*) group by time",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select count(*) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h) fill(0) order by time`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","count_v1","count_v2","count_v3","count_v4"],"values":[["1970-01-01T00:00:00Z",3600,3600,3600,3600],["1970-01-01T01:00:00Z",3600,3600,3600,3600],["1970-01-01T02:00:00Z",3600,3600,3600,3600],["1970-01-01T03:00:00Z",3600,3600,3600,3600],["1970-01-01T04:00:00Z",3600,3600,3600,3600],["1970-01-01T05:00:00Z",2480,2480,2480,2480]]}]}]}`,
		},
		{
			name:    "count(*) group by *",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select count(*) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by * order by az asc, region asc`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","count_v1","count_v2","count_v3","count_v4"],"values":[["1970-01-01T00:00:00Z",2048,2048,2048,2048]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","count_v1","count_v2","count_v3","count_v4"],"values":[["1970-01-01T00:00:00Z",2048,2048,2048,2048]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","count_v1","count_v2","count_v3","count_v4"],"values":[["1970-01-01T00:00:00Z",2048,2048,2048,2048]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","count_v1","count_v2","count_v3","count_v4"],"values":[["1970-01-01T00:00:00Z",2048,2048,2048,2048]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","count_v1","count_v2","count_v3","count_v4"],"values":[["1970-01-01T00:00:00Z",2048,2048,2048,2048]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","count_v1","count_v2","count_v3","count_v4"],"values":[["1970-01-01T00:00:00Z",2048,2048,2048,2048]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","count_v1","count_v2","count_v3","count_v4"],"values":[["1970-01-01T00:00:00Z",2048,2048,2048,2048]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","count_v1","count_v2","count_v3","count_v4"],"values":[["1970-01-01T00:00:00Z",2048,2048,2048,2048]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","count_v1","count_v2","count_v3","count_v4"],"values":[["1970-01-01T00:00:00Z",2048,2048,2048,2048]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","count_v1","count_v2","count_v3","count_v4"],"values":[["1970-01-01T00:00:00Z",2048,2048,2048,2048]]}]}]}`,
		},
		{
			name:    "count(*) group by time, *",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select count(*) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h),* fill(0) order by az asc, region asc, time asc`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","count_v1","count_v2","count_v3","count_v4"],"values":[["1970-01-01T00:00:00Z",2048,2048,2048,2048],["1970-01-01T01:00:00Z",0,0,0,0],["1970-01-01T02:00:00Z",0,0,0,0],["1970-01-01T03:00:00Z",0,0,0,0],["1970-01-01T04:00:00Z",0,0,0,0],["1970-01-01T05:00:00Z",0,0,0,0]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","count_v1","count_v2","count_v3","count_v4"],"values":[["1970-01-01T00:00:00Z",1552,1552,1552,1552],["1970-01-01T01:00:00Z",496,496,496,496],["1970-01-01T02:00:00Z",0,0,0,0],["1970-01-01T03:00:00Z",0,0,0,0],["1970-01-01T04:00:00Z",0,0,0,0],["1970-01-01T05:00:00Z",0,0,0,0]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","count_v1","count_v2","count_v3","count_v4"],"values":[["1970-01-01T00:00:00Z",0,0,0,0],["1970-01-01T01:00:00Z",2048,2048,2048,2048],["1970-01-01T02:00:00Z",0,0,0,0],["1970-01-01T03:00:00Z",0,0,0,0],["1970-01-01T04:00:00Z",0,0,0,0],["1970-01-01T05:00:00Z",0,0,0,0]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","count_v1","count_v2","count_v3","count_v4"],"values":[["1970-01-01T00:00:00Z",0,0,0,0],["1970-01-01T01:00:00Z",1056,1056,1056,1056],["1970-01-01T02:00:00Z",992,992,992,992],["1970-01-01T03:00:00Z",0,0,0,0],["1970-01-01T04:00:00Z",0,0,0,0],["1970-01-01T05:00:00Z",0,0,0,0]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","count_v1","count_v2","count_v3","count_v4"],"values":[["1970-01-01T00:00:00Z",0,0,0,0],["1970-01-01T01:00:00Z",0,0,0,0],["1970-01-01T02:00:00Z",2048,2048,2048,2048],["1970-01-01T03:00:00Z",0,0,0,0],["1970-01-01T04:00:00Z",0,0,0,0],["1970-01-01T05:00:00Z",0,0,0,0]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","count_v1","count_v2","count_v3","count_v4"],"values":[["1970-01-01T00:00:00Z",0,0,0,0],["1970-01-01T01:00:00Z",0,0,0,0],["1970-01-01T02:00:00Z",560,560,560,560],["1970-01-01T03:00:00Z",1488,1488,1488,1488],["1970-01-01T04:00:00Z",0,0,0,0],["1970-01-01T05:00:00Z",0,0,0,0]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","count_v1","count_v2","count_v3","count_v4"],"values":[["1970-01-01T00:00:00Z",0,0,0,0],["1970-01-01T01:00:00Z",0,0,0,0],["1970-01-01T02:00:00Z",0,0,0,0],["1970-01-01T03:00:00Z",2048,2048,2048,2048],["1970-01-01T04:00:00Z",0,0,0,0],["1970-01-01T05:00:00Z",0,0,0,0]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","count_v1","count_v2","count_v3","count_v4"],"values":[["1970-01-01T00:00:00Z",0,0,0,0],["1970-01-01T01:00:00Z",0,0,0,0],["1970-01-01T02:00:00Z",0,0,0,0],["1970-01-01T03:00:00Z",64,64,64,64],["1970-01-01T04:00:00Z",1984,1984,1984,1984],["1970-01-01T05:00:00Z",0,0,0,0]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","count_v1","count_v2","count_v3","count_v4"],"values":[["1970-01-01T00:00:00Z",0,0,0,0],["1970-01-01T01:00:00Z",0,0,0,0],["1970-01-01T02:00:00Z",0,0,0,0],["1970-01-01T03:00:00Z",0,0,0,0],["1970-01-01T04:00:00Z",1616,1616,1616,1616],["1970-01-01T05:00:00Z",432,432,432,432]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","count_v1","count_v2","count_v3","count_v4"],"values":[["1970-01-01T00:00:00Z",0,0,0,0],["1970-01-01T01:00:00Z",0,0,0,0],["1970-01-01T02:00:00Z",0,0,0,0],["1970-01-01T03:00:00Z",0,0,0,0],["1970-01-01T04:00:00Z",0,0,0,0],["1970-01-01T05:00:00Z",2048,2048,2048,2048]]}]}]}`,
		},

		{
			name:    "mean(*) group by time",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select mean(*) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h) order by time`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","mean_v1","mean_v2"],"values":[["1970-01-01T00:00:00Z",1799.5,1799.5],["1970-01-01T01:00:00Z",5399.5,5399.5],["1970-01-01T02:00:00Z",8999.5,8999.5],["1970-01-01T03:00:00Z",12599.5,12599.5],["1970-01-01T04:00:00Z",16199.5,16199.5],["1970-01-01T05:00:00Z",19239.5,19239.5]]}]}]}`,
		},
		{
			name:    "mean(*) group by *",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select mean(*) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by * order by az asc, region asc`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","mean_v1","mean_v2"],"values":[["1970-01-01T00:00:00Z",1023.5,1023.5]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","mean_v1","mean_v2"],"values":[["1970-01-01T00:00:00Z",3071.5,3071.5]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","mean_v1","mean_v2"],"values":[["1970-01-01T00:00:00Z",5119.5,5119.5]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","mean_v1","mean_v2"],"values":[["1970-01-01T00:00:00Z",7167.5,7167.5]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","mean_v1","mean_v2"],"values":[["1970-01-01T00:00:00Z",9215.5,9215.5]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","mean_v1","mean_v2"],"values":[["1970-01-01T00:00:00Z",11263.5,11263.5]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","mean_v1","mean_v2"],"values":[["1970-01-01T00:00:00Z",13311.5,13311.5]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","mean_v1","mean_v2"],"values":[["1970-01-01T00:00:00Z",15359.5,15359.5]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","mean_v1","mean_v2"],"values":[["1970-01-01T00:00:00Z",17407.5,17407.5]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","mean_v1","mean_v2"],"values":[["1970-01-01T00:00:00Z",19455.5,19455.5]]}]}]}`,
		},
		{
			name:    "mean(*) group by time, *",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select mean(*) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h),* order by az asc, region asc, time asc`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","mean_v1","mean_v2"],"values":[["1970-01-01T00:00:00Z",1023.5,1023.5],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","mean_v1","mean_v2"],"values":[["1970-01-01T00:00:00Z",2823.5,2823.5],["1970-01-01T01:00:00Z",3847.5,3847.5],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","mean_v1","mean_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",5119.5,5119.5],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","mean_v1","mean_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",6671.5,6671.5],["1970-01-01T02:00:00Z",7695.5,7695.5],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","mean_v1","mean_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",9215.5,9215.5],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","mean_v1","mean_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",10519.5,10519.5],["1970-01-01T03:00:00Z",11543.5,11543.5],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","mean_v1","mean_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",13311.5,13311.5],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","mean_v1","mean_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",14367.5,14367.5],["1970-01-01T04:00:00Z",15391.5,15391.5],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","mean_v1","mean_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",17191.5,17191.5],["1970-01-01T05:00:00Z",18215.5,18215.5]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","mean_v1","mean_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",19455.5,19455.5]]}]}]}`,
		},

		{
			name:    "sum(*) group by time",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select sum(*) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h) order by time`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","sum_v1","sum_v2"],"values":[["1970-01-01T00:00:00Z",6478200,6478200],["1970-01-01T01:00:00Z",19438200,19438200],["1970-01-01T02:00:00Z",32398200,32398200],["1970-01-01T03:00:00Z",45358200,45358200],["1970-01-01T04:00:00Z",58318200,58318200],["1970-01-01T05:00:00Z",47713960,47713960]]}]}]}`,
		},
		{
			name:    "sum(*) group by *",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select sum(*) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by * order by az asc, region asc`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","sum_v1","sum_v2"],"values":[["1970-01-01T00:00:00Z",2096128,2096128]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","sum_v1","sum_v2"],"values":[["1970-01-01T00:00:00Z",6290432,6290432]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","sum_v1","sum_v2"],"values":[["1970-01-01T00:00:00Z",10484736,10484736]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","sum_v1","sum_v2"],"values":[["1970-01-01T00:00:00Z",14679040,14679040]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","sum_v1","sum_v2"],"values":[["1970-01-01T00:00:00Z",18873344,18873344]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","sum_v1","sum_v2"],"values":[["1970-01-01T00:00:00Z",23067648,23067648]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","sum_v1","sum_v2"],"values":[["1970-01-01T00:00:00Z",27261952,27261952]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","sum_v1","sum_v2"],"values":[["1970-01-01T00:00:00Z",31456256,31456256]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","sum_v1","sum_v2"],"values":[["1970-01-01T00:00:00Z",35650560,35650560]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","sum_v1","sum_v2"],"values":[["1970-01-01T00:00:00Z",39844864,39844864]]}]}]}`,
		},
		{
			name:    "sum(*) group by time, *",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select sum(*) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h),* order by az asc, region asc, time asc`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","sum_v1","sum_v2"],"values":[["1970-01-01T00:00:00Z",2096128,2096128],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","sum_v1","sum_v2"],"values":[["1970-01-01T00:00:00Z",4382072,4382072],["1970-01-01T01:00:00Z",1908360,1908360],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","sum_v1","sum_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",10484736,10484736],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","sum_v1","sum_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",7045104,7045104],["1970-01-01T02:00:00Z",7633936,7633936],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","sum_v1","sum_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",18873344,18873344],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","sum_v1","sum_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",5890920,5890920],["1970-01-01T03:00:00Z",17176728,17176728],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","sum_v1","sum_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",27261952,27261952],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","sum_v1","sum_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",919520,919520],["1970-01-01T04:00:00Z",30536736,30536736],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","sum_v1","sum_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",27781464,27781464],["1970-01-01T05:00:00Z",7869096,7869096]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","sum_v1","sum_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",39844864,39844864]]}]}]}`,
		},

		{
			name:    "min(v1),min(v2) group by time",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select min(v1),min(v2) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h) order by time asc`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","min","min_1"],"values":[["1970-01-01T00:00:00Z",0,0],["1970-01-01T01:00:00Z",3600,3600],["1970-01-01T02:00:00Z",7200,7200],["1970-01-01T03:00:00Z",10800,10800],["1970-01-01T04:00:00Z",14400,14400],["1970-01-01T05:00:00Z",18000,18000]]}]}]}`,
		},
		{
			name:    "min(v1),min(v2) group by *",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select min(v1),min(v2) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by * order by az asc, region asc`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","min","min_1"],"values":[["1970-01-01T00:00:00Z",0,0]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","min","min_1"],"values":[["1970-01-01T00:00:00Z",2048,2048]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","min","min_1"],"values":[["1970-01-01T00:00:00Z",4096,4096]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","min","min_1"],"values":[["1970-01-01T00:00:00Z",6144,6144]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","min","min_1"],"values":[["1970-01-01T00:00:00Z",8192,8192]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","min","min_1"],"values":[["1970-01-01T00:00:00Z",10240,10240]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","min","min_1"],"values":[["1970-01-01T00:00:00Z",12288,12288]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","min","min_1"],"values":[["1970-01-01T00:00:00Z",14336,14336]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","min","min_1"],"values":[["1970-01-01T00:00:00Z",16384,16384]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","min","min_1"],"values":[["1970-01-01T00:00:00Z",18432,18432]]}]}]}`,
		},
		{
			name:    "min(v1),min(v2) group by time, *",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select min(v1),min(v2) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h),* order by az asc, region asc, time asc`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","min","min_1"],"values":[["1970-01-01T00:00:00Z",0,0],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","min","min_1"],"values":[["1970-01-01T00:00:00Z",2048,2048],["1970-01-01T01:00:00Z",3600,3600],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","min","min_1"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",4096,4096],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","min","min_1"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",6144,6144],["1970-01-01T02:00:00Z",7200,7200],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","min","min_1"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",8192,8192],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","min","min_1"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",10240,10240],["1970-01-01T03:00:00Z",10800,10800],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","min","min_1"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",12288,12288],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","min","min_1"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",14336,14336],["1970-01-01T04:00:00Z",14400,14400],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","min","min_1"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",16384,16384],["1970-01-01T05:00:00Z",18000,18000]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","min","min_1"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",18432,18432]]}]}]}`,
		},

		{
			name:    "max(v1),max(v2) group by time",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select max(v1),max(v2) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h) order by time asc`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","max","max_1"],"values":[["1970-01-01T00:00:00Z",3599,3599],["1970-01-01T01:00:00Z",7199,7199],["1970-01-01T02:00:00Z",10799,10799],["1970-01-01T03:00:00Z",14399,14399],["1970-01-01T04:00:00Z",17999,17999],["1970-01-01T05:00:00Z",20479,20479]]}]}]}`,
		},
		{
			name:    "max(v1),max(v2) group by *",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select max(v1),max(v2) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by * order by az asc, region asc`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","max","max_1"],"values":[["1970-01-01T00:00:00Z",2047,2047]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","max","max_1"],"values":[["1970-01-01T00:00:00Z",4095,4095]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","max","max_1"],"values":[["1970-01-01T00:00:00Z",6143,6143]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","max","max_1"],"values":[["1970-01-01T00:00:00Z",8191,8191]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","max","max_1"],"values":[["1970-01-01T00:00:00Z",10239,10239]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","max","max_1"],"values":[["1970-01-01T00:00:00Z",12287,12287]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","max","max_1"],"values":[["1970-01-01T00:00:00Z",14335,14335]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","max","max_1"],"values":[["1970-01-01T00:00:00Z",16383,16383]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","max","max_1"],"values":[["1970-01-01T00:00:00Z",18431,18431]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","max","max_1"],"values":[["1970-01-01T00:00:00Z",20479,20479]]}]}]}`,
		},
		{
			name:    "max(v1),max(v2) group by time, *",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select max(v1),max(v2) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h),* order by az asc, region asc, time asc`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","max","max_1"],"values":[["1970-01-01T00:00:00Z",2047,2047],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","max","max_1"],"values":[["1970-01-01T00:00:00Z",3599,3599],["1970-01-01T01:00:00Z",4095,4095],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","max","max_1"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",6143,6143],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","max","max_1"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",7199,7199],["1970-01-01T02:00:00Z",8191,8191],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","max","max_1"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",10239,10239],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","max","max_1"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",10799,10799],["1970-01-01T03:00:00Z",12287,12287],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","max","max_1"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",14335,14335],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","max","max_1"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",14399,14399],["1970-01-01T04:00:00Z",16383,16383],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","max","max_1"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",17999,17999],["1970-01-01T05:00:00Z",18431,18431]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","max","max_1"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",20479,20479]]}]}]}`,
		},
		{
			name:    "first(*) group by time",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select first(*) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h) order by time asc`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","first_v1","first_v2","first_v3","first_v4"],"values":[["1970-01-01T00:00:00Z",0,0,true,"abc0"],["1970-01-01T01:00:00Z",3600,3600,true,"abc3600"],["1970-01-01T02:00:00Z",7200,7200,true,"abc7200"],["1970-01-01T03:00:00Z",10800,10800,true,"abc10800"],["1970-01-01T04:00:00Z",14400,14400,true,"abc14400"],["1970-01-01T05:00:00Z",18000,18000,true,"abc18000"]]}]}]}`,
			skip:    true,
		},
		{
			name:    "first(*) group by *",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select first(*) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by * order by az asc, region asc`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","first_v1","first_v2","first_v3","first_v4"],"values":[["1970-01-01T00:00:00Z",0,0,true,"abc0"]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","first_v1","first_v2","first_v3","first_v4"],"values":[["1970-01-01T00:00:00Z",2048,2048,true,"abc2048"]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","first_v1","first_v2","first_v3","first_v4"],"values":[["1970-01-01T00:00:00Z",4096,4096,true,"abc4096"]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","first_v1","first_v2","first_v3","first_v4"],"values":[["1970-01-01T00:00:00Z",6144,6144,true,"abc6144"]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","first_v1","first_v2","first_v3","first_v4"],"values":[["1970-01-01T00:00:00Z",8192,8192,true,"abc8192"]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","first_v1","first_v2","first_v3","first_v4"],"values":[["1970-01-01T00:00:00Z",10240,10240,true,"abc10240"]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","first_v1","first_v2","first_v3","first_v4"],"values":[["1970-01-01T00:00:00Z",12288,12288,true,"abc12288"]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","first_v1","first_v2","first_v3","first_v4"],"values":[["1970-01-01T00:00:00Z",14336,14336,true,"abc14336"]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","first_v1","first_v2","first_v3","first_v4"],"values":[["1970-01-01T00:00:00Z",16384,16384,true,"abc16384"]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","first_v1","first_v2","first_v3","first_v4"],"values":[["1970-01-01T00:00:00Z",18432,18432,true,"abc18432"]]}]}]}`,
		},
		{
			name:    "first(*) group by time, *",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select first(*) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h),* order by az asc, region asc, time asc`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","first_v1","first_v2","first_v3","first_v4"],"values":[["1970-01-01T00:00:00Z",0,0,true,"abc0"],["1970-01-01T01:00:00Z",null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","first_v1","first_v2","first_v3","first_v4"],"values":[["1970-01-01T00:00:00Z",2048,2048,true,"abc2048"],["1970-01-01T01:00:00Z",3600,3600,true,"abc3600"],["1970-01-01T02:00:00Z",null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","first_v1","first_v2","first_v3","first_v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null],["1970-01-01T01:00:00Z",4096,4096,true,"abc4096"],["1970-01-01T02:00:00Z",null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","first_v1","first_v2","first_v3","first_v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null],["1970-01-01T01:00:00Z",6144,6144,true,"abc6144"],["1970-01-01T02:00:00Z",7200,7200,true,"abc7200"],["1970-01-01T03:00:00Z",null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","first_v1","first_v2","first_v3","first_v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null],["1970-01-01T02:00:00Z",8192,8192,true,"abc8192"],["1970-01-01T03:00:00Z",null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","first_v1","first_v2","first_v3","first_v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null],["1970-01-01T02:00:00Z",10240,10240,true,"abc10240"],["1970-01-01T03:00:00Z",10800,10800,true,"abc10800"],["1970-01-01T04:00:00Z",null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","first_v1","first_v2","first_v3","first_v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,null],["1970-01-01T03:00:00Z",12288,12288,true,"abc12288"],["1970-01-01T04:00:00Z",null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","first_v1","first_v2","first_v3","first_v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,null],["1970-01-01T03:00:00Z",14336,14336,true,"abc14336"],["1970-01-01T04:00:00Z",14400,14400,true,"abc14400"],["1970-01-01T05:00:00Z",null,null,null,null]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","first_v1","first_v2","first_v3","first_v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,null],["1970-01-01T04:00:00Z",16384,16384,true,"abc16384"],["1970-01-01T05:00:00Z",18000,18000,true,"abc18000"]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","first_v1","first_v2","first_v3","first_v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null],["1970-01-01T05:00:00Z",18432,18432,true,"abc18432"]]}]}]}`,
		},

		{
			name:    "last(*) group by time",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select last(*) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h) order by time asc`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","last_v1","last_v2","last_v3","last_v4"],"values":[["1970-01-01T00:00:00Z",3599,3599,false,"abc3599"],["1970-01-01T01:00:00Z",7199,7199,false,"abc7199"],["1970-01-01T02:00:00Z",10799,10799,false,"abc10799"],["1970-01-01T03:00:00Z",14399,14399,false,"abc14399"],["1970-01-01T04:00:00Z",17999,17999,false,"abc17999"],["1970-01-01T05:00:00Z",20479,20479,false,"abc20479"]]}]}]}`,
			skip:    true,
		},
		{
			name:    "last(*) group by *",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select last(*) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by * order by az asc, region asc`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","last_v1","last_v2","last_v3","last_v4"],"values":[["1970-01-01T00:00:00Z",2047,2047,false,"abc2047"]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","last_v1","last_v2","last_v3","last_v4"],"values":[["1970-01-01T00:00:00Z",4095,4095,false,"abc4095"]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","last_v1","last_v2","last_v3","last_v4"],"values":[["1970-01-01T00:00:00Z",6143,6143,false,"abc6143"]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","last_v1","last_v2","last_v3","last_v4"],"values":[["1970-01-01T00:00:00Z",8191,8191,false,"abc8191"]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","last_v1","last_v2","last_v3","last_v4"],"values":[["1970-01-01T00:00:00Z",10239,10239,false,"abc10239"]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","last_v1","last_v2","last_v3","last_v4"],"values":[["1970-01-01T00:00:00Z",12287,12287,false,"abc12287"]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","last_v1","last_v2","last_v3","last_v4"],"values":[["1970-01-01T00:00:00Z",14335,14335,false,"abc14335"]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","last_v1","last_v2","last_v3","last_v4"],"values":[["1970-01-01T00:00:00Z",16383,16383,false,"abc16383"]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","last_v1","last_v2","last_v3","last_v4"],"values":[["1970-01-01T00:00:00Z",18431,18431,false,"abc18431"]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","last_v1","last_v2","last_v3","last_v4"],"values":[["1970-01-01T00:00:00Z",20479,20479,false,"abc20479"]]}]}]}`,
		},
		{
			name:    "last(*) group by time, *",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select last(*) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h),* order by az asc, region asc, time asc`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","last_v1","last_v2","last_v3","last_v4"],"values":[["1970-01-01T00:00:00Z",2047,2047,false,"abc2047"],["1970-01-01T01:00:00Z",null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","last_v1","last_v2","last_v3","last_v4"],"values":[["1970-01-01T00:00:00Z",3599,3599,false,"abc3599"],["1970-01-01T01:00:00Z",4095,4095,false,"abc4095"],["1970-01-01T02:00:00Z",null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","last_v1","last_v2","last_v3","last_v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null],["1970-01-01T01:00:00Z",6143,6143,false,"abc6143"],["1970-01-01T02:00:00Z",null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","last_v1","last_v2","last_v3","last_v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null],["1970-01-01T01:00:00Z",7199,7199,false,"abc7199"],["1970-01-01T02:00:00Z",8191,8191,false,"abc8191"],["1970-01-01T03:00:00Z",null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","last_v1","last_v2","last_v3","last_v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null],["1970-01-01T02:00:00Z",10239,10239,false,"abc10239"],["1970-01-01T03:00:00Z",null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","last_v1","last_v2","last_v3","last_v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null],["1970-01-01T02:00:00Z",10799,10799,false,"abc10799"],["1970-01-01T03:00:00Z",12287,12287,false,"abc12287"],["1970-01-01T04:00:00Z",null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","last_v1","last_v2","last_v3","last_v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,null],["1970-01-01T03:00:00Z",14335,14335,false,"abc14335"],["1970-01-01T04:00:00Z",null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","last_v1","last_v2","last_v3","last_v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,null],["1970-01-01T03:00:00Z",14399,14399,false,"abc14399"],["1970-01-01T04:00:00Z",16383,16383,false,"abc16383"],["1970-01-01T05:00:00Z",null,null,null,null]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","last_v1","last_v2","last_v3","last_v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,null],["1970-01-01T04:00:00Z",17999,17999,false,"abc17999"],["1970-01-01T05:00:00Z",18431,18431,false,"abc18431"]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","last_v1","last_v2","last_v3","last_v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null],["1970-01-01T05:00:00Z",20479,20479,false,"abc20479"]]}]}]}`,
		},

		// sub-query
		{
			name:    "percentile from (select sum(v1) group by time,*) group by az",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select PERCENTILE(v1, 95) as p95 from (select sum(v1) as v1 from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z'  group by time(1h),region,az order by az,time) group by az`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0"},"columns":["time","p95"],"values":[["1970-01-01T00:00:00Z",2096128]]},{"name":"cpu","tags":{"az":"az_1"},"columns":["time","p95"],"values":[["1970-01-01T00:00:00Z",4382072]]},{"name":"cpu","tags":{"az":"az_2"},"columns":["time","p95"],"values":[["1970-01-01T01:00:00Z",10484736]]},{"name":"cpu","tags":{"az":"az_3"},"columns":["time","p95"],"values":[["1970-01-01T01:00:00Z",7633936]]},{"name":"cpu","tags":{"az":"az_4"},"columns":["time","p95"],"values":[["1970-01-01T02:00:00Z",18873344]]},{"name":"cpu","tags":{"az":"az_5"},"columns":["time","p95"],"values":[["1970-01-01T02:00:00Z",17176728]]},{"name":"cpu","tags":{"az":"az_6"},"columns":["time","p95"],"values":[["1970-01-01T03:00:00Z",27261952]]},{"name":"cpu","tags":{"az":"az_7"},"columns":["time","p95"],"values":[["1970-01-01T03:00:00Z",30536736]]},{"name":"cpu","tags":{"az":"az_8"},"columns":["time","p95"],"values":[["1970-01-01T04:00:00Z",27781464]]},{"name":"cpu","tags":{"az":"az_9"},"columns":["time","p95"],"values":[["1970-01-01T05:00:00Z",39844864]]}]}]}`,
		},
		{
			name:    "sum(v1),mean(v2) from (select v1 from (select v1,v2)),(select v2 from (select v1, v2))",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select sum(v1), mean(v2) from (select v1 from (select v1, v2 from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z') where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' ),(select v2 from (select v1, v2 from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' ) where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z')`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","sum","mean"],"values":[["1970-01-01T00:00:00Z",209704960,10239.5]]}]}]}`,
		},
		{
			name:    "select top from (select percentile (select sum group by time,region,az) group by region,az) group by region",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select region,az,top(p95,3) as p95 from (select PERCENTILE(v1, 95) as p95 from (select sum(v1)as v1 from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h),region,az order by region,az,time) where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by region,az) where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by region`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"region":"region_0"},"columns":["time","region","az","p95"],"values":[["1970-01-01T00:00:00Z","region_0","az_0",2096128]]},{"name":"cpu","tags":{"region":"region_1"},"columns":["time","region","az","p95"],"values":[["1970-01-01T00:00:00Z","region_1","az_1",4382072]]},{"name":"cpu","tags":{"region":"region_2"},"columns":["time","region","az","p95"],"values":[["1970-01-01T01:00:00Z","region_2","az_2",10484736]]},{"name":"cpu","tags":{"region":"region_3"},"columns":["time","region","az","p95"],"values":[["1970-01-01T01:00:00Z","region_3","az_3",7633936]]},{"name":"cpu","tags":{"region":"region_4"},"columns":["time","region","az","p95"],"values":[["1970-01-01T02:00:00Z","region_4","az_4",18873344]]},{"name":"cpu","tags":{"region":"region_5"},"columns":["time","region","az","p95"],"values":[["1970-01-01T02:00:00Z","region_5","az_5",17176728]]},{"name":"cpu","tags":{"region":"region_6"},"columns":["time","region","az","p95"],"values":[["1970-01-01T03:00:00Z","region_6","az_6",27261952]]},{"name":"cpu","tags":{"region":"region_7"},"columns":["time","region","az","p95"],"values":[["1970-01-01T03:00:00Z","region_7","az_7",30536736]]},{"name":"cpu","tags":{"region":"region_8"},"columns":["time","region","az","p95"],"values":[["1970-01-01T04:00:00Z","region_8","az_8",27781464]]},{"name":"cpu","tags":{"region":"region_9"},"columns":["time","region","az","p95"],"values":[["1970-01-01T05:00:00Z","region_9","az_9",39844864]]}]}]}`,
		},
		// show tag value & * & mean(v1)
		{
			name:    "show tag values key = region",
			params:  url.Values{"inner_chunk_size": []string{"1"}, "db": []string{"db0"}},
			command: `show tag values with key = "region" order by value asc`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["key","value"],"values":[["region","region_0"],["region","region_1"],["region","region_2"],["region","region_3"],["region","region_4"],["region","region_5"],["region","region_6"],["region","region_7"],["region","region_8"],["region","region_9"]]}]}]}`,
		},
		{
			name:    "show tag values key = az",
			params:  url.Values{"inner_chunk_size": []string{"1"}, "db": []string{"db0"}},
			command: `show tag values with key = "az" order by value asc`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["key","value"],"values":[["az","az_0"],["az","az_1"],["az","az_2"],["az","az_3"],["az","az_4"],["az","az_5"],["az","az_6"],["az","az_7"],["az","az_8"],["az","az_9"]]}]}]}`,
		},
		{
			name:    "count(time)",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select count(time) from db0.rp0.cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","count"],"values":[["1970-01-01T00:00:00Z",20480]]}]}]}`,
		},
		{
			name:    "select(*) az=az_7,region=region_7 limit 3",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select * from db0.rp0.cpu where time >= 6148000000000 and time < 16383000000000 and az = 'az_7' and region = 'region_7' limit 3`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","az","region","v1","v2","v3","v4"],"values":[["1970-01-01T03:58:56Z","az_7","region_7",14336,14336,true,"abc14336"],["1970-01-01T03:58:57Z","az_7","region_7",14337,14337,false,"abc14337"],["1970-01-01T03:58:58Z","az_7","region_7",14338,14338,true,"abc14338"]]}]}]}`,
		},
		{
			name:    "v1,v2,v3 az=az_7 limit 3",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select v1,v2,v3 from db0.rp0.cpu where time >= 6148000000000 and time < 16383000000000 and az = 'az_7' limit 3`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","v1","v2","v3"],"values":[["1970-01-01T03:58:56Z",14336,14336,true],["1970-01-01T03:58:57Z",14337,14337,false],["1970-01-01T03:58:58Z",14338,14338,true]]}]}]}`,
		},
		{
			name:    "mean(v1) group by time(5m)",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select mean(v1) as avg_v1 from db0.rp0.cpu where time >= 6148000000000 and time < 16383000000000 and az = 'az_6' group by time(5m) fill(none) order by time desc`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","avg_v1"],"values":[["1970-01-01T03:55:00Z",14217.5],["1970-01-01T03:50:00Z",13949.5],["1970-01-01T03:45:00Z",13649.5],["1970-01-01T03:40:00Z",13349.5],["1970-01-01T03:35:00Z",13049.5],["1970-01-01T03:30:00Z",12749.5],["1970-01-01T03:25:00Z",12449.5],["1970-01-01T03:20:00Z",12293.5]]}]}]}`,
		},
		{
			name:    "mean(v1) group by time(5m),az",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select mean(v1) as avg_v1 from db0.rp0.cpu where time >= 0 and time < '1970-01-01T00:30:00Z' group by time(5m),az `,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0"},"columns":["time","avg_v1"],"values":[["1970-01-01T00:00:00Z",149.5],["1970-01-01T00:05:00Z",449.5],["1970-01-01T00:10:00Z",749.5],["1970-01-01T00:15:00Z",1049.5],["1970-01-01T00:20:00Z",1349.5],["1970-01-01T00:25:00Z",1649.5]]}]}]}`,
		},
		{
			name:    "mean(v1),min(v1),max(v1) group by time(5m)",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select mean(v1) as avg_v1, min(v1) as min_v1, max(v1) as max_v1 from db0.rp0.cpu where time >= 6148000000000 and time < 16383000000000 and az = 'az_6' group by time(5m) fill(none) order by time desc`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","avg_v1","min_v1","max_v1"],"values":[["1970-01-01T03:55:00Z",14217.5,14100,14335],["1970-01-01T03:50:00Z",13949.5,13800,14099],["1970-01-01T03:45:00Z",13649.5,13500,13799],["1970-01-01T03:40:00Z",13349.5,13200,13499],["1970-01-01T03:35:00Z",13049.5,12900,13199],["1970-01-01T03:30:00Z",12749.5,12600,12899],["1970-01-01T03:25:00Z",12449.5,12300,12599],["1970-01-01T03:20:00Z",12293.5,12288,12299]]}]}]}`,
		},
		{
			name:    "mean(v1),min(v1),max(v1) group by time(5m),az",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select mean(v1) as avg_v1, min(v1) as min_v1, max(v1) as max_v1 from db0.rp0.cpu where time >= 0 and time < '1970-01-01T00:30:00Z' group by time(5m),az`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0"},"columns":["time","avg_v1","min_v1","max_v1"],"values":[["1970-01-01T00:00:00Z",149.5,0,299],["1970-01-01T00:05:00Z",449.5,300,599],["1970-01-01T00:10:00Z",749.5,600,899],["1970-01-01T00:15:00Z",1049.5,900,1199],["1970-01-01T00:20:00Z",1349.5,1200,1499],["1970-01-01T00:25:00Z",1649.5,1500,1799]]}]}]}`,
		},
		{
			name:    "mean(v1),count(az) group by time(5m),az",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select mean(v1) as avg_v1, count(az) as count_az from db0.rp0.cpu where time >= 0 and time < '1970-01-01T00:30:00Z' and az = 'az_0' group by time(5m),az`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0"},"columns":["time","avg_v1","count_az"],"values":[["1970-01-01T00:00:00Z",149.5,300],["1970-01-01T00:05:00Z",449.5,300],["1970-01-01T00:10:00Z",749.5,300],["1970-01-01T00:15:00Z",1049.5,300],["1970-01-01T00:20:00Z",1349.5,300],["1970-01-01T00:25:00Z",1649.5,300]]}]}]}`,
		},
	}...)

	for i, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
			if i == 0 {
				if err := test.init(s); err != nil {
					t.Fatalf("test init failed: %s", err)
				}
				time.Sleep(5 * time.Second)
			}
			if query.skip {
				t.Skipf("SKIP:: %s", query.name)
			}

			if err := query.Execute(s); err != nil {
				t.Error(query.Error(err))
			} else if !CompareSortedResults(query.exp, query.act) {
				t.Error(query.failureMessage())
			}
		})
	}
}

func TestServer_Query_FunctionIf(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()
	if err := s.CreateDatabaseAndRetentionPolicy("flowscope", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateMeasurement("CREATE MEASUREMENT flowscope.rp0.traffic (area tag, country tag, province tag,  region string, pop string, level int64, bps int64, isisp bool, iseip bool, eqtype float64, percent float64) WITH  ENGINETYPE = columnstore  PRIMARYKEY country,area,time"); err != nil {
		t.Fatal(err)
	}
	writes := []string{
		fmt.Sprintf(`traffic,area=国内,country=中国,province=北京 region="华北",pop="五道口",level=1i,bps=111i,isisp=True,iseip=False,eqtype=1.1,percent=0.1 %d`, 1629129600000000000),
		fmt.Sprintf(`traffic,area=国内,country=中国,province=上海 region="华东",pop="人民公园",level=2i,bps=222i,isisp=True,iseip=False,eqtype=2.2,percent=0.2 %d`, 1629129601000000000),
		fmt.Sprintf(`traffic,area=国内,country=中国,province=广州 region="华南",pop="广州塔",level=3i,bps=333i,isisp=True,iseip=False,eqtype=3.3,percent=0.3 %d`, 1629129602000000000),
		fmt.Sprintf(`traffic,area=海外,country=印度,province=孟买 region="海外",pop="恒河",level=4i,bps=444i,isisp=True,iseip=False,eqtype=4.4,percent=0.4 %d`, 1629129603000000000),
		fmt.Sprintf(`traffic,area=海外,country=美国,province=好莱坞 region="海外",pop="A",level=5i,bps=555i,isisp=True,iseip=False,eqtype=5.5,percent=0.5 %d`, 1629129604000000000),
		fmt.Sprintf(`traffic,area=海外,country=美国,province=拉斯维加斯 region="海外",pop="B",level=6i,bps=666i,isisp=True,iseip=False,eqtype=6.6,percent=0.6 %d`, 1629129605000000000),
	}

	test := NewTest("flowscope", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		{
			name:    "THEN:Tag, ELSE:Tag",
			params:  url.Values{"db": []string{"flowscope"}},
			command: `SELECT if('"area"=\'国内\'', province, country) From traffic`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"traffic","columns":["time","if"],"values":[["2021-08-16T16:00:00Z","北京"],["2021-08-16T16:00:01Z","上海"],["2021-08-16T16:00:02Z","广州"],["2021-08-16T16:00:03Z","印度"],["2021-08-16T16:00:04Z","美国"],["2021-08-16T16:00:05Z","美国"]]}]}]}`,
		},
		{
			name:    "THEN:Integer, ELSE:Integer",
			params:  url.Values{"db": []string{"flowscope"}},
			command: `SELECT if('"area"=\'国内\'', bps, level) From traffic`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"traffic","columns":["time","if"],"values":[["2021-08-16T16:00:00Z",111],["2021-08-16T16:00:01Z",222],["2021-08-16T16:00:02Z",333],["2021-08-16T16:00:03Z",4],["2021-08-16T16:00:04Z",5],["2021-08-16T16:00:05Z",6]]}]}]}`,
		},
		{
			name:    "THEN:Boolean, ELSE:Boolean",
			params:  url.Values{"db": []string{"flowscope"}},
			command: `SELECT if('"area"=\'国内\'', iseip, isisp) From traffic`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"traffic","columns":["time","if"],"values":[["2021-08-16T16:00:00Z",false],["2021-08-16T16:00:01Z",false],["2021-08-16T16:00:02Z",false],["2021-08-16T16:00:03Z",true],["2021-08-16T16:00:04Z",true],["2021-08-16T16:00:05Z",true]]}]}]}`,
		},
		{
			name:    "THEN:Float, ELSE:Float",
			params:  url.Values{"db": []string{"flowscope"}},
			command: `SELECT if('"area"=\'国内\'', percent, eqtype) From traffic`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"traffic","columns":["time","if"],"values":[["2021-08-16T16:00:00Z",0.1],["2021-08-16T16:00:01Z",0.2],["2021-08-16T16:00:02Z",0.3],["2021-08-16T16:00:03Z",4.4],["2021-08-16T16:00:04Z",5.5],["2021-08-16T16:00:05Z",6.6]]}]}]}`,
		},
		{
			name:    "CONDITION: KEY>x",
			params:  url.Values{"db": []string{"flowscope"}},
			command: `SELECT if('"level">3', percent, eqtype) From traffic`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"traffic","columns":["time","if"],"values":[["2021-08-16T16:00:00Z",1.1],["2021-08-16T16:00:01Z",2.2],["2021-08-16T16:00:02Z",3.3],["2021-08-16T16:00:03Z",0.4],["2021-08-16T16:00:04Z",0.5],["2021-08-16T16:00:05Z",0.6]]}]}]}`,
			skip:    true, // TODO: column store engine is being improved and will be adapted to this use case in a later MR
		},
		{
			name:    "Different type of THEN and Else",
			params:  url.Values{"db": []string{"flowscope"}},
			command: `SELECT if('\"area\"=\'国内\'', province, level) From traffic`,
			exp:     `{"results":[{"statement_id":0,"error":"the 2nd and 3rd argument must be of same type in if()"}]}`,
		},
		{
			name:    "Invalid number of arguments",
			params:  url.Values{"db": []string{"flowscope"}},
			command: "SELECT if('\"area\"=\"国内\"', bps) From traffic",
			exp:     `{"results":[{"statement_id":0,"error":"invalid number of arguments for if, expected 3, got 2"}]}`,
		},
		{
			name:    "Invalid operator",
			params:  url.Values{"db": []string{"flowscope"}},
			command: "SELECT if('\"area\"==\"国内\"', bps, level) From traffic",
			exp:     `{"results":[{"statement_id":0,"error":"invalid condition, input like '\"key\" [operator] \\'string\\'' or '\"key\" [operator] digit'"}]}`,
		},
	}...)

	for i, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
			if i == 0 {
				if err := test.init(s); err != nil {
					t.Fatalf("test init failed: %s", err)
				}
				time.Sleep(3 * time.Second)
			}
			if query.skip {
				t.Skipf("SKIP:: %s", query.name)
			}
			if err := query.Execute(s); err != nil {
				t.Error(query.Error(err))
			} else if !CompareSortedResults(query.exp, query.act) {
				t.Error(query.failureMessage())
			}
		})
	}
}

func TestServer_Select_PKkey(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()
	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateMeasurement("CREATE measurement db0.rp0.mst (country tag, name1 tag, age int64,  height float64,  address string, alive bool) WITH  ENGINETYPE = columnstore  PRIMARYKEY country,name1"); err != nil {
		t.Fatal(err)
	}
	writes := []string{
		fmt.Sprintf(`mst,country=china,name1=azhu age=12i,height=70,address="shenzhen",alive=TRUE %d`, 1629129600000000000),
		fmt.Sprintf(`mst,country=american,name1=alan age=20i,height=80,address="shanghai",alive=FALSE %d`, 1629129601000000000),
		fmt.Sprintf(`mst,country=germany,name1=alang age=3i,height=90,address="beijin",alive=TRUE %d`, 1629129602000000000),
		fmt.Sprintf(`mst,country=japan,name1=ahui age=30i,height=121,address="guangzhou",alive=FALSE %d`, 1629129603000000000),
		fmt.Sprintf(`mst,country=canada,name1=aqiu age=35i,height=138,address="chengdu",alive=TRUE %d`, 1629129604000000000),
		fmt.Sprintf(`mst,country=china,name1=agang age=48i,height=149,address="wuhan",alive=TRUE %d`, 1629129605000000000),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		{
			name:    "select * order by pkkey",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * from mst order by country`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","address","age","alive","country","height","name1"],"values":[["2021-08-16T16:00:01Z","shanghai",20,false,"american",80,"alan"],["2021-08-16T16:00:04Z","chengdu",35,true,"canada",138,"aqiu"],["2021-08-16T16:00:00Z","shenzhen",12,true,"china",70,"azhu"],["2021-08-16T16:00:05Z","wuhan",48,true,"china",149,"agang"],["2021-08-16T16:00:02Z","beijin",3,true,"germany",90,"alang"],["2021-08-16T16:00:03Z","guangzhou",30,false,"japan",121,"ahui"]]}]}]}`,
		},
		{
			name:    "select pkkey",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT country, name1 from mst order by name1`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","country","name1"],"values":[["2021-08-16T16:00:05Z","china","agang"],["2021-08-16T16:00:03Z","japan","ahui"],["2021-08-16T16:00:01Z","american","alan"],["2021-08-16T16:00:02Z","germany","alang"],["2021-08-16T16:00:04Z","canada","aqiu"],["2021-08-16T16:00:00Z","china","azhu"]]}]}]}`,
		},
		{
			name:    "select * with condition",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * from mst where country='canada'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","address","age","alive","country","height","name1"],"values":[["2021-08-16T16:00:04Z","chengdu",35,true,"canada",138,"aqiu"]]}]}]}`,
		},
	}...)

	for i, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
			if i == 0 {
				if err := test.init(s); err != nil {
					t.Fatalf("test init failed: %s", err)
				}
				time.Sleep(3 * time.Second)
			}
			if query.skip {
				t.Skipf("SKIP:: %s", query.name)
			}
			if err := query.Execute(s); err != nil {
				t.Error(query.Error(err))
			} else if !CompareSortedResults(query.exp, query.act) {
				t.Error(query.failureMessage())
			}
		})
	}
}
