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

package influxql

import (
	"strings"
	"testing"
)

var singleGroupBy181 = "SELECT max(usage_user) from cpu " +
	"where (hostname = 'host_3324' or hostname = 'host_248' or " +
	"hostname = 'host_3651' or hostname = 'host_1141' or hostname = " +
	"'host_2826' or hostname = 'host_1705' or hostname = 'host_4163' " +
	"or hostname = 'host_1275') " +
	"and time >= '2021-11-02T03:18:45Z' and time < '2021-11-02T04:18:45Z' " +
	"group by time(1m)"

/*
Before and After Optimization Comparison
1. Before:
cpu: Intel(R) Xeon(R) Gold 6266C CPU @ 3.00GHz
BenchmarkMethod_yyParser
BenchmarkMethod_yyParser-8   	  332272	      3523 ns/op	   13568 B/op	       1 allocs/op

2. After:
cpu: Intel(R) Xeon(R) Gold 6266C CPU @ 3.00GHz
BenchmarkMethod_yyParser
BenchmarkMethod_yyParser-8   	 6042177	       196.3 ns/op	       0 B/op	       0 allocs/op
*/
func BenchmarkMethod_yyParser(b *testing.B) {
	sqlReader := strings.NewReader(singleGroupBy181)
	parser := NewParser(sqlReader)
	yaccParser := NewYyParser(parser.GetScanner(), make(map[string]interface{}))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		yaccParser.ParseTokens()
	}
}
