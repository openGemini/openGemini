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

package geminiql

import (
	"reflect"
	"strings"
	"testing"

	"github.com/influxdata/influxdb/pkg/testing/assert"
)

func TestParser(t *testing.T) {
	p := QLNewParser()
	for _, tc := range []struct {
		name   string
		cmd    string
		expect Statement
	}{
		{
			name: "simple insert",
			cmd:  "insert c,t1=a v1=10 1",
			expect: &InsertStatement{
				DB:           "",
				RP:           "",
				LineProtocol: "c,t1=a v1=10 1",
			},
		},
		{
			name: "simple insert without time",
			cmd:  "insert c,t1=a v1=10",
			expect: &InsertStatement{
				DB:           "",
				RP:           "",
				LineProtocol: "c,t1=a v1=10",
			},
		},
		{
			name: "insert with time",
			cmd:  "insert c,t1=a,t2=b,t3=c v1=10,v2=20,v3=30 1",
			expect: &InsertStatement{
				DB:           "",
				RP:           "",
				LineProtocol: "c,t1=a,t2=b,t3=c v1=10,v2=20,v3=30 1",
			},
		},
		{
			name: "insert without time",
			cmd:  "insert c,t1=a,t2=b,t3=c v1=10,v2=20,v3=30",
			expect: &InsertStatement{
				DB:           "",
				RP:           "",
				LineProtocol: "c,t1=a,t2=b,t3=c v1=10,v2=20,v3=30",
			},
		},
		{
			name: "single quota tag value insert",
			cmd:  "insert c,t1='a',t2='b',t3='c' v1=10,v2=20,v3=30 1",
			expect: &InsertStatement{
				DB:           "",
				RP:           "",
				LineProtocol: "c,t1='a',t2='b',t3='c' v1=10,v2=20,v3=30 1",
			},
		},
		{
			name: "double quota tag value insert",
			cmd:  "insert c,t1=\"a\",t2=\"b\",t3=\"c\" v1=10,v2=20,v3=30 1",
			expect: &InsertStatement{
				DB:           "",
				RP:           "",
				LineProtocol: "c,t1=\"a\",t2=\"b\",t3=\"c\" v1=10,v2=20,v3=30 1",
			},
		},
		{
			name: "single quota field value insert",
			cmd:  "insert c,t1=a,t2=b,t3=c v1='10',v2='20',v3='30' 1",
			expect: &InsertStatement{
				DB:           "",
				RP:           "",
				LineProtocol: "c,t1=a,t2=b,t3=c v1='10',v2='20',v3='30' 1",
			},
		},
		{
			name: "double quota field value insert",
			cmd:  "insert c,t1=a,t2=b,t3=c v1=\"10\",v2=\"20\",v3=\"30\" 1",
			expect: &InsertStatement{
				DB:           "",
				RP:           "",
				LineProtocol: "c,t1=a,t2=b,t3=c v1=\"10\",v2=\"20\",v3=\"30\" 1",
			},
		},
		{
			name: "specify db and rp insert",
			cmd:  "insert into a.b c,t1=a,t2=b,t3=c v1=10,v2=20,v3=30 1",
			expect: &InsertStatement{
				DB:           "a",
				RP:           "b",
				LineProtocol: "c,t1=a,t2=b,t3=c v1=10,v2=20,v3=30 1",
			},
		},
		{
			name: "field_value with blank space insert",
			cmd:  `insert weather,location=us-midwest temperature_str="too warm" 1465839840100400200`,
			expect: &InsertStatement{
				DB:           "",
				RP:           "",
				LineProtocol: `weather,location=us-midwest temperature_str="too warm" 1465839840100400200`,
			},
		},
		{
			name: "field_value with comma insert",
			cmd:  `insert weather,location=us-midwest temperature_str="too,warm" 1465839840100400200`,
			expect: &InsertStatement{
				DB:           "",
				RP:           "",
				LineProtocol: `weather,location=us-midwest temperature_str="too,warm" 1465839840100400200`,
			},
		},
		{
			name: "field_value with double quota insert",
			cmd:  `insert weather,location=us-midwest temperature_str="too\"hot\"" 1465839830100400213`,
			expect: &InsertStatement{
				DB:           "",
				RP:           "",
				LineProtocol: `weather,location=us-midwest temperature_str="too\"hot\"" 1465839830100400213`,
			},
		},
		{
			name: "field_value with back slash insert",
			cmd:  `insert weather,location=us-midwest temperature_str="too hot\cold" 1465839830100400213`,
			expect: &InsertStatement{
				DB:           "",
				RP:           "",
				LineProtocol: `weather,location=us-midwest temperature_str="too hot\cold" 1465839830100400213`,
			},
		},
		{
			name: "field_value with forward slash insert",
			cmd:  `insert weather,location=us-midwest temperature_str="too hot/cold" 1465839830100400213`,
			expect: &InsertStatement{
				DB:           "",
				RP:           "",
				LineProtocol: `weather,location=us-midwest temperature_str="too hot/cold" 1465839830100400213`,
			},
		},
		{
			name: "field_value with many back slash insert",
			cmd:  `insert weather,location=us-midwest temperature_str="too hot\\\\\cold" 1465839830100400213`,
			expect: &InsertStatement{
				DB:           "",
				RP:           "",
				LineProtocol: `weather,location=us-midwest temperature_str="too hot\\\\\cold" 1465839830100400213`,
			},
		},
		{
			name: "tag_value with back slash and comma insert",
			cmd:  `insert weather,location=us\,midwest temperature=82 1465839830100400190`,
			expect: &InsertStatement{
				DB:           "",
				RP:           "",
				LineProtocol: `weather,location=us\,midwest temperature=82 1465839830100400190`,
			},
		},
		{
			name: "tag_key with back slash and blank space insert",
			cmd:  `insert weather,location\ place=us-midwest temperature=82 1465839830100400192`,
			expect: &InsertStatement{
				DB:           "",
				RP:           "",
				LineProtocol: `weather,location\ place=us-midwest temperature=82 1465839830100400192`,
			},
		},
		{
			name: "field_key with back slash insert",
			cmd:  `insert weather,location=us-midwest temp\=rature=82 1465839830100400191`,
			expect: &InsertStatement{
				DB:           "",
				RP:           "",
				LineProtocol: `weather,location=us-midwest temp\=rature=82 1465839830100400191`,
			},
		},
		// {
		// 	name: "measurement with back slash and comma insert",
		// 	cmd:  `insert wea\,ther,location=us-midwest temperature=82 1465839830100400200`,
		// 	expect: &InsertStatement{
		// 		DB:           "",
		// 		RP:           "",
		// 		LineProtocol: `wea\,ther,location=us-midwest temperature=82 1465839830100400200`,
		// 	},
		// },
		// {
		// 	name: "measurement with three back slash and blank space insert",
		// 	cmd:  `insert wea\\\ ther,location=us-midwest temperature=82 1465839830100400200`,
		// 	expect: &InsertStatement{
		// 		DB:           "",
		// 		RP:           "",
		// 		LineProtocol: `wea\\\ ther,location=us-midwest temperature=82 1465839830100400200`,
		// 	},
		// },
		{
			name: "measurement with back slash and blank space insert",
			cmd:  `insert wea\ ther,location=us-midwest temperature=82 1465839830100400200`,
			expect: &InsertStatement{
				DB:           "",
				RP:           "",
				LineProtocol: `wea\ ther,location=us-midwest temperature=82 1465839830100400200`,
			},
		},
		{
			name: "measurement with emoji insert",
			cmd:  `insert we⛅️ther,location=us-midwest temper🔥ture=82 1465839830100400200`,
			expect: &InsertStatement{
				DB:           "",
				RP:           "",
				LineProtocol: `we⛅️ther,location=us-midwest temper🔥ture=82 1465839830100400200`,
			},
		},
		{
			name: "use db and rp",
			cmd:  "use db.rp",
			expect: &UseStatement{
				DB: "db",
				RP: "rp",
			},
		},
		{
			name: "use db only",
			cmd:  "use db",
			expect: &UseStatement{
				DB: "db",
				RP: "",
			},
		},
		{
			name:   "switch chunk model",
			cmd:    "chunked",
			expect: &ChunkedStatement{},
		},
		{
			name: "set chunk size",
			cmd:  "chunk_size 1000",
			expect: &ChunkSizeStatement{
				Size: 1000,
			},
		},
		{
			name:   "auth with username & passowrd",
			cmd:    "auth",
			expect: &AuthStatement{},
		},
		{
			name: "set precision",
			cmd:  "precision ns",
			expect: &PrecisionStatement{
				Precision: "ns",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ast := &QLAst{}
			l := QLNewLexer(NewTokenizer(strings.NewReader(tc.cmd)), ast)
			p.Parse(l)
			assert.Equal(t, ast.Error, nil)
			assert.Equal(t, reflect.DeepEqual(ast.Stmt, tc.expect), true)
		})
	}
}
