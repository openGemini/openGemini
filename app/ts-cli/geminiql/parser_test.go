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
	"strings"
	"testing"

	"github.com/influxdata/influxdb/pkg/testing/assert"
)

func TestParser(t *testing.T) {
	p := QLNewParser()
	for _, tc := range []struct {
		name string
		cmd  string
	}{
		{
			name: "simple insert",
			cmd:  "insert into a.b c,t1=a,t2=b,t3=c v1=10,v2=20,v3=30 1",
		},
		{
			name: "string insert",
			cmd:  "insert into a.b c,t1=a,t2=b,t3=c v1='10',v2='20',v3=30 1",
		},
		{
			name: "string time insert",
			cmd:  "insert into a.b c,t1=a,t2=b,t3=c v1='10',v2='20',v3=30 '2022/110/18 15:50:00'",
		},
		{
			name: "omit db and rp insert",
			cmd:  "insert into c,t1=a,t2=b,t3=c v1='10',v2='20',v3=30 '2022/110/18 15:50:00'",
		},
		{
			name: "use db and rp",
			cmd:  "use db.rp",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ast := &CmdAst{}
			l := QLNewLexer(NewTokenizer(strings.NewReader(tc.cmd)), ast)
			p.Parse(l)
			assert.NotEqual(t, ast.Stmt, nil)
			assert.Equal(t, ast.Error, nil)
		})
	}
}
