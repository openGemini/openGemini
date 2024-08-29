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

package hybridqp_test

import (
	"strings"
	"testing"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	qry "github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsFullSeriesQuery(t *testing.T) {
	sql := `select /*+ full_series */ value from cpu where (host = 'server05' AND region = 'uswest')`
	sqlReader := strings.NewReader(sql)
	parser := influxql.NewParser(sqlReader)
	yaccParser := influxql.NewYyParser(parser.GetScanner(), make(map[string]interface{}))
	yaccParser.ParseTokens()
	query, err := yaccParser.GetQuery()
	if err != nil {
		t.Fatal("parse error", err.Error())
	}

	stmt := query.Statements[0]

	stmt, err = qry.RewriteStatement(stmt)
	if err != nil {
		t.Fatal("rewrite error", err.Error())
	}
	selectStmt, ok := stmt.(*influxql.SelectStatement)
	if !ok {
		t.Fatal("stmt is not influxql.SelectStatement")
	}
	assert.Equal(t, true, hybridqp.IsFullSeriesQuery(selectStmt))
}

func TestIsSpecificSeriesQuery(t *testing.T) {
	sql := `select /*+ specific_series */ value from cpu where (host = 'server01')`
	sqlReader := strings.NewReader(sql)
	parser := influxql.NewParser(sqlReader)
	yaccParser := influxql.NewYyParser(parser.GetScanner(), make(map[string]interface{}))
	yaccParser.ParseTokens()
	query, err := yaccParser.GetQuery()
	if err != nil {
		t.Fatal("parse error", err.Error())
	}

	stmt := query.Statements[0]

	stmt, err = qry.RewriteStatement(stmt)
	if err != nil {
		t.Fatal("rewrite error", err.Error())
	}
	selectStmt, ok := stmt.(*influxql.SelectStatement)
	if !ok {
		t.Fatal("stmt is not influxql.SelectStatement")
	}
	assert.Equal(t, true, hybridqp.IsSpecificSeriesQuery(selectStmt))

}

func TestVerifyHintStmt(t *testing.T) {
	sql := `select /*+ Exact_Statistic_Query */ value from cpu limit 1`
	sqlReader := strings.NewReader(sql)
	parser := influxql.NewParser(sqlReader)
	yaccParser := influxql.NewYyParser(parser.GetScanner(), make(map[string]interface{}))
	yaccParser.ParseTokens()
	query, err := yaccParser.GetQuery()
	if err != nil {
		t.Fatal("parse error", err.Error())
	}

	stmt := query.Statements[0]

	stmt, err = qry.RewriteStatement(stmt)
	if err != nil {
		t.Fatal("rewrite error", err.Error())
	}
	selectStmt, ok := stmt.(*influxql.SelectStatement)
	if !ok {
		t.Fatal("stmt is not influxql.SelectStatement")
	}
	opt := qry.ProcessorOptions{}
	assert.Equal(t, nil, hybridqp.VerifyHintStmt(selectStmt, &opt))
}

func TestIsExactStatisticQuery(t *testing.T) {
	conf := config.GetCommon()
	conf.PreAggEnabled = false
	defer func() {
		conf.PreAggEnabled = true
	}()

	require.True(t, hybridqp.IsExactStatisticQuery(nil))
}
