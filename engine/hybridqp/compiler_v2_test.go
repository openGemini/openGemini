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

package hybridqp_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	qry "github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper function to parse and rewrite an InfluxQL statement
func parseAndRewrite(sql string) (influxql.Statement, error) {
	sqlReader := strings.NewReader(sql)
	parser := influxql.NewParser(sqlReader)
	yaccParser := influxql.NewYyParser(parser.GetScanner(), make(map[string]interface{}))
	yaccParser.ParseTokens()

	query, err := yaccParser.GetQuery()
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}

	if len(query.Statements) == 0 {
		return nil, fmt.Errorf("no statements found in query")
	}

	stmt, err := qry.RewriteStatement(query.Statements[0])
	if err != nil {
		return nil, fmt.Errorf("rewrite error: %w", err)
	}

	return stmt, nil
}

func TestVerifyHintStmt(t *testing.T) {
	testCases := []struct {
		name    string // A descriptive name for the test case.
		sql     string // The input SQL query with hints.
		wantErr bool   // Whether we expect an error from VerifyHintStmt: always false.
	}{
		{
			name:    "Valid hint: Filter_Null_Column",
			sql:     "SELECT /*+ Filter_Null_Column */ value FROM cpu",
			wantErr: false,
		},
		{
			name:    "Valid hint: Exact_Statistic_Query",
			sql:     "SELECT /*+ Exact_Statistic_Query */ value FROM cpu",
			wantErr: false,
		},
		{
			name:    "Valid hint: Full_Series",
			sql:     "SELECT /*+ Full_Series */ value FROM cpu",
			wantErr: false,
		},
		{
			name:    "Valid hint: Specific_Series",
			sql:     "SELECT /*+ Specific_Series */ value FROM cpu",
			wantErr: false,
		},
		{
			name:    "Valid hint: Query_Push_Down",
			sql:     "SELECT /*+ Query_Push_Down */ value FROM cpu",
			wantErr: false,
		},
		{
			name:    "No hint provided",
			sql:     "SELECT value FROM cpu",
			wantErr: false,
		},
		{
			name:    "Invalid hint",
			sql:     "SELECT /*+ This_Is_An_Invalid_Hint */ value FROM cpu",
			wantErr: false,
		},
		{
			name:    "Multiple valid hints",
			sql:     "SELECT /*+ Exact_Statistic_Query, Filter_Null_Column */ value FROM cpu",
			wantErr: false,
		},
		{
			name:    "One valid and one invalid hint",
			sql:     "SELECT /*+ Exact_Statistic_Query, Bogus_Hint */ value FROM cpu",
			wantErr: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// --- Test Setup: Parse the SQL string ---
			stmt, err := parseAndRewrite(tc.sql)
			if err != nil {
				t.Fatalf("failed to parse and rewrite sql: %v", err)
			}

			selectStmt, ok := stmt.(*influxql.SelectStatement)
			if !ok {
				t.Fatal("statement is not a *influxql.SelectStatement")
			}

			// --- Execute the function under test ---
			opt := qry.ProcessorOptions{}
			err = hybridqp.VerifyHintStmt(selectStmt, &opt)

			// --- Assert the outcome ---
			if tc.wantErr {
				assert.Error(t, err, "expected an error but got none")
			} else {
				assert.NoError(t, err, "expected no error but got one")
			}
		})
	}
}

func TestIsFilterNullColumnQuery(t *testing.T) {
	stmt, err := parseAndRewrite(`select /*+ Filter_Null_Column */ value from cpu`)
	if err != nil {
		t.Fatalf("failed to parse and rewrite sql: %v", err)
	}

	selectStmt, ok := stmt.(*influxql.SelectStatement)
	if !ok {
		t.Fatal("statement is not a *influxql.SelectStatement")
	}

	assert.Equal(t, true, hybridqp.IsFilterNullColumnQuery(selectStmt))
}

func TestIsExactStatisticQuery(t *testing.T) {
	conf := config.GetCommon()
	conf.PreAggEnabled = false
	defer func() {
		conf.PreAggEnabled = true
	}()

	// --- Test case where PreAggEnabled is false
	require.True(t, hybridqp.IsExactStatisticQuery(nil))

	// --- Test case where PreAggEnabled is true and hint is present
	conf.PreAggEnabled = true

	stmt, err := parseAndRewrite(`select /*+ exact_statistic_query */ value from cpu`)
	if err != nil {
		t.Fatalf("failed to parse and rewrite sql: %v", err)
	}

	selectStmt, ok := stmt.(*influxql.SelectStatement)
	if !ok {
		t.Fatal("statement is not a *influxql.SelectStatement")
	}

	assert.Equal(t, true, hybridqp.IsExactStatisticQuery(selectStmt))
}

func TestIsFullSeriesQuery(t *testing.T) {
	stmt, err := parseAndRewrite(`select /*+ full_series */ value from cpu`)
	if err != nil {
		t.Fatalf("failed to parse and rewrite sql: %v", err)
	}

	selectStmt, ok := stmt.(*influxql.SelectStatement)
	if !ok {
		t.Fatal("statement is not a *influxql.SelectStatement")
	}

	assert.Equal(t, true, hybridqp.IsFullSeriesQuery(selectStmt))
}

func TestIsSpecificSeriesQuery(t *testing.T) {
	stmt, err := parseAndRewrite(`select /*+ Specific_Series */ value from cpu`)
	if err != nil {
		t.Fatalf("failed to parse and rewrite sql: %v", err)
	}

	selectStmt, ok := stmt.(*influxql.SelectStatement)
	if !ok {
		t.Fatal("statement is not a *influxql.SelectStatement")
	}

	assert.Equal(t, true, hybridqp.IsSpecificSeriesQuery(selectStmt))
}

func TestQueryPushDown(t *testing.T) {
	stmt, err := parseAndRewrite(`select /*+ Query_Push_Down */ value from cpu`)
	if err != nil {
		t.Fatalf("failed to parse and rewrite sql: %v", err)
	}

	selectStmt, ok := stmt.(*influxql.SelectStatement)
	if !ok {
		t.Fatal("statement is not a *influxql.SelectStatement")
	}

	assert.Equal(t, true, hybridqp.IsQueryPushDown(selectStmt))
}

func TestGetHintType(t *testing.T) {
	// Test case 1: Measurement with hint
	t.Run("Measurement with hint", func(t *testing.T) {
		stmt := &influxql.SelectStatement{
			Sources: []influxql.Source{
				&influxql.Measurement{Name: "cpu"},
			},
			Hints: []*influxql.Hint{{Expr: &influxql.StringLiteral{Val: "full_series"}}},
		}
		result := hybridqp.GetHintType(stmt)
		expected := hybridqp.FullSeriesQuery
		if result != expected {
			t.Errorf("Expected %v, got %v", expected, result)
		}
	})

	// Test case 2: SubQuery with hint
	t.Run("SubQuery with hint", func(t *testing.T) {
		subStmt := &influxql.SelectStatement{
			Sources: []influxql.Source{
				&influxql.Measurement{Name: "cpu"},
			},
			Hints: []*influxql.Hint{{Expr: &influxql.StringLiteral{Val: "full_series"}}},
		}
		stmt := &influxql.SelectStatement{
			Sources: []influxql.Source{
				&influxql.SubQuery{Statement: subStmt},
			},
		}
		result := hybridqp.GetHintType(stmt)
		expected := hybridqp.FullSeriesQuery
		if result != expected {
			t.Errorf("Expected %v, got %v", expected, result)
		}
	})

	// Test case 3: No hint found
	t.Run("No hint found", func(t *testing.T) {
		stmt := &influxql.SelectStatement{
			Sources: []influxql.Source{
				&influxql.Measurement{Name: "cpu"},
			},
		}
		result := hybridqp.GetHintType(stmt)
		expected := hybridqp.DefaultNoHint
		if result != expected {
			t.Errorf("Expected %v, got %v", expected, result)
		}
	})

	// Test case 4: No hint found with join
	t.Run("No hint found with join", func(t *testing.T) {
		stmt := &influxql.SelectStatement{
			Sources: []influxql.Source{
				&influxql.Join{},
			},
		}
		result := hybridqp.GetHintType(stmt)
		expected := hybridqp.DefaultNoHint
		if result != expected {
			t.Errorf("Expected %v, got %v", expected, result)
		}
	})

	// Test case 5: Multiple sources, first has hint
	t.Run("Multiple sources, first has hint", func(t *testing.T) {
		stmt := &influxql.SelectStatement{
			Sources: []influxql.Source{
				&influxql.Measurement{Name: "cpu"},
				&influxql.Measurement{Name: "mem"},
			},
			Hints: []*influxql.Hint{{Expr: &influxql.StringLiteral{Val: "full_series"}}},
		}
		result := hybridqp.GetHintType(stmt)
		expected := hybridqp.FullSeriesQuery
		if result != expected {
			t.Errorf("Expected %v, got %v", expected, result)
		}
	})
}
