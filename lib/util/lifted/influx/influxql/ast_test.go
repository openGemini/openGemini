package influxql

import (
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/stretchr/testify/assert"
)

func TestRewritePercentileOGSketchStatement(t *testing.T) {
	st := &SelectStatement{}
	st.Sources = append(st.Sources, &SubQuery{})
	st.Fields = append(st.Fields, &Field{Expr: &Call{Name: "percentile_ogsketch"}})
	RewritePercentileOGSketchStatement(st)
	assert.Equal(t, "percentile_approx", st.Fields[0].Expr.(*Call).Name)

	st1 := &SelectStatement{}
	st1.Fields = append(st1.Fields, &Field{Expr: &Call{Name: "percentile_ogsketch"}})
	RewritePercentileOGSketchStatement(st1)
	assert.Equal(t, "percentile_ogsketch", st1.Fields[0].Expr.(*Call).Name)

	st2 := &SelectStatement{}
	st2.Sources = append(st2.Sources, &SubQuery{})
	st2.Fields = append(st2.Fields, &Field{Expr: &Call{Name: "max"}})
	RewritePercentileOGSketchStatement(st)
	assert.Equal(t, "max", st2.Fields[0].Expr.(*Call).Name)
}

func Test_RewriteCondForLogKeeper(t *testing.T) {
	schema := map[string]Expr{
		"content": &VarRef{Val: "content", Type: String},
		"age":     &VarRef{Val: "age", Type: Integer},
		"weight":  &VarRef{Val: "weight", Type: Unsigned},
		"height":  &VarRef{Val: "height", Type: Float},
		"alive":   &VarRef{Val: "alive", Type: Boolean},
	}
	condition := &BinaryExpr{
		Op: AND,
		LHS: &BinaryExpr{
			Op: OR,
			LHS: &BinaryExpr{
				Op: OR,
				LHS: &BinaryExpr{
					Op:  MATCHPHRASE,
					LHS: &VarRef{Val: "content"},
					RHS: &StringLiteral{Val: "shanghai"},
				},
				RHS: &BinaryExpr{
					Op:  MATCHPHRASE,
					LHS: &VarRef{Val: "alive"},
					RHS: &StringLiteral{Val: "TRUE"},
				},
			},
			RHS: &BinaryExpr{
				Op: OR,
				LHS: &BinaryExpr{
					Op:  GT,
					LHS: &VarRef{Val: "height"},
					RHS: &StringLiteral{Val: "120.5"},
				},
				RHS: &BinaryExpr{
					Op:  LT,
					LHS: &VarRef{Val: "height"},
					RHS: &StringLiteral{Val: "180.9"},
				},
			},
		},
		RHS: &BinaryExpr{
			Op: AND,
			LHS: &BinaryExpr{
				Op: AND,
				LHS: &BinaryExpr{
					Op:  GTE,
					LHS: &VarRef{Val: "weight"},
					RHS: &StringLiteral{Val: "120"},
				},
				RHS: &BinaryExpr{
					Op:  LTE,
					LHS: &VarRef{Val: "weight"},
					RHS: &StringLiteral{Val: "180"},
				},
			},
			RHS: &BinaryExpr{
				Op:  GTE,
				LHS: &VarRef{Val: "age"},
				RHS: &StringLiteral{Val: "18"},
			},
		},
	}
	config.SetProductType("logkeeper")
	err := RewriteCondVarRef(condition, schema)
	if err != nil {
		t.Fatal(err)
	}
}

func Test_NumberLiteralString(t *testing.T) {
	tMap := []float64{123456789, 12345678.9, 12345.6789, 1.23456789, 0.123456789, 1, 1.123456, 1.123456789123456}
	var number NumberLiteral
	for _, v := range tMap {
		number.Val = v
		newNum, err := strconv.ParseFloat(number.String(), 64)
		if err != nil {
			t.Fatal(err)
		}
		if newNum != v {
			t.Fatalf("NumberLiteral format failed, expected: %f, get:%f", newNum, v)
		}
	}
}

func Test_rewriteIpString(t *testing.T) {
	type args struct {
		expr *BinaryExpr
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "when IPINRANGE(ip, '99fa:25a4:b87c:3d56:99b2:46d0:3c9e:17b1') should rewrite to IPINRANGE(ip, '99fa:25a4:b87c:3d56:99b2:46d0:3c9e:17b1/128')",
			args: args{
				expr: &BinaryExpr{
					Op:  IPINRANGE,
					LHS: &VarRef{Val: "ip"},
					RHS: &StringLiteral{Val: "99fa:25a4:b87c:3d56:99b2:46d0:3c9e:17b1"},
				},
			},
			want: "'99fa:25a4:b87c:3d56:99b2:46d0:3c9e:17b1/128'",
		},
		{
			name: "when IPINRANGE(ip, '1.2.3.4') should rewrite to IPINRANGE(ip, '1.2.3.4/32')",
			args: args{
				expr: &BinaryExpr{
					Op:  IPINRANGE,
					LHS: &VarRef{Val: "ip"},
					RHS: &StringLiteral{Val: "1.2.3.4"},
				},
			},
			want: "'1.2.3.4/32'",
		},
		{
			name: "when IPINRANGE(ip, '1.2.3.4/32') should rewrite to IPINRANGE(ip, '1.2.3.4/32')",
			args: args{
				expr: &BinaryExpr{
					Op:  IPINRANGE,
					LHS: &VarRef{Val: "ip"},
					RHS: &StringLiteral{Val: "1.2.3.4/32"},
				},
			},
			want: "'1.2.3.4/32'",
		},
		{
			name: "when IPINRANGE(ip, 'abc') should do nothing",
			args: args{
				expr: &BinaryExpr{
					Op:  IPINRANGE,
					LHS: &VarRef{Val: "ip"},
					RHS: &StringLiteral{Val: "abc"},
				},
			},
			want: "'abc'",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_ = rewriteIpString(tt.args.expr)
			assert.Equalf(t, tt.want, tt.args.expr.RHS.String(), "rewriteIpString()")
		})
	}
}

func TestCreateStreamStatement_Check_NoGroupBy(t *testing.T) {
	c := &CreateStreamStatement{Target: &Target{Measurement: &Measurement{Database: "testdb", RetentionPolicy: "autogen"}}}
	for _, testcase := range []struct {
		sql    string
		expect string
	}{
		{"select a::tag, b::tag, c::field from testdb.autogen.mst where c::field >= 1.0", ""},
		{"select sum(c::field) from testdb.autogen.mst where c::field >= 1.0", "the filter-only stream task can contain only var_ref(tag or field)"},
		{"select a::tag, c::field from testdb.autogen.mst where c::field >= 1.0", "all tags must be selected for the filter-only stream task"},
		{"select a::tag, b::tag, c::field from (select * from testdb.autogen.mst)", "only measurement is allowed for the data source"},
		{"select a::tag, b::tag, c::field from testdb.autogen.mst1, testdb.autogen.mst2", "only one source can be selected"},
		{"select a::tag, b::tag, c::field from testdb.autogen.mst1 where d::field =~ /test*/", "only support int/float/bool/string/field condition"},
		{"select a::tag, b::tag, c::field from testdb2.autogen.mst1 where c::field >= 1.0", "the source and destination measurement must be in the same database"},
		{"select a::tag, b::tag, c::field from testdb.testrp.mst1 where c::field >= 1.0", "the source and destination measurement must be in the same retention policy"},
	} {
		stmt, err := ParseStatement(testcase.sql)
		if err != nil {
			t.Fatal(err)
		}

		selectStmt, ok := stmt.(*SelectStatement)
		if !ok {
			t.Fatal("not a select")
		}
		err = c.Check(selectStmt, map[string]bool{}, 2)
		if testcase.expect == "" {
			assert.NoError(t, err)
		} else {
			assert.EqualError(t, err, testcase.expect)
		}
	}
}

func TestRewriteCompare(t *testing.T) {
	stmt := &SelectStatement{
		Fields: []*Field{{Expr: &Call{Name: "compare", Args: []Expr{&VarRef{Val: "PV"}, &IntegerLiteral{Val: 86400}}}}},
		Sources: []Source{&SubQuery{Statement: &SelectStatement{
			Fields: []*Field{{Expr: &Call{Name: "count", Args: []Expr{&VarRef{Val: "f"}}}, Alias: "PV"}},
		}}},
	}
	min := time.Unix(1740452400, 0)
	max := time.Unix(1740456000, 0)
	tr := &TimeRange{
		Min: min,
		Max: max,
	}
	stmt.RewriteCompare(tr)

	assert.Equal(t, min.Add(-time.Hour*24).UnixNano(), tr.Min.UnixNano())
	assert.Equal(t, "PV1", stmt.Fields[0].Name())
	assert.Equal(t, "PV2", stmt.Fields[1].Name())
}

func TestRewriteCompare2(t *testing.T) {
	stmt := &SelectStatement{
		Fields: []*Field{{Expr: &Call{Name: "compare", Args: []Expr{&VarRef{Val: "PV"}, &IntegerLiteral{Val: 86400}}}}},
		Sources: []Source{&SubQuery{Statement: &SelectStatement{
			Fields:     []*Field{{Expr: &Call{Name: "count", Args: []Expr{&VarRef{Val: "f"}}}, Alias: "PV"}},
			Dimensions: []*Dimension{{Expr: &Call{Name: "time", Args: []Expr{&DurationLiteral{Val: time.Duration(2000 * 1e9)}}}}},
		}}},
		Dimensions: []*Dimension{{Expr: &Call{Name: "time", Args: []Expr{&DurationLiteral{Val: time.Duration(2000 * 1e9)}}}}},
	}
	min := time.Unix(1740452400, 0)
	max := time.Unix(1740456000, 0)
	tr := &TimeRange{
		Min: min,
		Max: max,
	}
	stmt.RewriteCompare(tr)

	assert.Equal(t, min.Add(-time.Hour*24).UnixNano(), tr.Min.UnixNano())
	assert.Equal(t, "PV1", stmt.Fields[0].Name())
	assert.Equal(t, "PV2", stmt.Fields[1].Name())
}

func TestRewriteJoinCond(t *testing.T) {
	innerStmt := &SelectStatement{Dimensions: Dimensions{{Expr: &VarRef{Val: "tag3"}}, {Expr: &VarRef{Val: "tag2"}}, {Expr: &VarRef{Val: "tag1"}}}}
	stmt := &SelectStatement{
		Sources: []Source{&Join{
			LSrc: &SubQuery{Statement: &SelectStatement{
				Sources: []Source{&SubQuery{
					Statement: innerStmt,
				}},
				Dimensions: Dimensions{{Expr: &VarRef{Val: "tag3"}}, {Expr: &VarRef{Val: "tag2"}}, {Expr: &VarRef{Val: "tag1"}}},
			}, Alias: "t1"},
			RSrc: &SubQuery{Statement: &SelectStatement{
				Dimensions: Dimensions{{Expr: &VarRef{Val: "tag2"}}, {Expr: &VarRef{Val: "tag1"}}, {Expr: &VarRef{Val: "tag3"}}},
			}, Alias: "t2"},
			Condition: &BinaryExpr{
				Op: AND,
				LHS: &BinaryExpr{
					Op:  EQ,
					LHS: &VarRef{Val: "t1.tag1"},
					RHS: &VarRef{Val: "t2.tag1"},
				},
				RHS: &BinaryExpr{
					Op:  EQ,
					LHS: &VarRef{Val: "t1.tag3"},
					RHS: &VarRef{Val: "t2.tag3"},
				},
			},
		}},
		Dimensions: Dimensions{{Expr: &VarRef{Val: "tag3"}}, {Expr: &VarRef{Val: "tag2"}}, {Expr: &VarRef{Val: "tag1"}}},
	}

	stmt.RewriteJoinDims(nil)

	expDims := Dimensions{{Expr: &VarRef{Val: "tag1"}}, {Expr: &VarRef{Val: "tag3"}}, {Expr: &VarRef{Val: "tag2"}}}

	assert.Equal(t, expDims, stmt.Dimensions)
	assert.Equal(t, expDims, stmt.Sources[0].(*Join).LSrc.(*SubQuery).Statement.Dimensions)
	assert.Equal(t, expDims, stmt.Sources[0].(*Join).RSrc.(*SubQuery).Statement.Dimensions)
	assert.Equal(t, expDims, innerStmt.Dimensions)
}

func TestRewriteJoinCond2(t *testing.T) {
	stmt := &SelectStatement{
		Sources: []Source{&Join{
			LSrc: &SubQuery{Statement: &SelectStatement{
				Dimensions: Dimensions{{Expr: &VarRef{Val: "tag1"}}, {Expr: &VarRef{Val: "tag3"}}},
			}, Alias: "t1"},
			RSrc: &SubQuery{Statement: &SelectStatement{
				Dimensions: Dimensions{{Expr: &VarRef{Val: "tag4"}}, {Expr: &VarRef{Val: "tag2"}}},
			}, Alias: "t2"},
			Condition: &BinaryExpr{
				Op: AND,
				LHS: &BinaryExpr{
					Op:  EQ,
					RHS: &VarRef{Val: "t2.tag2"},
					LHS: &VarRef{Val: "t1.tag1"},
				},
				RHS: &BinaryExpr{
					Op:  EQ,
					LHS: &VarRef{Val: "t1.tag3"},
					RHS: &VarRef{Val: "t2.tag4"},
				},
			},
		}},
		Dimensions: Dimensions{{Expr: &VarRef{Val: "tag3"}}, {Expr: &VarRef{Val: "tag2"}}, {Expr: &VarRef{Val: "tag1"}}, {Expr: &VarRef{Val: "tag4"}}},
	}

	stmt.RewriteJoinDims(nil)

	expDims1 := Dimensions{{Expr: &VarRef{Val: "tag1"}}, {Expr: &VarRef{Val: "tag3"}}}
	expDims2 := Dimensions{{Expr: &VarRef{Val: "tag2"}}, {Expr: &VarRef{Val: "tag4"}}}
	expDims3 := Dimensions{{Expr: &VarRef{Val: "tag1"}}, {Expr: &VarRef{Val: "tag3"}}, {Expr: &VarRef{Val: "tag2"}}, {Expr: &VarRef{Val: "tag4"}}}

	assert.Equal(t, expDims3, stmt.Dimensions)
	assert.Equal(t, expDims1, stmt.Sources[0].(*Join).LSrc.(*SubQuery).Statement.Dimensions)
	assert.Equal(t, expDims2, stmt.Sources[0].(*Join).RSrc.(*SubQuery).Statement.Dimensions)
}

func TestRewriteJoinMeasurement(t *testing.T) {
	m1 := &Measurement{Name: ""}
	outerStatement1 := &SelectStatement{
		Fields: []*Field{
			{Expr: &VarRef{Val: "test.val", Type: 0, Alias: ""}},
		},
	}
	subQuery1, err1 := RewriteJoinMeasurement(m1, outerStatement1, nil)
	assert.Nil(t, subQuery1)
	assert.EqualError(t, err1, "expect measurement name when converting the measurement type to the subquery type in the join scenario")

	m2 := &Measurement{Name: "test"}
	outerStatement2 := &SelectStatement{
		Fields: []*Field{},
	}
	subQuery2, err2 := RewriteJoinMeasurement(m2, outerStatement2, nil)
	assert.Nil(t, subQuery2)
	assert.EqualError(t, err2, "empty fields when converting the measurement type to the subquery type in the join scenario")

	m3 := &Measurement{Name: "test", Database: "testdb", RetentionPolicy: "autogen"}
	outerStatement3 := &SelectStatement{
		Fields: []*Field{
			{Expr: &VarRef{Val: "test.val1", Type: 0, Alias: ""}},
			{Expr: &Call{Name: "abs", Args: []Expr{&VarRef{Val: "test.val1"}}}},
			{Expr: &BinaryExpr{
				Op:  AND,
				LHS: &VarRef{Val: "test.val2"},
				RHS: &Call{Name: "abs", Args: []Expr{&VarRef{Val: "test.val3"}}},
			}},
			{Expr: &ParenExpr{Expr: &Call{Name: "abs", Args: []Expr{&VarRef{Val: "test.val4"}}}}},
			{Expr: &VarRef{Val: "test2.val21", Type: 0, Alias: ""}},
			{Expr: &Call{Name: "abs", Args: []Expr{&VarRef{Val: "test2.val22"}}}},
			{Expr: &BinaryExpr{
				Op:  AND,
				LHS: &VarRef{Val: "test2.val23"},
				RHS: &Call{Name: "abs", Args: []Expr{&VarRef{Val: "test2.val24"}}},
			}},
			{Expr: &ParenExpr{Expr: &Call{Name: "abs", Args: []Expr{&VarRef{Val: "test2.val25"}}}}},
		},
	}
	subQuery3, err3 := RewriteJoinMeasurement(m3, outerStatement3, nil)
	assert.NoError(t, err3)
	assert.NotNil(t, subQuery3)
	assert.Equal(t, m3.Name, subQuery3.Alias)
	assert.Equal(t, "test", subQuery3.Statement.Sources[0].(*Measurement).Name)
	assert.Equal(t, "testdb", subQuery3.Statement.Sources[0].(*Measurement).Database)
	assert.Equal(t, "autogen", subQuery3.Statement.Sources[0].(*Measurement).RetentionPolicy)
	assert.Nil(t, subQuery3.Statement.JoinSource)
	assert.Len(t, subQuery3.Statement.Fields, 4)
	fieldsNames := make(map[string]int, 4)
	for _, field := range subQuery3.Statement.Fields {
		fieldsNames[field.Expr.(*VarRef).Val]++
	}
	assert.Equal(t, 1, fieldsNames["val1"])
	assert.Equal(t, 1, fieldsNames["val2"])
	assert.Equal(t, 1, fieldsNames["val3"])
	assert.Equal(t, 1, fieldsNames["val4"])

	m4 := &Measurement{Name: "test4", Database: "testdb4", RetentionPolicy: "autogen"}
	outerStatement4 := &SelectStatement{
		Fields: []*Field{
			{Expr: &Wildcard{Type: 0}},
		},
	}
	subQuery4, err4 := RewriteJoinMeasurement(m4, outerStatement4, nil)
	assert.NoError(t, err4)
	assert.NotNil(t, subQuery4)
	assert.Equal(t, m4.Name, subQuery4.Alias)
	assert.Equal(t, "test4", subQuery4.Statement.Sources[0].(*Measurement).Name)
	assert.Equal(t, "testdb4", subQuery4.Statement.Sources[0].(*Measurement).Database)
	assert.Equal(t, "autogen", subQuery4.Statement.Sources[0].(*Measurement).RetentionPolicy)
	assert.Nil(t, subQuery4.Statement.JoinSource)
	assert.Len(t, subQuery4.Statement.Fields, 1)
	_, ok := subQuery4.Statement.Fields[0].Expr.(*Wildcard)
	assert.Equal(t, true, ok)

	m5 := &Measurement{Name: "test5", Database: "testdb5", RetentionPolicy: "autogen", Alias: "t5"}
	outerStatement5 := &SelectStatement{
		Fields: []*Field{
			{Expr: &VarRef{Val: "t5.val5", Type: 0, Alias: ""}},
		},
	}
	subQuery5, err5 := RewriteJoinMeasurement(m5, outerStatement5, nil)
	assert.NoError(t, err5)
	assert.NotNil(t, subQuery5)
	assert.Equal(t, "t5", subQuery5.Alias)
	assert.Equal(t, "test5", subQuery5.Statement.Sources[0].(*Measurement).Name)
	assert.Equal(t, "", subQuery5.Statement.Sources[0].(*Measurement).Alias)
	assert.Equal(t, "testdb5", subQuery5.Statement.Sources[0].(*Measurement).Database)
	assert.Equal(t, "autogen", subQuery5.Statement.Sources[0].(*Measurement).RetentionPolicy)
	assert.Nil(t, subQuery5.Statement.JoinSource)
	assert.Len(t, subQuery5.Statement.Fields, 1)
	field5 := subQuery5.Statement.Fields[0].Expr.(*VarRef).Val
	assert.Equal(t, "val5", field5)

	m6 := &Measurement{Name: "test6", Database: "testdb6", RetentionPolicy: "autogen", Alias: "t6"}
	outerStatement6 := &SelectStatement{
		Fields: []*Field{
			{Expr: &VarRef{Val: "t6.val1", Type: 0, Alias: ""}},
		},
	}
	subQuery6, err6 := RewriteJoinMeasurement(m6, outerStatement6, []string{"t6.val6"})
	assert.NoError(t, err6)
	assert.NotNil(t, subQuery6)
	assert.Equal(t, "t6", subQuery6.Alias)
	assert.Equal(t, "test6", subQuery6.Statement.Sources[0].(*Measurement).Name)
	assert.Equal(t, "", subQuery6.Statement.Sources[0].(*Measurement).Alias)
	assert.Equal(t, "testdb6", subQuery6.Statement.Sources[0].(*Measurement).Database)
	assert.Equal(t, "autogen", subQuery6.Statement.Sources[0].(*Measurement).RetentionPolicy)
	assert.Nil(t, subQuery6.Statement.JoinSource)
	assert.Len(t, subQuery6.Statement.Fields, 2)
	field6 := subQuery6.Statement.Fields[1].Expr.(*VarRef).Val
	assert.Equal(t, "val6", field6)
}

func TestRewriteJoinTemporaryMeasurement(t *testing.T) {
	m1 := &Measurement{Name: "cte", MstType: TEMPORARY}
	m2 := &Measurement{Name: "cte2", MstType: TEMPORARY}
	cteStatement := &SelectStatement{
		Fields: []*Field{
			{Expr: &VarRef{Val: "test.val", Type: 0, Alias: ""}},
		},
		Sources: []Source{m1},
	}
	cte := &CTE{Query: cteStatement, Alias: "cte"}
	outerStatement := &SelectStatement{
		Fields: []*Field{
			{Expr: &VarRef{Val: "test.val", Type: 0, Alias: ""}},
		},
		DirectDependencyCTEs: []*CTE{cte},
		Sources:              []Source{m1},
	}
	subStatement := &SelectStatement{
		Fields: []*Field{
			{Expr: &VarRef{Val: "test.val", Type: 0, Alias: ""}},
		},
		DirectDependencyCTEs: []*CTE{cte},
		Sources:              []Source{m1},
	}

	subQuery := &SubQuery{
		Statement: subStatement,
	}
	sources := Sources{m1, m2}

	newSources := SetCTEForCTESubQuery(m1, subQuery, outerStatement, sources)
	assert.True(t, len(newSources) == 1)
}

func TestReduce(t *testing.T) {
	expr := &BinaryExpr{
		Op: ADD,
		LHS: &BinaryExpr{
			Op:  ADD,
			LHS: &IntegerLiteral{Val: 1},
			RHS: &IntegerLiteral{Val: 1},
		},
		RHS: &BinaryExpr{
			Op:  ADD,
			LHS: &IntegerLiteral{Val: 1},
			RHS: &IntegerLiteral{Val: 1},
		},
	}
	res := Reduce(expr, nil)
	assert.Equal(t, &IntegerLiteral{Val: 4}, res)
}

func TestDeepLeftReduce(t *testing.T) {
	var expr Expr
	expr = &IntegerLiteral{Val: 1}
	for range 50000 {
		expr = &BinaryExpr{
			Op:  ADD,
			LHS: expr,
			RHS: &IntegerLiteral{Val: 0},
		}
	}
	res := Reduce(expr, nil)
	assert.Equal(t, &IntegerLiteral{Val: 1}, res)
}

func TestDeepRightReduce(t *testing.T) {
	var expr Expr
	expr = &IntegerLiteral{Val: 1}
	for range 50000 {
		expr = &BinaryExpr{
			Op:  ADD,
			LHS: &IntegerLiteral{Val: 0},
			RHS: expr,
		}
	}
	res := Reduce(expr, nil)
	assert.Equal(t, &IntegerLiteral{Val: 1}, res)
}

func TestNoReduce(t *testing.T) {
	var expr Expr
	for range 50000 {
		branch := &BinaryExpr{
			Op:  EQ,
			LHS: &VarRef{Val: "time", Type: Integer},
			RHS: &IntegerLiteral{Val: 0},
		}
		if expr != nil {
			expr = &BinaryExpr{
				Op:  OR,
				LHS: expr,
				RHS: branch,
			}
		} else {
			expr = branch
		}
	}
	res := Reduce(expr, nil)
	assert.Equal(t, expr, res)
}

func TestConditionExprNoTime(t *testing.T) {
	var expr Expr
	expr = &BinaryExpr{
		Op: AND,
		LHS: &BinaryExpr{
			Op:    EQ,
			LHS:   &VarRef{Val: "value", Type: Integer},
			RHS:   &IntegerLiteral{Val: 0},
			depth: 1,
		},
		RHS: &BinaryExpr{
			Op:    EQ,
			LHS:   &VarRef{Val: "value", Type: Integer},
			RHS:   &IntegerLiteral{Val: 0},
			depth: 1,
		},
		depth: 2,
	}
	res, timerange, err := ConditionExpr(expr, nil)
	assert.Equal(t, expr, res)
	assert.Equal(t, TimeRange{}, timerange)
	assert.Equal(t, nil, err)
}

func TestConditionExprTimeReduce(t *testing.T) {
	expr := &BinaryExpr{
		Op: AND,
		LHS: &BinaryExpr{
			Op:  EQ,
			LHS: &VarRef{Val: "time", Type: Integer},
			RHS: &BinaryExpr{
				Op:    ADD,
				LHS:   &IntegerLiteral{Val: 0},
				RHS:   &IntegerLiteral{Val: 0},
				depth: 1,
			},
			depth: 2,
		},
		RHS: &BinaryExpr{
			Op:    EQ,
			LHS:   &VarRef{Val: "value", Type: Integer},
			RHS:   &IntegerLiteral{Val: 0},
			depth: 1,
		},
		depth: 3,
	}
	res, timerange, err := ConditionExpr(expr, nil)
	assert.Equal(t, expr.RHS, res)
	assert.Equal(t, TimeRange{time.Unix(0, 0).UTC(), time.Unix(0, 0).UTC()}, timerange)
	assert.Equal(t, nil, err)
}

func TestConditionExprTime(t *testing.T) {
	expr := &BinaryExpr{
		Op: AND,
		LHS: &BinaryExpr{
			Op:    EQ,
			LHS:   &VarRef{Val: "time", Type: Integer},
			RHS:   &IntegerLiteral{Val: 0},
			depth: 1,
		},
		RHS: &BinaryExpr{
			Op:    EQ,
			LHS:   &VarRef{Val: "value", Type: Integer},
			RHS:   &IntegerLiteral{Val: 0},
			depth: 1,
		},
		depth: 2,
	}
	res, timerange, err := ConditionExpr(expr, nil)
	assert.Equal(t, expr.RHS, res)
	assert.Equal(t, TimeRange{time.Unix(0, 0).UTC(), time.Unix(0, 0).UTC()}, timerange)
	assert.Equal(t, nil, err)
}

func TestDeepConditionExprNoTime(t *testing.T) {
	var expr Expr
	for range 50000 {
		branch := &BinaryExpr{
			Op:  EQ,
			LHS: &VarRef{Val: "value", Type: Integer},
			RHS: &IntegerLiteral{Val: 0},
		}
		if expr != nil {
			expr = &BinaryExpr{
				Op:  OR,
				LHS: expr,
				RHS: branch,
			}
		} else {
			expr = branch
		}
	}
	res, timerange, err := ConditionExpr(expr, nil)
	assert.Equal(t, expr, res)
	assert.Equal(t, TimeRange{}, timerange)
	assert.Equal(t, nil, err)
}

func TestDeepConditionExprTime(t *testing.T) {
	var expr Expr
	for range 50000 {
		branch := &BinaryExpr{
			Op:  EQ,
			LHS: &VarRef{Val: "time", Type: Integer},
			RHS: &IntegerLiteral{Val: 0},
		}
		if expr != nil {
			expr = &BinaryExpr{
				Op:  AND,
				LHS: expr,
				RHS: branch,
			}
		} else {
			expr = branch
		}
	}
	res, timerange, err := ConditionExpr(expr, nil)
	assert.Equal(t, nil, res)
	assert.Equal(t, TimeRange{time.Unix(0, 0).UTC(), time.Unix(0, 0).UTC()}, timerange)
	assert.Equal(t, nil, err)
}

// TestGetJoinKeyByCondition tests the GetJoinKeyByCondition function
func TestGetJoinKeyByCondition(t *testing.T) {
	joinKeyMap := make(map[string]string)
	joinCondition := &BinaryExpr{
		Op:  EQ,
		LHS: &VarRef{Val: "leftKey"},
		RHS: &VarRef{Val: "rightKey"},
	}

	err := GetJoinKeyByCondition(joinCondition, joinKeyMap)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if joinKeyMap["leftKey"] != "rightKey" {
		t.Errorf("Expected joinKeyMap['leftKey'] to be 'rightKey', got '%s'", joinKeyMap["leftKey"])
	}
}

// TestGetJoinKeyByName tests the getJoinKeyByName function
func TestGetJoinKeyByName(t *testing.T) {
	joinKeyMap := map[string]string{
		"table.leftKey":  "otherTable.leftKey",
		"table.rightKey": "otherTable.rightKey",
	}

	joinKeys := getJoinKeyByName(joinKeyMap, "table")
	expectedKeys := []string{"table.leftKey", "table.rightKey"}

	if len(joinKeys) != len(expectedKeys) {
		t.Errorf("Expected %d keys, got %d", len(expectedKeys), len(joinKeys))
	}
	sort.Strings(joinKeys)
	sort.Strings(expectedKeys)
	for i := range joinKeys {
		assert.Equal(t, joinKeys[i], expectedKeys[i])
	}
}

// TestGetAuxiliaryField tests the getAuxiliaryField function
func TestGetAuxiliaryField(t *testing.T) {
	fields := []*Field{
		{Auxiliary: true, Alias: "aux1"},
		{Auxiliary: false, Alias: "field1"},
		{Auxiliary: true, Alias: "aux2"},
	}

	auxFields := getAuxiliaryField(fields)
	if len(auxFields) != 2 {
		t.Errorf("Expected 2 auxiliary fields, got %d", len(auxFields))
	}

	if auxFields[0].Alias != "aux1" || auxFields[1].Alias != "aux2" {
		t.Errorf("Expected auxiliary fields aux1 and aux2, got %s and %s", auxFields[0].Alias, auxFields[1].Alias)
	}
}

// TestRewriteAuxiliaryField tests the rewriteAuxiliaryField function
func TestRewriteAuxiliaryField(t *testing.T) {
	fields := []*Field{
		{Alias: "aux1"},
		{Alias: "aux2"},
	}

	auxFields := rewriteAuxiliaryField(fields)
	if len(auxFields) != 2 {
		t.Errorf("Expected 2 auxiliary fields, got %d", len(auxFields))
	}
	expr1, ok := auxFields[0].Expr.(*VarRef)
	if !ok {
		t.Errorf("invalid expr")
	}
	expr2, ok := auxFields[1].Expr.(*VarRef)
	if !ok {
		t.Errorf("invalid expr")
	}
	if expr1.Val != "aux1" || expr2.Val != "aux2" {
		t.Errorf("Expected auxiliary fields aux1 and aux2, got %s and %s", expr1, expr2)
	}
}

// TestRewriteAuxiliaryStmt tests the rewriteAuxiliaryStmt function
func TestRewriteAuxiliaryStmt(t *testing.T) {
	other := &SelectStatement{
		Fields: []*Field{
			{Alias: "field1"},
		},
	}

	auxFields := []*Field{
		{Alias: "aux1"},
	}

	sources := []Source{}
	_, err := rewriteAuxiliaryStmt(other, sources, auxFields, nil, false, false)
	assert.EqualError(t, err, "declare empty collection")
}

func TestInNotIn(t *testing.T) {
	v := &ValuerEval{}

	expr := &BinaryExpr{
		Op:  IN,
		LHS: &NumberLiteral{Val: 0.1},
		RHS: &SetLiteral{make(map[interface{}]bool)},
	}
	v.Eval(expr)
	expr.RHS = &NumberLiteral{}
	v.Eval(expr)
	assert.Equal(t, false, false)

	expr = &BinaryExpr{
		Op:  NOTIN,
		LHS: &NumberLiteral{Val: 0.1},
		RHS: &SetLiteral{make(map[interface{}]bool)},
	}
	v.Eval(expr)
	expr.RHS = &NumberLiteral{}
	v.Eval(expr)
	assert.Equal(t, false, false)

	expr = &BinaryExpr{
		Op:  IN,
		LHS: &IntegerLiteral{Val: 0},
		RHS: &SetLiteral{make(map[interface{}]bool)},
	}
	v.Eval(expr)
	expr.RHS = &NumberLiteral{}
	v.Eval(expr)
	assert.Equal(t, false, false)

	expr = &BinaryExpr{
		Op:  NOTIN,
		LHS: &IntegerLiteral{Val: 0},
		RHS: &SetLiteral{make(map[interface{}]bool)},
	}
	v.Eval(expr)
	expr.RHS = &NumberLiteral{}
	v.Eval(expr)
	assert.Equal(t, false, false)

	expr = &BinaryExpr{
		Op:  IN,
		LHS: &UnsignedLiteral{Val: 0},
		RHS: &SetLiteral{make(map[interface{}]bool)},
	}
	v.Eval(expr)
	expr.RHS = &NumberLiteral{}
	v.Eval(expr)
	assert.Equal(t, false, false)

	expr = &BinaryExpr{
		Op:  NOTIN,
		LHS: &UnsignedLiteral{Val: 0},
		RHS: &SetLiteral{make(map[interface{}]bool)},
	}
	v.Eval(expr)
	expr.RHS = &NumberLiteral{}
	v.Eval(expr)
	assert.Equal(t, false, false)

	expr = &BinaryExpr{
		Op:  IN,
		LHS: &StringLiteral{Val: "0"},
		RHS: &SetLiteral{make(map[interface{}]bool)},
	}
	v.Eval(expr)
	expr.RHS = &NumberLiteral{}
	v.Eval(expr)
	assert.Equal(t, false, false)

	expr = &BinaryExpr{
		Op:  NOTIN,
		LHS: &StringLiteral{Val: "0"},
		RHS: &SetLiteral{make(map[interface{}]bool)},
	}
	v.Eval(expr)
	expr.RHS = &NumberLiteral{}
	v.Eval(expr)
	assert.Equal(t, false, false)
}
