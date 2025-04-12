package influxql

import (
	"strconv"
	"testing"

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
