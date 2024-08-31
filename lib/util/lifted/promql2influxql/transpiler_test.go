package promql2influxql

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"
)

var endTime, endTime2, startTime2 time.Time
var timezone *time.Location
var step = 1 * time.Minute

func TestMain(m *testing.M) {
	timezone, _ = time.LoadLocation("Asia/Shanghai")
	time.Local = timezone
	endTime = time.Date(2023, 1, 8, 10, 0, 0, 0, time.Local)
	endTime2 = time.Date(2023, 1, 6, 15, 0, 0, 0, time.Local)
	startTime2 = time.Date(2023, 1, 6, 12, 0, 0, 0, time.Local)
	m.Run()
}

func numberLiteralExpr(input string) *parser.NumberLiteral {
	expr, err := parser.ParseExpr(input)
	if err != nil {
		panic(err)
	}
	v, ok := expr.(*parser.NumberLiteral)
	if !ok {
		panic("bad input")
	}
	return v
}

func SubqueryExpr(input string) *parser.SubqueryExpr {
	expr, err := parser.ParseExpr(input)
	if err != nil {
		panic(err)
	}
	v, ok := expr.(*parser.SubqueryExpr)
	if !ok {
		panic("bad input")
	}
	return v
}

func StringLiteralExpr(input string) *parser.StringLiteral {
	expr, err := parser.ParseExpr(input)
	if err != nil {
		panic(err)
	}
	v, ok := expr.(*parser.StringLiteral)
	if !ok {
		panic("bad input")
	}
	return v
}

func ParseExpr(input string) parser.Expr {
	expr, err := parser.ParseExpr(input)
	if err != nil {
		panic(err)
	}
	return expr
}

type fields struct {
	Start          *time.Time
	End            *time.Time
	Timezone       *time.Location
	Evaluation     *time.Time
	Step           time.Duration
	DataType       DataType
	timeRange      time.Duration
	parenExprCount int
	condition      influxql.Expr
	Database       string
	LabelName      string
}
type args struct {
	expr parser.Expr
}

func TestTranspiler_transpile_sql(t1 *testing.T) {
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    influxql.Node
		wantErr bool
		skip    bool
	}{
		{
			name: "1",
			fields: fields{
				Start: &startTime2, End: &endTime, Step: step,
			},
			args: args{
				expr: VectorSelector(`cpu{host=~"tele.*"}`),
			},
			want:    influxql.MustParseStatement(`SELECT value AS value FROM cpu WHERE time >= '2023-01-06T03:55:00Z' AND time <= '2023-01-08T02:00:00Z' AND host =~ /tele.*/ GROUP BY *`),
			wantErr: false,
		},
		{
			name: "2",
			fields: fields{
				Evaluation: &endTime,
			},
			args: args{
				expr: VectorSelector(`cpu{host=~"tele.*"}`),
			},
			want:    influxql.MustParseStatement(`SELECT value AS value FROM cpu WHERE time >= '2023-01-08T01:55:00Z' AND time <= '2023-01-08T02:00:00Z' AND host =~ /tele.*/ GROUP BY *`),
			wantErr: false,
		},
		{
			name: "3",
			fields: fields{
				Evaluation: &endTime,
			},
			args: args{
				expr: MatrixSelector(`cpu{host=~"tele.*"}[5m]`),
			},
			want:    influxql.MustParseStatement(`SELECT value AS value FROM cpu WHERE time >= '2023-01-08T01:55:00Z' AND time <= '2023-01-08T02:00:00Z' AND host =~ /tele.*/ GROUP BY *`),
			wantErr: false,
		},
		{
			name: "4",
			fields: fields{
				Evaluation: &endTime2,
			},
			args: args{
				expr: MatrixSelector(`cpu{host=~"tele.*"}[5m]`),
			},
			want:    influxql.MustParseStatement(`SELECT value AS value FROM cpu WHERE time >= '2023-01-06T06:55:00Z' AND time <= '2023-01-06T07:00:00Z' AND host =~ /tele.*/ GROUP BY *`),
			wantErr: false,
		},
		{
			name: `invalid expression type "range vector" for range query, must be Scalar or instant Vector`,
			fields: fields{
				Start: &startTime2,
				End:   &endTime2,
				Step:  step,
			},
			args: args{
				expr: MatrixSelector(`cpu{host=~"tele.*"}[5m]`),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "5",
			fields: fields{
				Evaluation: &endTime2,
			},
			args: args{
				expr: UnaryExpr(`-go_gc_duration_seconds_count`),
			},
			want:    influxql.MustParseStatement(`SELECT -1 * value AS value FROM go_gc_duration_seconds_count WHERE time >= '2023-01-06T06:55:00Z' AND time <= '2023-01-06T07:00:00Z' GROUP BY *`),
			wantErr: false,
		},
		{
			name: "6",
			fields: fields{
				Evaluation: &endTime2,
			},
			args: args{
				expr: BinaryExpr(`5 * go_gc_duration_seconds_count`),
			},
			want:    influxql.MustParseStatement(`SELECT 5 * value AS value FROM go_gc_duration_seconds_count WHERE time >= '2023-01-06T06:55:00Z' AND time <= '2023-01-06T07:00:00Z' GROUP BY *`),
			wantErr: false,
		},
		{
			name: "7",
			fields: fields{
				Evaluation: &endTime2,
			},
			args: args{
				expr: BinaryExpr(`5 * 6 * go_gc_duration_seconds_count`),
			},
			want:    influxql.MustParseStatement(`SELECT 5 * 6 * value AS value FROM go_gc_duration_seconds_count WHERE time >= '2023-01-06T06:55:00Z' AND time <= '2023-01-06T07:00:00Z' GROUP BY *`),
			wantErr: false,
		},
		{
			name: "8",
			fields: fields{
				Evaluation: &endTime2,
			},
			args: args{
				expr: BinaryExpr(`5 * (go_gc_duration_seconds_count - 6)`),
			},
			want:    influxql.MustParseStatement(`SELECT 5 * (value - 6) AS value FROM go_gc_duration_seconds_count WHERE time >= '2023-01-06T06:55:00Z' AND time <= '2023-01-06T07:00:00Z' GROUP BY *`),
			wantErr: false,
		},
		{
			name: "9",
			fields: fields{
				Evaluation: &endTime2,
			},
			args: args{
				expr: BinaryExpr(`(5 * go_gc_duration_seconds_count) - 6`),
			},
			want:    influxql.MustParseStatement(`SELECT (5 * value) - 6 AS value FROM go_gc_duration_seconds_count WHERE time >= '2023-01-06T06:55:00Z' AND time <= '2023-01-06T07:00:00Z' GROUP BY *`),
			wantErr: false,
		},
		{
			name: "10",
			fields: fields{
				Evaluation: &endTime2,
			},
			args: args{
				expr: BinaryExpr(`5 > go_gc_duration_seconds_count`),
			},
			want:    influxql.MustParseStatement(`SELECT value AS value FROM go_gc_duration_seconds_count WHERE time >= '2023-01-06T06:55:00Z' AND time <= '2023-01-06T07:00:00Z' AND 5 > value GROUP BY *`),
			wantErr: false,
		},
		{
			name: "11",
			fields: fields{
				Evaluation: &endTime2,
			},
			args: args{
				expr: BinaryExpr(`go_gc_duration_seconds_count^3`),
			},
			want:    influxql.MustParseStatement(`SELECT pow(value, 3) AS value FROM go_gc_duration_seconds_count WHERE time >= '2023-01-06T06:55:00Z' AND time <= '2023-01-06T07:00:00Z' GROUP BY *`),
			wantErr: false,
		},
		{
			name: "12",
			fields: fields{
				Evaluation: &endTime2,
			},
			args: args{
				expr: BinaryExpr(`go_gc_duration_seconds_count^3^4`),
			},
			want:    influxql.MustParseStatement(`SELECT pow(value, pow(3, 4)) AS value FROM go_gc_duration_seconds_count WHERE time >= '2023-01-06T06:55:00Z' AND time <= '2023-01-06T07:00:00Z' GROUP BY *`),
			wantErr: false,
		},
		{
			name: "13",
			fields: fields{
				Evaluation: &endTime2,
			},
			args: args{
				expr: BinaryExpr(`go_gc_duration_seconds_count^(3^4)`),
			},
			want:    influxql.MustParseStatement(`SELECT pow(value, (pow(3, 4))) AS value FROM go_gc_duration_seconds_count WHERE time >= '2023-01-06T06:55:00Z' AND time <= '2023-01-06T07:00:00Z' GROUP BY *`),
			wantErr: false,
		},
		{
			name: "14",
			fields: fields{
				Evaluation: &endTime2,
			},
			args: args{
				expr: BinaryExpr(`(go_gc_duration_seconds_count^3)^4`),
			},
			want:    influxql.MustParseStatement(`SELECT pow(pow(value, 3), 4) AS value FROM go_gc_duration_seconds_count WHERE time >= '2023-01-06T06:55:00Z' AND time <= '2023-01-06T07:00:00Z' GROUP BY *`),
			wantErr: false,
		},
		{
			name: "15",
			fields: fields{
				Evaluation: &endTime2,
			},
			args: args{
				expr: BinaryExpr(`4^go_gc_duration_seconds_count`),
			},
			want:    influxql.MustParseStatement(`SELECT pow(4, value) AS value FROM go_gc_duration_seconds_count WHERE time >= '2023-01-06T06:55:00Z' AND time <= '2023-01-06T07:00:00Z' GROUP BY *`),
			wantErr: false,
		},
		{
			name: "16",
			fields: fields{
				Evaluation: &endTime2,
			},
			args: args{
				expr: BinaryExpr(`go_gc_duration_seconds_count>=3<4`),
			},
			want:    influxql.MustParseStatement(`SELECT value AS value FROM go_gc_duration_seconds_count WHERE time >= '2023-01-06T06:55:00Z' AND time <= '2023-01-06T07:00:00Z' AND value >= 3 AND value < 4 GROUP BY *`),
			wantErr: false,
		},
		{
			name: "17",
			fields: fields{
				Evaluation: &endTime2,
			},
			args: args{
				expr: BinaryExpr(`sum(go_gc_duration_seconds_count>=1000) > 10000`),
			},
			want:    influxql.MustParseStatement(`SELECT value AS value FROM (SELECT sum(value) AS value FROM go_gc_duration_seconds_count WHERE time >= '2023-01-06T06:55:00Z' AND time <= '2023-01-06T07:00:00Z' AND value >= 1000) WHERE value > 10000 AND time >= '2023-01-06T06:55:00Z' AND time <= '2023-01-06T07:00:00Z'`),
			wantErr: false,
		},
		{
			name: "18",
			fields: fields{
				Evaluation: &endTime2,
			},
			args: args{
				expr: numberLiteralExpr(`1`),
			},
			want:    influxql.MustParseExpr("1.000"),
			wantErr: false,
		},
		{
			name: "19",
			fields: fields{
				Start:    &startTime2,
				End:      &endTime2,
				Step:     step,
				DataType: GRAPH_DATA,
			},
			args: args{
				expr: CallExpr(`sum_over_time(go_gc_duration_seconds_count[5m])`),
			},
			want:    parseInfluxqlByYacc(`SELECT sum_over_time(value) AS value FROM go_gc_duration_seconds_count WHERE time >= '2023-01-06T03:55:00Z' AND time <= '2023-01-06T07:00:00Z' GROUP BY *, time(1m,0s) fill(none)`),
			wantErr: false,
		},
		{
			name: "20",
			fields: fields{
				Evaluation: &endTime2,
			},
			args: args{
				expr: StringLiteralExpr(`"justastring"`),
			},
			want:    influxql.MustParseExpr("'justastring'"),
			wantErr: false,
		},
		{
			name: "21",
			fields: fields{
				Evaluation: &endTime2,
			},
			args: args{
				expr: BinaryExpr(`-10 * cpu`),
			},
			want:    influxql.MustParseStatement("SELECT -10 * value AS value FROM cpu WHERE time >= '2023-01-06T06:55:00Z' AND time <= '2023-01-06T07:00:00Z' GROUP BY *"),
			wantErr: false,
		},
		{
			name: "22",
			fields: fields{
				Start:    &startTime2,
				End:      &endTime2,
				Step:     step,
				Timezone: timezone,
			},
			args: args{
				expr: VectorSelector(`cpu{host="telegraf"}`),
			},
			want:    influxql.MustParseStatement("SELECT value AS value FROM cpu WHERE time >= '2023-01-06T03:55:00Z' AND time <= '2023-01-06T07:00:00Z' AND host = 'telegraf' GROUP BY *"),
			wantErr: false,
		},
		{
			name: "23",
			fields: fields{
				Evaluation: &endTime,
				DataType:   LABEL_VALUES_DATA,
				Database:   "prometheus",
				LabelName:  "job",
			},
			args: args{
				expr: VectorSelector(`go_goroutines{instance=~"192.168.*"}`),
			},
			want:    influxql.MustParseStatement(`SHOW TAG VALUES ON prometheus FROM go_goroutines WITH KEY = job WHERE time >= '2023-01-08T01:55:00Z' AND time <= '2023-01-08T02:00:00Z' AND instance =~ /192.168.*/`),
			wantErr: false,
		},
		{
			name: "24",
			fields: fields{
				Evaluation: &endTime,
				DataType:   LABEL_VALUES_DATA,
				Database:   "prometheus",
				LabelName:  "job",
			},
			args: args{
				expr: VectorSelector(`go_goroutines{instance=~"192.168.*"}`),
			},
			want:    influxql.MustParseStatement(`SHOW TAG VALUES ON prometheus FROM go_goroutines WITH KEY = job WHERE time >= '2023-01-08T01:55:00Z' AND time <= '2023-01-08T02:00:00Z' AND instance =~ /192.168.*/`),
			wantErr: false,
		},
		{
			name: "25",
			fields: fields{
				Evaluation: &endTime2,
			},
			args: args{
				expr: ParseExpr(`topk(3, sum by (app, proc) (rate(instance_cpu_time_ns[5m])))`),
			},
			want:    parseInfluxqlByYacc(`SELECT top(value, 3) AS value, *::tag FROM (SELECT sum(rate_prom(value)) AS value FROM instance_cpu_time_ns WHERE time >= '2023-01-06T06:55:00Z' AND time <= '2023-01-06T07:00:00Z' GROUP BY app, proc) WHERE time >= '2023-01-06T06:55:00Z' AND time <= '2023-01-06T07:00:00Z'`),
			wantErr: false,
		},
		{
			name: "26",
			fields: fields{
				Start:    &startTime2,
				End:      &endTime2,
				Step:     step,
				DataType: GRAPH_DATA,
			},
			args: args{
				expr: ParseExpr(`topk(3, sum by (app, proc) (rate(instance_cpu_time_ns[5m])))`),
			},
			want:    parseInfluxqlByYacc(`SELECT top(value, 3) AS value, *::tag FROM (SELECT sum(rate_prom(value)) AS value FROM instance_cpu_time_ns WHERE time >= '2023-01-06T03:55:00Z' AND time <= '2023-01-06T07:00:00Z' GROUP BY app, proc, time(1m, 0s) fill(none)) WHERE time >= '2023-01-06T03:55:00Z' AND time <= '2023-01-06T07:00:00Z' GROUP BY time(1m, 0s) fill(none)`),
			wantErr: false,
		},
		{
			name: "27",
			fields: fields{
				Start:    &startTime2,
				End:      &endTime2,
				Step:     step,
				DataType: GRAPH_DATA,
			},
			args: args{
				expr: ParseExpr(`sum by(group) (http_requests @ 3000.000)`),
			},
			want:    parseInfluxqlByYacc(`SELECT sum(value) AS value FROM http_requests WHERE time >= '1970-01-01T00:45:00Z' AND time <= '1970-01-01T00:50:00Z' GROUP BY "group", time(1m,0s) fill(none)`),
			wantErr: false,
		},
		{
			name: "28",
			fields: fields{
				Start:    &startTime2,
				End:      &endTime2,
				Step:     step,
				DataType: GRAPH_DATA,
			},
			args: args{
				expr: ParseExpr(`avg((3<round(http_requests/60000)<2880)) by (tenantid,envname)`),
			},
			want:    parseInfluxqlByYacc(`SELECT mean(value) AS value FROM (SELECT value AS value FROM (SELECT round_prom((value / 60000)) AS value FROM http_requests WHERE time >= '2023-01-06T03:55:00Z' AND time <= '2023-01-06T07:00:00Z' GROUP BY *) WHERE 3 < value AND time >= '2023-01-06T03:55:00Z' AND time <= '2023-01-06T07:00:00Z' GROUP BY *) WHERE value < 2880 AND time >= '2023-01-06T03:55:00Z' AND time <= '2023-01-06T07:00:00Z' GROUP BY tenantid, envname, time(1m, 0s) fill(none)`),
			wantErr: false,
		},
		{
			name: "29",
			fields: fields{
				Start:    &startTime2,
				End:      &endTime2,
				Step:     step,
				DataType: GRAPH_DATA,
			},
			args: args{
				expr: ParseExpr(`count({__name__="down"} ) by (job)`),
			},
			want:    parseInfluxqlByYacc(`SELECT count_prom(value) AS value FROM down WHERE time >= '2023-01-06T03:55:00Z' AND time <= '2023-01-06T07:00:00Z' GROUP BY job, time(1m, 0s) fill(none)`),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		if tt.skip {
			continue
		}
		t1.Run(tt.name, func(t1 *testing.T) {
			t := &Transpiler{
				PromCommand: PromCommand{
					Start:         tt.fields.Start,
					End:           tt.fields.End,
					Timezone:      tt.fields.Timezone,
					Evaluation:    tt.fields.Evaluation,
					Step:          tt.fields.Step,
					DataType:      tt.fields.DataType,
					Database:      tt.fields.Database,
					LabelName:     tt.fields.LabelName,
					LookBackDelta: DefaultLookBackDelta,
				},
				timeRange:      tt.fields.timeRange,
				parenExprCount: tt.fields.parenExprCount,
				timeCondition:  tt.fields.condition,
			}
			got, err := t.Transpile(tt.args.expr)
			if tt.name == "not support both sides has VectorSelector in binary expression" {
				fmt.Println(got.String())
			}
			if (err != nil) != tt.wantErr {
				t1.Errorf("transpile() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if got != nil {
				if !reflect.DeepEqual(got.String(), tt.want.String()) {
					t1.Errorf("transpile() got = %v, want %v", got, tt.want)
				}
			}
		})
	}
}

func parseInfluxqlByYacc(sql string) influxql.Node {
	YyParser := &influxql.YyParser{Query: influxql.Query{}}
	YyParser.Scanner = influxql.NewScanner(strings.NewReader(sql))
	YyParser.ParseTokens()
	q, err := YyParser.GetQuery()
	if err != nil {
		panic(err)
	}
	return q.Statements[0]
}

func TestCondition_Or(t *testing.T) {
	type args struct {
		expr *influxql.BinaryExpr
	}
	tests := []struct {
		name     string
		receiver *influxql.BinaryExpr
		args     args
		want     *influxql.BinaryExpr
	}{
		{
			name: "",
			receiver: &influxql.BinaryExpr{
				Op: influxql.EQREGEX,
				LHS: &influxql.VarRef{
					Val: "cpu",
				},
				RHS: &influxql.StringLiteral{
					Val: "cpu.*",
				},
			},
			args: args{
				expr: &influxql.BinaryExpr{
					Op: influxql.EQ,
					LHS: &influxql.VarRef{
						Val: "host",
					},
					RHS: &influxql.StringLiteral{
						Val: "prometheus-server",
					},
				},
			},
			want: &influxql.BinaryExpr{
				Op: influxql.OR,
				LHS: &influxql.BinaryExpr{
					Op: influxql.EQREGEX,
					LHS: &influxql.VarRef{
						Val: "cpu",
					},
					RHS: &influxql.StringLiteral{
						Val: "cpu.*",
					},
				},
				RHS: &influxql.BinaryExpr{
					Op: influxql.EQ,
					LHS: &influxql.VarRef{
						Val: "host",
					},
					RHS: &influxql.StringLiteral{
						Val: "prometheus-server",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CombineConditionOr(tt.receiver, tt.args.expr); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Or() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_makeInt64Pointer(t *testing.T) {
	var a int64 = 1
	type args struct {
		val int64
	}
	tests := []struct {
		name string
		args args
		want *int64
	}{
		{
			name: "",
			args: args{
				val: 1,
			},
			want: &a,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := makeInt64Pointer(tt.args.val); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("makeInt64Pointer() = %v, want %v", got, tt.want)
			}
		})
	}
}

// name: "not support both sides has VectorSelector in binary expression"
func Test_SubQueryAndBinOp(t1 *testing.T) {
	tests := []struct {
		name   string
		fields fields
		args   args
		want   string
		skip   bool
	}{
		{
			name: "binop",
			fields: fields{
				Evaluation: &endTime2,
			},
			args: args{
				expr: BinaryExpr("avg(node_load5{instance=\"\",job=\"\"}) /  count(count(node_cpu_seconds_total{instance=\"\",job=\"\"}) by (cpu)) * 100"),
			},
			want: "SELECT value * 100 AS value FROM (SELECT mean(value) AS value FROM node_load5 WHERE time >= '2023-01-06T06:55:00Z' AND time <= '2023-01-06T07:00:00Z') binary op (SELECT count_prom(value) AS value FROM (SELECT count_prom(value) AS value FROM node_cpu_seconds_total WHERE time >= '2023-01-06T06:55:00Z' AND time <= '2023-01-06T07:00:00Z' GROUP BY cpu) WHERE time >= '2023-01-06T06:55:00Z' AND time <= '2023-01-06T07:00:00Z') false false() 0() WHERE time >= '2023-01-06T06:55:00Z' AND time <= '2023-01-06T07:00:00Z'",
		},
		{
			name: "subquery",
			fields: fields{
				Evaluation: &endTime2,
			},
			args: args{
				expr: SubqueryExpr(`sum_over_time(go_gc_duration_seconds_count[5m])[1h:10m]`),
			},
			want: "SELECT sum_over_time(value) AS value FROM go_gc_duration_seconds_count WHERE time >= '2023-01-06T05:55:00Z' AND time <= '2023-01-06T07:00:00Z' GROUP BY *, time(10m) fill(none)",
		},
	}
	for _, tt := range tests {
		if tt.skip {
			continue
		}
		t1.Run(tt.name, func(t1 *testing.T) {
			t := &Transpiler{
				PromCommand: PromCommand{
					Start:         tt.fields.Start,
					End:           tt.fields.End,
					Timezone:      tt.fields.Timezone,
					Evaluation:    tt.fields.Evaluation,
					Step:          tt.fields.Step,
					DataType:      tt.fields.DataType,
					Database:      tt.fields.Database,
					LabelName:     tt.fields.LabelName,
					LookBackDelta: DefaultLookBackDelta,
				},
				timeRange:      tt.fields.timeRange,
				parenExprCount: tt.fields.parenExprCount,
				timeCondition:  tt.fields.condition,
			}
			got, err := t.Transpile(tt.args.expr)
			if err != nil {
				t1.Fatal("Test_SubQueryAndBinOp err")
			}
			if !reflect.DeepEqual(got.String(), tt.want) {
				t1.Errorf("transpile() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPreprocessAndWrapWithStepInvariantExpr(t *testing.T) {
	startTime := time.Unix(1000, 0)
	endTime := time.Unix(9999, 0)
	testCases := []struct {
		input      string      // The input to be parsed.
		expected   parser.Expr // The expected expression AST.
		outputTest bool
	}{
		{
			input: "123.4567",
			expected: &parser.StepInvariantExpr{
				Expr: &parser.NumberLiteral{
					Val:      123.4567,
					PosRange: parser.PositionRange{Start: 0, End: 8},
				},
			},
		},
		{
			input: `"foo"`,
			expected: &parser.StepInvariantExpr{
				Expr: &parser.StringLiteral{
					Val:      "foo",
					PosRange: parser.PositionRange{Start: 0, End: 5},
				},
			},
		},
		{
			input: "foo * bar",
			expected: &parser.BinaryExpr{
				Op: parser.MUL,
				LHS: &parser.VectorSelector{
					Name: "foo",
					LabelMatchers: []*labels.Matcher{
						parser.MustLabelMatcher(labels.MatchEqual, "__name__", "foo"),
					},
					PosRange: parser.PositionRange{
						Start: 0,
						End:   3,
					},
				},
				RHS: &parser.VectorSelector{
					Name: "bar",
					LabelMatchers: []*labels.Matcher{
						parser.MustLabelMatcher(labels.MatchEqual, "__name__", "bar"),
					},
					PosRange: parser.PositionRange{
						Start: 6,
						End:   9,
					},
				},
				VectorMatching: &parser.VectorMatching{Card: parser.CardOneToOne},
			},
		},
		{
			input: "foo * bar @ 10",
			expected: &parser.BinaryExpr{
				Op: parser.MUL,
				LHS: &parser.VectorSelector{
					Name: "foo",
					LabelMatchers: []*labels.Matcher{
						parser.MustLabelMatcher(labels.MatchEqual, "__name__", "foo"),
					},
					PosRange: parser.PositionRange{
						Start: 0,
						End:   3,
					},
				},
				RHS: &parser.StepInvariantExpr{
					Expr: &parser.VectorSelector{
						Name: "bar",
						LabelMatchers: []*labels.Matcher{
							parser.MustLabelMatcher(labels.MatchEqual, "__name__", "bar"),
						},
						PosRange: parser.PositionRange{
							Start: 6,
							End:   14,
						},
						Timestamp: makeInt64Pointer(10000),
					},
				},
				VectorMatching: &parser.VectorMatching{Card: parser.CardOneToOne},
			},
		},
		{
			input: "foo @ 20 * bar @ 10",
			expected: &parser.StepInvariantExpr{
				Expr: &parser.BinaryExpr{
					Op: parser.MUL,
					LHS: &parser.VectorSelector{
						Name: "foo",
						LabelMatchers: []*labels.Matcher{
							parser.MustLabelMatcher(labels.MatchEqual, "__name__", "foo"),
						},
						PosRange: parser.PositionRange{
							Start: 0,
							End:   8,
						},
						Timestamp: makeInt64Pointer(20000),
					},
					RHS: &parser.VectorSelector{
						Name: "bar",
						LabelMatchers: []*labels.Matcher{
							parser.MustLabelMatcher(labels.MatchEqual, "__name__", "bar"),
						},
						PosRange: parser.PositionRange{
							Start: 11,
							End:   19,
						},
						Timestamp: makeInt64Pointer(10000),
					},
					VectorMatching: &parser.VectorMatching{Card: parser.CardOneToOne},
				},
			},
		},
		{
			input: "test[5s]",
			expected: &parser.MatrixSelector{
				VectorSelector: &parser.VectorSelector{
					Name: "test",
					LabelMatchers: []*labels.Matcher{
						parser.MustLabelMatcher(labels.MatchEqual, "__name__", "test"),
					},
					PosRange: parser.PositionRange{
						Start: 0,
						End:   4,
					},
				},
				Range:  5 * time.Second,
				EndPos: 8,
			},
		},
		{
			input: `test{a="b"}[5y] @ 1603774699`,
			expected: &parser.StepInvariantExpr{
				Expr: &parser.MatrixSelector{
					VectorSelector: &parser.VectorSelector{
						Name:      "test",
						Timestamp: makeInt64Pointer(1603774699000),
						LabelMatchers: []*labels.Matcher{
							parser.MustLabelMatcher(labels.MatchEqual, "a", "b"),
							parser.MustLabelMatcher(labels.MatchEqual, "__name__", "test"),
						},
						PosRange: parser.PositionRange{
							Start: 0,
							End:   11,
						},
					},
					Range:  5 * 365 * 24 * time.Hour,
					EndPos: 28,
				},
			},
		},
		{
			input: "sum by (foo)(some_metric)",
			expected: &parser.AggregateExpr{
				Op: parser.SUM,
				Expr: &parser.VectorSelector{
					Name: "some_metric",
					LabelMatchers: []*labels.Matcher{
						parser.MustLabelMatcher(labels.MatchEqual, "__name__", "some_metric"),
					},
					PosRange: parser.PositionRange{
						Start: 13,
						End:   24,
					},
				},
				Grouping: []string{"foo"},
				PosRange: parser.PositionRange{
					Start: 0,
					End:   25,
				},
			},
		},
		{
			input: "sum by (foo)(some_metric @ 10)",
			expected: &parser.StepInvariantExpr{
				Expr: &parser.AggregateExpr{
					Op: parser.SUM,
					Expr: &parser.VectorSelector{
						Name: "some_metric",
						LabelMatchers: []*labels.Matcher{
							parser.MustLabelMatcher(labels.MatchEqual, "__name__", "some_metric"),
						},
						PosRange: parser.PositionRange{
							Start: 13,
							End:   29,
						},
						Timestamp: makeInt64Pointer(10000),
					},
					Grouping: []string{"foo"},
					PosRange: parser.PositionRange{
						Start: 0,
						End:   30,
					},
				},
			},
		},
		{
			input: "sum(some_metric1 @ 10) + sum(some_metric2 @ 20)",
			expected: &parser.StepInvariantExpr{
				Expr: &parser.BinaryExpr{
					Op:             parser.ADD,
					VectorMatching: &parser.VectorMatching{},
					LHS: &parser.AggregateExpr{
						Op: parser.SUM,
						Expr: &parser.VectorSelector{
							Name: "some_metric1",
							LabelMatchers: []*labels.Matcher{
								parser.MustLabelMatcher(labels.MatchEqual, "__name__", "some_metric1"),
							},
							PosRange: parser.PositionRange{
								Start: 4,
								End:   21,
							},
							Timestamp: makeInt64Pointer(10000),
						},
						PosRange: parser.PositionRange{
							Start: 0,
							End:   22,
						},
					},
					RHS: &parser.AggregateExpr{
						Op: parser.SUM,
						Expr: &parser.VectorSelector{
							Name: "some_metric2",
							LabelMatchers: []*labels.Matcher{
								parser.MustLabelMatcher(labels.MatchEqual, "__name__", "some_metric2"),
							},
							PosRange: parser.PositionRange{
								Start: 29,
								End:   46,
							},
							Timestamp: makeInt64Pointer(20000),
						},
						PosRange: parser.PositionRange{
							Start: 25,
							End:   47,
						},
					},
				},
			},
		},
		{
			input: "some_metric and topk(5, rate(some_metric[1m] @ 20))",
			expected: &parser.BinaryExpr{
				Op: parser.LAND,
				VectorMatching: &parser.VectorMatching{
					Card: parser.CardManyToMany,
				},
				LHS: &parser.VectorSelector{
					Name: "some_metric",
					LabelMatchers: []*labels.Matcher{
						parser.MustLabelMatcher(labels.MatchEqual, "__name__", "some_metric"),
					},
					PosRange: parser.PositionRange{
						Start: 0,
						End:   11,
					},
				},
				RHS: &parser.StepInvariantExpr{
					Expr: &parser.AggregateExpr{
						Op: parser.TOPK,
						Expr: &parser.Call{
							Func: parser.MustGetFunction("rate"),
							Args: parser.Expressions{
								&parser.MatrixSelector{
									VectorSelector: &parser.VectorSelector{
										Name: "some_metric",
										LabelMatchers: []*labels.Matcher{
											parser.MustLabelMatcher(labels.MatchEqual, "__name__", "some_metric"),
										},
										PosRange: parser.PositionRange{
											Start: 29,
											End:   40,
										},
										Timestamp: makeInt64Pointer(20000),
									},
									Range:  1 * time.Minute,
									EndPos: 49,
								},
							},
							PosRange: parser.PositionRange{
								Start: 24,
								End:   50,
							},
						},
						Param: &parser.NumberLiteral{
							Val: 5,
							PosRange: parser.PositionRange{
								Start: 21,
								End:   22,
							},
						},
						PosRange: parser.PositionRange{
							Start: 16,
							End:   51,
						},
					},
				},
			},
		},
		{
			input: "time()",
			expected: &parser.Call{
				Func: parser.MustGetFunction("time"),
				Args: parser.Expressions{},
				PosRange: parser.PositionRange{
					Start: 0,
					End:   6,
				},
			},
		},
		{
			input: `foo{bar="baz"}[10m:6s]`,
			expected: &parser.SubqueryExpr{
				Expr: &parser.VectorSelector{
					Name: "foo",
					LabelMatchers: []*labels.Matcher{
						parser.MustLabelMatcher(labels.MatchEqual, "bar", "baz"),
						parser.MustLabelMatcher(labels.MatchEqual, "__name__", "foo"),
					},
					PosRange: parser.PositionRange{
						Start: 0,
						End:   14,
					},
				},
				Range:  10 * time.Minute,
				Step:   6 * time.Second,
				EndPos: 22,
			},
		},
		{
			input: `foo{bar="baz"}[10m:6s] @ 10`,
			expected: &parser.StepInvariantExpr{
				Expr: &parser.SubqueryExpr{
					Expr: &parser.VectorSelector{
						Name: "foo",
						LabelMatchers: []*labels.Matcher{
							parser.MustLabelMatcher(labels.MatchEqual, "bar", "baz"),
							parser.MustLabelMatcher(labels.MatchEqual, "__name__", "foo"),
						},
						PosRange: parser.PositionRange{
							Start: 0,
							End:   14,
						},
					},
					Range:     10 * time.Minute,
					Step:      6 * time.Second,
					Timestamp: makeInt64Pointer(10000),
					EndPos:    27,
				},
			},
		},
		{ // Even though the subquery is step invariant, the inside is also wrapped separately.
			input: `sum(foo{bar="baz"} @ 20)[10m:6s] @ 10`,
			expected: &parser.StepInvariantExpr{
				Expr: &parser.SubqueryExpr{
					Expr: &parser.StepInvariantExpr{
						Expr: &parser.AggregateExpr{
							Op: parser.SUM,
							Expr: &parser.VectorSelector{
								Name: "foo",
								LabelMatchers: []*labels.Matcher{
									parser.MustLabelMatcher(labels.MatchEqual, "bar", "baz"),
									parser.MustLabelMatcher(labels.MatchEqual, "__name__", "foo"),
								},
								PosRange: parser.PositionRange{
									Start: 4,
									End:   23,
								},
								Timestamp: makeInt64Pointer(20000),
							},
							PosRange: parser.PositionRange{
								Start: 0,
								End:   24,
							},
						},
					},
					Range:     10 * time.Minute,
					Step:      6 * time.Second,
					Timestamp: makeInt64Pointer(10000),
					EndPos:    37,
				},
			},
		},
		{
			input: `min_over_time(rate(foo{bar="baz"}[2s])[5m:] @ 1603775091)[4m:3s]`,
			expected: &parser.SubqueryExpr{
				Expr: &parser.StepInvariantExpr{
					Expr: &parser.Call{
						Func: parser.MustGetFunction("min_over_time"),
						Args: parser.Expressions{
							&parser.SubqueryExpr{
								Expr: &parser.Call{
									Func: parser.MustGetFunction("rate"),
									Args: parser.Expressions{
										&parser.MatrixSelector{
											VectorSelector: &parser.VectorSelector{
												Name: "foo",
												LabelMatchers: []*labels.Matcher{
													parser.MustLabelMatcher(labels.MatchEqual, "bar", "baz"),
													parser.MustLabelMatcher(labels.MatchEqual, "__name__", "foo"),
												},
												PosRange: parser.PositionRange{
													Start: 19,
													End:   33,
												},
											},
											Range:  2 * time.Second,
											EndPos: 37,
										},
									},
									PosRange: parser.PositionRange{
										Start: 14,
										End:   38,
									},
								},
								Range:     5 * time.Minute,
								Timestamp: makeInt64Pointer(1603775091000),
								EndPos:    56,
							},
						},
						PosRange: parser.PositionRange{
							Start: 0,
							End:   57,
						},
					},
				},
				Range:  4 * time.Minute,
				Step:   3 * time.Second,
				EndPos: 64,
			},
		},
		{
			input: `some_metric @ 123 offset 1m [10m:5s]`,
			expected: &parser.SubqueryExpr{
				Expr: &parser.StepInvariantExpr{
					Expr: &parser.VectorSelector{
						Name: "some_metric",
						LabelMatchers: []*labels.Matcher{
							parser.MustLabelMatcher(labels.MatchEqual, "__name__", "some_metric"),
						},
						PosRange: parser.PositionRange{
							Start: 0,
							End:   27,
						},
						Timestamp:      makeInt64Pointer(123000),
						OriginalOffset: 1 * time.Minute,
					},
				},
				Range:  10 * time.Minute,
				Step:   5 * time.Second,
				EndPos: 36,
			},
		},
		{
			input: `some_metric[10m:5s] offset 1m @ 123`,
			expected: &parser.StepInvariantExpr{
				Expr: &parser.SubqueryExpr{
					Expr: &parser.VectorSelector{
						Name: "some_metric",
						LabelMatchers: []*labels.Matcher{
							parser.MustLabelMatcher(labels.MatchEqual, "__name__", "some_metric"),
						},
						PosRange: parser.PositionRange{
							Start: 0,
							End:   11,
						},
					},
					Timestamp:      makeInt64Pointer(123000),
					OriginalOffset: 1 * time.Minute,
					Range:          10 * time.Minute,
					Step:           5 * time.Second,
					EndPos:         35,
				},
			},
		},
		{
			input: `(foo + bar{nm="val"} @ 1234)[5m:] @ 1603775019`,
			expected: &parser.StepInvariantExpr{
				Expr: &parser.SubqueryExpr{
					Expr: &parser.ParenExpr{
						Expr: &parser.BinaryExpr{
							Op: parser.ADD,
							VectorMatching: &parser.VectorMatching{
								Card: parser.CardOneToOne,
							},
							LHS: &parser.VectorSelector{
								Name: "foo",
								LabelMatchers: []*labels.Matcher{
									parser.MustLabelMatcher(labels.MatchEqual, "__name__", "foo"),
								},
								PosRange: parser.PositionRange{
									Start: 1,
									End:   4,
								},
							},
							RHS: &parser.StepInvariantExpr{
								Expr: &parser.VectorSelector{
									Name: "bar",
									LabelMatchers: []*labels.Matcher{
										parser.MustLabelMatcher(labels.MatchEqual, "nm", "val"),
										parser.MustLabelMatcher(labels.MatchEqual, "__name__", "bar"),
									},
									Timestamp: makeInt64Pointer(1234000),
									PosRange: parser.PositionRange{
										Start: 7,
										End:   27,
									},
								},
							},
						},
						PosRange: parser.PositionRange{
							Start: 0,
							End:   28,
						},
					},
					Range:     5 * time.Minute,
					Timestamp: makeInt64Pointer(1603775019000),
					EndPos:    46,
				},
			},
		},
		{
			input: "abs(abs(metric @ 10))",
			expected: &parser.StepInvariantExpr{
				Expr: &parser.Call{
					Func: &parser.Function{
						Name:       "abs",
						ArgTypes:   []parser.ValueType{parser.ValueTypeVector},
						ReturnType: parser.ValueTypeVector,
					},
					Args: parser.Expressions{&parser.Call{
						Func: &parser.Function{
							Name:       "abs",
							ArgTypes:   []parser.ValueType{parser.ValueTypeVector},
							ReturnType: parser.ValueTypeVector,
						},
						Args: parser.Expressions{&parser.VectorSelector{
							Name: "metric",
							LabelMatchers: []*labels.Matcher{
								parser.MustLabelMatcher(labels.MatchEqual, "__name__", "metric"),
							},
							PosRange: parser.PositionRange{
								Start: 8,
								End:   19,
							},
							Timestamp: makeInt64Pointer(10000),
						}},
						PosRange: parser.PositionRange{
							Start: 4,
							End:   20,
						},
					}},
					PosRange: parser.PositionRange{
						Start: 0,
						End:   21,
					},
				},
			},
		},
		{
			input: "sum(sum(some_metric1 @ 10) + sum(some_metric2 @ 20))",
			expected: &parser.StepInvariantExpr{
				Expr: &parser.AggregateExpr{
					Op: parser.SUM,
					Expr: &parser.BinaryExpr{
						Op:             parser.ADD,
						VectorMatching: &parser.VectorMatching{},
						LHS: &parser.AggregateExpr{
							Op: parser.SUM,
							Expr: &parser.VectorSelector{
								Name: "some_metric1",
								LabelMatchers: []*labels.Matcher{
									parser.MustLabelMatcher(labels.MatchEqual, "__name__", "some_metric1"),
								},
								PosRange: parser.PositionRange{
									Start: 8,
									End:   25,
								},
								Timestamp: makeInt64Pointer(10000),
							},
							PosRange: parser.PositionRange{
								Start: 4,
								End:   26,
							},
						},
						RHS: &parser.AggregateExpr{
							Op: parser.SUM,
							Expr: &parser.VectorSelector{
								Name: "some_metric2",
								LabelMatchers: []*labels.Matcher{
									parser.MustLabelMatcher(labels.MatchEqual, "__name__", "some_metric2"),
								},
								PosRange: parser.PositionRange{
									Start: 33,
									End:   50,
								},
								Timestamp: makeInt64Pointer(20000),
							},
							PosRange: parser.PositionRange{
								Start: 29,
								End:   52,
							},
						},
					},
					PosRange: parser.PositionRange{
						Start: 0,
						End:   52,
					},
				},
			},
		},
		{
			input: `foo @ start()`,
			expected: &parser.StepInvariantExpr{
				Expr: &parser.VectorSelector{
					Name: "foo",
					LabelMatchers: []*labels.Matcher{
						parser.MustLabelMatcher(labels.MatchEqual, "__name__", "foo"),
					},
					PosRange: parser.PositionRange{
						Start: 0,
						End:   13,
					},
					Timestamp:  makeInt64Pointer(timestamp.FromTime(startTime)),
					StartOrEnd: parser.START,
				},
			},
		},
		{
			input: `foo @ end()`,
			expected: &parser.StepInvariantExpr{
				Expr: &parser.VectorSelector{
					Name: "foo",
					LabelMatchers: []*labels.Matcher{
						parser.MustLabelMatcher(labels.MatchEqual, "__name__", "foo"),
					},
					PosRange: parser.PositionRange{
						Start: 0,
						End:   11,
					},
					Timestamp:  makeInt64Pointer(timestamp.FromTime(endTime)),
					StartOrEnd: parser.END,
				},
			},
		},
		{
			input: `test[5y] @ start()`,
			expected: &parser.StepInvariantExpr{
				Expr: &parser.MatrixSelector{
					VectorSelector: &parser.VectorSelector{
						Name:       "test",
						Timestamp:  makeInt64Pointer(timestamp.FromTime(startTime)),
						StartOrEnd: parser.START,
						LabelMatchers: []*labels.Matcher{
							parser.MustLabelMatcher(labels.MatchEqual, "__name__", "test"),
						},
						PosRange: parser.PositionRange{
							Start: 0,
							End:   4,
						},
					},
					Range:  5 * 365 * 24 * time.Hour,
					EndPos: 18,
				},
			},
		},
		{
			input: `test[5y] @ end()`,
			expected: &parser.StepInvariantExpr{
				Expr: &parser.MatrixSelector{
					VectorSelector: &parser.VectorSelector{
						Name:       "test",
						Timestamp:  makeInt64Pointer(timestamp.FromTime(endTime)),
						StartOrEnd: parser.END,
						LabelMatchers: []*labels.Matcher{
							parser.MustLabelMatcher(labels.MatchEqual, "__name__", "test"),
						},
						PosRange: parser.PositionRange{
							Start: 0,
							End:   4,
						},
					},
					Range:  5 * 365 * 24 * time.Hour,
					EndPos: 16,
				},
			},
		},
		{
			input: `some_metric[10m:5s] @ start()`,
			expected: &parser.StepInvariantExpr{
				Expr: &parser.SubqueryExpr{
					Expr: &parser.VectorSelector{
						Name: "some_metric",
						LabelMatchers: []*labels.Matcher{
							parser.MustLabelMatcher(labels.MatchEqual, "__name__", "some_metric"),
						},
						PosRange: parser.PositionRange{
							Start: 0,
							End:   11,
						},
					},
					Timestamp:  makeInt64Pointer(timestamp.FromTime(startTime)),
					StartOrEnd: parser.START,
					Range:      10 * time.Minute,
					Step:       5 * time.Second,
					EndPos:     29,
				},
			},
		},
		{
			input: `some_metric[10m:5s] @ end()`,
			expected: &parser.StepInvariantExpr{
				Expr: &parser.SubqueryExpr{
					Expr: &parser.VectorSelector{
						Name: "some_metric",
						LabelMatchers: []*labels.Matcher{
							parser.MustLabelMatcher(labels.MatchEqual, "__name__", "some_metric"),
						},
						PosRange: parser.PositionRange{
							Start: 0,
							End:   11,
						},
					},
					Timestamp:  makeInt64Pointer(timestamp.FromTime(endTime)),
					StartOrEnd: parser.END,
					Range:      10 * time.Minute,
					Step:       5 * time.Second,
					EndPos:     27,
				},
			},
		},
		{
			input:      `floor(some_metric / (3 * 1024))`,
			outputTest: true,
			expected: &parser.Call{
				Func: &parser.Function{
					Name:       "floor",
					ArgTypes:   []parser.ValueType{parser.ValueTypeVector},
					ReturnType: parser.ValueTypeVector,
				},
				Args: parser.Expressions{
					&parser.BinaryExpr{
						Op: parser.DIV,
						LHS: &parser.VectorSelector{
							Name: "some_metric",
							LabelMatchers: []*labels.Matcher{
								parser.MustLabelMatcher(labels.MatchEqual, "__name__", "some_metric"),
							},
							PosRange: parser.PositionRange{
								Start: 6,
								End:   17,
							},
						},
						RHS: &parser.StepInvariantExpr{
							Expr: &parser.ParenExpr{
								Expr: &parser.BinaryExpr{
									Op: parser.MUL,
									LHS: &parser.NumberLiteral{
										Val: 3,
										PosRange: parser.PositionRange{
											Start: 21,
											End:   22,
										},
									},
									RHS: &parser.NumberLiteral{
										Val: 1024,
										PosRange: parser.PositionRange{
											Start: 25,
											End:   29,
										},
									},
								},
								PosRange: parser.PositionRange{
									Start: 20,
									End:   30,
								},
							},
						},
					},
				},
				PosRange: parser.PositionRange{
					Start: 0,
					End:   31,
				},
			},
		},
	}

	trans := &Transpiler{}
	for _, test := range testCases {
		t.Run(test.input, func(t *testing.T) {
			expr, err := parser.ParseExpr(test.input)
			require.NoError(t, err)
			expr = trans.PreprocessExpr(expr, startTime, endTime)
			if test.outputTest {
				require.Equal(t, test.input, expr.String(), "error on input '%s'", test.input)
			}
			require.Equal(t, test.expected, expr, "error on input '%s'", test.input)
		})
	}
}
