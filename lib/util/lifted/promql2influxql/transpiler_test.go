package promql2influxql

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/prometheus/prometheus/promql/parser"
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
			want:    influxql.MustParseStatement(`SELECT value AS value FROM cpu WHERE time >= '2023-01-06T03:55:00Z' AND time <= '2023-01-08T02:00:00Z' AND host =~ /^(?:tele.*)$/ GROUP BY *`),
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
			want:    influxql.MustParseStatement(`SELECT value AS value FROM cpu WHERE time >= '2023-01-08T01:55:00Z' AND time <= '2023-01-08T02:00:00Z' AND host =~ /^(?:tele.*)$/ GROUP BY *`),
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
			want:    influxql.MustParseStatement(`SELECT value AS value FROM cpu WHERE time >= '2023-01-08T01:55:00Z' AND time <= '2023-01-08T02:00:00Z' AND host =~ /^(?:tele.*)$/ GROUP BY *`),
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
			want:    influxql.MustParseStatement(`SELECT value AS value FROM cpu WHERE time >= '2023-01-06T06:55:00Z' AND time <= '2023-01-06T07:00:00Z' AND host =~ /^(?:tele.*)$/ GROUP BY *`),
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
			want:    influxql.MustParseStatement(`SELECT pow(value, pow(3, 4)) AS value FROM go_gc_duration_seconds_count WHERE time >= '2023-01-06T06:55:00Z' AND time <= '2023-01-06T07:00:00Z' GROUP BY *`),
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
			want:    parseInfluxqlByYacc(`SELECT sum_over_time(value) AS value FROM go_gc_duration_seconds_count WHERE time >= '2023-01-06T03:55:00Z' AND time <= '2023-01-06T07:00:00Z' GROUP BY *, time(1m) fill(none)`),
			wantErr: false,
		},
		{
			name: "not support PromQL subquery expression",
			fields: fields{
				Evaluation: &endTime2,
			},
			args: args{
				expr: SubqueryExpr(`sum_over_time(go_gc_duration_seconds_count[5m])[1h:10m]`),
			},
			want:    nil,
			wantErr: true,
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
			want:    influxql.MustParseStatement(`SHOW TAG VALUES ON prometheus FROM go_goroutines WITH KEY = job WHERE time >= '2023-01-08T01:55:00Z' AND time <= '2023-01-08T02:00:00Z' AND instance =~ /^(?:192.168.*)$/`),
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
			want:    influxql.MustParseStatement(`SHOW TAG VALUES ON prometheus FROM go_goroutines WITH KEY = job WHERE time >= '2023-01-08T01:55:00Z' AND time <= '2023-01-08T02:00:00Z' AND instance =~ /^(?:192.168.*)$/`),
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
			want:    parseInfluxqlByYacc(`SELECT top(value, 3) AS value FROM (SELECT sum(rate_prom(value)) AS value FROM instance_cpu_time_ns WHERE time >= '2023-01-06T06:55:00Z' AND time <= '2023-01-06T07:00:00Z' GROUP BY app, proc) WHERE time >= '2023-01-06T06:55:00Z' AND time <= '2023-01-06T07:00:00Z' GROUP BY app, proc`),
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
			want:    parseInfluxqlByYacc(`SELECT top(value, 3) AS value FROM (SELECT sum(rate_prom(value)) AS value FROM instance_cpu_time_ns WHERE time >= '2023-01-06T03:55:00Z' AND time <= '2023-01-06T07:00:00Z' GROUP BY app, proc, time(1m) fill(none)) WHERE time >= '2023-01-06T03:55:00Z' AND time <= '2023-01-06T07:00:00Z' GROUP BY app, proc, time(1m)`),
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
func Test_SubQueryAndBinOp(t *testing.T) {
	fields := fields{
		Evaluation: &endTime2,
	}
	args := args{
		expr: BinaryExpr("avg(node_load5{instance=\"\",job=\"\"}) /  count(count(node_cpu_seconds_total{instance=\"\",job=\"\"}) by (cpu)) * 100"),
	}
	want := "SELECT value * 100 AS value FROM (SELECT mean(value) AS value FROM node_load5 WHERE time >= '2023-01-06T06:55:00Z' AND time <= '2023-01-06T07:00:00Z') binary op (SELECT count_prom(value) AS value FROM (SELECT count_prom(value) AS value FROM node_cpu_seconds_total WHERE time >= '2023-01-06T06:55:00Z' AND time <= '2023-01-06T07:00:00Z' GROUP BY cpu) WHERE time >= '2023-01-06T06:55:00Z' AND time <= '2023-01-06T07:00:00Z') false false() 0() WHERE time >= '2023-01-06T06:55:00Z' AND time <= '2023-01-06T07:00:00Z'"
	trans := &Transpiler{
		PromCommand: PromCommand{
			Start:         fields.Start,
			End:           fields.End,
			Timezone:      fields.Timezone,
			Evaluation:    fields.Evaluation,
			Step:          fields.Step,
			DataType:      fields.DataType,
			Database:      fields.Database,
			LabelName:     fields.LabelName,
			LookBackDelta: DefaultLookBackDelta,
		},
		timeRange:      fields.timeRange,
		parenExprCount: fields.parenExprCount,
		timeCondition:  fields.condition,
	}
	got, err := trans.Transpile(args.expr)
	if err != nil {
		t.Fatal("Test_SubQueryAndBinOp err")
	}
	if !reflect.DeepEqual(got.String(), want) {
		t.Errorf("transpile() got = %v, want %v", got, want)
	}
}
