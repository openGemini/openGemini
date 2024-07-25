package promql2influxql

import (
	"reflect"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/prometheus/prometheus/promql/parser"
)

func BinaryExpr(input string) *parser.BinaryExpr {
	expr, err := parser.ParseExpr(input)
	if err != nil {
		panic(err)
	}
	v, ok := expr.(*parser.BinaryExpr)
	if !ok {
		panic("bad input")
	}
	return v
}

func TestTranspiler_transpileBinaryExpr(t1 *testing.T) {
	type fields struct {
		Start      *time.Time
		End        *time.Time
		Timezone   *time.Location
		Evaluation *time.Time
	}
	type args struct {
		b *parser.BinaryExpr
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    influxql.Node
		wantErr bool
	}{
		{
			name: "1",
			fields: fields{
				Evaluation: &endTime2,
			},
			args: args{
				b: BinaryExpr(`5 * go_gc_duration_seconds_count`),
			},
			want:    parseInfluxqlByYacc(`SELECT 5 * value AS value FROM go_gc_duration_seconds_count WHERE time >= '2023-01-06T06:55:00Z' AND time <= '2023-01-06T07:00:00Z' GROUP BY *`),
			wantErr: false,
		},
		{
			name: "2",
			fields: fields{
				Evaluation: &endTime2,
			},
			args: args{
				b: BinaryExpr(`5 * rate(go_gc_duration_seconds_count[1m])`),
			},
			want:    parseInfluxqlByYacc(`SELECT 5.000000000 * rate_prom(value) AS value FROM go_gc_duration_seconds_count WHERE time >= '2023-01-06T06:59:00Z' AND time <= '2023-01-06T07:00:00Z' GROUP BY *`),
			wantErr: false,
		},
		{
			name: "3",
			fields: fields{
				Evaluation: &endTime2,
			},
			args: args{
				b: BinaryExpr(`5 * 6 * go_gc_duration_seconds_count`),
			},
			want:    parseInfluxqlByYacc(`SELECT 5 * 6 * value AS value FROM go_gc_duration_seconds_count WHERE time >= '2023-01-06T06:55:00Z' AND time <= '2023-01-06T07:00:00Z' GROUP BY *`),
			wantErr: false,
		},
		{
			name: "4",
			fields: fields{
				Evaluation: &endTime2,
			},
			args: args{
				b: BinaryExpr(`5 * (go_gc_duration_seconds_count - 6)`),
			},
			want:    parseInfluxqlByYacc(`SELECT 5 * (value - 6) AS value FROM go_gc_duration_seconds_count WHERE time >= '2023-01-06T06:55:00Z' AND time <= '2023-01-06T07:00:00Z' GROUP BY *`),
			wantErr: false,
		},
		{
			name: "5",
			fields: fields{
				Evaluation: &endTime2,
			},
			args: args{
				b: BinaryExpr(`(5 * go_gc_duration_seconds_count) - 6`),
			},
			want:    parseInfluxqlByYacc(`SELECT (5 * value) - 6 AS value FROM go_gc_duration_seconds_count WHERE time >= '2023-01-06T06:55:00Z' AND time <= '2023-01-06T07:00:00Z' GROUP BY *`),
			wantErr: false,
		},
		{
			name: "6",
			fields: fields{
				Evaluation: &endTime2,
			},
			args: args{
				b: BinaryExpr(`5 > go_gc_duration_seconds_count`),
			},
			want:    parseInfluxqlByYacc(`SELECT value AS value FROM go_gc_duration_seconds_count WHERE time >= '2023-01-06T06:55:00Z' AND time <= '2023-01-06T07:00:00Z' AND 5 > value GROUP BY *`),
			wantErr: false,
		},
		{
			name: "7",
			fields: fields{
				Evaluation: &endTime2,
			},
			args: args{
				b: BinaryExpr(`go_gc_duration_seconds_count^3`),
			},
			want:    parseInfluxqlByYacc(`SELECT pow(value, 3) AS value FROM go_gc_duration_seconds_count WHERE time >= '2023-01-06T06:55:00Z' AND time <= '2023-01-06T07:00:00Z' GROUP BY *`),
			wantErr: false,
		},
		{
			name: "8",
			fields: fields{
				Evaluation: &endTime2,
			},
			args: args{
				b: BinaryExpr(`go_gc_duration_seconds_count^3^4`),
			},
			want:    parseInfluxqlByYacc(`SELECT pow(value, pow(3, 4)) AS value FROM go_gc_duration_seconds_count WHERE time >= '2023-01-06T06:55:00Z' AND time <= '2023-01-06T07:00:00Z' GROUP BY *`),
			wantErr: false,
		},
		{
			name: "9",
			fields: fields{
				Evaluation: &endTime2,
			},
			args: args{
				b: BinaryExpr(`go_gc_duration_seconds_count^(3^4)`),
			},
			want:    parseInfluxqlByYacc(`SELECT pow(value, (pow(3, 4))) AS value FROM go_gc_duration_seconds_count WHERE time >= '2023-01-06T06:55:00Z' AND time <= '2023-01-06T07:00:00Z' GROUP BY *`),
			wantErr: false,
		},
		{
			name: "10",
			fields: fields{
				Evaluation: &endTime2,
			},
			args: args{
				b: BinaryExpr(`(go_gc_duration_seconds_count^3)^4`),
			},
			want:    parseInfluxqlByYacc(`SELECT pow(pow(value, 3), 4) AS value FROM go_gc_duration_seconds_count WHERE time >= '2023-01-06T06:55:00Z' AND time <= '2023-01-06T07:00:00Z' GROUP BY *`),
			wantErr: false,
		},
		{
			name: "11",
			fields: fields{
				Evaluation: &endTime2,
			},
			args: args{
				b: BinaryExpr(`4^go_gc_duration_seconds_count`),
			},
			want:    parseInfluxqlByYacc(`SELECT pow(4, value) AS value FROM go_gc_duration_seconds_count WHERE time >= '2023-01-06T06:55:00Z' AND time <= '2023-01-06T07:00:00Z' GROUP BY *`),
			wantErr: false,
		},
		{
			name: "12",
			fields: fields{
				Evaluation: &endTime2,
			},
			args: args{
				b: BinaryExpr(`go_gc_duration_seconds_count>=3<4`),
			},
			want:    parseInfluxqlByYacc(`SELECT value AS value FROM go_gc_duration_seconds_count WHERE time >= '2023-01-06T06:55:00Z' AND time <= '2023-01-06T07:00:00Z' AND value >= 3 AND value < 4 GROUP BY *`),
			wantErr: false,
		},
		{
			name: "13",
			fields: fields{
				Evaluation: &endTime2,
			},
			args: args{
				b: BinaryExpr(`sum(go_gc_duration_seconds_count)>=3<4`),
			},
			want:    parseInfluxqlByYacc(`SELECT value AS value FROM (SELECT value AS value FROM (SELECT sum(value) AS value FROM go_gc_duration_seconds_count WHERE time >= '2023-01-06T06:55:00Z' AND time <= '2023-01-06T07:00:00Z') WHERE value >= 3 AND time >= '2023-01-06T06:55:00Z' AND time <= '2023-01-06T07:00:00Z') WHERE value < 4 AND time >= '2023-01-06T06:55:00Z' AND time <= '2023-01-06T07:00:00Z'`),
			wantErr: false,
		},
		{
			name: "14",
			fields: fields{
				Evaluation: &endTime2,
			},
			args: args{
				b: BinaryExpr(`sum_over_time(go_gc_duration_seconds_count[1m])>=3<4`),
			},
			want:    parseInfluxqlByYacc(`SELECT value AS value FROM (SELECT value AS value FROM (SELECT sum_over_time(value) AS value FROM go_gc_duration_seconds_count WHERE time >= '2023-01-06T06:59:00Z' AND time <= '2023-01-06T07:00:00Z' GROUP BY *) WHERE value >= 3 AND time >= '2023-01-06T06:59:00Z' AND time <= '2023-01-06T07:00:00Z' GROUP BY *) WHERE value < 4 AND time >= '2023-01-06T06:59:00Z' AND time <= '2023-01-06T07:00:00Z' GROUP BY *`),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			t := &Transpiler{
				PromCommand: PromCommand{
					Start:         tt.fields.Start,
					End:           tt.fields.End,
					LookBackDelta: DefaultLookBackDelta,
					Timezone:      tt.fields.Timezone,
					Evaluation:    tt.fields.Evaluation,
				},
			}
			t.rewriteMinMaxTime()
			got, err := t.transpileBinaryExpr(tt.args.b)
			if (err != nil) != tt.wantErr {
				t1.Errorf("transpileBinaryExpr() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if !reflect.DeepEqual(got.String(), tt.want.String()) {
					t1.Errorf("transpileBinaryExpr() got = %v, want %v", got, tt.want)
				}
			}
		})
	}
}

func Test_BothVectorWithoutMstOfBinOp(t *testing.T) {
	fields := fields{
		Evaluation: &endTime2,
	}
	args := args{
		expr: BinaryExpr("year() / year()"),
	}
	want := "year_prom(prom_time) / year_prom(prom_time)"
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
		t.Fatal("Test_BothVectorWithoutMstOfBinOp err")
	}
	if !reflect.DeepEqual(got.String(), want) {
		t.Errorf("transpile() got = %v, want %v", got, want)
	}
}
