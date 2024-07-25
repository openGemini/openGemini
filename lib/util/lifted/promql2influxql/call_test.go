package promql2influxql

import (
	"reflect"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/prometheus/prometheus/promql/parser"
)

func CallExpr(input string) *parser.Call {
	expr, err := parser.ParseExpr(input)
	if err != nil {
		panic(err)
	}
	v, ok := expr.(*parser.Call)
	if !ok {
		panic("bad input")
	}
	return v
}

func TestTranspiler_transpileCall(t1 *testing.T) {
	type fields struct {
		Start          *time.Time
		End            *time.Time
		Timezone       *time.Location
		Evaluation     *time.Time
		Step           time.Duration
		DataType       DataType
		timeRange      time.Duration
		parenExprCount int
		timeCondition  influxql.Expr
		tagDropped     bool
	}
	type args struct {
		a *parser.Call
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
				a: CallExpr(`abs(go_gc_duration_seconds_count)`),
			},
			want:    parseInfluxqlByYacc(`SELECT abs(value) AS value FROM go_gc_duration_seconds_count WHERE time >= '2023-01-06T07:00:00Z' AND time <= '2023-01-06T07:00:00Z' GROUP BY *`),
			wantErr: false,
		},
		{
			name: "2",
			fields: fields{
				Evaluation: &endTime2,
			},
			args: args{
				a: CallExpr(`quantile_over_time(0.5, go_gc_duration_seconds_count[5m])`),
			},
			want:    parseInfluxqlByYacc(`SELECT quantile_over_time(value, 0.500000000) AS value FROM go_gc_duration_seconds_count WHERE time >= '2023-01-06T06:55:00Z' AND time <= '2023-01-06T07:00:00Z' GROUP BY *`),
			wantErr: false,
		},
		{
			name: "3",
			fields: fields{
				Start: &startTime2, End: &endTime2, Step: step,
			},
			args: args{
				a: CallExpr(`rate(go_gc_duration_seconds_count[5m])`),
			},
			want:    parseInfluxqlByYacc(`SELECT rate_prom(value) AS value FROM go_gc_duration_seconds_count WHERE time >= '2023-01-06T03:55:00Z' AND time <= '2023-01-06T07:00:00Z' GROUP BY *, time(1m) fill(none)`),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			t := &Transpiler{
				PromCommand: PromCommand{
					Start:      tt.fields.Start,
					End:        tt.fields.End,
					Timezone:   tt.fields.Timezone,
					Evaluation: tt.fields.Evaluation,
					Step:       tt.fields.Step,
					DataType:   tt.fields.DataType,
				},
				timeRange:      tt.fields.timeRange,
				parenExprCount: tt.fields.parenExprCount,
				timeCondition:  tt.fields.timeCondition,
			}
			t.rewriteMinMaxTime()
			got, err := t.transpileCall(tt.args.a)
			if (err != nil) != tt.wantErr {
				t1.Errorf("transpileCall() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got.String(), tt.want.String()) {
				t1.Errorf("transpileCall() got = %v, want %v", got, tt.want)
			}
		})
	}
}
