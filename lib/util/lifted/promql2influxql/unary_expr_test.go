package promql2influxql

import (
	"reflect"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/prometheus/prometheus/promql/parser"
)

func UnaryExpr(input string) *parser.UnaryExpr {
	expr, err := parser.ParseExpr(input)
	if err != nil {
		panic(err)
	}
	v, ok := expr.(*parser.UnaryExpr)
	if !ok {
		panic("bad input")
	}
	return v
}

func TestTranspiler_transpileUnaryExpr(t1 *testing.T) {
	type fields struct {
		Start      *time.Time
		End        *time.Time
		Timezone   *time.Location
		Evaluation *time.Time
	}
	type args struct {
		ue *parser.UnaryExpr
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    influxql.Node
		wantErr bool
	}{
		{
			name: "",
			fields: fields{
				Evaluation: &endTime2,
			},
			args: args{
				ue: UnaryExpr(`-(3 * go_gc_duration_seconds_count)`),
			},
			want:    influxql.MustParseStatement(`SELECT -1 * (3 * value) AS value FROM go_gc_duration_seconds_count WHERE time >= '2023-01-06T07:00:00Z' AND time <= '2023-01-06T07:00:00Z' GROUP BY *`),
			wantErr: false,
		},
		{
			name: "",
			fields: fields{
				Evaluation: &endTime2,
			},
			args: args{
				ue: UnaryExpr(`-(3^2 + 3)`),
			},
			want:    influxql.MustParseExpr(`-1 * (pow(3.000, 2.000) + 3.000)`),
			wantErr: false,
		},
		{
			name: "",
			fields: fields{
				Evaluation: &endTime2,
			},
			args: args{
				ue: UnaryExpr(`-(20)`),
			},
			want:    influxql.MustParseExpr(`-1 * (20.000)`),
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
				},
			}
			s := t.newEvalStmt(tt.args.ue)
			t.minT, t.maxT = t.findMinMaxTime(s)
			got, err := t.transpileUnaryExpr(tt.args.ue)
			if (err != nil) != tt.wantErr {
				t1.Errorf("transpileUnaryExpr() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got.String(), tt.want.String()) {
				t1.Errorf("transpileUnaryExpr() got = %v, want %v", got, tt.want)
			}
		})
	}
}
