package promql2influxql

import (
	"reflect"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql/parser"
)

func VectorSelector(input string) *parser.VectorSelector {
	expr, err := parser.ParseExpr(input)
	if err != nil {
		panic(err)
	}
	v, ok := expr.(*parser.VectorSelector)
	if !ok {
		panic("bad input")
	}
	return v
}

func MatrixSelector(input string) *parser.MatrixSelector {
	expr, err := parser.ParseExpr(input)
	if err != nil {
		panic(err)
	}
	v, ok := expr.(*parser.MatrixSelector)
	if !ok {
		panic("bad input")
	}
	return v
}

func TestTranspiler_TranspileVectorSelector2ConditionExpr(t1 *testing.T) {
	type fields struct {
		Start *time.Time
		End   *time.Time
	}
	type args struct {
		v *parser.VectorSelector
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    influxql.Expr
		wantErr bool
	}{
		{
			name: "1",
			fields: fields{
				Start: nil,
				End:   &endTime,
			},
			args: args{
				v: VectorSelector(`cpu{host=~"tele.*"}`),
			},
			want:    influxql.MustParseExpr("host =~ /^(?:tele.*)$/"),
			wantErr: false,
		},
		{
			name: "2",
			fields: fields{
				Start: nil,
				End:   &endTime,
			},
			args: args{
				v: VectorSelector(`cpu{host=~"tele.*", cpu="cpu0"}`),
			},
			want:    influxql.MustParseExpr("host =~ /^(?:tele.*)$/ AND cpu = 'cpu0'"),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			t := &Transpiler{
				PromCommand: PromCommand{
					Start: tt.fields.Start,
					End:   tt.fields.End,
				},
			}
			_, got, err := t.transpileVectorSelector2ConditionExpr(tt.args.v)
			if (err != nil) != tt.wantErr {
				t1.Errorf("transpileVectorSelector2ConditionExpr() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got.String(), tt.want.String()) {
				t1.Errorf("transpileVectorSelector2ConditionExpr() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTranspiler_transpileVectorSelector(t1 *testing.T) {
	type fields struct {
		Start      *time.Time
		End        *time.Time
		Step       time.Duration
		Timezone   *time.Location
		Evaluation *time.Time
		DataType   DataType
		Database   string
		LabelName  string
	}
	type args struct {
		v *parser.VectorSelector
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
				Start: &startTime2, End: &endTime2, Step: step,
			},
			args: args{
				v: VectorSelector(`cpu{host=~"tele.*"}`),
			},
			want:    influxql.MustParseStatement(`SELECT value AS value FROM cpu WHERE time >= '2023-01-06T04:00:00Z' AND time <= '2023-01-06T07:00:00Z' AND host =~ /^(?:tele.*)$/ GROUP BY *`),
			wantErr: false,
		},
		{
			name: "2",
			fields: fields{
				Evaluation: &endTime,
			},
			args: args{
				v: VectorSelector(`cpu{host=~"tele.*"}`),
			},
			want:    influxql.MustParseStatement(`SELECT value AS value FROM cpu WHERE time >= '2023-01-08T02:00:00Z' AND time <= '2023-01-08T02:00:00Z' AND host =~ /^(?:tele.*)$/ GROUP BY *`),
			wantErr: false,
		},
		{
			name: "3",
			fields: fields{
				Evaluation: &endTime,
				DataType:   LABEL_VALUES_DATA,
				Database:   "prometheus",
				LabelName:  "job",
			},
			args: args{
				v: VectorSelector(`go_goroutines{instance=~"192.168.*"}`),
			},
			want:    influxql.MustParseStatement(`SHOW TAG VALUES ON prometheus FROM go_goroutines WITH KEY = job WHERE time >= '2023-01-08T02:00:00Z' AND time <= '2023-01-08T02:00:00Z' AND instance =~ /^(?:192.168.*)$/`),
			wantErr: false,
		},
		{
			name: "4",
			fields: fields{
				Evaluation: &endTime,
				DataType:   LABEL_VALUES_DATA,
				Database:   "prometheus",
			},
			args: args{
				v: VectorSelector(`go_goroutines{instance=~"192.168.*"}`),
			},
			want:    influxql.MustParseStatement(`SHOW TAG VALUES ON prometheus FROM go_goroutines WITH KEY = "" WHERE time >= '2023-01-08T02:00:00Z' AND time <= '2023-01-08T02:00:00Z' AND instance =~ /^(?:192.168.*)$/`),
			wantErr: false,
		},
		{
			name: "5",
			fields: fields{
				Evaluation: &endTime,
				DataType:   LABEL_VALUES_DATA,
			},
			args: args{
				v: VectorSelector(`go_goroutines{instance=~"192.168.*"}`),
			},
			want:    influxql.MustParseStatement(`SHOW TAG VALUES FROM go_goroutines WITH KEY = "" WHERE time >= '2023-01-08T02:00:00Z' AND time <= '2023-01-08T02:00:00Z' AND instance =~ /^(?:192.168.*)$/`),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			t := &Transpiler{
				PromCommand: PromCommand{
					Start:      tt.fields.Start,
					End:        tt.fields.End,
					Step:       tt.fields.Step,
					Timezone:   tt.fields.Timezone,
					Evaluation: tt.fields.Evaluation,
					DataType:   tt.fields.DataType,
					Database:   tt.fields.Database,
					LabelName:  tt.fields.LabelName,
				},
			}
			t.minT, t.maxT = t.findMinMaxTime(t.newEvalStmt(tt.args.v))
			got, err := t.transpileInstantVectorSelector(tt.args.v)
			if (err != nil) != tt.wantErr {
				t1.Errorf("transpileInstantVectorSelector() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got.String(), tt.want.String()) {
				t1.Errorf("transpileInstantVectorSelector() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTranspiler_transpileRangeVectorSelector(t1 *testing.T) {
	type fields struct {
		Start      *time.Time
		End        *time.Time
		Step       time.Duration
		Timezone   *time.Location
		Evaluation *time.Time
	}
	type args struct {
		v *parser.MatrixSelector
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
				Start: &startTime2, End: &endTime2, Step: step,
			},
			args: args{
				v: MatrixSelector(`cpu{host=~"tele.*"}[5m]`),
			},
			want:    influxql.MustParseStatement(`SELECT value AS value FROM cpu WHERE time >= '2023-01-06T03:55:00Z' AND time <= '2023-01-06T07:00:00Z' AND host =~ /^(?:tele.*)$/ GROUP BY *`),
			wantErr: false,
		},
		{
			name: "2",
			fields: fields{
				Evaluation: &endTime2,
			},
			args: args{
				v: MatrixSelector(`cpu{host=~"tele.*"}[5m]`),
			},
			want:    influxql.MustParseStatement(`SELECT value AS value FROM cpu WHERE time >= '2023-01-06T06:55:00Z' AND time <= '2023-01-06T07:00:00Z' AND host =~ /^(?:tele.*)$/ GROUP BY *`),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			t := &Transpiler{
				PromCommand: PromCommand{
					Start:      tt.fields.Start,
					End:        tt.fields.End,
					Step:       tt.fields.Step,
					Timezone:   tt.fields.Timezone,
					Evaluation: tt.fields.Evaluation,
				},
			}
			t.rewriteMinMaxTime()
			got, err := t.transpileRangeVectorSelector(tt.args.v)
			if (err != nil) != tt.wantErr {
				t1.Errorf("transpileRangeVectorSelector() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got.String(), tt.want.String()) {
				t1.Errorf("transpileRangeVectorSelector() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTranspiler_transpileMetaQuery(t1 *testing.T) {
	type fields struct {
		Start      *time.Time
		End        *time.Time
		Timezone   *time.Location
		Evaluation *time.Time
		DataType   DataType
		Database   string
		LabelName  string
	}
	tests := []struct {
		name     string
		dataType DataType
		fields   fields
		selector *parser.VectorSelector
		matchers []*labels.Matcher
		want     influxql.Node
		wantErr  bool
	}{
		{
			name: "show tag keys",
			fields: fields{
				DataType: LABEL_KEYS_DATA,
				Database: "prometheus",
			},
			want:    influxql.MustParseStatement(`SHOW TAG KEYS ON prometheus`),
			wantErr: false,
		},
		{
			name: "show tag keys with matchers",
			fields: fields{
				DataType: LABEL_KEYS_DATA,
				Database: "prometheus",
			},
			matchers: []*labels.Matcher{
				{Name: DefaultMetricKeyLabel, Value: "up1"},
				{Name: DefaultMetricKeyLabel, Value: "up2"},
			},
			want:    influxql.MustParseStatement(`SHOW TAG KEYS ON prometheus FROM up1, up2`),
			wantErr: false,
		},
		{
			name: "show tag values",
			fields: fields{
				DataType:  LABEL_VALUES_DATA,
				Database:  "prometheus",
				LabelName: "job",
				Start:     &startTime2,
				End:       &endTime2,
			},
			want:    influxql.MustParseStatement(`SHOW TAG VALUES ON prometheus WITH KEY = job WHERE time >= '2023-01-06T04:00:00Z' AND time <= '2023-01-06T07:00:00Z'`),
			wantErr: false,
		},
		{
			name: "show tag values with matchers",
			fields: fields{
				DataType:  LABEL_VALUES_DATA,
				Database:  "prometheus",
				LabelName: "job",
				Start:     &startTime2,
				End:       &endTime2,
			},
			matchers: []*labels.Matcher{
				{Name: DefaultMetricKeyLabel, Value: "up1"},
				{Name: DefaultMetricKeyLabel, Value: "up2"},
			},
			want:    influxql.MustParseStatement(`SHOW TAG VALUES ON prometheus FROM up1,up2 WITH key = job WHERE time >= '2023-01-06T04:00:00Z' AND time <= '2023-01-06T07:00:00Z'`),
			wantErr: false,
		},
		{
			name: "show series",
			fields: fields{
				DataType: SERIES_DATA,
				Database: "prometheus",
			},
			want:    influxql.MustParseStatement(`SHOW SERIES ON prometheus`),
			wantErr: false,
		},
		{
			name: "show series with matchers",
			fields: fields{
				DataType: SERIES_DATA,
				Database: "prometheus",
			},
			matchers: []*labels.Matcher{
				{Name: DefaultMetricKeyLabel, Value: "up1"},
				{Name: DefaultMetricKeyLabel, Value: "up2"},
			},
			want:    influxql.MustParseStatement(`SHOW SERIES ON prometheus FROM up1,up2`),
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
					DataType:   tt.fields.DataType,
					Database:   tt.fields.Database,
					LabelName:  tt.fields.LabelName,
				},
			}
			selector := &parser.VectorSelector{
				LabelMatchers: tt.matchers,
			}
			got, err := t.transpileInstantVectorSelector(selector)
			if (err != nil) != tt.wantErr {
				t1.Errorf("transpileInstantVectorSelector() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got.String(), tt.want.String()) {
				t1.Errorf("%s: transpileInstantVectorSelector() got = %v, want %v", tt.name, got, tt.want)
			}
		})
	}
}
