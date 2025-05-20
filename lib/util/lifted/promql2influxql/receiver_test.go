package promql2influxql

import (
	"testing"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestContains(t *testing.T) {
	label := labels.Label{
		Name:  "__name__",
		Value: "up",
	}

	var labs []labels.Label
	labs = append(labs, label)

	require.False(t, contains(labs, "test"))
	require.True(t, contains(labs, "__name__"))
}

func TestReceiver_InfluxResultToPromQLValue(t *testing.T) {
	type fields struct {
		PromCommand     PromCommand
		DropMetric      bool
		DuplicateResult bool
	}
	type args struct {
		result *query.Result
		expr   parser.Expr
		cmd    PromCommand
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *PromData
	}{
		{
			name:   "1",
			fields: fields{PromCommand: PromCommand{}},
			args: args{
				result: &query.Result{Series: []*models.Row{
					{Name: "metric",
						Columns: []string{"time", "value", "tagKey"},
						Values:  [][]interface{}{{time.Time{}, float64(1), "tagValue1"}, {time.Time{}, float64(1), nil}}},
				}},
				expr: VectorSelector("metric"),
			},
			want: NewPromData(nil, "vector"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Receiver{
				PromCommand:     tt.fields.PromCommand,
				DropMetric:      tt.fields.DropMetric,
				DuplicateResult: tt.fields.DuplicateResult,
			}
			got, err := r.InfluxResultToPromQLValue(tt.args.result, tt.args.expr, tt.args.cmd)
			assert.NoError(t, err)
			assert.Equal(t, tt.want.ResultType, got.ResultType)
		})
	}
}
