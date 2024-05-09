package promql2influxql

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
)

// Receiver is a query engine to run models.PromCommand.
// It contains a reference to QueryCommandRunnerFactory instance for putting itself back to the factory from which it was born after work.
type Receiver struct {
	DropMetric      bool
	RemoveTableName bool
}

// InfluxLiteralToPromQLValue converts influxql.Literal expression to parser.Value of Prometheus
func (r *Receiver) InfluxLiteralToPromQLValue(result influxql.Literal, cmd PromCommand) (value parser.Value, resultType string) {
	now := time.Now()
	if cmd.Evaluation != nil {
		now = *cmd.Evaluation
	} else if cmd.End != nil {
		now = *cmd.End
	}
	switch lit := result.(type) {
	case *influxql.NumberLiteral:
		return promql.Scalar{
			T: timestamp.FromTime(now),
			V: lit.Val,
		}, string(parser.ValueTypeScalar)
	case *influxql.IntegerLiteral:
		return promql.Scalar{
			T: timestamp.FromTime(now),
			V: float64(lit.Val),
		}, string(parser.ValueTypeScalar)
	default:
		return promql.String{
			T: timestamp.FromTime(now),
			V: lit.String(),
		}, string(parser.ValueTypeString)
	}
}

// populatePromSeriesByTag populates *promql.Series slice from models.Row returned by InfluxDB
func (r *Receiver) populatePromSeriesByTag(promSeries *[]*promql.Series, table *models.Row) error {
	var metric labels.Labels
	if !r.DropMetric {
		metric = labels.FromMap(table.Tags)
		if !r.RemoveTableName {
			metric = append(metric, labels.FromStrings(DefaultMetricKeyLabel, table.Name)...)
		}
	} else {
		metric = FromMapWithoutMetric(table.Tags)
	}
	var points []promql.Point
	for _, row := range table.Values {
		point, err := Row2Point(row)
		if err != nil {
			return err
		}
		points = append(points, point)
	}
	*promSeries = append(*promSeries, &promql.Series{
		Metric: metric,
		Points: points,
	})
	return nil
}

// populatePromSeriesByHash is used to populate *promql.Series slice from models.Row returned from InfluxDB
// when raw result has not grouped by series(measurement + tag key/value pairs).
// Iterate the whole result table to collect all series into seriesMap. The map key is hash of label set, the map value is
// a pointer to promql.Series. Each series may contain one or more points.
func (r *Receiver) populatePromSeriesByHash(promSeries *[]*promql.Series, table *models.Row) error {
	seriesMap := make(map[uint64]*promql.Series)
	for _, row := range table.Values {
		kvs := make(map[string]string)
		for i, col := range row {
			if i == 0 || i == len(row)-1 {
				continue
			}
			kvs[table.Columns[i]] = col.(string)
		}
		var metric labels.Labels
		if !r.DropMetric {
			metric = labels.FromMap(kvs)
			metric = append(metric, labels.FromStrings(DefaultMetricKeyLabel, table.Name)...)
		} else {
			metric = FromMapWithoutMetric(kvs)
		}
		point, err := Row2Point(row)
		if err != nil {
			return err
		}
		if series, exists := seriesMap[metric.Hash()]; exists {
			series.Points = append(series.Points, point)
		} else {
			seriesMap[metric.Hash()] = &promql.Series{
				Metric: metric,
				Points: []promql.Point{
					point,
				},
			}
		}
	}
	for _, series := range seriesMap {
		*promSeries = append(*promSeries, series)
	}
	return nil
}

// InfluxResultToPromQLValue converts query.Result slice to parser.Value of Prometheus
func (r *Receiver) InfluxResultToPromQLValue(result *query.Result, expr parser.Expr, cmd PromCommand) (*PromResult, error) {
	if result == nil {
		return NewPromResult(nil, ""), nil
	}
	if result.Err != nil {
		return NewPromResult(nil, ""), result.Err
	}
	var promSeries []*promql.Series
	for _, item := range result.Series {
		if len(item.Tags) > 0 {
			if err := r.populatePromSeriesByTag(&promSeries, item); err != nil {
				return NewPromResult(nil, ""), errno.NewError(errno.ErrPopulatePromSeries, err.Error())
			}
		} else {
			if err := r.populatePromSeriesByHash(&promSeries, item); err != nil {
				return NewPromResult(nil, ""), errno.NewError(errno.ErrGroupResultBySeries, err.Error())
			}
		}
	}
	switch expr.Type() {
	case parser.ValueTypeMatrix:
		return NewPromResult(r.handleValueTypeMatrix(promSeries), string(parser.ValueTypeMatrix)), nil
	case parser.ValueTypeVector:
		switch cmd.DataType {
		case GRAPH_DATA:
			return NewPromResult(r.handleValueTypeMatrix(promSeries), string(parser.ValueTypeMatrix)), nil
		default:
			value, err := r.handleValueTypeVector(promSeries)
			return NewPromResult(value, string(parser.ValueTypeVector)), err
		}
	default:
		return NewPromResult(nil, ""), errno.NewError(errno.UnsupportedValueType, expr.Type())
	}
}

// InfluxResultToStringSlice converts query.Result slice to string slice
func (r *Receiver) InfluxResultToStringSlice(result *query.Result, dest *[]string) error {
	if result == nil {
		return nil
	}
	if result.Err != nil {
		return result.Err
	}
	if len(result.Series) == 0 {
		return nil
	}
	tagValueMap := make(map[string]struct{})
	for _, item := range result.Series {
		for _, item1 := range item.Values {
			if len(item1) <= 1 {
				continue
			}
			tagValue := item1[1].(string)
			if _, exists := tagValueMap[tagValue]; exists {
				continue
			} else {
				tagValueMap[tagValue] = struct{}{}
			}
			*dest = append(*dest, tagValue)
		}
	}
	return nil
}

func (r *Receiver) InfluxTagsToPromLabels(result *query.Result) ([]string, error) {
	if result == nil {
		return []string{}, nil
	}
	if result.Err != nil {
		return []string{}, result.Err
	}

	promLabelsMap := make(map[string]struct{})
	promLabels := make([]string, 0)

	for _, item := range result.Series {
		for _, item1 := range item.Values {
			tagValue, ok := item1[0].(string)
			if !ok {
				return promLabels, fmt.Errorf("label is not a string value")
			}
			if _, exists := promLabelsMap[tagValue]; exists {
				continue
			} else {
				promLabelsMap[tagValue] = struct{}{}
			}
			promLabels = append(promLabels, tagValue)
		}
	}

	return promLabels, nil
}

func (r *Receiver) handleValueTypeMatrix(promSeries []*promql.Series) promql.Matrix {
	matrix := make(promql.Matrix, 0, len(promSeries))
	for _, ser := range promSeries {
		matrix = append(matrix, *ser)
	}
	sort.Sort(matrix)
	return matrix
}

func (r *Receiver) handleValueTypeVector(promSeries []*promql.Series) (promql.Vector, error) {
	vector := make(promql.Vector, 0, len(promSeries))
	for _, ser := range promSeries {
		if len(ser.Points) != 1 {
			return nil, errno.NewError(errno.InvalidOutputPoint)
		}
		vector = append(vector, promql.Sample{
			Metric: ser.Metric,
			Point:  ser.Points[0],
		})
	}
	return vector, nil
}

func Row2Point(row []interface{}) (promql.Point, error) {
	ts, ok := row[0].(time.Time)
	if !ok {
		return promql.Point{}, errno.NewError(errno.ParseTimeFail)
	}
	point := promql.Point{
		T: timestamp.FromTime(ts),
	}
	switch number := row[len(row)-1].(type) {
	case json.Number:
		if v, err := number.Float64(); err == nil {
			point.V = v
		} else {
			if v, err := number.Int64(); err == nil {
				point.V = float64(v)
			}
		}
	case float64:
		point.V = number
	case int64:
		point.V = float64(number)
	default:
		return promql.Point{}, fmt.Errorf("invalid the data type for prom response")
	}
	return point, nil
}

func FromMapWithoutMetric(m map[string]string) labels.Labels {
	l := make([]labels.Label, 0, len(m))
	for k, v := range m {
		if k == DefaultMetricKeyLabel {
			continue
		}
		l = append(l, labels.Label{Name: k, Value: v})
	}
	return labels.New(l...)
}
