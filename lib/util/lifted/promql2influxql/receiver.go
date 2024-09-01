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
	"github.com/openGemini/openGemini/lib/util/lifted/promtheus/pkg/labels"
	"github.com/openGemini/openGemini/lib/util/lifted/promtheus/promql"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/prometheus/promql/parser"
)

const (
	SampleTimeColIdx  = 0
	SampleValueColIdx = 1
	SampleColNum      = 2
)

// Receiver is a query engine to run models.PromCommand.
// It contains a reference to QueryCommandRunnerFactory instance for putting itself back to the factory from which it was born after work.
type Receiver struct {
	PromCommand

	DropMetric      bool
	RemoveTableName bool
	DuplicateResult bool
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
	} else {
		metric = FromMapWithoutMetric(table.Tags)
	}
	if len(table.Columns) <= SampleColNum {
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
	} else {
		seriesMap := make(map[uint64]*promql.Series)
		for _, row := range table.Values {
			m := metric
			for i := SampleColNum; i < len(table.Columns); i++ {
				m = append(m, labels.Label{Name: table.Columns[i], Value: row[i].(string)})
			}
			point, err := Row2Point(row)
			if err != nil {
				return err
			}
			sort.Sort(m)
			if series, exists := seriesMap[m.Hash()]; exists {
				series.Points = append(series.Points, point)
			} else {
				seriesMap[m.Hash()] = &promql.Series{
					Metric: m,
					Points: []promql.Point{
						point,
					},
				}
			}
		}
		for _, series := range seriesMap {
			*promSeries = append(*promSeries, series)
		}
	}
	if !r.DuplicateResult || r.PromCommand.Step == 0 || len((*promSeries)[len(*promSeries)-1].Points) > 1 {
		return nil
	}
	// For every evaluation while the value remains same, the timestamp for that
	// value would change for different eval times. Hence we duplicate the result
	// with changed timestamps.
	start, end, interval := r.Start.UnixMilli(), r.End.UnixMilli(), r.Step.Milliseconds()
	series := (*promSeries)[len(*promSeries)-1]
	idx := len(series.Points)
	ReservePoints(series, int((end-start-interval)/interval)+1)
	for ts := start + interval; ts <= end; ts += interval {
		series.Points[idx] = promql.Point{T: ts, V: series.Points[0].V}
		idx++
	}
	return nil
}

// PopulatePromSeriesByHash is used to populate *promql.Series slice from models.Row returned from InfluxDB
// when raw result has not grouped by series(measurement + tag key/value pairs).
// Iterate the whole result table to collect all series into seriesMap. The map key is hash of label set, the map value is
// a pointer to promql.Series. Each series may contain one or more points.
func (r *Receiver) PopulatePromSeriesByHash(promSeries *[]*promql.Series, table *models.Row) error {
	seriesMap := make(map[uint64]*promql.Series)
	for _, row := range table.Values {
		kvs := make(map[string]string)
		for i, col := range row {
			if i == SampleTimeColIdx || i == SampleValueColIdx {
				continue
			}
			kvs[table.Columns[i]] = col.(string)
		}
		var metric labels.Labels
		if !r.DropMetric {
			metric = labels.FromMap(kvs)
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
	if !r.DuplicateResult || r.PromCommand.Step == 0 || len((*promSeries)[len(*promSeries)-1].Points) > 1 {
		return nil
	}
	// For every evaluation while the value remains same, the timestamp for that
	// value would change for different eval times. Hence we duplicate the result
	// with changed timestamps.
	start, end, interval := r.Start.UnixMilli(), r.End.UnixMilli(), r.Step.Milliseconds()
	for _, series := range *promSeries {
		idx := len(series.Points)
		ReservePoints(series, int((end-start-interval)/interval)+1)
		for ts := start + interval; ts <= end; ts += interval {
			series.Points[idx] = promql.Point{T: ts, V: series.Points[0].V}
			idx++
		}
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
			if err := r.PopulatePromSeriesByHash(&promSeries, item); err != nil {
				return NewPromResult(nil, ""), errno.NewError(errno.ErrGroupResultBySeries, err.Error())
			}
		}
	}

	switch expr.Type() {
	case parser.ValueTypeMatrix:
		return NewPromResult(HandleValueTypeMatrix(promSeries), string(parser.ValueTypeMatrix)), nil
	case parser.ValueTypeVector:
		switch cmd.DataType {
		case GRAPH_DATA:
			return NewPromResult(HandleValueTypeMatrix(promSeries), string(parser.ValueTypeMatrix)), nil
		default:
			value, err := r.handleValueTypeVector(promSeries)
			return NewPromResult(value, string(parser.ValueTypeVector)), err
		}
	case parser.ValueTypeScalar:
		switch cmd.DataType {
		case GRAPH_DATA:
			return NewPromResult(HandleValueTypeMatrix(promSeries), string(parser.ValueTypeMatrix)), nil
		default:
			value, err := r.handleValueTypeScalar(promSeries)
			return NewPromResult(value, string(parser.ValueTypeScalar)), err
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

func HandleValueTypeMatrix(promSeries []*promql.Series) promql.Matrix {
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

func (r *Receiver) handleValueTypeScalar(promSeries []*promql.Series) (promql.Scalar, error) {
	scalar := promql.Scalar{}
	if len(promSeries) > 0 && len(promSeries[0].Points) > 0 {
		point := promSeries[0].Points[0]
		scalar.T = point.T
		scalar.V = point.V
	}
	return scalar, nil
}

func Row2Point(row []interface{}) (promql.Point, error) {
	ts, ok := row[0].(time.Time)
	if !ok {
		return promql.Point{}, errno.NewError(errno.ParseTimeFail)
	}
	point := promql.Point{
		T: timestamp.FromTime(ts),
	}
	switch number := row[1].(type) {
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

func ReservePoints(series *promql.Series, size int) {
	sCap := cap(series.Points)
	if sCap == 0 {
		series.Points = make([]promql.Point, size)
		return
	}
	sLen := len(series.Points)
	remain := sCap - sLen
	if delta := size - remain; delta > 0 {
		series.Points = append(series.Points[:sCap], make([]promql.Point, delta)...)
	}
	series.Points = series.Points[:sLen+size]
	return
}
