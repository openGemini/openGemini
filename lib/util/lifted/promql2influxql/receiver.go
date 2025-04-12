package promql2influxql

import (
	"encoding/json"
	"fmt"
	"math"
	"runtime/debug"
	"sort"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"go.uber.org/zap"
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
func (r *Receiver) populatePromSeriesByTag(pPreMetric *labels.Labels, promSeries *[]*promql.Series, table *models.Row) error {
	var metric labels.Labels
	if !r.DropMetric {
		metric = labels.FromMap(table.Tags)
	} else {
		metric = FromMapWithoutMetric(table.Tags)
	}
	if *pPreMetric != nil && labels.Equal(metric, *pPreMetric) {
		logger.GetLogger().Error(fmt.Sprintf("vector cannot contain metrics with the same labelset: metric=%v", metric.String()))
		return fmt.Errorf("vector cannot contain metrics with the same labelset")
	}
	*pPreMetric = metric

	if len(table.Columns) <= SampleColNum {
		var points []promql.FPoint
		preT := int64(math.MinInt64)
		for _, row := range table.Values {
			point, err := Row2Point(row)
			if err != nil {
				return err
			}
			if preT >= point.T {
				logger.GetLogger().Error(fmt.Sprintf("vector cannot contain metrics with the same labelset: metric=%v point=%v preT=%v", metric.String(), point.String(), preT))
				return fmt.Errorf("vector cannot contain metrics with the same labelset")
			}
			preT = point.T
			points = append(points, point)
		}
		*promSeries = append(*promSeries, &promql.Series{
			Metric: metric,
			Floats: points,
		})
	} else {
		seriesMap := make(map[uint64]*promql.Series)
		metricContainsMap := metricContainsCol(table.Columns, metric)
		for _, row := range table.Values {
			m := metric
			for i := SampleColNum; i < len(table.Columns); i++ {
				if row[i] == nil || (r.DropMetric && DefaultMetricKeyLabel == table.Columns[i]) {
					continue
				}
				if !metricContainsMap[table.Columns[i]] {
					m = append(m, labels.Label{Name: table.Columns[i], Value: row[i].(string)})
				}
			}
			point, err := Row2Point(row)
			if err != nil {
				return err
			}
			sort.Sort(m)
			if series, exists := seriesMap[m.Hash()]; exists {
				if series.Floats[len(series.Floats)-1].T == point.T {
					logger.GetLogger().Error(fmt.Sprintf("vector cannot contain metrics with the same labelset: metric=%v point=%v", metric.String(), point.String()))
					return fmt.Errorf("vector cannot contain metrics with the same labelset")
				}
				series.Floats = append(series.Floats, point)
			} else {
				seriesMap[m.Hash()] = &promql.Series{
					Metric: m,
					Floats: []promql.FPoint{
						point,
					},
				}
			}
		}
		for _, series := range seriesMap {
			*promSeries = append(*promSeries, series)
		}
	}
	if !r.DuplicateResult || r.PromCommand.Step == 0 || len((*promSeries)[len(*promSeries)-1].Floats) > 1 {
		return nil
	}
	// For every evaluation while the value remains same, the timestamp for that
	// value would change for different eval times. Hence we duplicate the result
	// with changed timestamps.
	start, end, interval := r.Start.UnixMilli(), r.End.UnixMilli(), r.Step.Milliseconds()
	series := (*promSeries)[len(*promSeries)-1]
	idx := len(series.Floats)
	ReservePoints(series, int((end-start-interval)/interval)+1)
	for ts := start + interval; ts <= end; ts += interval {
		series.Floats[idx] = promql.FPoint{T: ts, F: series.Floats[0].F}
		idx++
	}
	return nil
}

func metricContainsCol(columns []string, metric labels.Labels) map[string]bool {
	var res map[string]bool = make(map[string]bool)
	for i := range columns {
		if contains(metric, columns[i]) {
			res[columns[i]] = true
		} else {
			res[columns[i]] = false
		}
	}
	return res
}

func contains(m labels.Labels, s string) bool {
	for i := range m {
		if m[i].Name == s {
			return true
		}
	}
	return false
}

// PopulatePromSeriesByHash is used to populate *promql.Series slice from models.Row returned from InfluxDB
// when raw result has not grouped by series(measurement + tag key/value pairs).
// Iterate the whole result table to collect all series into seriesMap. The map key is hash of label set, the map value is
// a pointer to promql.Series. Each series may contain one or more points.
func (r *Receiver) PopulatePromSeriesByHash(promSeries *[]*promql.Series, table *models.Row) error {
	seriesMap := make(map[uint64]*promql.Series)
	orderList := make([]uint64, 0)
	for _, row := range table.Values {
		kvs := make(map[string]string)
		for i, col := range row {
			if i <= SampleValueColIdx || col == nil {
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
			series.Floats = append(series.Floats, point)
		} else {
			sid := metric.Hash()
			orderList = append(orderList, sid)
			seriesMap[sid] = &promql.Series{
				Metric: metric,
				Floats: []promql.FPoint{
					point,
				},
			}
		}
	}
	for _, sid := range orderList {
		*promSeries = append(*promSeries, seriesMap[sid])
	}
	if !r.DuplicateResult || r.PromCommand.Step == 0 || (len((*promSeries)[len(*promSeries)-1].Floats)) > 1 {
		return nil
	}
	// For every evaluation while the value remains same, the timestamp for that
	// value would change for different eval times. Hence we duplicate the result
	// with changed timestamps.
	start, end, interval := r.Start.UnixMilli(), r.End.UnixMilli(), r.Step.Milliseconds()
	for _, series := range *promSeries {
		idx := len(series.Floats)
		ReservePoints(series, int((end-start-interval)/interval)+1)
		for ts := start + interval; ts <= end; ts += interval {
			series.Floats[idx] = promql.FPoint{T: ts, F: series.Floats[0].F}
			idx++
		}
	}
	return nil
}

// InfluxResultToPromQLValue converts query2.Result slice to parser.Value of Prometheus
func (r *Receiver) InfluxResultToPromQLValue(result *query.Result, expr parser.Expr, cmd PromCommand) (promRes *PromData, promErr error) {
	defer func() {
		if re := recover(); re != nil {
			promRes, promErr = NewPromData(nil, ""), errno.NewError(errno.PromReceiverErr)
			logger.GetLogger().Error(promErr.Error(), zap.String("InfluxResultToPromQLValue", string(debug.Stack())))
		}
	}()
	if result == nil {
		return NewPromData(nil, ""), nil
	}
	if result.Err != nil {
		return NewPromData(nil, ""), result.Err
	}
	var promSeries []*promql.Series
	var preMetric labels.Labels
	for _, item := range result.Series {
		if len(item.Tags) > 0 {
			if err := r.populatePromSeriesByTag(&preMetric, &promSeries, item); err != nil {
				return NewPromData(nil, ""), errno.NewError(errno.ErrPopulatePromSeries, err.Error())
			}
		} else {
			if err := r.PopulatePromSeriesByHash(&promSeries, item); err != nil {
				return NewPromData(nil, ""), errno.NewError(errno.ErrGroupResultBySeries, err.Error())
			}
		}
	}

	switch expr.Type() {
	case parser.ValueTypeMatrix:
		return NewPromData(HandleValueTypeMatrix(promSeries), string(parser.ValueTypeMatrix)), nil
	case parser.ValueTypeVector:
		switch cmd.DataType {
		case GRAPH_DATA:
			return NewPromData(HandleValueTypeMatrix(promSeries), string(parser.ValueTypeMatrix)), nil
		default:
			value, err := r.handleValueTypeVector(promSeries)
			return NewPromData(value, string(parser.ValueTypeVector)), err
		}
	case parser.ValueTypeScalar:
		switch cmd.DataType {
		case GRAPH_DATA:
			return NewPromData(HandleValueTypeMatrix(promSeries), string(parser.ValueTypeMatrix)), nil
		default:
			value, err := r.handleValueTypeScalar(promSeries)
			return NewPromData(value, string(parser.ValueTypeScalar)), err
		}
	default:
		return NewPromData(nil, ""), errno.NewError(errno.UnsupportedValueType, expr.Type())
	}
}

// InfluxResultToStringSlice converts query2.Result slice to string slice
func (r *Receiver) InfluxResultToStringSlice(result *query.Result, dest *[]string) (promErr error) {
	defer func() {
		if re := recover(); re != nil {
			promErr = errno.NewError(errno.PromReceiverErr)
			logger.GetLogger().Error(promErr.Error(), zap.String("InfluxResultToStringSlice", string(debug.Stack())))
		}
	}()
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
	for _, row := range result.Series {
		for _, item := range row.Values {
			if len(item) < 2 || item[1] == nil {
				continue
			}
			tagValue := item[1].(string)
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

func (r *Receiver) InfluxTagsToPromLabels(result *query.Result) (promRes []string, promErr error) {
	defer func() {
		if re := recover(); re != nil {
			promRes, promErr = []string{}, errno.NewError(errno.PromReceiverErr)
			logger.GetLogger().Error(promErr.Error(), zap.String("InfluxTagsToPromLabels", string(debug.Stack())))
		}
	}()
	if result == nil {
		return []string{}, nil
	}
	if result.Err != nil {
		return []string{}, result.Err
	}

	promLabelsMap := make(map[string]struct{})
	promLabels := make([]string, 0)
	for _, row := range result.Series {
		for _, item := range row.Values {
			if len(item) < 1 || item[0] == nil {
				continue
			}
			tagValue, ok := item[0].(string)
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

func (r *Receiver) InfluxRowsToPromMetaData(result *query.Result) (promRes map[string][]Metadata, promErr error) {
	defer func() {
		if re := recover(); re != nil {
			promRes, promErr = map[string][]Metadata{}, errno.NewError(errno.PromReceiverErr)
			logger.GetLogger().Error(promErr.Error(), zap.String("InfluxRowsToPromMetaData", string(debug.Stack())))
		}
	}()
	if result == nil {
		return map[string][]Metadata{}, nil
	}
	if result.Err != nil {
		return map[string][]Metadata{}, result.Err
	}

	res := map[string][]Metadata{}
	for _, series := range result.Series {
		metric, ok := series.Tags[PromMetric]
		if !ok {
			continue
		}
		metadata := make([]Metadata, len(series.Values))
		res[metric] = metadata
		for i, point := range series.Values {
			for j := range series.Columns {
				switch series.Columns[i] {
				case PromMetaDataType:
					metadata[i].Type = MetadataType2MetricType(point[j].(int64))
				case PromMetaDataHelp:
					if point[j] == nil {
						metadata[i].Help = ""
					} else {
						metadata[i].Help = point[j].(string)
					}
				case PromMetaDataUnit:
					if point[j] == nil {
						metadata[i].Unit = ""
					} else {
						metadata[i].Unit = point[j].(string)
					}
				default:
				}
			}
		}
	}
	return res, nil
}

func HandleValueTypeMatrix(promSeries []*promql.Series) PromDataResult {
	matrix := make(promql.Matrix, 0, len(promSeries))
	for _, ser := range promSeries {
		matrix = append(matrix, *ser)
	}
	sort.Sort(matrix)
	return &PromDataMatrix{&matrix}
}

func (r *Receiver) handleValueTypeVector(promSeries []*promql.Series) (PromDataResult, error) {
	vector := make(promql.Vector, 0, len(promSeries))
	for _, ser := range promSeries {
		if len(ser.Floats) != 1 {
			return nil, errno.NewError(errno.InvalidOutputPoint)
		}
		vector = append(vector, promql.Sample{
			Metric: ser.Metric,
			T:      ser.Floats[0].T,
			F:      ser.Floats[0].F,
		})
	}
	return &PromDataVector{&vector}, nil
}

func (r *Receiver) handleValueTypeScalar(promSeries []*promql.Series) (PromDataResult, error) {
	scalar := promql.Scalar{}
	if len(promSeries) > 0 && len(promSeries[0].Floats) > 0 {
		point := promSeries[0].Floats[0]
		scalar.T = point.T
		scalar.V = point.F
	}
	return &PromDataScalar{&scalar}, nil
}

func (r *Receiver) AbsentNoMstResult(expr parser.Expr) (*PromData, error) {
	if r.Evaluation != nil {
		metric := getAbsentLabelsFromExpr(expr)
		res := &promql.Vector{promql.Sample{T: r.Evaluation.UnixMilli(), F: 1, Metric: metric}}
		return NewPromData(&PromDataVector{res}, string(parser.ValueTypeVector)), nil
	}
	start, end, interval := r.Start.UnixMilli(), r.End.UnixMilli(), r.Step.Milliseconds()
	point := promql.FPoint{
		T: start,
		F: 1,
	}
	var points []promql.FPoint
	points = append(points, point)
	metric := getAbsentLabelsFromExpr(expr)
	series := &promql.Series{
		Metric: metric,
		Floats: points,
	}
	idx := len(series.Floats)
	ReservePoints(series, int((end-start-interval)/interval)+1)
	for ts := start + interval; ts <= end; ts += interval {
		series.Floats[idx] = promql.FPoint{T: ts, F: series.Floats[0].F}
		idx++
	}
	var seriesSlice promql.Matrix
	seriesSlice = append(seriesSlice, *series)
	if r.DataType == GRAPH_DATA {
		return NewPromData(&PromDataMatrix{&seriesSlice}, string(parser.ValueTypeMatrix)), nil
	} else {
		return NewPromData(&PromDataMatrix{&seriesSlice}, string(parser.ValueTypeVector)), nil
	}
}

func getAbsentLabelsFromExpr(expr parser.Expr) labels.Labels {
	b := labels.NewBuilder(labels.EmptyLabels())

	e, ok := expr.(*parser.Call)
	if !ok {
		return labels.EmptyLabels()
	}
	if len(e.Args) == 0 {
		return labels.EmptyLabels()
	}

	var lm []*labels.Matcher
	switch n := e.Args[0].(type) {
	case *parser.VectorSelector:
		lm = n.LabelMatchers
	case *parser.MatrixSelector:
		lm = n.VectorSelector.(*parser.VectorSelector).LabelMatchers
	default:
		return labels.EmptyLabels()
	}

	exist := make(map[string]bool, len(lm))
	for _, ma := range lm {
		if ma.Name == labels.MetricName {
			continue
		}
		if ma.Type == labels.MatchEqual && !exist[ma.Name] {
			b.Set(ma.Name, ma.Value)
			exist[ma.Name] = true
		} else {
			b.Del(ma.Name)
		}
	}

	return b.Labels()
}

func Row2Point(row []interface{}) (promql.FPoint, error) {
	ts, ok := row[0].(time.Time)
	if !ok {
		return promql.FPoint{}, errno.NewError(errno.ParseTimeFail)
	}
	point := promql.FPoint{
		T: timestamp.FromTime(ts),
	}
	switch number := row[1].(type) {
	case json.Number:
		if v, err := number.Float64(); err == nil {
			point.F = v
		} else {
			if v, err := number.Int64(); err == nil {
				point.F = float64(v)
			}
		}
	case float64:
		point.F = number
	case int64:
		point.F = float64(number)
	default:
		return promql.FPoint{}, fmt.Errorf("invalid the data type for prom response")
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
	sCap := cap(series.Floats)
	if sCap == 0 {
		series.Floats = make([]promql.FPoint, size)
		return
	}
	sLen := len(series.Floats)
	remain := sCap - sLen
	if delta := size - remain; delta > 0 {
		series.Floats = append(series.Floats[:sCap], make([]promql.FPoint, delta)...)
	}
	series.Floats = series.Floats[:sLen+size]
	return
}

type Metadata struct {
	Type model.MetricType `json:"type"`
	Help string           `json:"help"`
	Unit string           `json:"unit"`
}

func MetadataType2MetricType(dt int64) model.MetricType {
	return model.MetricType(prompb.MetricMetadata_MetricType_name[int32(dt)])
}
