package promql2influxql

import (
	"regexp"
	"time"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/prometheus/promql/parser"
)

var reservedTags = map[string]struct{}{
	DefaultMetricKeyLabel: {},
}

func durationMilliseconds(d time.Duration) int64 {
	return int64(d / (time.Millisecond / time.Nanosecond))
}

func GetTimeCondition(start, end *time.Time) influxql.Expr {
	var timeLhs, timeRhs *influxql.BinaryExpr
	if start != nil {
		timeLhs = &influxql.BinaryExpr{
			Op: influxql.GTE,
			LHS: &influxql.VarRef{
				Val: TimeField,
			},
			RHS: &influxql.TimeLiteral{
				Val: *start,
			},
		}
	}
	if end != nil {
		timeRhs = &influxql.BinaryExpr{
			Op: influxql.LTE,
			LHS: &influxql.VarRef{
				Val: TimeField,
			},
			RHS: &influxql.TimeLiteral{
				Val: *end,
			},
		}
	}
	if timeLhs == nil {
		return timeRhs
	}
	if timeRhs == nil {
		return timeLhs
	}
	timeCond := &influxql.BinaryExpr{
		Op:  influxql.AND,
		LHS: timeLhs,
		RHS: timeRhs,
	}
	return timeCond
}

func GetTagCondition(v *parser.VectorSelector, haveMetricStore bool) (influxql.Expr, error) {
	var tagCond influxql.Expr
	for _, item := range v.LabelMatchers {
		if _, ok := reservedTags[item.Name]; ok && !haveMetricStore {
			continue
		}
		if len(item.Value) == 0 {
			continue
		}
		var cond *influxql.BinaryExpr
		switch item.Type {
		case labels.MatchEqual:
			cond = &influxql.BinaryExpr{
				Op: influxql.EQ,
				LHS: &influxql.VarRef{
					Val: item.Name,
				},
				RHS: &influxql.StringLiteral{
					Val: item.Value,
				},
			}
		case labels.MatchNotEqual:
			cond = &influxql.BinaryExpr{
				Op: influxql.NEQ,
				LHS: &influxql.VarRef{
					Val: item.Name,
				},
				RHS: &influxql.StringLiteral{
					Val: item.Value,
				},
			}
		case labels.MatchRegexp, labels.MatchNotRegexp:
			// TODO: to support the FastRegexMatcher
			promRegexStr := "^(?:" + item.Value + ")$"
			re, err := regexp.Compile(promRegexStr)
			if err != nil {
				return nil, errno.NewError(errno.ErrRegularExpSyntax, err.Error())
			}
			cond = &influxql.BinaryExpr{
				Op: influxql.EQREGEX,
				LHS: &influxql.VarRef{
					Val: item.Name,
				},
				RHS: &influxql.RegexLiteral{
					Val: re,
				},
			}
			if item.Type == labels.MatchNotRegexp {
				cond.Op = influxql.NEQREGEX
			}
		default:
			return nil, errno.NewError(errno.UnsupportedMatchType, item.Type.String())
		}
		tagCond = CombineConditionAnd(tagCond, cond)
	}
	return tagCond, nil
}

// findStartEndTime return start and end time.
// End time is calculated as below priority order from highest to lowest:
//   - ```Timestamp``` attribute of PromQL VectorSelector v
//   - ```End``` attribute of Transpiler t
//   - ```Evaluation``` attribute of Transpiler t
//   - time.Now()
//     yielded result from above calculation will be calculated with v's OriginalOffset attribute at last.
//
// Start time is calculated as below priority order from highest to lowest:
//   - ```Start``` attribute of Transpiler t
//   - End time subtracts time range of PromQL MatrixSelector
func (t *Transpiler) findStartEndTime(v *parser.VectorSelector) (startTime, endTime *time.Time) {
	start, end := int64(-1), time.Now().UnixMilli()
	if t.Evaluation != nil {
		start, end = timestamp.FromTime(*t.Evaluation), timestamp.FromTime(*t.Evaluation)
	}
	if t.Start != nil {
		start = timestamp.FromTime(*t.Start)
	}
	if t.End != nil {
		end = timestamp.FromTime(*t.End)
	}

	// TODO: to support the subquery
	if start >= 0 && v.StartOrEnd == parser.START {
		v.Timestamp = makeInt64Pointer(start)
	}
	if end >= 0 && v.StartOrEnd == parser.END {
		v.Timestamp = makeInt64Pointer(end)
	}
	if v.Timestamp != nil {
		// The timestamp on the selector overrides everything.
		start = *v.Timestamp
		end = *v.Timestamp
	}

	// change the query end time to the time of the last sample.
	if t.Step > 0 && start >= 0 {
		step := durationMilliseconds(t.Step)
		end = start + (end-start)/step*step
	}
	if t.timeRange == 0 {
		start = start - durationMilliseconds(t.LookBackDelta)
	} else {
		// For all matrix queries we want to ensure that we have (end-start) + range selected
		// this way we have `range` data before the start time
		if start == -1 {
			start = end - durationMilliseconds(t.timeRange)
		} else {
			start = start - durationMilliseconds(t.timeRange)
		}
	}

	offsetMilliseconds := durationMilliseconds(v.OriginalOffset)
	start = start - offsetMilliseconds
	end = end - offsetMilliseconds
	if start >= 0 {
		startTs := timestamp.Time(start)
		startTime = &startTs
	}
	if end >= 0 {
		endTs := timestamp.Time(end)
		endTime = &endTs
	}
	return
}

// transpileVectorSelector2ConditionExpr transpiles PromQL VectorSelector to time condition and tag condition separately.
// The time condition will be applied at the most outer expression for improving performance.
// Refer to https://docs.influxdata.com/influxdb/v1.8/query_language/explore-data/#improve-performance-of-time-bound-subqueries
func (t *Transpiler) transpileVectorSelector2ConditionExpr(v *parser.VectorSelector) (influxql.Expr, influxql.Expr, error) {
	var timeCondition influxql.Expr
	if IsMetaQuery(t.DataType) {
		start, end := t.findStartEndTime(v)
		timeCondition = GetTimeCondition(start, end)
	} else {
		if t.timeRange == 0 {
			start, end := timestamp.Time(t.minT-t.LookBackDelta.Milliseconds()-durationMilliseconds(v.Offset)), timestamp.Time(t.maxT-durationMilliseconds(v.Offset))
			timeCondition = GetTimeCondition(&start, &end)
		} else {
			start, end := timestamp.Time(t.minT-t.timeRange.Milliseconds()-durationMilliseconds(v.Offset)), timestamp.Time(t.maxT-durationMilliseconds(v.Offset))
			timeCondition = GetTimeCondition(&start, &end)
		}
	}
	// if the API corresponding to MetricStore is used, the __name__ field is used as the condition, otherwise, drop it.
	tagCondition, err := GetTagCondition(v, t.HaveMetricStore())
	return timeCondition, tagCondition, err
}

// transpileInstantVectorSelector transpiles PromQL VectorSelector to InfluxQL statement
func (t *Transpiler) transpileInstantVectorSelector(v *parser.VectorSelector) (influxql.Node, error) {
	var (
		err          error
		tagCondition influxql.Expr
	)
	t.timeCondition, tagCondition, err = t.transpileVectorSelector2ConditionExpr(v)
	if err != nil {
		return nil, errno.NewError(errno.TranspileIVSFail, err.Error())
	}
	condition := CombineConditionAnd(t.timeCondition, tagCondition)
	switch t.DataType {
	case LABEL_KEYS_DATA:
		showTagKeysStatement := influxql.ShowTagKeysStatement{
			Database: t.Database,
			// TODO support time condition
			Condition: tagCondition,
		}
		if len(v.LabelMatchers) > 0 {
			showTagKeysStatement.Sources = make([]influxql.Source, 0, len(v.LabelMatchers))
		}
		if t.HaveMetricStore() {
			showTagKeysStatement.Sources = append(showTagKeysStatement.Sources, &influxql.Measurement{Name: t.Measurement})
			return &showTagKeysStatement, nil
		}
		for _, matcher := range v.LabelMatchers {
			if _, ok := reservedTags[matcher.Name]; ok {
				showTagKeysStatement.Sources = append(showTagKeysStatement.Sources, &influxql.Measurement{Name: matcher.Value})
			}
		}
		return &showTagKeysStatement, nil
	case LABEL_VALUES_DATA:
		showTagValuesStatement := influxql.ShowTagValuesStatement{
			Database:   t.Database,
			Op:         influxql.EQ,
			TagKeyExpr: &influxql.StringLiteral{Val: t.LabelName},
			// TODO support time condition
			Condition: condition,
		}
		if t.HaveMetricStore() {
			showTagValuesStatement.Sources = append(showTagValuesStatement.Sources, &influxql.Measurement{Name: t.Measurement})
			return &showTagValuesStatement, nil
		}
		if len(v.LabelMatchers) > 0 {
			showTagValuesStatement.Sources = make([]influxql.Source, 0, len(v.LabelMatchers))
		}
		for _, matcher := range v.LabelMatchers {
			if _, ok := reservedTags[matcher.Name]; ok {
				showTagValuesStatement.Sources = append(showTagValuesStatement.Sources, &influxql.Measurement{Name: matcher.Value})
			}
		}
		return &showTagValuesStatement, nil
	case SERIES_DATA:
		showSeriesStatement := influxql.ShowSeriesStatement{
			Database: t.Database,
			// TODO support time condition
			Condition: tagCondition,
		}
		if t.HaveMetricStore() {
			showSeriesStatement.Sources = append(showSeriesStatement.Sources, &influxql.Measurement{Name: t.Measurement})
			return &showSeriesStatement, nil
		}
		if len(v.LabelMatchers) > 0 {
			showSeriesStatement.Sources = make([]influxql.Source, 0, len(v.LabelMatchers))
		}
		for _, matcher := range v.LabelMatchers {
			showSeriesStatement.Sources = append(showSeriesStatement.Sources, &influxql.Measurement{Name: matcher.Value})
		}
		return &showSeriesStatement, nil
	default:
	}
	// metricName is used as the measurement by default.
	selectStatement := &influxql.SelectStatement{
		Sources:     []influxql.Source{&influxql.Measurement{Name: v.Name}},
		Condition:   condition,
		Dimensions:  []*influxql.Dimension{{Expr: &influxql.Wildcard{}}},
		IsPromQuery: true,
	}
	// if the API corresponding to MetricStore is used, MetricStore in the API is used as the measurement.
	if t.HaveMetricStore() {
		selectStatement.Sources = []influxql.Source{&influxql.Measurement{Name: t.Measurement}}
	}
	valueFieldKey := DefaultFieldKey
	if len(t.ValueFieldKey) == 0 {
		t.ValueFieldKey = valueFieldKey
	}

	selectStatement.Fields = append(selectStatement.Fields, &influxql.Field{
		Expr:  &influxql.VarRef{Val: valueFieldKey, Alias: DefaultFieldKey},
		Alias: DefaultFieldKey,
	})

	// set parameters required for promql query, such as Step, Range and LookBackDelta.
	if t.Step > 0 {
		selectStatement.Step = t.Step
	}
	if t.timeRange > 0 {
		selectStatement.Range = t.timeRange
		// reset the range duration after using it.
		t.timeRange = 0
	}
	selectStatement.LookBackDelta = t.LookBackDelta
	selectStatement.QueryOffset = v.Offset
	return selectStatement, nil
}

// transpileRangeVectorSelector transpiles PromQL MatrixSelector to InfluxQL SelectStatement
func (t *Transpiler) transpileRangeVectorSelector(v *parser.MatrixSelector) (influxql.Node, error) {
	if v.Range > 0 {
		t.timeRange = v.Range
	}
	return t.transpileExpr(v.VectorSelector)
}

func (t *Transpiler) HaveMetricStore() bool {
	return len(t.Measurement) > 0
}
