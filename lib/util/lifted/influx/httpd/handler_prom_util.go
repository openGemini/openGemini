// Copyright Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package httpd

import (
	"errors"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
	"unsafe"

	prompb2 "github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
	"github.com/gorilla/mux"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/promql2influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"go.uber.org/zap"
)

const MaxPointsForSeries = 11000

type errorType string

const (
	errorNone          errorType = ""
	errorTimeout       errorType = "timeout"
	errorCanceled      errorType = "canceled"
	errorExec          errorType = "execution"
	errorBadData       errorType = "bad_data"
	errorInternal      errorType = "internal"
	errorUnavailable   errorType = "unavailable"
	errorNotFound      errorType = "not_found"
	errorNotAcceptable errorType = "not_acceptable"
	errorForbidden     errorType = "forbidden"
)

type status string

const (
	StatusSuccess status = "success"
	StatusError   status = "error"

	// Non-standard status code (originally introduced by nginx) for the case when a client closes
	// the connection while the server is still processing the request.
	StatusClientClosedConnection = 499
)

var (
	minTime = time.Unix(math.MinInt64/1000+62135596801, 0).UTC()
	maxTime = time.Unix(math.MaxInt64/1000-62135596801, 999999999).UTC()

	minTimeFormatted = minTime.Format(time.RFC3339Nano)
	maxTimeFormatted = maxTime.Format(time.RFC3339Nano)
)

func parseTimeParam(r *http.Request, paramName string, defaultValue time.Time) (time.Time, error) {
	val := r.FormValue(paramName)
	if val == "" {
		return defaultValue, nil
	}
	result, err := parseTime(val)
	if err != nil {
		return time.Time{}, fmt.Errorf("Invalid time value for '%s': %v", paramName, err)
	}
	return result, nil
}

func parseTime(s string) (time.Time, error) {
	if t, err := strconv.ParseFloat(s, 64); err == nil {
		s, ns := math.Modf(t)
		ns = math.Round(ns*1000) / 1000
		return time.Unix(int64(s), int64(ns*float64(time.Second))).UTC(), nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}

	// Stdlib's time parser can only handle 4 digit years. As a workaround until
	// that is fixed we want to at least support our own boundary times.
	// Context: https://github.com/prometheus/client_golang/issues/614
	// Upstream issue: https://github.com/golang/go/issues/20555
	switch s {
	case minTimeFormatted:
		return minTime, nil
	case maxTimeFormatted:
		return maxTime, nil
	}
	return time.Time{}, fmt.Errorf("cannot parse %q to a valid timestamp", s)
}

func parseDuration(s string) (time.Duration, error) {
	if d, err := strconv.ParseFloat(s, 64); err == nil {
		ts := d * float64(time.Second)
		if ts > float64(math.MaxInt64) || ts < float64(math.MinInt64) {
			return 0, fmt.Errorf("cannot parse %q to a valid duration. It overflows int64", s)
		}
		return time.Duration(ts), nil
	}
	if d, err := model.ParseDuration(s); err == nil {
		return time.Duration(d), nil
	}
	return 0, fmt.Errorf("cannot parse %q to a valid duration", s)
}

type apiError struct {
	typ errorType
	err error
}

func (e *apiError) Error() string {
	return fmt.Sprintf("%s: %s", e.typ, e.err)
}

func invalidParamError(w http.ResponseWriter, err error, parameter string) {
	respondError(w, &apiError{
		errorBadData, fmt.Errorf("invalid parameter %q: %w", parameter, err),
	}, nil)
}

func (h *Handler) getPromResult(w http.ResponseWriter, stmtID2Result map[int]*query.Result, expr parser.Expr, cmd promql2influxql.PromCommand, dropMetric, removeTableName, duplicateResult bool) (PromResponse, bool) {
	r := &promql2influxql.Receiver{PromCommand: cmd, DropMetric: dropMetric, RemoveTableName: removeTableName, DuplicateResult: duplicateResult}
	resp := PromResponse{Data: &promql2influxql.PromResult{}, Status: "success"}
	if len(stmtID2Result) > 0 {
		if stmtID2Result[0].Err != nil {
			resp.Data = getEmptyResponse(cmd)
		} else {
			data, err := r.InfluxResultToPromQLValue(stmtID2Result[0], expr, cmd)
			if err != nil {
				respondError(w, &apiError{errorBadData, err}, nil)
				return resp, false
			} else {
				resp.Data = data
			}
		}
	}
	return resp, true
}

func (h *Handler) getRangePromResultForEmptySeries(expr influxql.Expr, promCommand *promql2influxql.PromCommand, valuer *influxql.ValuerEval, timeValuer *PromTimeValuer) (*PromResponse, bool) {
	points := make([]promql.Point, 0)
	start, end, step := GetRangeTimeForEmptySeriesResult(promCommand)
	for s := start; s <= end; s += step {
		timeValuer.tmpTime = s
		value := valuer.Eval(expr)
		points = append(points, promql.Point{T: s, V: value.(float64)})
	}
	matrix := promql.Matrix{promql.Series{Metric: labels.Labels{}, Points: points}}
	resp := &PromResponse{Status: "success"}
	resp.Data = &promql2influxql.PromResult{Result: matrix, ResultType: string(parser.ValueTypeMatrix)}
	return resp, true
}

func (h *Handler) getInstantPromResultForEmptySeries(expr influxql.Expr, promCommand *promql2influxql.PromCommand, valuer *influxql.ValuerEval, timeValuer *PromTimeValuer, typ parser.ValueType) (*PromResponse, bool) {
	vector := make(promql.Vector, 0)
	time := GetTimeForEmptySeriesResult(promCommand)
	timeValuer.tmpTime = time
	value := valuer.Eval(expr)
	Add2EmptySeriesResult(time, value.(float64), &vector)
	resp := &PromResponse{Status: "success"}
	resp.Data = &promql2influxql.PromResult{Result: vector, ResultType: string(typ)}
	return resp, true
}

func Add2EmptySeriesResult(t int64, val float64, vector *promql.Vector) {
	point := promql.Sample{Point: promql.Point{T: t, V: val}, Metric: labels.Labels{}}
	*vector = append(*vector, point)
}

func GetRangeTimeForEmptySeriesResult(promComand *promql2influxql.PromCommand) (int64, int64, int64) {
	if promComand.Start != nil {
		if promComand.End != nil {
			return promComand.Start.UnixMilli(), promComand.End.UnixMilli(), promComand.Step.Milliseconds()
		} else {
			return promComand.Start.UnixMilli(), time.Now().UnixMilli(), promComand.Step.Milliseconds()
		}
	} else {
		if promComand.End != nil {
			return time.Now().UnixMilli(), promComand.End.UnixMilli(), promComand.Step.Microseconds()
		} else {
			return time.Now().UnixMilli(), time.Now().UnixMilli(), promComand.Step.Microseconds()
		}
	}
}

func GetTimeForEmptySeriesResult(promComand *promql2influxql.PromCommand) int64 {
	if promComand.Evaluation != nil {
		return promComand.Evaluation.UnixMilli()
	} else {
		return time.Now().UnixMilli()
	}
}

func getEmptyResponse(cmd promql2influxql.PromCommand) *promql2influxql.PromResult {
	if cmd.DataType == promql2influxql.GRAPH_DATA {
		return promql2influxql.NewPromResult([]interface{}{}, string(parser.ValueTypeMatrix))
	} else {
		return promql2influxql.NewPromResult([]interface{}{}, string(parser.ValueTypeVector))

	}
}

func respondError(w http.ResponseWriter, apiErr *apiError, data interface{}) {
	b, err := json2.Marshal(&PromResponse{
		Status:    StatusError,
		ErrorType: apiErr.typ,
		Error:     apiErr.err.Error(),
		Data:      data,
	})
	if err != nil {
		logger.GetLogger().Error("error marshaling json response", zap.String("err", err.Error()))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var code int
	switch apiErr.typ {
	case errorBadData:
		code = http.StatusBadRequest
	case errorExec:
		code = http.StatusUnprocessableEntity
	case errorCanceled:
		code = StatusClientClosedConnection
	case errorTimeout:
		code = http.StatusServiceUnavailable
	case errorInternal:
		code = http.StatusInternalServerError
	case errorNotFound:
		code = http.StatusNotFound
	case errorNotAcceptable:
		code = http.StatusNotAcceptable
	case errorForbidden:
		code = http.StatusForbidden
	default:
		code = http.StatusInternalServerError
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if n, err := w.Write(b); err != nil {
		logger.GetLogger().Error("error writing response", zap.Int("bytesWritten", n), zap.String("err", err.Error()))
	}
}

// Response represents a list of statement results.
type PromResponse struct {
	Status    status      `json:"status"`
	Data      interface{} `json:"data,omitempty"`
	ErrorType errorType   `json:"errorType,omitempty"`
	Error     string      `json:"error,omitempty"`
}

// MarshalJSON encodes a Response struct into JSON.
func (r PromResponse) MarshalJSON() ([]byte, error) {
	// Define a struct that outputs "error" as a string.
	var o struct {
		Status    status      `json:"status"`
		Data      interface{} `json:"data,omitempty"`
		ErrorType errorType   `json:"errorType,omitempty"`
		Error     string      `json:"error,omitempty"`
	}

	// Copy fields to output struct.
	o.Status = r.Status
	o.ErrorType = r.ErrorType
	o.Data = r.Data
	if r.Error != "" {
		o.Error = r.Error
	}

	return json2.Marshal(&o)
}

// UnmarshalJSON decodes the data into the Response struct.
func (r *PromResponse) UnmarshalJSON(b []byte) error {
	var o struct {
		Status    status      `json:"status"`
		Data      interface{} `json:"data,omitempty"`
		ErrorType errorType   `json:"errorType,omitempty"`
		Error     string      `json:"error,omitempty"`
	}

	err := json2.Unmarshal(b, &o)
	if err != nil {
		return err
	}
	r.Status = o.Status
	r.ErrorType = o.ErrorType
	r.Data = o.Data
	if o.Error != "" {
		r.Error = o.Error
	}
	return nil
}

type timeSeries2RowsFunc func(mst string, dst []influx.Row, tss []prompb2.TimeSeries) ([]influx.Row, error)

func timeSeries2Rows(mst string, dst []influx.Row, tss []prompb2.TimeSeries) ([]influx.Row, error) {
	var r influx.Row
	var t time.Time
	for _, ts := range tss {
		tags := make(influx.PointTags, len(ts.Labels))
		tags, mst = unmarshalPromTags(tags, ts)
		for _, s := range ts.Samples {
			// convert and append
			t = time.Unix(0, s.Timestamp*int64(time.Millisecond))
			r = influx.Row{
				Tags:      tags,
				Name:      mst,
				Timestamp: t.UnixNano(),
				Fields: []influx.Field{
					influx.Field{
						Type:     influx.Field_Type_Float,
						Key:      promql2influxql.DefaultFieldKey,
						NumValue: s.Value,
					},
				},
			}
			dst = append(dst, r)
		}
	}
	return dst, nil
}

func unmarshalPromTags(dst influx.PointTags, ts prompb2.TimeSeries) (influx.PointTags, string) {
	measurement := promql2influxql.DefaultMeasurementName
	for i, label := range ts.Labels {
		if *(*string)(unsafe.Pointer(&label.Name)) == promql2influxql.DefaultMetricKeyLabel {
			measurement = *(*string)(unsafe.Pointer(&label.Value))
		}
		dst[i].Key = *(*string)(unsafe.Pointer(&label.Name))
		dst[i].Value = *(*string)(unsafe.Pointer(&label.Value))
	}
	sort.Sort(&dst)
	return dst, measurement
}

func timeSeries2RowsV2(mst string, dst []influx.Row, tss []prompb2.TimeSeries) ([]influx.Row, error) {
	var r influx.Row
	var t time.Time
	for _, ts := range tss {
		tags := make(influx.PointTags, len(ts.Labels))
		tags = unmarshalPromTagsV2(tags, ts)
		for _, s := range ts.Samples {
			// convert and append
			t = time.Unix(0, s.Timestamp*int64(time.Millisecond))
			r = influx.Row{
				Tags:      tags,
				Name:      mst,
				Timestamp: t.UnixNano(),
				Fields: []influx.Field{
					influx.Field{
						Type:     influx.Field_Type_Float,
						Key:      promql2influxql.DefaultFieldKey,
						NumValue: s.Value,
					},
				},
			}
			dst = append(dst, r)
		}
	}
	return dst, nil
}

func unmarshalPromTagsV2(dst influx.PointTags, ts prompb2.TimeSeries) influx.PointTags {
	for i, label := range ts.Labels {
		dst[i].Key = *(*string)(unsafe.Pointer(&label.Name))
		dst[i].Value = *(*string)(unsafe.Pointer(&label.Value))
	}
	return dst
}

type getQueryCmd func(r *http.Request, w http.ResponseWriter) (promql2influxql.PromCommand, bool)

func getInstantQueryCmd(r *http.Request, w http.ResponseWriter) (cmd promql2influxql.PromCommand, ok bool) {
	// Instant_query Query time. if this parameter is not specified, the current time is used by default.
	ts, err := parseTimeParam(r, "time", time.Now())
	if err != nil {
		invalidParamError(w, err, "time")
		return
	}
	lookBackDelta := promql2influxql.DefaultLookBackDelta
	if r.FormValue("lookback-delta") != "" {
		lookBackDelta, err = parseDuration(r.FormValue("lookback-delta"))
		if err != nil {
			invalidParamError(w, err, "lookback-delta")
			return
		}
	}
	promCommand := promql2influxql.PromCommand{
		Cmd:           r.FormValue("query"),
		Evaluation:    &ts,
		LookBackDelta: lookBackDelta,
	}
	return promCommand, true
}

func getRangeQueryCmd(r *http.Request, w http.ResponseWriter) (cmd promql2influxql.PromCommand, ok bool) {
	// Range_query query time. if this parameter is not specified, the current time is used by default.
	start, err := parseTime(r.FormValue("start"))
	if err != nil {
		invalidParamError(w, err, "start")
		return
	}
	end, err := parseTime(r.FormValue("end"))
	if err != nil {
		invalidParamError(w, err, "end")
		return
	}
	if end.Before(start) {
		invalidParamError(w, fmt.Errorf("end timestamp must not be before start time"), "end")
		return
	}
	step, err := parseDuration(r.FormValue("step"))
	if err != nil {
		invalidParamError(w, err, "step")
		return
	}

	if step <= 0 {
		invalidParamError(w, fmt.Errorf("zero or negative query resolution step widths are not accepted. Try a positive integer"), "step")
		return
	}

	// For safety, limit the number of returned points per time series.
	// This is sufficient for 60s resolution for a week or 1h resolution for a year.
	if end.Sub(start)/step > MaxPointsForSeries {
		respondError(w, &apiError{errorBadData, fmt.Errorf("exceeded maximum resolution of 11,000 points per timeseries. Try decreasing the query resolution (?step=XX)")}, nil)
		return
	}
	lookBackDelta := promql2influxql.DefaultLookBackDelta
	if r.FormValue("lookback-delta") != "" {
		lookBackDelta, err = parseDuration(r.FormValue("lookback-delta"))
		if err != nil {
			invalidParamError(w, err, "lookback-delta")
			return
		}
	}
	promCommand := promql2influxql.PromCommand{
		Cmd:           r.FormValue("query"),
		Start:         &start,
		End:           &end,
		Step:          step,
		LookBackDelta: lookBackDelta,
		DataType:      promql2influxql.GRAPH_DATA,
	}
	return promCommand, true
}

type getMetaQuery func(r *http.Request, w http.ResponseWriter, mst string) (*influxql.Query, bool)

func getLabelsQuery(r *http.Request, w http.ResponseWriter, mst string) (q *influxql.Query, ok bool) {
	start, end, ok := getTimeRange(w, r)
	if !ok {
		return
	}
	db, rp := getDbRpByProm(r)
	promCommand := promql2influxql.PromCommand{
		Database:        db,
		RetentionPolicy: rp,
		Measurement:     mst,
		Start:           &start,
		End:             &end,
		DataType:        promql2influxql.LABEL_KEYS_DATA,
	}

	nodes := transpileToStat(r, w, promCommand)
	if nodes == nil {
		return
	}
	showTagKeysStatement, ok := nodes.(*influxql.ShowTagKeysStatement)
	if !ok {
		respondError(w, &apiError{errorBadData, fmt.Errorf("invalid the show tag keys statement for promql")}, nil)
	}
	q = &influxql.Query{Statements: []influxql.Statement{showTagKeysStatement}}

	return q, true
}

func getLabelValuesQuery(r *http.Request, w http.ResponseWriter, mst string) (q *influxql.Query, ok bool) {
	name := mux.Vars(r)["name"]
	if !model.LabelNameRE.MatchString(name) {
		respondError(w, &apiError{errorBadData, fmt.Errorf("invalid label name: %q", name)}, nil)
		return
	}

	start, end, ok := getTimeRange(w, r)
	if !ok {
		return
	}

	db, rp := getDbRpByProm(r)
	promCommand := promql2influxql.PromCommand{
		Database:        db,
		RetentionPolicy: rp,
		Measurement:     mst,
		Start:           &start,
		End:             &end,
		LabelName:       name,
		DataType:        promql2influxql.LABEL_VALUES_DATA,
	}

	nodes := transpileToStat(r, w, promCommand)
	if nodes == nil {
		return
	}
	showTagValuesStatement, ok := nodes.(*influxql.ShowTagValuesStatement)
	if !ok {
		respondError(w, &apiError{errorBadData, fmt.Errorf("invalid the show tag values statement for promql")}, nil)
	}
	q = &influxql.Query{Statements: []influxql.Statement{showTagValuesStatement}}

	return q, true
}

func getSeriesQuery(r *http.Request, w http.ResponseWriter, mst string) (q *influxql.Query, ok bool) {
	if len(r.Form["match[]"]) == 0 {
		respondError(w, &apiError{errorBadData, fmt.Errorf("no match[] parameter provided")}, nil)
		return
	}

	start, end, ok := getTimeRange(w, r)
	if !ok {
		return
	}

	db, rp := getDbRpByProm(r)
	promCommand := promql2influxql.PromCommand{
		Database:        db,
		RetentionPolicy: rp,
		Measurement:     mst,
		Start:           &start,
		End:             &end,
		DataType:        promql2influxql.SERIES_DATA,
	}

	nodes := transpileToStat(r, w, promCommand)
	if nodes == nil {
		return nil, false
	}
	showSeriesStatement, ok := nodes.(*influxql.ShowSeriesStatement)
	if !ok {
		respondError(w, &apiError{errorBadData, fmt.Errorf("invalid the show tag keys statement for promql")}, nil)
	}
	q = &influxql.Query{Statements: []influxql.Statement{showSeriesStatement}}

	return q, true
}

func transpileToStat(r *http.Request, w http.ResponseWriter, promCommand promql2influxql.PromCommand) influxql.Node {
	matcherSets, err := parseMatchersParam(r.Form["match[]"])
	if err != nil {
		respondError(w, &apiError{errorBadData, err}, nil)
		return nil
	}
	labelMatchers := make([]*labels.Matcher, 0, len(matcherSets))
	for _, matchers := range matcherSets {
		labelMatchers = append(labelMatchers, matchers...)
	}
	vectorSelector := &parser.VectorSelector{
		LabelMatchers: labelMatchers,
	}

	// Transpiler as the key converter for promql2influxql
	transpiler := &promql2influxql.Transpiler{
		PromCommand: promCommand,
	}
	nodes, err := transpiler.Transpile(vectorSelector)
	if err != nil {
		respondError(w, &apiError{errorBadData, err}, nil)
		return nil
	}

	return nodes
}

func getTimeRange(w http.ResponseWriter, r *http.Request) (time.Time, time.Time, bool) {
	start, err := parseTimeParam(r, "start", minTime)
	if err != nil {
		invalidParamError(w, err, "start")
		return time.Time{}, time.Time{}, false
	}
	end, err := parseTimeParam(r, "end", maxTime)
	if err != nil {
		invalidParamError(w, err, "end")
		return time.Time{}, time.Time{}, false
	}
	return start, end, true
}

func parseMatchersParam(matchers []string) ([][]*labels.Matcher, error) {
	var matcherSets [][]*labels.Matcher
	for _, s := range matchers {
		matchers, err := parser.ParseMetricSelector(s)
		if err != nil {
			return nil, err
		}
		matcherSets = append(matcherSets, matchers)
	}

OUTER:
	for _, ms := range matcherSets {
		for _, lm := range ms {
			if lm != nil && !lm.Matches("") {
				continue OUTER
			}
		}
		return nil, errors.New("match[] must contain at least one non-empty matcher")
	}
	return matcherSets, nil
}

func seriesParse(s string) map[string]string {
	seriesMap := make(map[string]string)
	series := strings.Split(s, ",")
	series = series[1:]
	for _, item := range series {
		keyValue := strings.Split(item, "=")
		seriesMap[keyValue[0]] = keyValue[1]
	}
	return seriesMap

}

// getDbRpByProm get the names of db and rp from the url parameters. if there is no value, use the default names.
func getDbRpByProm(r *http.Request) (string, string) {
	db := r.FormValue("db")
	if db == "" {
		db = promql2influxql.DefaultDatabaseName
	}
	rp := r.FormValue("rp")
	if rp == "" {
		rp = promql2influxql.DefaultRetentionPolicyName
	}
	return db, rp
}

// getMstByProm get the mst name from the url path parameters.
func getMstByProm(h *Handler, w http.ResponseWriter, r *http.Request) (string, bool) {
	mst := strings.TrimSpace(mux.Vars(r)[MetricStore])
	if len(mst) == 0 {
		err := errno.NewError(errno.InvalidPromMstName, mst)
		h.httpError(w, err.Error(), http.StatusNotFound)
		h.Logger.Error("invalid the metric store", zap.Error(err))
		return mst, false
	}
	return mst, true
}

type promQueryParam struct {
	mst          string
	getQueryCmd  getQueryCmd
	getMetaQuery getMetaQuery
}
