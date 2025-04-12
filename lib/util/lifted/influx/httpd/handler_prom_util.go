// Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.
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
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/promql2influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/openGemini/openGemini/lib/validation"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
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

var reportedError = map[errno.Errno]struct{}{
	errno.ErrSameTagSet: struct{}{},
}

func isPromReportedError(err error) bool {
	if strings.Contains(err.Error(), `label_join`) || strings.Contains(err.Error(), `label_replace`) {
		return true
	}
	e, ok := err.(*errno.Error)
	if !ok {
		return false
	}
	if _, ok := reportedError[e.Errno()]; ok {
		return true
	}
	return false
}

var (
	minTime = time.Unix(0, influxql.MinTime).UTC()
	maxTime = time.Unix(0, influxql.MaxTime).UTC()

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

func invalidParamError(w http.ResponseWriter, err error, parameter string) *apiError {
	apiErr := &apiError{
		errorBadData, fmt.Errorf("invalid parameter %q: %w", parameter, err),
	}
	respondError(w, apiErr)
	return apiErr
}

func (h *Handler) getPromResult(stmtID2Result map[int]*query.Result, expr parser.Expr, cmd promql2influxql.PromCommand, dropMetric, removeTableName, duplicateResult bool) (*promql2influxql.PromQueryResponse, *apiError) {
	r := &promql2influxql.Receiver{PromCommand: cmd, DropMetric: dropMetric, RemoveTableName: removeTableName, DuplicateResult: duplicateResult}
	resp := &promql2influxql.PromQueryResponse{Data: &promql2influxql.PromData{}, Status: "success"}
	if len(stmtID2Result) > 0 {
		if stmtID2Result[0].Err != nil && !isPromReportedError(stmtID2Result[0].Err) {
			if isPromAbsentCall(expr) && (errors.Is(stmtID2Result[0].Err, meta.ErrMeasurementNotFound) || errno.Equal(stmtID2Result[0].Err, errno.DatabaseNotFound)) {
				result, err := r.AbsentNoMstResult(expr)
				if err != nil {
					apiErr := &apiError{errorBadData, err}
					return resp, apiErr
				} else {
					resp.Data = result
				}
			} else {
				resp.Data = getEmptyResponse(cmd)
			}
		} else {
			data, err := r.InfluxResultToPromQLValue(stmtID2Result[0], expr, cmd)
			if err != nil {
				apiErr := &apiError{errorExec, err}
				return resp, apiErr
			} else {
				resp.Data = data
			}
		}
	}
	return resp, nil
}

func isPromAbsentCall(expr parser.Expr) bool {
	call, ok := expr.(*parser.Call)
	return ok && (strings.EqualFold(call.Func.Name, "absent") || strings.EqualFold(call.Func.Name, "absent_over_time"))
}

func (h *Handler) getRangePromResultForEmptySeries(expr influxql.Expr, promCommand *promql2influxql.PromCommand, valuer *influxql.ValuerEval, timeValuer *PromTimeValuer) *promql2influxql.PromQueryResponse {
	points := make([]promql.FPoint, 0)
	start, end, step := GetRangeTimeForEmptySeriesResult(promCommand)
	for s := start; s <= end; s += step {
		timeValuer.tmpTime = s
		value := valuer.Eval(expr)
		points = append(points, promql.FPoint{T: s, F: value.(float64)})
	}
	matrix := &promql2influxql.PromDataMatrix{Matrix: &promql.Matrix{promql.Series{Metric: labels.Labels{}, Floats: points}}}
	resp := &promql2influxql.PromQueryResponse{Status: "success"}
	resp.Data = &promql2influxql.PromData{Result: matrix, ResultType: string(parser.ValueTypeMatrix)}
	return resp
}

func (h *Handler) getInstantPromResultForEmptySeries(expr influxql.Expr, promCommand *promql2influxql.PromCommand, valuer *influxql.ValuerEval, timeValuer *PromTimeValuer, typ parser.ValueType) (*promql2influxql.PromQueryResponse, *apiError) {
	time := GetTimeForEmptySeriesResult(promCommand)
	timeValuer.tmpTime = time
	value := valuer.Eval(expr)

	var result promql2influxql.PromDataResult
	switch typ {
	case parser.ValueTypeScalar:
		result = &promql2influxql.PromDataScalar{Scalar: &promql.Scalar{T: time, V: value.(float64)}}
	case parser.ValueTypeVector:
		result = &promql2influxql.PromDataVector{Vector: &promql.Vector{promql.Sample{T: time, F: value.(float64), Metric: labels.Labels{}}}}
	case parser.ValueTypeString:
		result = &promql2influxql.PromDataString{String: &promql.String{T: time, V: value.(string)}}
	default:
		return nil, &apiError{typ: errorBadData, err: fmt.Errorf("getInstantPromResultForEmptySeries: unexpected type: %s", typ)}
	}

	resp := &promql2influxql.PromQueryResponse{Status: "success"}
	resp.Data = &promql2influxql.PromData{Result: result, ResultType: string(typ)}
	return resp, nil
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

func getEmptyResponse(cmd promql2influxql.PromCommand) *promql2influxql.PromData {
	if cmd.DataType == promql2influxql.GRAPH_DATA {
		return promql2influxql.NewPromData(&promql2influxql.PromDataMatrix{}, string(parser.ValueTypeMatrix))
	} else {
		return promql2influxql.NewPromData(&promql2influxql.PromDataMatrix{}, string(parser.ValueTypeVector))

	}
}

func respondError(w http.ResponseWriter, apiErr *apiError) {
	logger.GetLogger().Error("serve prom query error", zap.Error(apiErr.err))
	b, err := json2.Marshal(&promql2influxql.PromQueryResponse{
		Status:    string(StatusError),
		ErrorType: string(apiErr.typ),
		Error:     apiErr.err.Error(),
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

type timeSeries2RowsFunc func(mst string, dst []influx.Row, tss []prompb2.TimeSeries, invalidTss map[int]bool) ([]influx.Row, error)

func timeSeries2Rows(mst string, dst []influx.Row, tss []prompb2.TimeSeries, invalidTss map[int]bool) ([]influx.Row, error) {
	var i int
	for tsIdx, ts := range tss {
		if invalidTss[tsIdx] {
			continue
		}
		dst[i].ResizeTags(len(ts.Labels))
		tags := dst[i].Tags
		tags, mst = unmarshalPromTags(tags, ts)
		for j, s := range ts.Samples {
			// convert and append
			dst[i].Timestamp = s.Timestamp * int64(time.Millisecond)
			dst[i].Name = mst
			if j == 0 {
				dst[i].Tags = tags
			} else {
				dst[i].CloneTags(tags)
			}
			if cap(dst[i].Fields) > 0 {
				dst[i].Fields = dst[i].Fields[:1]
				dst[i].Fields[0].Type = influx.Field_Type_Float
				dst[i].Fields[0].Key = promql2influxql.DefaultFieldKey
				dst[i].Fields[0].NumValue = s.Value
			} else {
				dst[i].Fields = []influx.Field{
					influx.Field{
						Type:     influx.Field_Type_Float,
						Key:      promql2influxql.DefaultFieldKey,
						NumValue: s.Value,
					},
				}
			}
			i++
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

func timeSeries2RowsV2(mst string, dst []influx.Row, tss []prompb2.TimeSeries, invalidTss map[int]bool) ([]influx.Row, error) {
	var i int
	for tsIdx, ts := range tss {
		if invalidTss[tsIdx] {
			continue
		}
		dst[i].ResizeTags(len(ts.Labels))
		tags := dst[i].Tags
		tags = unmarshalPromTagsV2(tags, ts)
		for j, s := range ts.Samples {
			// convert and append
			dst[i].Timestamp = s.Timestamp * int64(time.Millisecond)
			dst[i].Name = mst
			if j == 0 {
				dst[i].Tags = tags
			} else {
				dst[i].CloneTags(tags)
			}
			if cap(dst[i].Fields) > 0 {
				dst[i].Fields = dst[i].Fields[:1]
				dst[i].Fields[0].Type = influx.Field_Type_Float
				dst[i].Fields[0].Key = promql2influxql.DefaultFieldKey
				dst[i].Fields[0].NumValue = s.Value
			} else {
				dst[i].Fields = []influx.Field{
					influx.Field{
						Type:     influx.Field_Type_Float,
						Key:      promql2influxql.DefaultFieldKey,
						NumValue: s.Value,
					},
				}
			}
			i++
		}
	}
	return dst, nil
}

func unmarshalPromTagsV2(dst influx.PointTags, ts prompb2.TimeSeries) influx.PointTags {
	for i, label := range ts.Labels {
		dst[i].Key = *(*string)(unsafe.Pointer(&label.Name))
		dst[i].Value = *(*string)(unsafe.Pointer(&label.Value))
	}
	sort.Sort(&dst)
	return dst
}

func promMetaData2Tag(mst string, row *influx.Row, md *prompb.MetricMetadata) {
	if len(mst) > 0 {
		row.ResizeTags(2)
		row.Tags[0].Key = promql2influxql.PromMetric
		row.Tags[0].Value = md.MetricFamilyName
		row.Tags[1].Key = promql2influxql.PromProjectID
		row.Tags[1].Value = mst
		return
	}
	row.ResizeTags(1)
	row.Tags[0].Key = promql2influxql.PromMetric
	row.Tags[0].Value = md.MetricFamilyName
}

func promMetaData2Field(row *influx.Row, md *prompb.MetricMetadata) {
	row.ResizeFields(3)
	row.Fields[0].Key = promql2influxql.PromMetaDataHelp
	row.Fields[0].StrValue = md.Help
	row.Fields[0].Type = influx.Field_Type_String
	row.Fields[1].Key = promql2influxql.PromMetaDataType
	row.Fields[1].StrValue = strconv.Itoa(int(md.Type))
	row.Fields[1].Type = influx.Field_Type_Int
	row.Fields[2].Key = promql2influxql.PromMetaDataUnit
	row.Fields[2].StrValue = md.Unit
	row.Fields[2].Type = influx.Field_Type_String
}

// promMetaData2Row used to convert metadata to row
// mst: prom_meta_data
// time |                  Field                    |                 Tag                  |
// time | help(string) | type(int64) | unit(string) | metric(string) | project_id(string)  |
func promMetaData2Row(mst string, row *influx.Row, md *prompb.MetricMetadata, timeStamp int64) {
	row.Name = promql2influxql.PromMetaDataMst
	row.Timestamp = timeStamp
	promMetaData2Tag(mst, row, md)
	promMetaData2Field(row, md)
}

type getQueryCmd func(r *http.Request, w http.ResponseWriter, mst string) (promql2influxql.PromCommand, bool)

func getInstantQueryCmd(r *http.Request, w http.ResponseWriter, mst string) (cmd promql2influxql.PromCommand, ok bool) {
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

func getRangeQueryCmd(r *http.Request, w http.ResponseWriter, mst string) (cmd promql2influxql.PromCommand, ok bool) {
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

	if err := validation.ValidateQueryTimeRange(mst, start, end); err != nil {
		invalidParamError(w, err, "start_end")
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
		respondError(w, &apiError{errorBadData, fmt.Errorf("exceeded maximum resolution of 11,000 points per timeseries. Try decreasing the query resolution (?step=XX)")})
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

type getMetaQuery func(h *Handler, r *http.Request, w http.ResponseWriter, mst string) (*influxql.Query, bool)

func getLabelsQuery(h *Handler, r *http.Request, w http.ResponseWriter, mst string) (q *influxql.Query, ok bool) {
	start, end, ok := getTimeRange(w, r, mst)
	if !ok {
		return
	}
	db, rp := getDbRpByProm(h, r)
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
		respondError(w, &apiError{errorBadData, fmt.Errorf("invalid labels query for promql")})
	}
	q = &influxql.Query{Statements: []influxql.Statement{showTagKeysStatement}}

	return q, true
}

func getLabelValuesQuery(h *Handler, r *http.Request, w http.ResponseWriter, mst string) (q *influxql.Query, ok bool) {
	name := mux.Vars(r)["name"]
	if !model.LabelNameRE.MatchString(name) {
		respondError(w, &apiError{errorBadData, fmt.Errorf("invalid label name: %q", name)})
		return
	}

	start, end, ok := getTimeRange(w, r, mst)
	if !ok {
		return
	}

	db, rp := getDbRpByProm(h, r)
	promCommand := promql2influxql.PromCommand{
		Database:        db,
		RetentionPolicy: rp,
		Measurement:     mst,
		Exact:           isExact(r),
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
		respondError(w, &apiError{errorBadData, fmt.Errorf("invalid label values query for promql")})
	}
	q = &influxql.Query{Statements: []influxql.Statement{showTagValuesStatement}}

	return q, true
}

func getSeriesQuery(h *Handler, r *http.Request, w http.ResponseWriter, mst string) (q *influxql.Query, ok bool) {
	if len(r.Form["match[]"]) == 0 {
		respondError(w, &apiError{errorBadData, fmt.Errorf("no match[] parameter provided")})
		return
	}

	start, end, ok := getTimeRange(w, r, mst)
	if !ok {
		return
	}

	db, rp := getDbRpByProm(h, r)
	promCommand := promql2influxql.PromCommand{
		Database:        db,
		RetentionPolicy: rp,
		Measurement:     mst,
		Exact:           isExact(r),
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
		respondError(w, &apiError{errorBadData, fmt.Errorf("invalid series query for promql")})
	}
	q = &influxql.Query{Statements: []influxql.Statement{showSeriesStatement}}

	return q, true
}

func genFieldByCall(call string, field string, dt influxql.DataType) *influxql.Field {
	return &influxql.Field{
		Alias: field,
		Expr: &influxql.Call{
			Name: call,
			Args: []influxql.Expr{
				&influxql.VarRef{
					Val:  field,
					Type: dt,
				},
			},
		},
	}
}

func genConditionByReq(mst string, r *http.Request, cmd *promql2influxql.PromCommand) influxql.Expr {
	condition := promql2influxql.GetTimeCondition(cmd.Start, cmd.End)
	if len(mst) > 0 {
		tagCond := &influxql.BinaryExpr{
			Op:  influxql.EQ,
			LHS: &influxql.VarRef{Val: promql2influxql.PromProjectID},
			RHS: &influxql.StringLiteral{Val: mst},
		}
		condition = promql2influxql.CombineConditionAnd(condition, tagCond)
	}
	if metric := r.FormValue("metric"); len(metric) > 0 {
		tagCond := &influxql.BinaryExpr{
			Op:  influxql.EQ,
			LHS: &influxql.VarRef{Val: promql2influxql.PromMetric},
			RHS: &influxql.StringLiteral{Val: metric},
		}
		condition = promql2influxql.CombineConditionAnd(condition, tagCond)
	}
	return condition
}

func genDimensionByReq(mst string) influxql.Dimensions {
	dims := []*influxql.Dimension{
		{
			Expr: &influxql.VarRef{
				Val: promql2influxql.PromMetric,
			},
		},
	}
	if len(mst) > 0 {
		dims = append(dims, &influxql.Dimension{
			Expr: &influxql.VarRef{
				Val: promql2influxql.PromProjectID,
			},
		})
	}
	return dims
}

func getMetaDataQuery(h *Handler, r *http.Request, w http.ResponseWriter, mst string) (q *influxql.Query, ok bool) {
	db, rp := getDbRpByProm(h, r)
	promCommand := promql2influxql.PromCommand{
		Database:        db,
		RetentionPolicy: rp,
		Measurement:     promql2influxql.PromMetaDataMst,
		DataType:        promql2influxql.META_DATA,
	}
	source := &influxql.Measurement{Database: db, RetentionPolicy: rp, Name: promCommand.Measurement}
	fields := []*influxql.Field{
		genFieldByCall("last", promql2influxql.PromMetaDataHelp, influxql.String),
		genFieldByCall("last", promql2influxql.PromMetaDataType, influxql.Integer),
		genFieldByCall("last", promql2influxql.PromMetaDataUnit, influxql.String),
	}
	condition := genConditionByReq(mst, r, &promCommand)
	var limit int
	if n, err := strconv.ParseInt(r.FormValue("limit"), 10, 64); err == nil && int(n) > 0 {
		limit = int(n)
	}
	dimension := genDimensionByReq(mst)
	statement := &influxql.SelectStatement{
		Sources:    []influxql.Source{source},
		Fields:     fields,
		Condition:  condition,
		Dimensions: dimension,
		Limit:      limit,
	}
	q = &influxql.Query{Statements: []influxql.Statement{statement}}
	return q, true
}

func transpileToStat(r *http.Request, w http.ResponseWriter, promCommand promql2influxql.PromCommand) influxql.Node {
	if len(promCommand.Measurement) > 0 {
		return transpileToStmtWithMetricStore(r, w, promCommand)
	}
	matcherSets, err := parseMatchersParam(r.Form["match[]"])
	if err != nil {
		respondError(w, &apiError{errorBadData, err})
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
		respondError(w, &apiError{errorBadData, err})
		return nil
	}
	return nodes
}

func transpileToStmtWithMetricStore(r *http.Request, w http.ResponseWriter, promCommand promql2influxql.PromCommand) influxql.Node {
	matcherSets, err := parseMatchersParam(r.Form["match[]"])
	if err != nil {
		respondError(w, &apiError{errorBadData, err})
		return nil
	}

	var labelMatchers []*labels.Matcher
	if len(matcherSets) >= 1 {
		labelMatchers = matcherSets[0]
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
		respondError(w, &apiError{errorBadData, err})
		return nil
	}
	if len(matcherSets) <= 1 {
		return nodes
	}

	condition := getConditionByNode(nodes)
	for _, matcher := range matcherSets[1:] {
		vectorSelector = &parser.VectorSelector{
			LabelMatchers: matcher,
		}
		LabelCondition, err := promql2influxql.GetTagCondition(vectorSelector, true)
		if err != nil {
			respondError(w, &apiError{errorBadData, err})
			return nil
		}
		condition = promql2influxql.CombineConditionOr(condition, LabelCondition)
	}
	setConditionByNode(nodes, condition)
	return nodes
}

func getTimeRange(w http.ResponseWriter, r *http.Request, mst string) (time.Time, time.Time, bool) {
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

	if err := validation.ValidateQueryTimeRange(mst, start, end); err != nil {
		invalidParamError(w, err, "start_end")
		return time.Time{}, time.Time{}, false
	}

	return start, end, true
}

func isExact(r *http.Request) bool {
	return strings.ToLower(r.FormValue("exact")) == "true"
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
func getDbRpByProm(h *Handler, r *http.Request) (string, string) {
	db := r.FormValue("db")
	if db == "" {
		db = promql2influxql.DefaultDatabaseName
	}
	rp := r.FormValue("rp")
	if rp == "" && h.MetaClient != nil {
		dbi, err := h.MetaClient.Database(db)
		if dbi != nil && err == nil {
			rp = dbi.DefaultRetentionPolicy
		}
	}
	if rp == "" {
		rp = promql2influxql.DefaultRetentionPolicyName
	}
	return db, rp
}

// isMetaDataReq used to check whether to write metadata
func isMetaDataReq(r *http.Request) bool {
	return r.FormValue("metadata") == WriteMetaOK
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

func getConditionByNode(node influxql.Node) influxql.Expr {
	switch stmt := node.(type) {
	case *influxql.SelectStatement:
		return stmt.Condition
	case *influxql.ShowTagValuesStatement:
		return stmt.Condition
	case *influxql.ShowTagKeysStatement:
		return stmt.Condition
	case *influxql.ShowSeriesStatement:
		return stmt.Condition
	default:
		return nil
	}
}

func setConditionByNode(node influxql.Node, condition influxql.Expr) {
	switch stmt := node.(type) {
	case *influxql.SelectStatement:
		stmt.Condition = condition
	case *influxql.ShowTagValuesStatement:
		stmt.Condition = condition
	case *influxql.ShowTagKeysStatement:
		stmt.Condition = condition
	case *influxql.ShowSeriesStatement:
		stmt.Condition = condition
	default:
	}
}
