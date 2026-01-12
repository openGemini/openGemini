// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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
	"fmt"
	"sort"

	"github.com/openGemini/openGemini/lib/util/lifted/prometheus/promql"
	"github.com/openGemini/openGemini/lib/util/lifted/promql2influxql"
	"github.com/prometheus/prometheus/promql/parser"
)

type sortPlan int

const (
	mergeOnly        sortPlan = 0
	sortByValuesAsc  sortPlan = 1
	sortByValuesDesc sortPlan = 2
	sortByLabels     sortPlan = 3
)

func ExtractResponse(res *promql2influxql.PromQueryResponse, start, end int64) *promql2influxql.PromQueryResponse {
	newRes := &promql2influxql.PromQueryResponse{
		Status: res.Status,
		Data: &promql2influxql.PromData{
			ResultType: res.Data.ResultType,
			Result:     &promql2influxql.PromDataMatrix{},
		},
		Error:     res.Error,
		ErrorType: res.ErrorType,
	}
	m := res.Data.GetMatrix()
	if m == nil {
		return newRes
	}
	matrix := ExtractMatrix(*m, start, end)
	newRes.Data.Result = &promql2influxql.PromDataMatrix{
		Matrix: &matrix,
	}
	return newRes
}

func ExtractMatrix(m promql.Matrix, start, end int64) promql.Matrix {
	matrix := make(promql.Matrix, 0, len(m))
	for _, s := range m {
		series, ok := ExtractSeries(s, start, end)
		if ok {
			matrix = append(matrix, series)
		}
	}
	return matrix
}

func ExtractSeries(s promql.Series, start, end int64) (promql.Series, bool) {
	series := promql.Series{
		Metric: s.Metric,
		Floats: make([]promql.FPoint, 0, len(s.Floats)),
	}
	for _, point := range s.Floats {
		if start <= point.T && point.T <= end {
			series.Floats = append(series.Floats, point)
		}
	}
	if len(series.Floats) == 0 {
		return promql.Series{}, false
	}
	return series, true
}

func MergeResponse(command *promql2influxql.PromCommand, responses ...*promql2influxql.PromQueryResponse) (*promql2influxql.PromQueryResponse, error) {
	if len(responses) == 0 {
		return nil, fmt.Errorf("merge empty response")
	}
	if len(responses) == 1 {
		return responses[0], nil
	}

	var resultType string
	for _, r := range responses {
		if r.Data != nil {
			resultType = r.Data.ResultType
			break
		}
	}
	var data *promql2influxql.PromData
	switch resultType {
	case string(parser.ValueTypeVector):
		v, err := MergeVector(command, responses)
		if err != nil {
			return nil, err
		}
		data = &promql2influxql.PromData{
			ResultType: responses[0].Data.ResultType,
			Result: &promql2influxql.PromDataVector{
				Vector: v,
			},
		}
	case string(parser.ValueTypeMatrix):
		m, err := MergeMatrix(responses)
		if err != nil {
			return nil, err
		}
		data = &promql2influxql.PromData{
			ResultType: responses[0].Data.ResultType,
			Result: &promql2influxql.PromDataMatrix{
				Matrix: m,
			},
		}
	default:
		return nil, fmt.Errorf("unexpected result type: %s", resultType)
	}
	res := &promql2influxql.PromQueryResponse{
		Status: responses[0].Status,
		Data:   data,
	}
	return res, nil
}

func MergeVector(command *promql2influxql.PromCommand, responses []*promql2influxql.PromQueryResponse) (*promql.Vector, error) {
	output := map[string]promql.Sample{}
	metrics := []string{}
	sortPlan, err := sortPlanForQuery(command.Cmd)
	if err != nil {
		return nil, err
	}

	for _, resp := range responses {
		if resp == nil {
			continue
		}
		vector := resp.Data.GetVector()
		if vector == nil {
			continue
		}

		for _, sample := range *vector {
			metric := sample.Metric.String()
			if exist, ok := output[metric]; !ok {
				output[metric] = sample
				metrics = append(metrics, metric)
			} else if exist.T < sample.T {
				output[metric] = sample
			}
		}
	}
	result := promql.Vector{}
	if len(output) == 0 {
		return &result, nil
	}

	for _, m := range metrics {
		result = append(result, output[m])
	}
	if sortPlan == mergeOnly {
		return &result, nil
	}

	switch sortPlan {
	case sortByValuesAsc:
		sort.Slice(result, func(i, j int) bool {
			return result[i].F < result[j].F
		})
	case sortByValuesDesc:
		sort.Slice(result, func(i, j int) bool {
			return result[i].F > result[j].F
		})
	default:
		sort.Slice(result, func(i, j int) bool {
			return result[i].Metric.String() > result[j].Metric.String()
		})
	}

	return &result, nil
}

func MergeMatrix(responses []*promql2influxql.PromQueryResponse) (*promql.Matrix, error) {
	output := make(map[string]promql.Series)

	for _, r := range responses {
		if responses == nil {
			continue
		}
		matrix := r.Data.GetMatrix()
		if matrix == nil {
			continue
		}
		mergeSeries(output, *matrix)
	}
	keys := make([]string, 0, len(output))
	for key := range output {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	result := make(promql.Matrix, len(keys))
	for i, key := range keys {
		result[i] = output[key]
	}
	return &result, nil
}

func mergeSeries(output map[string]promql.Series, matrix promql.Matrix) {
	for _, series := range matrix {
		m := series.Metric.String()
		curSeries, ok := output[m]
		if !ok {
			curSeries = promql.Series{
				Metric: series.Metric,
			}
		}

		if len(curSeries.Floats) > 0 && len(series.Floats) > 0 {
			curEndTime := curSeries.Floats[len(curSeries.Floats)-1].T
			if curEndTime == series.Floats[0].T {
				series.Floats = series.Floats[1:]
			} else if curEndTime > series.Floats[0].T {
				series.Floats = sliceFloats(series.Floats, curEndTime)
			}
		}

		curSeries.Floats = append(curSeries.Floats, series.Floats...)

		output[m] = curSeries
	}
}

func sliceFloats(points []promql.FPoint, minT int64) []promql.FPoint {
	if minT > points[len(points)-1].T {
		return points[:0]
	}
	index := sort.Search(len(points), func(i int) bool {
		return points[i].T > minT
	})
	return points[index:]
}

func sortPlanForQuery(q string) (sortPlan, error) {
	expr, err := parser.ParseExpr(q)
	if err != nil {
		return 0, err
	}
	// Check if the root expression is topk or bottomk
	if aggr, ok := expr.(*parser.AggregateExpr); ok {
		if aggr.Op == parser.TOPK || aggr.Op == parser.BOTTOMK {
			return mergeOnly, nil
		}
	}
	checkForSort := func(expr parser.Expr) (sortAsc, sortDesc bool) {
		if n, ok := expr.(*parser.Call); ok {
			if n.Func != nil {
				if n.Func.Name == "sort" {
					sortAsc = true
				}
				if n.Func.Name == "sort_desc" {
					sortDesc = true
				}
			}
		}
		return sortAsc, sortDesc
	}
	// Check the root expression for sort
	if sortAsc, sortDesc := checkForSort(expr); sortAsc || sortDesc {
		if sortAsc {
			return sortByValuesAsc, nil
		}
		return sortByValuesDesc, nil
	}

	// If the root expression is a binary expression, check the LHS and RHS for sort
	if bin, ok := expr.(*parser.BinaryExpr); ok {
		if sortAsc, sortDesc := checkForSort(bin.LHS); sortAsc || sortDesc {
			if sortAsc {
				return sortByValuesAsc, nil
			}
			return sortByValuesDesc, nil
		}
		if sortAsc, sortDesc := checkForSort(bin.RHS); sortAsc || sortDesc {
			if sortAsc {
				return sortByValuesAsc, nil
			}
			return sortByValuesDesc, nil
		}
	}
	return sortByLabels, nil
}
