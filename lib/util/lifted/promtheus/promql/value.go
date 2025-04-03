/*
Copyright (c) 2017 Prometheus
This code is originally from: https://github.com/prometheus/prometheus@v1.8.2/promql/value.go

2024.04.26 It has been modified to compatible files in influx/query.
Huawei Cloud Computing Technologies Co., Ltd.
*/

package promql

import (
	"fmt"
	"strconv"
	"strings"

	jsoniter "github.com/json-iterator/go"
	strings2 "github.com/openGemini/openGemini/lib/strings"
	"github.com/openGemini/openGemini/lib/util/lifted/promtheus/pkg/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/util/annotations"
)

func (Matrix) Type() parser.ValueType { return parser.ValueTypeMatrix }

func (Vector) Type() parser.ValueType { return parser.ValueTypeVector }

func (Scalar) Type() parser.ValueType { return parser.ValueTypeScalar }

func (String) Type() parser.ValueType { return parser.ValueTypeString }

// String represents a string value.
type String struct {
	T int64
	V string
}

func (s String) String() string {
	return s.V
}

func (s String) MarshalJSON() ([]byte, error) {
	return jsoniter.Marshal([...]interface{}{float64(s.T) / 1000, s.V})
}

// Scalar is a data point that's explicitly not associated with a metric.
type Scalar struct {
	T int64
	V float64
}

func (s Scalar) String() string {
	v := strconv.FormatFloat(s.V, 'f', -1, 64)
	return fmt.Sprintf("scalar: %v @[%v]", v, s.T)
}

func (s Scalar) MarshalJSON() ([]byte, error) {
	v := strconv.FormatFloat(s.V, 'f', -1, 64)
	return jsoniter.Marshal([...]interface{}{float64(s.T) / 1000, v})
}

// Series is a stream of data points belonging to a metric.
type Series struct {
	Metric labels.Labels `json:"metric"`
	Points []Point       `json:"values"`
}

func (s Series) String() string {
	vals := make([]string, len(s.Points))
	for i, v := range s.Points {
		vals[i] = v.String()
	}
	return fmt.Sprintf("%s =>\n%s", s.Metric, strings.Join(vals, "\n"))
}

var sbPool = strings2.NewBuilderPool()

// Point represents a single data point for a given timestamp.
type Point struct {
	T int64
	V float64
}

func (p Point) String() string {
	v := strconv.FormatFloat(p.V, 'f', -1, 64)
	return fmt.Sprintf("%v @[%v]", v, p.T)
}

// MarshalJSON implements json.Marshaler.
func (p Point) MarshalJSON() ([]byte, error) {
	k := strconv.FormatFloat(float64(p.T)/1000, 'f', -1, 64)
	v := strconv.FormatFloat(p.V, 'f', -1, 64)
	sb := strings2.NewStringBuilder(32)
	sb.AppendString("[")
	sb.AppendString(k)
	sb.AppendString(",")
	sb.AppendString("\"")
	sb.AppendString(v)
	sb.AppendString("\"")
	sb.AppendString("]")
	return sb.Bytes(), nil
}

// Sample is a single sample belonging to a metric.
type Sample struct {
	Point

	Metric labels.Labels
}

func (s Sample) String() string {
	return fmt.Sprintf("%s => %s", s.Metric, s.Point)
}

func (s Sample) MarshalJSON() ([]byte, error) {
	v := struct {
		M labels.Labels `json:"metric"`
		V Point         `json:"value"`
	}{
		M: s.Metric,
		V: s.Point,
	}
	return jsoniter.Marshal(v)
}

// Vector is basically only an alias for model.Samples, but the
// contract is that in a Vector, all Samples have the same timestamp.
type Vector []Sample

func (vec Vector) String() string {
	entries := make([]string, len(vec))
	for i, s := range vec {
		entries[i] = s.String()
	}
	return strings.Join(entries, "\n")
}

// ContainsSameLabelset checks if a vector has samples with the same labelset
// Such a behavior is semantically undefined
// https://github.com/prometheus/prometheus/issues/4562
func (vec Vector) ContainsSameLabelset() bool {
	l := make(map[uint64]struct{}, len(vec))
	for _, s := range vec {
		hash := s.Metric.Hash()
		if _, ok := l[hash]; ok {
			return true
		}
		l[hash] = struct{}{}
	}
	return false
}

// Matrix is a slice of Series that implements sort.Interface and
// has a String method.
type Matrix []Series

func (m Matrix) String() string {
	// TODO(fabxc): sort, or can we rely on order from the querier?
	strs := make([]string, len(m))

	for i, ss := range m {
		strs[i] = ss.String()
	}

	return strings.Join(strs, "\n")
}

// TotalSamples returns the total number of samples in the series within a matrix.
func (m Matrix) TotalSamples() int {
	numSamples := 0
	for _, series := range m {
		numSamples += len(series.Points)
	}
	return numSamples
}

func (m Matrix) Len() int { return len(m) }

func (m Matrix) Less(i, j int) bool { return labels.Compare(m[i].Metric, m[j].Metric) < 0 }

func (m Matrix) Swap(i, j int) { m[i], m[j] = m[j], m[i] }

// ContainsSameLabelset checks if a matrix has samples with the same labelset.
// Such a behavior is semantically undefined.
// https://github.com/prometheus/prometheus/issues/4562
func (m Matrix) ContainsSameLabelset() bool {
	l := make(map[uint64]struct{}, len(m))
	for _, ss := range m {
		hash := ss.Metric.Hash()
		if _, ok := l[hash]; ok {
			return true
		}
		l[hash] = struct{}{}
	}
	return false
}

// Result holds the resulting value of an execution or an error
// if any occurred.
type Result struct {
	Err      error
	Value    parser.Value
	Warnings annotations.Annotations
}

// Vector returns a Vector if the result value is one. An error is returned if
// the result was an error or the result value is not a Vector.
func (r *Result) Vector() (Vector, error) {
	if r.Err != nil {
		return nil, r.Err
	}
	v, ok := r.Value.(Vector)
	if !ok {
		return nil, fmt.Errorf("query result is not a Vector")
	}
	return v, nil
}

// Matrix returns a Matrix. An error is returned if
// the result was an error or the result value is not a Matrix.
func (r *Result) Matrix() (Matrix, error) {
	if r.Err != nil {
		return nil, r.Err
	}
	v, ok := r.Value.(Matrix)
	if !ok {
		return nil, fmt.Errorf("query result is not a range Vector")
	}
	return v, nil
}

// Scalar returns a Scalar value. An error is returned if
// the result was an error or the result value is not a Scalar.
func (r *Result) Scalar() (Scalar, error) {
	if r.Err != nil {
		return Scalar{}, r.Err
	}
	v, ok := r.Value.(Scalar)
	if !ok {
		return Scalar{}, fmt.Errorf("query result is not a Scalar")
	}
	return v, nil
}

func (r *Result) String() string {
	if r.Err != nil {
		return r.Err.Error()
	}
	if r.Value == nil {
		return ""
	}
	return r.Value.String()
}
