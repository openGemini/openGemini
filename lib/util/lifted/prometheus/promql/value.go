//go:generate msgp
// Copyright 2017 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//2025.02.25 Add msgp flag to structs
// This code is originally from https://github.com/prometheus/prometheus/blob/v0.50.1/promql/value.go
//Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.

package promql

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/openGemini/openGemini/lib/util/lifted/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
)

func (Matrix) Type() parser.ValueType { return parser.ValueTypeMatrix }
func (Vector) Type() parser.ValueType { return parser.ValueTypeVector }
func (Scalar) Type() parser.ValueType { return parser.ValueTypeScalar }
func (String) Type() parser.ValueType { return parser.ValueTypeString }

// String represents a string value.
type String struct {
	T int64  `msg:"t"`
	V string `msg:"v"`
}

func (s String) String() string {
	return s.V
}

func (s String) MarshalJSON() ([]byte, error) {
	return json.Marshal([...]interface{}{float64(s.T) / 1000, s.V})
}

// Scalar is a data point that's explicitly not associated with a metric.
type Scalar struct {
	T int64   `msg:"t"`
	V float64 `msg:"v"`
}

func (s Scalar) String() string {
	v := strconv.FormatFloat(s.V, 'f', -1, 64)
	return fmt.Sprintf("scalar: %v @[%v]", v, s.T)
}

func (s Scalar) MarshalJSON() ([]byte, error) {
	v := strconv.FormatFloat(s.V, 'f', -1, 64)
	return json.Marshal([...]interface{}{float64(s.T) / 1000, v})
}

// Series is a stream of data points belonging to a metric.
type Series struct {
	Metric labels.Labels `json:"metric" msg:"metric"`
	Floats []FPoint      `json:"values,omitempty" msg:"floats"`
}

func (s Series) String() string {
	// TODO(beorn7): This currently renders floats first and then
	// histograms, each sorted by timestamp. Maybe, in mixed series, that's
	// fine. Maybe, however, primary sorting by timestamp is preferred, in
	// which case this has to be changed.
	vals := make([]string, 0, len(s.Floats))
	for _, f := range s.Floats {
		vals = append(vals, f.String())
	}
	return fmt.Sprintf("%s =>\n%s", s.Metric, strings.Join(vals, "\n"))
}

// FPoint represents a single float data point for a given timestamp.
type FPoint struct {
	T int64
	F float64
}

func (p FPoint) String() string {
	s := strconv.FormatFloat(p.F, 'f', -1, 64)
	return fmt.Sprintf("%s @[%v]", s, p.T)
}

// MarshalJSON implements json.Marshaler.
//
// JSON marshaling is only needed for the HTTP API. Since FPoint is such a
// frequently marshaled type, it gets an optimized treatment directly in
// web/api/v1/api.go. Therefore, this method is unused within Prometheus. It is
// still provided here as convenience for debugging and for other users of this
// code. Also note that the different marshaling implementations might lead to
// slightly different results in terms of formatting and rounding of the
// timestamp.
func (p FPoint) MarshalJSON() ([]byte, error) {
	v := strconv.FormatFloat(p.F, 'f', -1, 64)
	return json.Marshal([...]interface{}{float64(p.T) / 1000, v})
}

// Sample is a single sample belonging to a metric. It represents either a float
// sample or a histogram sample. If H is nil, it is a float sample. Otherwise,
// it is a histogram sample.
type Sample struct {
	T int64   `msg:"t"`
	F float64 `msg:"f"`

	Metric labels.Labels `msg:"metric"`
}

func (s Sample) String() string {
	var str string
	p := FPoint{T: s.T, F: s.F}
	str = p.String()
	return fmt.Sprintf("%s => %s", s.Metric, str)
}

// MarshalJSON is mirrored in web/api/v1/api.go with jsoniter because FPoint and
// HPoint wouldn't be marshaled with jsoniter otherwise.
func (s Sample) MarshalJSON() ([]byte, error) {
	f := struct {
		M labels.Labels `json:"metric"`
		F FPoint        `json:"value"`
	}{
		M: s.Metric,
		F: FPoint{T: s.T, F: s.F},
	}
	return json.Marshal(f)
}

// Vector is basically only an alias for []Sample, but the contract is that
// in a Vector, all Samples have the same timestamp.
type Vector []Sample

func (vec Vector) String() string {
	entries := make([]string, len(vec))
	for i, s := range vec {
		entries[i] = s.String()
	}
	return strings.Join(entries, "\n")
}

// TotalSamples returns the total number of samples in the series within a vector.
// Float samples have a weight of 1 in this number, while histogram samples have a higher
// weight according to their size compared with the size of a float sample.
// See HPoint.size for details.
func (vec Vector) TotalSamples() int {
	return len(vec)
}

// ContainsSameLabelset checks if a vector has samples with the same labelset
// Such a behavior is semantically undefined
// https://github.com/prometheus/prometheus/issues/4562
func (vec Vector) ContainsSameLabelset() bool {
	switch len(vec) {
	case 0, 1:
		return false
	case 2:
		return vec[0].Metric.Hash() == vec[1].Metric.Hash()
	default:
		l := make(map[uint64]struct{}, len(vec))
		for _, ss := range vec {
			hash := ss.Metric.Hash()
			if _, ok := l[hash]; ok {
				return true
			}
			l[hash] = struct{}{}
		}
		return false
	}
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
// Float samples have a weight of 1 in this number, while histogram samples have a higher
// weight according to their size compared with the size of a float sample.
// See HPoint.size for details.
func (m Matrix) TotalSamples() int {
	numSamples := 0
	for _, series := range m {
		numSamples += len(series.Floats)
	}
	return numSamples
}

func (m Matrix) Len() int           { return len(m) }
func (m Matrix) Less(i, j int) bool { return labels.Compare(m[i].Metric, m[j].Metric) < 0 }
func (m Matrix) Swap(i, j int)      { m[i], m[j] = m[j], m[i] }

// ContainsSameLabelset checks if a matrix has samples with the same labelset.
// Such a behavior is semantically undefined.
// https://github.com/prometheus/prometheus/issues/4562
func (m Matrix) ContainsSameLabelset() bool {
	switch len(m) {
	case 0, 1:
		return false
	case 2:
		return m[0].Metric.Hash() == m[1].Metric.Hash()
	default:
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
}
