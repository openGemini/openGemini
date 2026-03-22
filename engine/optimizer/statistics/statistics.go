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

package statistics

import (
	"bytes"
	"encoding/gob"
	"sort"
	"time"

	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
)

const DefaultSelectivity = 0.0

// StatisticsProvider is the interface for providing statistical information.
type StatisticsProvider interface {
	// GetTableStats retrieves table-level statistics for a given measurement.
	GetTableStats(measurement string) (*TableStatistics, error)

	// GetColumnStats retrieves column-level statistics for a specified measurement and column.
	GetColumnStats(measurement, column string) (*ColumnStatistics, error)

	// GetTagStats retrieves tag-level statistics for a specified measurement and tag.
	GetTagStats(measurement, tag string) (*TagStatistics, error)

	// GetJoinStats retrieves join operation statistics for specified left and right measurements with a condition.
	GetJoinStats(left, right string, condition influxql.Expr) (*JoinStatistics, error)

	// UpdateTableStats updates the table-level statistics for a given measurement.
	UpdateTableStats(measurement string, stats *TableStatistics) error

	// UpdateColumnStats updates the column-level statistics for a specified measurement and column.
	UpdateColumnStats(measurement, column string, stats *ColumnStatistics) error

	// UpdateTagStats updates the tag-level statistics for a specified measurement and tag.
	UpdateTagStats(measurement, tag string, stats *TagStatistics) error

	// SaveToStorage persists the statistical information to storage.
	SaveToStorage() error

	// LoadFromStorage loads the statistical information from storage.
	LoadFromStorage() error
}

// TableStatistics represents table-level statistical information.
type TableStatistics struct {
	Measurement   string
	RowCount      uint64
	LastUpdated   int64
	TagCount      int             // Number of tags
	FieldCount    int             // Number of fields
	SeriesCount   uint64          // Number of series
	AvgSeriesSize float64         // Average series size
	TimeRange     *TimeRangeStats // Time range statistics
}

// TimeRangeStats represents time range statistical information.
type TimeRangeStats struct {
	MinTime     int64
	MaxTime     int64
	TimeSpan    time.Duration // Time span
	AvgInterval float64       // Average time interval (in seconds)
}

// ColumnStatistics represents column-level statistical information.
type ColumnStatistics struct {
	ColumnName  string
	RowCount    uint64
	NullCount   uint64
	Cardinality uint64
	LastUpdated int64
	MinVal      interface{}
	MaxVal      interface{}
	Histogram   Histogram // Histogram
}

// TagStatistics represents tag-level statistical information.
type TagStatistics struct {
	TagName     string
	Cardinality uint64 // Cardinality
	LastUpdated int64
	TopValues   []TagValueFreq // Top values
}

// TagValueFreq represents the frequency of a tag value.
type TagValueFreq struct {
	Value interface{}
	Freq  float64 // Frequency proportion
	Count uint64  // Count
}

// JoinStatistics represents join operation statistical information.
type JoinStatistics struct {
	LeftRows      uint64
	RightRows     uint64
	EstimatedRows uint64  // Estimated number of join result rows
	Selectivity   float64 // Selectivity
}

// Histogram is the interface for a histogram.
type Histogram interface {
	// EstimateSelectivity estimates the selectivity of a given expression.
	EstimateSelectivity(expr influxql.Expr) float64

	// Update updates the histogram with a set of values.
	Update(values ...interface{})

	// Serialize serializes the histogram into a byte slice.
	Serialize() ([]byte, error)

	// Deserialize deserializes the histogram from a byte slice.
	Deserialize(data []byte) error
}

// NumericBin represents a bin in a numeric histogram.
type NumericBin struct {
	Min     float64
	Max     float64
	Count   uint64
	Density float64
}

// NumericHistogram represents a numeric histogram.
// All data should fall between the minimum value of the first bucket and the maximum value of the last bucket.
type NumericHistogram struct {
	Bins       []NumericBin // sorted by Min Max value
	TotalCount uint64
	Name       string
}

// EstimateSelectivity estimates the selectivity of a given expression using binary search and cumulative density.
func (h *NumericHistogram) EstimateSelectivity(expr influxql.Expr) float64 {
	be, ok := expr.(*influxql.BinaryExpr)
	if !ok {
		return DefaultSelectivity
	}
	switch be.Op {
	case influxql.LT, influxql.LTE:
		return h.estimateSelectivityForLessThan(be.LHS, be.RHS)
	case influxql.GT, influxql.GTE:
		return h.estimateSelectivityForGreaterThan(be.LHS, be.RHS)
	default:
		return DefaultSelectivity
	}
}

func (h *NumericHistogram) estimateSelectivityForGreaterThan(left, right influxql.Expr) float64 {
	ref, ok := left.(*influxql.VarRef)
	if !ok || ref.Val != h.Name {
		return DefaultSelectivity
	}
	val, ok := right.(*influxql.NumberLiteral)
	if !ok {
		return DefaultSelectivity
	}

	value := val.Val
	total := float64(h.TotalCount)
	if total == 0 {
		return DefaultSelectivity
	}

	// Use binary search to find the appropriate bin
	binIndex := findBinForGreaterThan(h.Bins, value)
	if binIndex == len(h.Bins) {
		// Value is above the last bin
		return 0.0
	}

	// Calculate cumulative density
	return calculateCumulativeDensityGreaterThan(h.Bins, binIndex, value)
}

func (h *NumericHistogram) estimateSelectivityForLessThan(left, right influxql.Expr) float64 {
	ref, ok := left.(*influxql.VarRef)
	if !ok || ref.Val != h.Name {
		return DefaultSelectivity
	}
	val, ok := right.(*influxql.NumberLiteral)
	if !ok {
		return DefaultSelectivity
	}

	value := val.Val
	total := float64(h.TotalCount)
	if total == 0 {
		return DefaultSelectivity
	}

	// Use binary search to find the appropriate bin
	binIndex := findBinForLessThan(h.Bins, value)
	if binIndex == len(h.Bins) {
		// Value is above the last bin
		return 1.0
	}
	// Calculate cumulative density
	return calculateCumulativeDensityLessThan(h.Bins, binIndex, value)
}

// findBinForGreaterThan finds the appropriate bin index for a value using binary search.
func findBinForGreaterThan(bins []NumericBin, value float64) int {
	return sort.Search(len(bins), func(i int) bool {
		return bins[i].Max >= value
	})
}

// findBinForLessThan finds the appropriate bin index for a value using binary search.
func findBinForLessThan(bins []NumericBin, value float64) int {
	return sort.Search(len(bins), func(i int) bool {
		return bins[i].Min >= value
	})
}

// calculateCumulativeDensityLessThan calculates the cumulative density
func calculateCumulativeDensityLessThan(bins []NumericBin, binIndex int, value float64) float64 {
	bin := bins[binIndex]
	// Calculate the cumulative density up to the lower bin
	cumulativeDensity := 0.0
	for _, b := range bins[:binIndex] {
		cumulativeDensity += b.Density
	}

	// Add the interpolated density between lower and upper bin
	rangeWidth := bin.Max - bin.Min
	if rangeWidth == 0 {
		return cumulativeDensity
	}
	cumulativeDensity += bin.Density * (value - bin.Min) / rangeWidth
	return cumulativeDensity
}

// calculateCumulativeDensityGreaterThan calculates the cumulative density
func calculateCumulativeDensityGreaterThan(bins []NumericBin, binIndex int, value float64) float64 {
	bin := bins[binIndex]
	cumulativeDensity := 0.0
	for _, b := range bins[binIndex+1:] {
		cumulativeDensity += b.Density
	}
	rangeWidth := bin.Max - bin.Min
	if rangeWidth == 0 {
		return cumulativeDensity
	}
	cumulativeDensity += bin.Density * (bin.Max - value) / rangeWidth
	return cumulativeDensity
}

// Update updates the histogram with a set of values.
func (h *NumericHistogram) Update(values ...interface{}) {
	for _, value := range values {
		v, ok := value.(float64)
		if !ok {
			continue
		}
		h.updateBin(v)
	}
	h.recalculateDensity()
}

func (h *NumericHistogram) recalculateDensity() {
	if h.TotalCount == 0 {
		return
	}
	for i := range h.Bins {
		h.Bins[i].Density = float64(h.Bins[i].Count) / float64(h.TotalCount)
	}
}

func (h *NumericHistogram) updateBin(value float64) {
	idx := sort.Search(len(h.Bins), func(i int) bool {
		return h.Bins[i].Max > value
	})
	if idx < len(h.Bins) && value >= h.Bins[idx].Min {
		h.Bins[idx].Count++
		h.TotalCount++
	}
}

// Serialize serializes the histogram into a byte slice.
func (h *NumericHistogram) Serialize() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(h)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Deserialize deserializes the histogram from a byte slice.
func (h *NumericHistogram) Deserialize(data []byte) error {
	dec := gob.NewDecoder(bytes.NewReader(data))
	return dec.Decode(h)
}
