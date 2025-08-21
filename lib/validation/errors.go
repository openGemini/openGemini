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

package validation

import (
	"fmt"
	"strings"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
	"github.com/prometheus/common/model"
)

// ValidationError is an error returned by series validation.
//
// nolint:golint ignore stutter warning
type ValidationError error

// genericValidationError is a basic implementation of ValidationError which can be used when the
// error format only contains the cause and the series.
type genericValidationError struct {
	message string
	cause   string
	series  []prompb.Label
}

func (e *genericValidationError) Error() string {
	return fmt.Sprintf(e.message, e.cause, formatLabelSet(e.series))
}

func newLabelNameTooLongError(series []prompb.Label, labelName string) ValidationError {
	return &genericValidationError{
		message: "label name too long: %.200q metric %.200q",
		cause:   labelName,
		series:  series,
	}
}

// labelValueTooLongError is a customized ValidationError, in that the cause and the series are
// are formatted in different order in Error.
type labelValueTooLongError struct {
	labelValue string
	series     []prompb.Label
}

func (e *labelValueTooLongError) Error() string {
	return fmt.Sprintf("label value too long for metric: %.200q label value: %.200q", formatLabelSet(e.series), e.labelValue)
}

func newLabelValueTooLongError(series []prompb.Label, labelValue string) ValidationError {
	return &labelValueTooLongError{
		labelValue: labelValue,
		series:     series,
	}
}

func newInvalidLabelError(series []prompb.Label, labelName string) ValidationError {
	return &genericValidationError{
		message: "sample invalid label: %.200q metric %.200q",
		cause:   labelName,
		series:  series,
	}
}

func newDuplicatedLabelError(series []prompb.Label, labelName string) ValidationError {
	return &genericValidationError{
		message: "duplicate label name: %.200q metric %.200q",
		cause:   labelName,
		series:  series,
	}
}

func newLabelsNotSortedError(series []prompb.Label, labelName string) ValidationError {
	return &genericValidationError{
		message: "labels not sorted: %.200q metric %.200q",
		cause:   labelName,
		series:  series,
	}
}

type tooManyLabelsError struct {
	series []prompb.Label
	limit  int
}

func newTooManyLabelsError(series []prompb.Label, limit int) ValidationError {
	return &tooManyLabelsError{
		series: series,
		limit:  limit,
	}
}

// LabelsToMetric converts a Labels to Metric
// Don't do this on any performance sensitive paths.
func LabelsToMetric(ls []prompb.Label) model.Metric {
	m := make(model.Metric, len(ls))
	for _, l := range ls {
		m[model.LabelName(l.Name)] = model.LabelValue(l.Value)
	}
	return m
}

func (e *tooManyLabelsError) Error() string {
	return fmt.Sprintf(
		"series has too many labels (actual: %d, limit: %d) series: '%s'",
		len(e.series), e.limit, LabelsToMetric(e.series).String())
}

type noMetricNameError struct{}

func newNoMetricNameError() ValidationError {
	return &noMetricNameError{}
}

func (e *noMetricNameError) Error() string {
	return "sample missing metric name"
}

type invalidMetricNameError struct {
	metricName string
}

func newInvalidMetricNameError(metricName string) ValidationError {
	return &invalidMetricNameError{
		metricName: metricName,
	}
}

func (e *invalidMetricNameError) Error() string {
	return fmt.Sprintf("sample invalid metric name: %.200q", e.metricName)
}

// sampleValidationError is a ValidationError implementation suitable for sample validation errors.
type sampleValidationError struct {
	message    string
	metricName string
	timestamp  int64
}

func (e *sampleValidationError) Error() string {
	return fmt.Sprintf(e.message, e.timestamp, e.metricName)
}

func newSampleTimestampTooOldError(metricName string, timestamp int64) ValidationError {
	return &sampleValidationError{
		message:    "timestamp too old: %d metric: %.200q",
		metricName: metricName,
		timestamp:  timestamp,
	}
}

func newSampleTimestampTooNewError(metricName string, timestamp int64) ValidationError {
	return &sampleValidationError{
		message:    "timestamp too new: %d metric: %.200q",
		metricName: metricName,
		timestamp:  timestamp,
	}
}

type noSampleError struct{}

func newNoSampleError() ValidationError {
	return &noSampleError{}
}

func (e *noSampleError) Error() string {
	return "timeseries have no sample"
}

// formatLabelSet formats label adapters as a metric name with labels, while preserving
// label order, and keeping duplicates. If there are multiple "__name__" labels, only
// first one is used as metric name, other ones will be included as regular labels.
func formatLabelSet(ls []prompb.Label) string {
	metricName, hasMetricName := "", false

	labelStrings := make([]string, 0, len(ls))
	for _, l := range ls {
		if string(l.Name) == model.MetricNameLabel && !hasMetricName && len(l.Value) != 0 {
			metricName = string(l.Value)
			hasMetricName = true
		} else {
			labelStrings = append(labelStrings, fmt.Sprintf("%s=%q", string(l.Name), string(l.Value)))
		}
	}

	if len(labelStrings) == 0 {
		if hasMetricName {
			return metricName
		}
		return "{}"
	}

	return fmt.Sprintf("%s{%s}", metricName, strings.Join(labelStrings, ", "))
}

type metadataNoMetricError struct{}

func newMetadataNoMetricError() ValidationError {
	return &metadataNoMetricError{}
}

func (e *metadataNoMetricError) Error() string {
	return "metadata missing metric name"
}

type metadataValidationError struct {
	message       string
	metadataType  string
	metadataValue string
	metricName    string
}

func (e *metadataValidationError) Error() string {
	return fmt.Sprintf(e.message, e.metadataType, e.metadataValue, e.metricName)
}

func newMetadataTooLong(metadataType, metadataValue, metricName string) ValidationError {
	return &metadataValidationError{
		message:       "metadata '%s' value too long: %.200q metric %.200q",
		metadataType:  metadataType,
		metadataValue: metadataValue,
		metricName:    metricName,
	}
}

type queryTooLongError struct {
	start time.Time
	end   time.Time
	limit time.Duration
}

func newQueryTooLongError(start, end time.Time, limit time.Duration) ValidationError {
	return &queryTooLongError{
		start: start,
		end:   end,
		limit: limit,
	}
}

func (e *queryTooLongError) Error() string {
	return fmt.Sprintf(
		"the query time range exceeds the limit (start: %v, end %v, query_len: %s, limit: %s)",
		e.start, e.end, e.end.Sub(e.start).String(), e.limit)
}
