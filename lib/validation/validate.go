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
	"bytes"
	"time"

	prompb2 "github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
)

const (
	typeMetricName = "METRIC_NAME"
	typeHelp       = "HELP"
	typeUnit       = "UNIT"

	metricNameTooLong = "metric_name_too_long"
	helpTooLong       = "help_too_long"
	unitTooLong       = "unit_too_long"
)

// MetricNameFromLabels extracts the metric name from a list of LabelPairs.
// The returned metric name string is a reference to the label value (no copy).
func MetricNameFromLabels(labels []prompb2.Label) string {
	for _, label := range labels {
		if util.Bytes2str(label.Name) == model.MetricNameLabel {
			return util.Bytes2str(label.Value)
		}
	}
	return ""
}

// ValidateSample returns an err if the sample is invalid.
// The returned error may retain the provided series labels.
func ValidateSample(userID string, metricName string, s prompb2.Sample) ValidationError {
	if limits == nil {
		return nil
	}

	if limits.RejectOldSamples(userID) && model.Time(s.Timestamp) < model.Now().Add(-limits.RejectOldSamplesMaxAge(userID)) {
		return newSampleTimestampTooOldError(metricName, s.Timestamp)
	}

	creationGracePeriod := limits.CreationGracePeriod(userID)
	if creationGracePeriod != 0 && model.Time(s.Timestamp) > model.Now().Add(creationGracePeriod) {
		return newSampleTimestampTooNewError(metricName, s.Timestamp)
	}

	return nil
}

// ValidateLabels returns an err if the labels are invalid.
// The returned error may retain the provided series labels.
func ValidateLabels(userID string, ls []prompb2.Label, unsafeMetricName string) ValidationError {
	if limits == nil {
		return nil
	}

	if limits.EnforceMetricName(userID) {
		if len(unsafeMetricName) == 0 {
			return newNoMetricNameError()
		}

		if !model.IsValidMetricName(model.LabelValue(unsafeMetricName)) {
			return newInvalidMetricNameError(unsafeMetricName)
		}
	}

	numLabelNames := len(ls)
	if numLabelNames > limits.MaxLabelNamesPerSeries(userID) {
		return newTooManyLabelsError(ls, limits.MaxLabelNamesPerSeries(userID))
	}

	maxLabelNameLength := limits.MaxLabelNameLength(userID)
	maxLabelValueLength := limits.MaxLabelValueLength(userID)
	lastLabelName := []byte{}
	for _, l := range ls {
		if !model.LabelName(l.Name).IsValid() {
			return newInvalidLabelError(ls, l.Name)
		} else if len(l.Name) > maxLabelNameLength {
			return newLabelNameTooLongError(ls, l.Name)
		} else if len(l.Value) > maxLabelValueLength {
			return newLabelValueTooLongError(ls, l.Value)
		} else if cmp := bytes.Compare(lastLabelName, l.Name); cmp >= 0 {
			if cmp == 0 {
				return newDuplicatedLabelError(ls, l.Name)
			}

			return newLabelsNotSortedError(ls, l.Name)
		}

		lastLabelName = l.Name
	}
	return nil
}

// ValidateMetadata returns an err if a metric metadata is invalid.
func ValidateMetadata(userID string, metadata *prompb.MetricMetadata) ValidationError {
	if limits == nil {
		return nil
	}

	if limits.EnforceMetadataMetricName(userID) && metadata.GetMetricFamilyName() == "" {
		return newMetadataNoMetricError()
	}

	maxMetadataValueLength := limits.MaxMetadataLength(userID)
	var reason string
	var cause string
	var metadataType string
	if len(metadata.GetMetricFamilyName()) > maxMetadataValueLength {
		metadataType = typeMetricName
		reason = metricNameTooLong
		cause = metadata.GetMetricFamilyName()
	} else if len(metadata.Help) > maxMetadataValueLength {
		metadataType = typeHelp
		reason = helpTooLong
		cause = metadata.Help
	} else if len(metadata.Unit) > maxMetadataValueLength {
		metadataType = typeUnit
		reason = unitTooLong
		cause = metadata.Unit
	}

	if reason != "" {
		return newMetadataTooLong(metadataType, cause, metadata.GetMetricFamilyName())
	}

	return nil
}

// Validates a single series from a write request.
func ValidateSeries(userID string, ts *prompb2.TimeSeries) ValidationError {
	if limits == nil {
		return nil
	}

	unsafeMetricName := MetricNameFromLabels(ts.Labels)

	if err := ValidateLabels(userID, ts.Labels, unsafeMetricName); err != nil {
		return err
	}

	if len(ts.Samples) > 0 {
		for _, s := range ts.Samples {
			if err := ValidateSample(userID, unsafeMetricName, s); err != nil {
				return err
			}
		}
	} else {
		return newNoSampleError()
	}

	return nil
}

// validate query time range.
func ValidateQueryTimeRange(userID string, startTime, endTime time.Time) ValidationError {
	if limits == nil {
		return nil
	}
	maxQueryLength := limits.MaxQueryLength(userID)
	if maxQueryLength > 0 && endTime.Sub(startTime) > maxQueryLength {
		return newQueryTooLongError(startTime, endTime, maxQueryLength)
	}
	return nil
}
