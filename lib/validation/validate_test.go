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
	"testing"
	"time"

	prompb2 "github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const userID = "testUser"

func StubInitOverrides() {
	limits := config.Limits{
		PromLimitEnabled:          true,
		MaxLabelNameLength:        25,   // Maximum length accepted for label names.
		MaxLabelValueLength:       25,   // Maximum length accepted for label value. This setting also applies to the metric name.
		MaxLabelNamesPerSeries:    3,    // Maximum number of label names per series.
		MaxMetadataLength:         22,   // Maximum length accepted for metric metadata. Metadata refers to Metric Name, HELP and UNIT.
		RejectOldSamples:          true, // Reject old samples.
		EnforceMetadataMetricName: true, // Enforce every sample has a metric name.
		EnforceMetricName:         true, // Enforce every metadata has a metric name.
		MaxQueryLength:            model.Duration(10 * time.Second),
	}
	limits.RejectOldSamplesMaxAge.Set("24h") // Maximum accepted sample age before rejecting.
	limits.CreationGracePeriod.Set("2h")
	InitOverrides(limits, nil)
}

func TestValidateLabels(t *testing.T) {
	StubInitOverrides()

	for _, c := range []struct {
		desc string
		ls   []prompb2.Label
		err  error
	}{
		{
			"no metric name",
			[]prompb2.Label{},
			newNoMetricNameError(),
		},
		{
			"invalid metric name",
			[]prompb2.Label{
				{Name: model.MetricNameLabel, Value: " "},
			},
			newInvalidMetricNameError(" "),
		},
		{
			"invalid metric value",
			[]prompb2.Label{
				{Name: model.MetricNameLabel, Value: "valid"},
				{Name: "foo ", Value: "bar"},
			},
			newInvalidLabelError([]prompb2.Label{
				{Name: model.MetricNameLabel, Value: "valid"},
				{Name: "foo ", Value: "bar"},
			}, "foo "),
		},
		{
			"valid labels",
			[]prompb2.Label{
				{Name: model.MetricNameLabel, Value: "valid"},
				{Name: "foo", Value: "bar"},
			},
			nil,
		},
		{
			"long label name",
			[]prompb2.Label{
				{Name: model.MetricNameLabel, Value: "badLabelName"},
				{Name: "this_is_a_really_really_long_name_that_should_cause_an_error", Value: "test_value_please_ignore"},
			},
			newLabelNameTooLongError([]prompb2.Label{
				{Name: model.MetricNameLabel, Value: "badLabelName"},
				{Name: "this_is_a_really_really_long_name_that_should_cause_an_error", Value: "test_value_please_ignore"},
			}, "this_is_a_really_really_long_name_that_should_cause_an_error"),
		},
		{
			"long label value",
			[]prompb2.Label{
				{Name: model.MetricNameLabel, Value: "badLabelValue"},
				{Name: "much_shorter_name", Value: "test_value_please_ignore_no_really_nothing_to_see_here"},
			},
			newLabelValueTooLongError([]prompb2.Label{
				{Name: model.MetricNameLabel, Value: "badLabelValue"},
				{Name: "much_shorter_name", Value: "test_value_please_ignore_no_really_nothing_to_see_here"},
			}, "test_value_please_ignore_no_really_nothing_to_see_here"),
		},
		{
			"too many labels",
			[]prompb2.Label{
				{Name: model.MetricNameLabel, Value: "foo"},
				{Name: "bar", Value: "bar"},
				{Name: "blip", Value: "blop"},
				{Name: "cccc", Value: "ccccc"},
			},
			newTooManyLabelsError([]prompb2.Label{
				{Name: model.MetricNameLabel, Value: "foo"},
				{Name: "bar", Value: "bar"},
				{Name: "blip", Value: "blop"},
				{Name: "cccc", Value: "ccccc"},
			}, 3),
		},
		{
			"unordered labels",
			[]prompb2.Label{
				{Name: model.MetricNameLabel, Value: "m"},
				{Name: "b", Value: "b"},
				{Name: "a", Value: "a"},
			},
			newLabelsNotSortedError([]prompb2.Label{
				{Name: model.MetricNameLabel, Value: "m"},
				{Name: "b", Value: "b"},
				{Name: "a", Value: "a"},
			}, "a"),
		},
		{
			"duplicate labels",
			[]prompb2.Label{
				{Name: model.MetricNameLabel, Value: "a"},
				{Name: model.MetricNameLabel, Value: "b"},
			},
			newDuplicatedLabelError([]prompb2.Label{
				{Name: model.MetricNameLabel, Value: "a"},
				{Name: model.MetricNameLabel, Value: "b"},
			}, model.MetricNameLabel),
		},
	} {
		t.Run(c.desc, func(t *testing.T) {
			err := ValidateLabels(userID, c.ls, MetricNameFromLabels(c.ls))
			assert.Equal(t, c.err, err, "ValidateSeries error")
		})
	}
}

func TestValidateMetadata(t *testing.T) {
	StubInitOverrides()

	for _, c := range []struct {
		desc     string
		metadata *prompb.MetricMetadata
		errMsg   string
	}{
		{
			"with a valid config",
			&prompb.MetricMetadata{MetricFamilyName: "go_goroutines", Type: prompb.MetricMetadata_COUNTER, Help: "Number of goroutines.", Unit: ""},
			"",
		},
		{
			"with no metric name",
			&prompb.MetricMetadata{MetricFamilyName: "", Type: prompb.MetricMetadata_COUNTER, Help: "Number of goroutines.", Unit: ""},
			"metadata missing metric name",
		},
		{
			"with a long metric name",
			&prompb.MetricMetadata{MetricFamilyName: "go_goroutines_and_routines_and_routines", Type: prompb.MetricMetadata_COUNTER, Help: "Number of goroutines.", Unit: ""},
			"metadata 'METRIC_NAME' value too long: \"go_goroutines_and_routines_and_routines\" metric \"go_goroutines_and_routines_and_routines\"",
		},
		{
			"with a long help",
			&prompb.MetricMetadata{MetricFamilyName: "go_goroutines", Type: prompb.MetricMetadata_COUNTER, Help: "Number of goroutines that currently exist.", Unit: ""},
			"metadata 'HELP' value too long: \"Number of goroutines that currently exist.\" metric \"go_goroutines\"",
		},
		{
			"with a long unit",
			&prompb.MetricMetadata{MetricFamilyName: "go_goroutines", Type: prompb.MetricMetadata_COUNTER, Help: "Number of goroutines.", Unit: "a_made_up_unit_that_is_really_long"},
			"metadata 'UNIT' value too long: \"a_made_up_unit_that_is_really_long\" metric \"go_goroutines\"",
		},
	} {
		t.Run(c.desc, func(t *testing.T) {
			err := ValidateMetadata(userID, c.metadata)
			if len(c.errMsg) != 0 {
				assert.Equal(t, c.errMsg, err.Error(), "ValidateMetadata error")
			} else {
				assert.Nil(t, err)
			}
		})
	}
}

func TestValidateSeries(t *testing.T) {
	StubInitOverrides()

	now := model.Now()
	future, past := now.Add(5*time.Hour), now.Add(-25*time.Hour)
	for _, c := range []struct {
		desc   string
		ts     prompb2.TimeSeries
		errMsg string
	}{
		{
			"validation pass",
			prompb2.TimeSeries{
				Labels: []prompb2.Label{
					{Name: model.MetricNameLabel, Value: "testmetric"},
					{Name: "foo", Value: "bar"},
				},
				Samples: []prompb2.Sample{
					{Value: 1, Timestamp: int64(now)},
				},
			},
			"",
		},
		{
			"validation fails for no sample",
			prompb2.TimeSeries{
				Labels: []prompb2.Label{
					{Name: model.MetricNameLabel, Value: "testmetric"},
					{Name: "foo", Value: "bar"},
				},
				Samples: []prompb2.Sample{},
			},
			"timeseries have no sample",
		},
		{
			"validation fails for very old samples",
			prompb2.TimeSeries{
				Labels: []prompb2.Label{
					{Name: model.MetricNameLabel, Value: "testmetric"},
					{Name: "foo", Value: "bar"},
				},
				Samples: []prompb2.Sample{
					{Value: 2, Timestamp: int64(past)},
				},
			},
			fmt.Sprintf("timestamp too old: %d metric: \"testmetric\"", past),
		},
		{
			"validation fails for samples from the future",
			prompb2.TimeSeries{
				Labels: []prompb2.Label{
					{Name: model.MetricNameLabel, Value: "testmetric"},
					{Name: "foo", Value: "bar"},
				},
				Samples: []prompb2.Sample{
					{Value: 4, Timestamp: int64(future)},
				},
			},
			fmt.Sprintf("timestamp too new: %d metric: \"testmetric\"", future),
		},
		{
			"maximum labels names per series",
			prompb2.TimeSeries{
				Labels: []prompb2.Label{
					{Name: model.MetricNameLabel, Value: "testmetric"},
					{Name: "foo", Value: "bar"},
					{Name: "foo2", Value: "bar2"},
					{Name: "foo3", Value: "bar3"},
				},
				Samples: []prompb2.Sample{
					{Value: 2, Timestamp: int64(now)},
				},
			},
			`series has too many labels (actual: 4, limit: 3) series: 'testmetric{foo2="bar2", foo3="bar3", foo="bar"}'`,
		},
		{
			"too long label value",
			prompb2.TimeSeries{
				Labels: []prompb2.Label{
					{Name: model.MetricNameLabel, Value: "testmetricasdaaaaaaaaaaaaaaaaaaaaaaaaaa"},
				},
				Samples: []prompb2.Sample{
					{Value: 2, Timestamp: int64(now)},
				},
			},
			`label value too long for metric: "testmetricasdaaaaaaaaaaaaaaaaaaaaaaaaaa" label value: "testmetricasdaaaaaaaaaaaaaaaaaaaaaaaaaa"`,
		},
	} {
		t.Run(c.desc, func(t *testing.T) {
			err := ValidateSeries(userID, &c.ts)
			if len(c.errMsg) != 0 {
				assert.Equal(t, c.errMsg, err.Error(), "ValidateMetadata error")
			} else {
				assert.Nil(t, err)
			}
		})
	}
}

func TestValidateQueryTimeRange(t *testing.T) {
	StubInitOverrides()

	// valid time range
	start := time.Unix(1, 0)
	validEnd := time.Unix(10, 0) // 9s
	err := ValidateQueryTimeRange("", start, validEnd)
	require.NoError(t, err)
	// invalid time range
	exceedEnd := time.Unix(12, 0) // 11s
	err = ValidateQueryTimeRange("", start, exceedEnd)
	require.Equal(t, "the query time range exceeds the limit (start: 1970-01-01 08:00:01 +0800 CST, end 1970-01-01 08:00:12 +0800 CST, query_len: 11s, limit: 10s)", err.Error())

	// no time range limits
	limits.defaultLimits.MaxQueryLength = 0
	err = ValidateQueryTimeRange("", start, exceedEnd)
	require.NoError(t, err)
}

func TestValidateNoLimits(t *testing.T) {
	limits = nil
	ts := prompb2.TimeSeries{
		Labels: []prompb2.Label{
			{Name: model.MetricNameLabel, Value: "testmetric"},
			{Name: "foo", Value: "bar"},
		},
		Samples: []prompb2.Sample{
			{Value: 1, Timestamp: int64(model.Now())},
		},
	}
	metadata := prompb.MetricMetadata{MetricFamilyName: "go_goroutines", Type: prompb.MetricMetadata_COUNTER, Help: "Number of goroutines.", Unit: ""}
	require.NotNil(t, Limits())
	require.Nil(t, ValidateSample("", "test", ts.Samples[0]))
	require.Nil(t, ValidateLabels("", ts.Labels, "test"))
	require.Nil(t, ValidateMetadata("test", &metadata))
	require.Nil(t, ValidateSeries("test", &ts))
	require.Nil(t, ValidateQueryTimeRange("test", time.Unix(1, 0), time.Unix(10, 0)))
}
