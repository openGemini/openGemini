/*
Copyright (c) 2013-2018 InfluxData Inc.
This code is originally from: https://github.com/influxdata/influxdb/blob/1.8/services/graphite/parser_test.go

2026.11.11 Changed to fit openGemini:
- compare the returned Point instead of models.Point
- turn the filter matching tests into a table driven test
- add tests for Inf values, the -1 timestamp, fractional and out of range timestamps
Copyright 2026 openGemini Authors.
*/

package graphite_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/util/lifted/influx/graphite"
	"github.com/stretchr/testify/require"
)

func newParser(t *testing.T, templates []string, defaultTags map[string]string) *graphite.Parser {
	p, err := graphite.NewParserWithOptions(graphite.Options{
		Templates:   templates,
		DefaultTags: defaultTags,
		Separator:   graphite.DefaultSeparator,
	})
	require.NoError(t, err)
	return p
}

func BenchmarkParse(b *testing.B) {
	p, err := graphite.NewParserWithOptions(graphite.Options{
		Templates: []string{
			"*.* .wrong.measurement*",
			"servers.* .host.measurement*",
			"servers.localhost .host.measurement*",
			"*.localhost .host.measurement*",
			"*.*.cpu .host.measurement*",
			"a.b.c .host.measurement*",
			"influxd.*.foo .host.measurement*",
			"prod.*.mem .host.measurement*",
		},
		Separator: graphite.DefaultSeparator,
	})
	if err != nil {
		b.Fatalf("unexpected error creating parser, got %v", err)
	}

	for i := 0; i < b.N; i++ {
		_, _ = p.Parse("servers.localhost.cpu.load 11 1435077219")
	}
}

func TestTemplateApply(t *testing.T) {
	var tests = []struct {
		test        string
		input       string
		template    string
		measurement string
		tags        map[string]string
		field       string
		err         string
	}{
		{
			test:        "metric only",
			input:       "cpu",
			template:    "measurement",
			measurement: "cpu",
			tags:        map[string]string{},
		},
		{
			test:        "metric with single series",
			input:       "cpu.server01",
			template:    "measurement.hostname",
			measurement: "cpu",
			tags:        map[string]string{"hostname": "server01"},
		},
		{
			test:        "metric with multiple series",
			input:       "cpu.us-west.server01",
			template:    "measurement.region.hostname",
			measurement: "cpu",
			tags:        map[string]string{"hostname": "server01", "region": "us-west"},
		},
		{
			test:        "metric with multiple tags",
			input:       "server01.example.org.cpu.us-west",
			template:    "hostname.hostname.hostname.measurement.region",
			measurement: "cpu",
			tags:        map[string]string{"hostname": "server01.example.org", "region": "us-west"},
		},
		{
			test: "no metric",
			err:  `no measurement specified for template. ""`,
		},
		{
			test:        "ignore unnamed",
			input:       "foo.cpu",
			template:    "measurement",
			measurement: "foo",
			tags:        map[string]string{},
		},
		{
			test:        "name shorter than template",
			input:       "foo",
			template:    "measurement.A.B.C",
			measurement: "foo",
			tags:        map[string]string{},
		},
		{
			test:        "wildcard measurement at end",
			input:       "prod.us-west.server01.cpu.load",
			template:    "env.zone.host.measurement*",
			measurement: "cpu.load",
			tags:        map[string]string{"env": "prod", "zone": "us-west", "host": "server01"},
		},
		{
			test:        "skip fields",
			input:       "ignore.us-west.ignore-this-too.cpu.load",
			template:    ".zone..measurement*",
			measurement: "cpu.load",
			tags:        map[string]string{"zone": "us-west"},
		},
		{
			test:        "conjoined fields",
			input:       "prod.us-west.server01.cpu.util.idle.percent",
			template:    "env.zone.host.measurement.measurement.field*",
			measurement: "cpu.util",
			tags:        map[string]string{"env": "prod", "zone": "us-west", "host": "server01"},
			field:       "idle.percent",
		},
		{
			test:        "single field",
			input:       "current.users.logged_in",
			template:    "measurement.measurement.field",
			measurement: "current.users",
			tags:        map[string]string{},
			field:       "logged_in",
		},
		{
			test:     "field used twice",
			input:    "current.users.logged_in",
			template: "measurement.field.field",
			err:      `'field' can only be used once in each template: "current.users.logged_in"`,
		},
	}

	for _, test := range tests {
		t.Run(test.test, func(t *testing.T) {
			tmpl, err := graphite.NewTemplate(test.template, nil, graphite.DefaultSeparator)
			if test.template == "" {
				require.EqualError(t, err, test.err)
				return
			}
			require.NoError(t, err)

			measurement, tags, field, err := tmpl.Apply(test.input)
			if test.err != "" {
				require.EqualError(t, err, test.err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.measurement, measurement)
			require.Equal(t, test.tags, tags)
			require.Equal(t, test.field, field)
		})
	}
}

func TestParseMissingMeasurement(t *testing.T) {
	_, err := graphite.NewParserWithOptions(graphite.Options{Templates: []string{"a.b.c"}})
	require.Error(t, err)
}

func TestParse(t *testing.T) {
	testTime := time.Now().Round(time.Second)
	strTime := strconv.FormatInt(testTime.Unix(), 10)

	var tests = []struct {
		test        string
		input       string
		template    string
		measurement string
		tags        map[string]string
		field       string
		value       float64
		time        time.Time
		err         string
	}{
		{
			test:        "normal case",
			input:       `cpu.foo.bar 50 ` + strTime,
			template:    "measurement.foo.bar",
			measurement: "cpu",
			tags:        map[string]string{"foo": "foo", "bar": "bar"},
			field:       "value",
			value:       50,
			time:        testTime,
		},
		{
			test:        "metric only with float value",
			input:       `cpu 50.554 ` + strTime,
			template:    "measurement",
			measurement: "cpu",
			tags:        map[string]string{},
			field:       "value",
			value:       50.554,
			time:        testTime,
		},
		{
			test:        "field from template",
			input:       `cpu.idle 99.5 ` + strTime,
			template:    "measurement.field",
			measurement: "cpu",
			tags:        map[string]string{},
			field:       "idle",
			value:       99.5,
			time:        testTime,
		},
		{
			test:        "fractional timestamp",
			input:       `cpu 1 1435077219.5`,
			template:    "measurement",
			measurement: "cpu",
			tags:        map[string]string{},
			field:       "value",
			value:       1,
			time:        time.Unix(1435077219, int64(500*time.Millisecond)),
		},
		{
			test:     "missing metric",
			input:    `1419972457825`,
			template: "measurement",
			err:      `received "1419972457825" which doesn't have required fields`,
		},
		{
			test:     "should error parsing invalid float",
			input:    `cpu 50.554z 1419972457825`,
			template: "measurement",
			err:      `field "cpu" value: strconv.ParseFloat: parsing "50.554z": invalid syntax`,
		},
		{
			test:     "should error parsing invalid int",
			input:    `cpu 50z 1419972457825`,
			template: "measurement",
			err:      `field "cpu" value: strconv.ParseFloat: parsing "50z": invalid syntax`,
		},
		{
			test:     "should error parsing invalid time",
			input:    `cpu 50.554 14199724z57825`,
			template: "measurement",
			err:      `field "cpu" time: strconv.ParseFloat: parsing "14199724z57825": invalid syntax`,
		},
		{
			test:     "should error on timestamp out of range",
			input:    `cpu 50.554 1419972457825`,
			template: "measurement",
			err:      `timestamp out of range`,
		},
		{
			test:     "measurement* and field* (invalid)",
			input:    `prod.us-west.server01.cpu.util.idle.percent 99.99 1419972457825`,
			template: "env.zone.host.measurement*.field*",
			err:      `either 'field*' or 'measurement*' can be used in each template (but not both together): "env.zone.host.measurement*.field*"`,
		},
	}

	for _, test := range tests {
		t.Run(test.test, func(t *testing.T) {
			p := newParser(t, []string{test.template}, nil)

			point, err := p.Parse(test.input)
			if test.err != "" {
				require.EqualError(t, err, test.err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.measurement, point.Measurement)
			require.Equal(t, test.tags, point.Tags)
			require.Equal(t, test.field, point.Field)
			require.Equal(t, test.value, point.Value)
			require.Equal(t, test.time.UnixNano(), point.Timestamp)
		})
	}
}

func TestParseCurrentTime(t *testing.T) {
	p := newParser(t, nil, nil)

	for _, line := range []string{"cpu 1", "cpu 1 -1"} {
		before := time.Now().UnixNano()
		point, err := p.Parse(line)
		require.NoError(t, err)
		require.GreaterOrEqual(t, point.Timestamp, before)
		require.LessOrEqual(t, point.Timestamp, time.Now().UnixNano())
	}
}

func TestParseUnsupportedValue(t *testing.T) {
	p := newParser(t, []string{"measurement*"}, nil)

	for _, value := range []string{"NaN", "Inf", "-Inf"} {
		_, err := p.Parse("servers.localhost.cpu_load " + value + " 1435077219")
		var unsupported *graphite.UnsupportedValueError
		require.ErrorAs(t, err, &unsupported)
		require.Equal(t, "servers.localhost.cpu_load", unsupported.Field)
	}
}

func TestParseTemplates(t *testing.T) {
	var tests = []struct {
		test        string
		templates   []string
		separator   string
		defaultTags map[string]string
		input       string
		measurement string
		tags        map[string]string
	}{
		{
			test:        "filter does not match, use default template",
			templates:   []string{"servers.localhost .host.measurement*"},
			input:       "miss.servers.localhost.cpu_load 11 1435077219",
			measurement: "miss.servers.localhost.cpu_load",
			tags:        map[string]string{},
		},
		{
			test:        "multiple measurement parts",
			templates:   []string{"servers.localhost .host.measurement.measurement*"},
			input:       "servers.localhost.cpu.cpu_load.10 11 1435077219",
			measurement: "cpu.cpu_load.10",
			tags:        map[string]string{"host": "localhost"},
		},
		{
			test:        "multiple measurement parts with separator",
			templates:   []string{"servers.localhost .host.measurement.measurement*"},
			separator:   "_",
			input:       "servers.localhost.cpu.cpu_load.10 11 1435077219",
			measurement: "cpu_cpu_load_10",
			tags:        map[string]string{"host": "localhost"},
		},
		{
			test:        "single filter",
			templates:   []string{"servers.localhost .host.measurement*"},
			input:       "servers.localhost.cpu_load 11 1435077219",
			measurement: "cpu_load",
			tags:        map[string]string{"host": "localhost"},
		},
		{
			test:        "no match",
			templates:   []string{"servers.*.cpu .host.measurement.cpu.measurement"},
			input:       "servers.localhost.memory.VmallocChunk 11 1435077219",
			measurement: "servers.localhost.memory.VmallocChunk",
			tags:        map[string]string{},
		},
		{
			test:        "wildcard filter",
			templates:   []string{"servers.* .host.measurement*"},
			input:       "servers.localhost.cpu_load 11 1435077219",
			measurement: "cpu_load",
			tags:        map[string]string{"host": "localhost"},
		},
		{
			test: "exact match before wildcard",
			templates: []string{
				"servers.* .wrong.measurement*",
				"servers.localhost .host.measurement*",
			},
			input:       "servers.localhost.cpu_load 11 1435077219",
			measurement: "cpu_load",
			tags:        map[string]string{"host": "localhost"},
		},
		{
			test: "longest filter",
			templates: []string{
				"*.* .wrong.measurement*",
				"servers.* .wrong.measurement*",
				"servers.localhost .wrong.measurement*",
				"servers.localhost.cpu .host.resource.measurement*",
				"*.localhost .wrong.measurement*",
			},
			input:       "servers.localhost.cpu.cpu_load 11 1435077219",
			measurement: "cpu_load",
			tags:        map[string]string{"host": "localhost", "resource": "cpu"},
		},
		{
			test: "multiple wildcards",
			templates: []string{
				"*.* .wrong.measurement*",
				"servers.* .host.measurement*",
				"servers.localhost .wrong.measurement*",
				"*.localhost .wrong.measurement*",
			},
			input:       "servers.server01.cpu_load 11 1435077219",
			measurement: "cpu_load",
			tags:        map[string]string{"host": "server01"},
		},
		{
			test: "most specific template",
			templates: []string{
				"current.* measurement.measurement",
				"current.*.* measurement.measurement.service",
			},
			separator:   "_",
			input:       "current.users.facebook 11 1435077219",
			measurement: "current_users",
			tags:        map[string]string{"service": "facebook"},
		},
		{
			test: "most specific template does not apply",
			templates: []string{
				"current.* measurement.service",
				"current.*.*.test measurement.measurement.service",
			},
			separator:   "_",
			input:       "current.users.facebook 11 1435077219",
			measurement: "current",
			tags:        map[string]string{"service": "users"},
		},
		{
			test:        "default tags",
			templates:   []string{"servers.localhost .host.measurement*"},
			defaultTags: map[string]string{"region": "us-east", "zone": "1c", "host": "should not set"},
			input:       "servers.localhost.cpu_load 11 1435077219",
			measurement: "cpu_load",
			tags:        map[string]string{"host": "localhost", "region": "us-east", "zone": "1c"},
		},
		{
			test:        "template tags",
			templates:   []string{"servers.localhost .host.measurement* zone=1c"},
			defaultTags: map[string]string{"region": "us-east", "host": "should not set"},
			input:       "servers.localhost.cpu_load 11 1435077219",
			measurement: "cpu_load",
			tags:        map[string]string{"host": "localhost", "region": "us-east", "zone": "1c"},
		},
		{
			test:        "template tags override default tags",
			templates:   []string{"servers.localhost .host.measurement* zone=1c,region=us-east"},
			defaultTags: map[string]string{"region": "should not be set", "host": "should not set"},
			input:       "servers.localhost.cpu_load 11 1435077219",
			measurement: "cpu_load",
			tags:        map[string]string{"host": "localhost", "region": "us-east", "zone": "1c"},
		},
		{
			test:        "template tags without filter",
			templates:   []string{"measurement.measurement region=us-west"},
			separator:   "_",
			input:       "current.users 11 1435077219",
			measurement: "current_users",
			tags:        map[string]string{"region": "us-west"},
		},
		{
			test:        "whitespace in template",
			templates:   []string{"servers.localhost        .host.measurement*           zone=1c"},
			defaultTags: map[string]string{"region": "us-east", "host": "should not set"},
			input:       "servers.localhost.cpu_load 11 1435077219",
			measurement: "cpu_load",
			tags:        map[string]string{"host": "localhost", "region": "us-east", "zone": "1c"},
		},
	}

	for _, test := range tests {
		t.Run(test.test, func(t *testing.T) {
			separator := test.separator
			if separator == "" {
				separator = graphite.DefaultSeparator
			}
			p, err := graphite.NewParserWithOptions(graphite.Options{
				Templates:   test.templates,
				DefaultTags: test.defaultTags,
				Separator:   separator,
			})
			require.NoError(t, err)

			point, err := p.Parse(test.input)
			require.NoError(t, err)
			require.Equal(t, graphite.Point{
				Measurement: test.measurement,
				Tags:        test.tags,
				Field:       graphite.DefaultField,
				Value:       11,
				Timestamp:   time.Unix(1435077219, 0).UnixNano(),
			}, point)
		})
	}
}
