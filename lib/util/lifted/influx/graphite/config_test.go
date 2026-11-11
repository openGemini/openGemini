/*
Copyright (c) 2013-2018 InfluxData Inc.
This code is originally from: https://github.com/influxdata/influxdb/blob/1.8/services/graphite/config_test.go

2026.11.11 Changed to fit openGemini:
- turn the validation tests into a table driven test
- add tests for the defaults, the protocol and the batch options
Copyright 2026 openGemini Authors.
*/

package graphite_test

import (
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	itoml "github.com/influxdata/influxdb/toml"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/graphite"
	"github.com/stretchr/testify/require"
)

func TestConfig_Parse(t *testing.T) {
	var c graphite.Config
	_, err := toml.Decode(`
bind-address = ":8080"
database = "mydb"
retention-policy = "myrp"
enabled = true
protocol = "udp"
batch-size=100
batch-pending=77
batch-timeout="2s"
templates=["servers.* .host.measurement*"]
tags=["region=us-east"]
separator = "_"
udp-read-buffer = 1024
`, &c)
	require.NoError(t, err)

	require.Equal(t, graphite.Config{
		Enabled:         true,
		BindAddress:     ":8080",
		Database:        "mydb",
		RetentionPolicy: "myrp",
		Protocol:        "udp",
		BatchSize:       100,
		BatchPending:    77,
		BatchTimeout:    itoml.Duration(2 * time.Second),
		Templates:       []string{"servers.* .host.measurement*"},
		Tags:            []string{"region=us-east"},
		Separator:       "_",
		UDPReadBuffer:   1024,
	}, c)
	require.NoError(t, c.Validate())
	require.Equal(t, map[string]string{"region": "us-east"}, c.DefaultTags())
}

func TestConfig_WithDefaults(t *testing.T) {
	c := &graphite.Config{Enabled: true}
	d := c.WithDefaults()

	expected := graphite.NewConfig()
	expected.Enabled = true
	require.Equal(t, &expected, d)
	// the original config is left untouched
	require.Equal(t, &graphite.Config{Enabled: true}, c)
	require.NoError(t, d.Validate())
}

func TestConfig_Validate(t *testing.T) {
	var tests = []struct {
		name   string
		config graphite.Config
		err    string
	}{
		{name: "unknown protocol", config: graphite.Config{Protocol: "http"}, err: `unrecognized protocol "http", must be tcp or udp`},
		{name: "upper case protocol", config: graphite.Config{Protocol: "UDP"}},
		{name: "negative batch size", config: graphite.Config{BatchSize: -1}, err: "batch-size must be greater than or equal to 0"},
		{name: "negative batch pending", config: graphite.Config{BatchPending: -1}, err: "batch-pending must be greater than or equal to 0"},
		{name: "negative batch timeout", config: graphite.Config{BatchTimeout: itoml.Duration(-time.Second)}, err: "batch-timeout must be greater than or equal to 0s"},
		{name: "negative udp read buffer", config: graphite.Config{UDPReadBuffer: -1}, err: "udp-read-buffer must be greater than or equal to 0"},
		{name: "empty template", config: graphite.Config{Templates: []string{""}}, err: "missing template at position: 0"},
		{name: "blank template", config: graphite.Config{Templates: []string{"     "}}, err: "missing template at position: 0"},
		{name: "too many template parts", config: graphite.Config{Templates: []string{"a measurement b c"}}, err: "invalid template format: 'a measurement b c'"},
		{name: "measurement with prefix wildcard", config: graphite.Config{Templates: []string{"*measurement"}}, err: "no measurement in template `*measurement`"},
		{name: "no measurement", config: graphite.Config{Templates: []string{".host.region"}}, err: "no measurement in template `.host.region`"},
		{name: "filter starts with blank section", config: graphite.Config{Templates: []string{".server measurement*"}}, err: "filter contains blank section: .server"},
		{name: "filter with blank sections", config: graphite.Config{Templates: []string{".    .server measurement*"}}, err: "no measurement in template `.server`"},
		{name: "filter with partial wildcard", config: graphite.Config{Templates: []string{"server* measurement*"}}, err: "invalid filter wildcard section: server*"},
		{name: "template tag without value", config: graphite.Config{Templates: []string{"*.server measurement* foo"}}, err: "invalid template tags: 'foo'"},
		{name: "template tag with two equals", config: graphite.Config{Templates: []string{"*.server measurement* foo=bar="}}, err: "invalid template tags: 'foo=bar='"},
		{name: "template tags with trailing comma", config: graphite.Config{Templates: []string{"*.server measurement* foo=bar,"}}, err: "invalid template tags: ''"},
		{name: "template tag without key and value", config: graphite.Config{Templates: []string{"*.server measurement* ="}}, err: "invalid template tags: '='"},
		{name: "default tag without value", config: graphite.Config{Tags: []string{"foo"}}, err: "invalid template tags: 'foo'"},
		{name: "default tag with two equals", config: graphite.Config{Tags: []string{"foo=bar="}}, err: "invalid template tags: 'foo=bar='"},
		{name: "empty default tag", config: graphite.Config{Tags: []string{"foo=bar", ""}}, err: "invalid template tags: ''"},
		{name: "default tag without key and value", config: graphite.Config{Tags: []string{"="}}, err: "invalid template tags: '='"},
		{name: "duplicate filters", config: graphite.Config{Templates: []string{"foo measurement*", "foo .host.measurement"}}, err: "duplicate filter 'foo' found at position: 1"},
		{name: "duplicate default templates", config: graphite.Config{Templates: []string{"measurement*", ".host.measurement"}}, err: "duplicate filter '' found at position: 1"},
		{
			name: "valid templates and tags",
			config: graphite.Config{
				Templates: []string{
					"*.app env.service.resource.measurement",
					"stats.* .host.measurement* region=us-west,agent=statsd",
					"servers.localhost .host.measurement.field",
					"measurement*",
				},
				Tags: []string{"region=us-east", "zone=1c"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.config.Validate()
			if test.err == "" {
				require.NoError(t, err)
				return
			}
			require.EqualError(t, err, test.err)
		})
	}
}
