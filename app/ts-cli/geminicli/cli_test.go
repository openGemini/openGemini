/*
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package geminicli

import (
	"context"
	"testing"
	"time"

	"github.com/influxdata/influxdb/client"
	"github.com/openGemini/openGemini/app/ts-cli/geminiql"
	"github.com/stretchr/testify/require"
)

type mockClient struct{}

func (m mockClient) Ping() (time.Duration, string, error) {
	return 0, "", nil
}

func (m mockClient) QueryContext(ctx context.Context, query client.Query) (*client.Response, error) {
	return nil, nil
}

func (m mockClient) Write(bp client.BatchPoints) (*client.Response, error) {
	return nil, nil
}

func (m mockClient) SetPrecision(precision string) {}

func TestPrecisionParser(t *testing.T) {
	gFlags := CommandLineConfig{}
	factory := CommandLineFactory{}
	gFlags.Host = "127.0.0.1"
	gFlags.Port = 8086
	cli, err := factory.CreateCommandLine(gFlags)
	require.Equal(t, err, nil)
	cli.clientCreator = func(config client.Config) (HttpClient, error) {
		return &mockClient{}, nil
	}
	mockedClient, _ := cli.clientCreator(client.Config{})
	cli.client = mockedClient

	for _, tc := range []struct {
		name   string
		stmt   geminiql.Statement
		expect string
	}{
		{
			name: "Precision rfc3339",
			stmt: &geminiql.PrecisionStatement{
				Precision: "rfc3339",
			},
			expect: "",
		},
		{
			name: "Precision ns",
			stmt: &geminiql.PrecisionStatement{
				Precision: "ns",
			},
			expect: "ns",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err = cli.executeOnLocal(tc.stmt)
			require.NoError(t, err)
			require.Equal(t, tc.expect, cli.config.Precision)
		})
	}

	// test for failed
	for _, tc := range []struct {
		name   string
		stmt   geminiql.Statement
		expect string
	}{
		{
			name: "Precision none",
			stmt: &geminiql.PrecisionStatement{
				Precision: "none",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err = cli.executeOnLocal(tc.stmt)
			require.EqualError(t, err, "precision must be rfc3339, h, m, s, ms, u or ns")
		})
	}

}
