/*
Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.

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
package httpd

import (
	"strings"
	"testing"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/query"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/stretchr/testify/assert"
)

func TestGetAnalysisResults(t *testing.T) {
	resp := &Response{
		Results: []*query.Result{
			&query.Result{
				Series: models.Rows{
					&models.Row{
						Name:    "mst",
						Tags:    nil,
						Columns: []string{"key", "value"},
						Values: [][]interface{}{
							{111, "author", "mao"},
						},
					},
				},
			},
		},
	}
	r := strings.NewReader("select count(*) from mst")
	p := influxql.NewParser(r)
	YyParser := influxql.NewYyParser(p.GetScanner(), p.GetPara())
	YyParser.ParseTokens()

	q, err := YyParser.GetQuery()
	if err != nil {
		t.Error(err)
	}

	re := GetAnalysisResults(resp, q)
	assert.Equal(t, 2, len(re))
	assert.Equal(t, 3, len(re[0][0]))
}

func TestGetAnalysisResultsByTime(t *testing.T) {
	resp := &Response{
		Results: []*query.Result{
			&query.Result{
				Series: models.Rows{
					&models.Row{
						Name:    "mst",
						Tags:    nil,
						Columns: []string{"time", "key", "value"},
						Values: [][]interface{}{
							{111, "author", "mao"},
						},
					},
				},
			},
		},
	}
	r := strings.NewReader("select count(*) from mst")
	p := influxql.NewParser(r)
	YyParser := influxql.NewYyParser(p.GetScanner(), p.GetPara())
	YyParser.ParseTokens()

	q, err := YyParser.GetQuery()
	if err != nil {
		t.Error(err)
	}

	re := GetAnalysisResults(resp, q)
	assert.Equal(t, 2, len(re))
	assert.Equal(t, 3, len(re[0][0]))
}
