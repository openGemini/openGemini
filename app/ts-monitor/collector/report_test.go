// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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

package collector

import (
	"errors"
	"net/http"
	"testing"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/mocks"
	"github.com/stretchr/testify/assert"
)

func Test_Reporter_CreateDatabase(t *testing.T) {
	type TestCase struct {
		Name   string
		DoFunc func(req *http.Request) (*http.Response, error)
		expect func(err error) error
	}

	count := 0
	var testCases = []TestCase{
		{
			"create database ok",
			func(r *http.Request) (*http.Response, error) {
				resp := &http.Response{
					StatusCode: http.StatusOK,
					Body:       r.Body,
				}
				if count == 0 {
					count++
					resp2 := &http.Response{
						StatusCode: http.StatusContinue,
						Body:       r.Body,
					}
					return resp2, nil
				}
				return resp, nil
			},
			func(err error) error {
				return err
			},
		},
		{
			"create database error",
			func(r *http.Request) (*http.Response, error) {
				return nil, errors.New("auth error")
			},
			func(err error) error {
				return err
			},
		},
	}

	for _, tt := range testCases {
		mr := NewReportJob(logger.NewLogger(errno.ModuleUnknown), config.NewTSMonitor(), false, config.DefaultHistoryFile)

		t.Run(tt.Name, func(t *testing.T) {
			mr.Client = &mocks.MockClient{DoFunc: tt.DoFunc}
			err := mr.CreateDatabase()
			if err = tt.expect(err); err != nil {
				t.Error(err)
			}
		})
	}
}

func TestReportStat(t *testing.T) {
	file := "/data/test.log"
	stat := NewReportStat()

	for i := 0; i < maxRetry+1; i++ {
		assert.Equal(t, i < maxRetry, stat.TryAgain(file))
	}

	stat.Delete(file)
	assert.Equal(t, true, stat.TryAgain(file))
}
