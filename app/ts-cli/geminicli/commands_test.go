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
	"reflect"
	"testing"
	"unsafe"

	"github.com/influxdata/influxdb/pkg/testing/assert"
)

func TestParser(t *testing.T) {
	gFlags := CommandLineConfig{}
	factory := CommandLineFactory{}
	cli, err := factory.CreateCommandLine(gFlags)
	assert.Equal(t, err, nil)

	for _, tc := range []struct {
		name      string
		cmd       string
		fieldName string // CommandLine.<fieldName>
		expect    interface{}
	}{
		{
			name:      "switch chunked model from disable to enable",
			cmd:       "chunked",
			fieldName: "chunked",
			expect:    true,
		},
		{
			name:      "switch chunked model from enable to disable",
			cmd:       "chunked",
			fieldName: "chunked",
			expect:    false,
		},
		{
			name:      "chunk size",
			cmd:       "chunk_size 1000",
			fieldName: "chunkSize",
			expect:    1000,
		},
		{
			name:      "switch timer model from disable to enable",
			cmd:       "timer",
			fieldName: "timer",
			expect:    true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err = cli.Execute(tc.cmd)
			assert.Equal(t, err, nil)

			cliValue := reflect.ValueOf(cli).Elem()
			fieldValue := cliValue.FieldByName(tc.fieldName)
			fieldValue = reflect.NewAt(fieldValue.Type(), unsafe.Pointer(fieldValue.UnsafeAddr())).Elem()
			assert.Equal(t, reflect.DeepEqual(fieldValue.Interface(), tc.expect), true)
		})
	}
}
