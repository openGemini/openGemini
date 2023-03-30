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

package opentelemetry

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/openGemini/openGemini/lib/cpu"
)

func TestOtelConext(t *testing.T) {
	var buf = []byte{1, 2, 3}
	reader := bufio.NewReader(bytes.NewReader(buf))

	var o []*otelConext

	for i := 0; i <= cpu.GetCpuNum()*2; i++ {
		octx := GetOtelContext(reader)
		o = append(o, octx)
	}

	for i := 0; i <= cpu.GetCpuNum()*2; i++ {
		PutOtelContext(o[i])
	}

	// get from chan
	for i := 0; i <= cpu.GetCpuNum(); i++ {
		GetOtelContext(reader)
	}
	// get from pool
	for i := 0; i <= cpu.GetCpuNum(); i++ {
		GetOtelContext(reader)
	}
}
