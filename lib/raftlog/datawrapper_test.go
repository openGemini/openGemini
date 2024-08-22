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
package raftlog

import (
	"fmt"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/stretchr/testify/require"
)

func TestDataWrapper_Marshal(t *testing.T) {
	var dst []byte
	dst = encoding.MarshalUint64(dst, 1001)
	dataWrapper := &DataWrapper{
		DataType: Snapshot,
		Data:     dst,
	}

	dataType := dataWrapper.GetDataType()
	require.Equal(t, dataType, Snapshot)

	marshal := dataWrapper.Marshal()
	unmarshal, _ := Unmarshal(marshal)
	getDataType := unmarshal.GetDataType()
	require.Equal(t, getDataType, Snapshot)

	data := unmarshal.GetData()
	unmarshalUint64 := encoding.UnmarshalUint64(data)
	fmt.Println(unmarshalUint64)
	require.EqualValues(t, unmarshalUint64, 1001)
}
