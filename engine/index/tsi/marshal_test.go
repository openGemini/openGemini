// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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

package tsi

import (
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/stretchr/testify/assert"
)

func TestUnmarshalCompositeTagKey(t *testing.T) {
	src := []byte{0x81, 0x82}
	_, _, err := unmarshalCompositeTagKey(src)
	if err == nil {
		t.Fatal("unexpect")
	}
}

func TestParseItem(t *testing.T) {
	item := []byte{}
	_, err := ParseItem(item)
	assert.Error(t, err)

	item = append(item, nsPrefixKeyToTSID)
	item = encoding.MarshalUint64(item, 2375)
	_, err = ParseItem(item)
	assert.Error(t, err)

	item = append(item, kvSeparatorChar)
	_, err = ParseItem(item)
	assert.Error(t, err)

	src := marshalExample([]byte("test_0000"))
	item = []byte{}
	item = append(item, nsPrefixKeyToTSID)
	item = append(item, src...)
	item = append(item, kvSeparatorChar)
	item = encoding.MarshalUint64(item, 2375)
	_, err = ParseItem(item)
	assert.Error(t, err)

	item = []byte{}
	item = append(item, nsPrefixKeyToTSID)
	b := encoding.MarshalUint16(src, uint16(0))
	item = append(item, b...)
	item = append(item, kvSeparatorChar)
	item = encoding.MarshalUint64(item, 2375)
	_, err = ParseItem(item)
	assert.Nil(t, err)

	item = []byte{}
	item = append(item, nsPrefixTSIDToKey)
	item = encoding.MarshalUint64(item, 2375)
	item = append(item, src...)
	_, err = ParseItem(item)
	assert.Error(t, err)

	item = []byte{}
	item = append(item, nsPrefixTSIDToKey)
	item = encoding.MarshalUint64(item, 2375)
	item = append(item, b...)
	_, err = ParseItem(item)
	assert.Nil(t, err)

	item = []byte{}
	item = append(item, nsPrefixTagToTSIDs)
	item = marshalTagValue(item, marshalCompositeTagKey([]byte{}, []byte("test_0000"), nil))
	item = marshalTagValue(item, nil)
	item = encoding.MarshalUint64(item, 2375)
	item = append(item, b...)
	_, err = ParseItem(item)
	assert.Nil(t, err)
}

func marshalExample(name []byte) []byte {
	var src []byte
	src = encoding.MarshalUint32(src, uint32(6+len(name)))
	src = encoding.MarshalUint16(src, uint16(len(name)))
	src = append(src, name...)
	return src
}
