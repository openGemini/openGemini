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

package codec

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeString(t *testing.T) {
	var testCases = []struct {
		name     string
		data     []byte
		key      string
		expected string
		err      string
	}{
		{
			name:     "case1:normal",
			data:     []byte{0x00, 0x05, 'H', 'e', 'l', 'l', 'o'},
			key:      "test",
			expected: "Hello",
		},
		{
			name:     "case2:decoding of uint16 failed",
			data:     []byte{0x00},
			key:      "test",
			expected: "",
			err:      "too small data for test. expected 2 byte(s), only 1 byte(s)",
		},
		{
			name:     "case3:size check failed",
			data:     []byte{0x00, 0x05, 'H', 'e', 'l'},
			key:      "test",
			expected: "",
			err:      "too small data for test. expected 5 byte(s), only 3 byte(s)",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dec := &BinaryDecoder{buf: tc.data}
			result, err := DecodeString(dec, tc.key)
			require.Equal(t, tc.expected, result)
			if err != nil {
				require.Equal(t, tc.err, err.Error())
			}
		})
	}
}

func TestDecodeArray(t *testing.T) {
	var testCases = []struct {
		name     string
		data     []byte
		key      string
		cb       func(dec *BinaryDecoder) error
		expected string
	}{
		{
			name: "case1:normal",
			data: []byte{0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02},
			key:  "test",
			cb: func(dec *BinaryDecoder) error {
				_, err := DecodeInt32(dec, "item")
				return err
			},
		},
		{
			name: "case2:decoding of uint16 failed",
			data: []byte{0x00, 0x00, 0x00},
			key:  "test",
			cb: func(dec *BinaryDecoder) error {
				return nil
			},
			expected: "too small data for test. expected 4 byte(s), only 3 byte(s)",
		},
		{
			name: "case3:callback function failed",
			data: []byte{0x00, 0x00, 0x00, 0x02, 0x01},
			key:  "test",
			cb: func(dec *BinaryDecoder) error {
				return errors.New("callback error")
			},
			expected: "callback error",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dec := &BinaryDecoder{buf: tc.data}
			err := DecodeArray(dec, tc.key, tc.cb)
			if err != nil {
				require.Equal(t, tc.expected, err.Error())
			}
		})
	}
}

type TestStruct struct {
	Uint16Val uint16
	Int32Val  int32
	Int64Val  int64
}

func (ts *TestStruct) decode(dec *BinaryDecoder, key string) error {
	var err error
	ts.Uint16Val, err = DecodeUint16(dec, key)
	ts.Int32Val, err = DecodeInt32(dec, key)
	ts.Int64Val, err = DecodeInt64(dec, key)

	return err
}

func (ts *TestStruct) encode(dst []byte) []byte {
	dst = AppendUint16(dst, ts.Uint16Val)
	dst = AppendInt32(dst, ts.Int32Val)
	dst = AppendInt64(dst, ts.Int64Val)
	return dst
}

func TestDecode(t *testing.T) {
	normalInstance := TestStruct{
		Uint16Val: 0x1234,
		Int32Val:  -2147483648,
		Int64Val:  -9223372036854775808,
	}

	var normalBytes []byte
	normalBytes = normalInstance.encode(normalBytes)
	normalDecoder := &BinaryDecoder{buf: normalBytes}
	err := normalInstance.decode(normalDecoder, "test")
	require.NoError(t, err)

	emptyInstance := TestStruct{}
	var emptyBytes []byte
	emptyDecoder := &BinaryDecoder{buf: emptyBytes}
	err = emptyInstance.decode(emptyDecoder, "test")
	require.Error(t, err)
}
