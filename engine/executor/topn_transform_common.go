// Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.
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

package executor

import "bytes"

type TCounter float64

const EulerNumber float64 = 2.7182818284
const beginNumber uint64 = 1

func CalcLog2(value float64) int {
	res := 0
	for float64((beginNumber << res)) < value {
		res++
	}
	return res
}

type FrequentResult struct {
	keys   []TKey
	values []TCounter
}

func (fr *FrequentResult) reSize(size int) {
	if size < len(fr.keys) {
		fr.keys = fr.keys[:size]
		fr.values = fr.values[:size]
	}
}

func (fr *FrequentResult) Len() int {
	return len(fr.values)
}

func (fr *FrequentResult) Less(i, j int) bool {
	return fr.values[i] > (fr.values[j])
}

func (fr *FrequentResult) Swap(i, j int) {
	tmpk := fr.keys[i]
	fr.keys[i] = fr.keys[j]
	fr.keys[j] = tmpk
	tmpv := fr.values[i]
	fr.values[i] = fr.values[j]
	fr.values[j] = tmpv
}

type TKey struct {
	Data []byte
}

func (k *TKey) Less(o TKey) bool {
	return bytes.Compare(k.Data, o.Data) < 0
}

func (k *TKey) Copy() *TKey {
	re := &TKey{
		Data: make([]byte, 0, len(k.Data)),
	}
	re.Data = append(re.Data, k.Data...)
	return re
}

func (k *TKey) GetSize() int {
	return len(k.Data) << 3
}

// [] func
func (k *TKey) Get(i int) bool {
	return (k.Data[i>>3] & (1 << (i & 7))) != 0
}

func (k *TKey) GetByte(i int) byte {
	return k.Data[i]
}

func (k *TKey) ChangeBit(i int) {
	k.Data[i>>3] ^= (1 << (i & 7))
}

// var DefaultSigma float64 = 1e-3
// var DefaultDelta float64 = 1e-3
var DefaultPhi float64 = 3e-5
var DefaultEps float64 = DefaultPhi / 10

var DefaultSigma float64 = 1e-8
var DefaultDelta float64 = 1e-8

func init() {
	DefaultEps = DefaultPhi / 10
}

func B2ui64(b bool) uint64 {
	if b {
		return 1
	}
	return 0
}
