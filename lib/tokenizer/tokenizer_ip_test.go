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

package tokenizer

import (
	"net"
	"testing"

	"github.com/openGemini/openGemini/lib/logstore"
	"github.com/stretchr/testify/assert"
)

func TestIpTokenizer_CurrentHash(t *testing.T) {
	type fields struct {
		hashValue uint64
	}
	tests := []struct {
		name   string
		fields fields
		want   uint64
	}{
		{
			name: "CurrentHash base",
			fields: fields{
				hashValue: 1,
			},
			want: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			it := &IpTokenizer{
				hashValue: tt.fields.hashValue,
			}
			assert.Equalf(t, tt.want, it.CurrentHash(), "CurrentHash()")
		})
	}
}

func TestIpTokenizer_FreeSimpleGramTokenizer(t *testing.T) {
	type fields struct {
	}
	tests := []struct {
		name   string
		fields fields
	}{
		{
			name:   "FreeSimpleGramTokenizer base",
			fields: fields{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			it := NewIpTokenizer()
			it.FreeSimpleGramTokenizer()
		})
	}
}

func TestIpTokenizer_GetMatchedMaskIndex(t *testing.T) {
	type fields struct{}
	type args struct {
		mask net.IPMask
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   int
	}{
		{
			name:   "Ipv4/17 should return 16 --> 2",
			fields: fields{},
			args: args{
				mask: net.CIDRMask(17, 32),
			},
			want: 2,
		},
		{
			name:   "Ipv6/49 should return 48 --> 10",
			fields: fields{},
			args: args{
				mask: net.CIDRMask(49, 128),
			},
			want: 10,
		},
		{
			name:   "Ipv4/7 should return 0 --> -1",
			fields: fields{},
			args: args{
				mask: net.CIDRMask(7, 32),
			},
			want: -1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			it := NewIpTokenizer()
			assert.Equalf(t, tt.want, it.GetMatchedMaskIndex(tt.args.mask), "GetMatchedMaskIndex(%v)", tt.args.mask)
		})
	}
}

func TestIpTokenizer_HashWithMaskIndex(t *testing.T) {
	type fields struct{}
	type args struct {
		inputIP      *net.IP
		curMaskIndex int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   uint64
	}{
		{
			name:   "Hash with wrong index should return 0",
			fields: fields{},
			args: args{
				inputIP:      &net.IP{1, 2, 3, 4},
				curMaskIndex: -1,
			},
			want: 0,
		},
		{
			name:   "Hash with wrong index should return 0",
			fields: fields{},
			args: args{
				inputIP:      &net.IP{1, 2, 3, 4},
				curMaskIndex: 4,
			},
			want: 0,
		},
		{
			name:   "Hash IPv4 /24",
			fields: fields{},
			args: args{
				inputIP:      &net.IP{1, 2, 3, 4},
				curMaskIndex: 1,
			},
			want: 0x4f2ec56385519739,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			it := NewIpTokenizer()
			it.HashWithMaskIndex(tt.args.inputIP, tt.args.curMaskIndex)
			assert.Equalf(t, tt.want, it.CurrentHash(), "CurrentHash()")
		})
	}
}

func TestIpTokenizer_InitInput(t *testing.T) {
	type fields struct{}
	type args struct {
		bytes []byte
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name:   "InitInput base",
			fields: fields{},
			args: args{
				bytes: []byte("1.2.3.4"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			it := NewIpTokenizer()
			it.InitInput(tt.args.bytes)
			assert.NotNil(t, it.inputIP)
		})
	}
}

func TestIpTokenizer_Next(t *testing.T) {
	type fields struct {
		inputIP []byte
	}
	tests := []struct {
		name   string
		fields fields
		want   []uint64
	}{
		{
			name: "Ipv4 Next test",
			fields: fields{
				inputIP: []byte("1.2.3.4"),
			},
			want: []uint64{0x37f333b992fedd25, 0x4f2ec56385519739, 0xa6e03cfd0e6e6679, 0x424c2a7f4536a8cb},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			it := NewIpTokenizer()
			it.InitInput(tt.fields.inputIP)
			i := 0
			for it.Next() {
				assert.Equalf(t, tt.want[i], it.CurrentHash(), "Next()")
				i++
			}
			assert.Equalf(t, i, len(tt.want), "Next()")
		})
	}
}

func TestIpTokenizer_ProcessTokenizerBatch(t *testing.T) {
	type fields struct{}
	type args struct {
		input   []byte
		output  []byte
		offsets []int32
		lens    []int32
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   int
	}{
		{
			name:   "ipv4 process",
			fields: fields{},
			args: args{
				input:   []byte("1.2.3.4"),
				output:  make([]byte, logstore.LogStoreConstantV2.FilterDataMemSize),
				offsets: []int32{0},
				lens:    []int32{int32(len([]byte("1.2.3.4")))},
			},
			want: 0,
		},
		{
			name:   "ipv6 process",
			fields: fields{},
			args: args{
				input:   []byte("99fa:25a4:b87c:3d56:99b2:46d0:3c9e:17b1"),
				output:  make([]byte, logstore.LogStoreConstantV2.FilterDataMemSize),
				offsets: []int32{0},
				lens:    []int32{int32(len([]byte("99fa:25a4:b87c:3d56:99b2:46d0:3c9e:17b1")))},
			},
			want: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			it := NewIpTokenizer()
			assert.Equalf(t, tt.want, it.ProcessTokenizerBatch(tt.args.input, tt.args.output, tt.args.offsets, tt.args.lens), "ProcessTokenizerBatch(%v, %v, %v, %v)", tt.args.input, tt.args.output, tt.args.offsets, tt.args.lens)
		})
	}
}

func Test_selectMasksByIp(t *testing.T) {
	type args struct {
		inputIP net.IP
	}
	tests := []struct {
		name string
		args args
		want []net.IPMask
	}{
		{
			name: "selectMasksByIp(1.2.3.4) show return IPv4Masks",
			args: args{
				inputIP: net.IP{1, 2, 3, 4},
			},
			want: IPv4Masks,
		},
		{
			name: "selectMasksByIp(99fa:25a4:b87c:3d56:99b2:46d0:3c9e:17b1) should return IPv6Masks",
			args: args{
				inputIP: net.ParseIP("99fa:25a4:b87c:3d56:99b2:46d0:3c9e:17b1"),
			},
			want: IPv6Masks,
		},
		{
			name: "selectMasksByIp(99fa:25a4:b87c:3d56:99b2:46d0:3c9e:) should return nil",
			args: args{
				inputIP: net.ParseIP("99fa:25a4:b87c:3d56:99b2:46d0:3c9e:"),
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, selectMasksByIp(&tt.args.inputIP), "selectMasksByIp(%v)", tt.args.inputIP)
		})
	}
}
