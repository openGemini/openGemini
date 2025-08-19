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

package protocol_test

import (
	"strings"
	"testing"

	"github.com/openGemini/openGemini/lib/codec"
	"github.com/openGemini/openGemini/services/consume/kafka/protocol"
	"github.com/stretchr/testify/require"
)

func TestRequestFetchV2(t *testing.T) {
	fetch := protocol.RequestFetchV2{}

	bb := &BufferBuilder{}
	bb.Add(bb.nothing, func() {
		bb.appendUint32()
		bb.appendUint32()
		bb.appendUint32()
		bb.buf = codec.AppendUint32(bb.buf, 1)
		bb.buf = codec.AppendUint16(bb.buf, 20)
		bb.appendString()
	},
		bb.appendString,
		bb.appendUint32,
		bb.appendUint32,
		bb.appendUint64,
	)

	for i, fn := range bb.fns {
		fn()
		require.Error(t, fetch.Unmarshal(bb.Decoder()), "step %d", i)
	}

	bb.appendUint32()
	require.NoError(t, fetch.Unmarshal(bb.Decoder()))
}

func TestRequestPartitionOffsetV1(t *testing.T) {
	req := protocol.RequestPartitionOffsetV1{}
	bb := &BufferBuilder{}
	bb.Add(bb.nothing, func() {
		bb.buf = codec.AppendUint32(bb.buf, 0)
		bb.buf = codec.AppendUint32(bb.buf, 1)
		bb.buf = codec.AppendUint16(bb.buf, 20)
		bb.appendString()
	},
		bb.appendString,
		bb.appendUint32,
		bb.appendUint32,
	)

	for i, fn := range bb.fns {
		fn()
		require.Error(t, req.Unmarshal(bb.Decoder()), "step %d", i)
	}

	bb.buf = codec.AppendUint64(bb.buf, 1)
	require.NoError(t, req.Unmarshal(bb.Decoder()))
}

func TestRequestOffsetCommitV2(t *testing.T) {
	req := protocol.RequestOffsetCommitV2{}
	bb := &BufferBuilder{}
	bb.Add(bb.nothing,
		bb.appendString,
		bb.appendInt32,
		bb.appendString,
		bb.appendInt64,
	)

	for i, fn := range bb.fns {
		fn()
		require.Error(t, req.Unmarshal(bb.Decoder()), "step %d", i)
	}
}

func TestRequestOffsetCommitV2Topic(t *testing.T) {
	req := protocol.RequestOffsetCommitV2Topic{}
	bb := &BufferBuilder{}
	bb.Add(bb.nothing, bb.appendString)

	for i, fn := range bb.fns {
		fn()
		require.Error(t, req.Unmarshal(bb.Decoder()), "step %d", i)
	}
}

func TestRequestOffsetCommitV2Partition(t *testing.T) {
	req := protocol.RequestOffsetCommitV2Partition{}
	bb := &BufferBuilder{}
	bb.Add(bb.nothing, func() {
		bb.appendInt32()
		bb.appendInt64()
		bb.buf = codec.AppendUint16(bb.buf, 10)
	},
	)

	for i, fn := range bb.fns {
		fn()
		require.Error(t, req.Unmarshal(bb.Decoder()), "step %d", i)
	}
}

func TestRequestHeader(t *testing.T) {
	req := protocol.RequestHeader{}
	bb := &BufferBuilder{}
	bb.Add(bb.nothing, func() {
		bb.buf = codec.AppendUint16(bb.buf, 1)
		bb.buf = codec.AppendUint16(bb.buf, 1)
		bb.buf = codec.AppendUint32(bb.buf, 1)
		bb.buf = codec.AppendUint16(bb.buf, 20)
		bb.appendString()
	})

	for i, fn := range bb.fns {
		fn()
		require.Error(t, req.Unmarshal(bb.Decoder()), "step %d", i)
	}

	bb.appendString()
	require.NoError(t, req.Unmarshal(bb.Decoder()))
}

func TestResponseApiVersion(t *testing.T) {
	ver := &protocol.ResponseApiVersion{
		Versions: []protocol.ApiVersion{
			{1, 1, 1},
		},
	}

	ver.ThrottleTime = -1
	buf := ver.Marshal(nil)
	require.Equal(t, 16, len(buf))

	ver.ThrottleTime = 1
	buf = ver.Marshal(nil)
	require.Equal(t, 20, len(buf))
}

type BufferBuilder struct {
	dec codec.BinaryDecoder
	buf []byte
	fns []func()
}

func (b *BufferBuilder) Decoder() *codec.BinaryDecoder {
	b.dec.Reset(b.buf)
	return &b.dec
}

func (b *BufferBuilder) Add(fn ...func()) {
	b.fns = append(b.fns, fn...)
}

func (b *BufferBuilder) nothing() {

}

func (b *BufferBuilder) appendUint32() {
	b.buf = codec.AppendUint32(b.buf, 1)
}

func (b *BufferBuilder) appendInt32() {
	b.buf = codec.AppendInt32(b.buf, 1)
}

func (b *BufferBuilder) appendString() {
	b.buf = append(b.buf, strings.Repeat("0", 10)...)
}

func (b *BufferBuilder) appendUint64() {
	b.buf = codec.AppendUint64(b.buf, 1)
}

func (b *BufferBuilder) appendInt64() {
	b.buf = codec.AppendInt64(b.buf, 1)
}
