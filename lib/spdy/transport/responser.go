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

package transport

import (
	"github.com/openGemini/openGemini/lib/spdy"
)

type Responser struct {
	spdy.BaseResponser
	callback Callback
	typ      uint8
}

func NewResponser(session *spdy.MultiplexedSession, typ uint8, seq uint64) spdy.Responser {
	rsp := &Responser{
		typ: typ,
	}
	rsp.InitDerive(session, seq, rsp)
	return rsp
}

func (rsp *Responser) SetCallback(c Callback) {
	rsp.callback = c
}

func (rsp *Responser) Callback(data interface{}) error {
	if rsp.callback == nil {
		return nil
	}

	return rsp.callback.Handle(data)
}

func (rsp *Responser) Type() uint8 {
	return rsp.typ
}

func (rsp *Responser) Encode(dst []byte, data interface{}) ([]byte, error) {
	codec, err := ConvertToCodec(data)
	if err != nil {
		return nil, err
	}

	dst = bufferResize(dst, codec.Size())
	return codec.Marshal(dst)
}

func (rsp *Responser) Decode(data []byte) (interface{}, error) {
	if rsp.callback == nil {
		return nil, nil
	}

	ret := rsp.callback.GetCodec()
	err := ret.Unmarshal(data)
	if err != nil {
		return nil, err
	}

	return ret, nil
}
