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

package transport

import (
	"github.com/openGemini/openGemini/engine/executor/spdy"
)

type Requester struct {
	spdy.BaseRequester
	data  interface{}
	codec Codec
	typ   uint8
}

func NewRequester(session *spdy.MultiplexedSession, typ uint8, seq uint64, codec Codec) spdy.Requester {
	req := &Requester{
		codec: codec,
		typ:   typ,
	}
	req.InitDerive(session, seq, req)
	return req
}

func (req *Requester) Data() interface{} {
	return req.data
}

func (req *Requester) SetData(data interface{}) {
	req.data = data
}

func (req *Requester) Encode(dst []byte, data interface{}) ([]byte, error) {
	codec, err := ConvertToCodec(data)
	if err != nil {
		return nil, err
	}

	dst = bufferResize(dst, codec.Size())
	return codec.Marshal(dst)
}

func (req *Requester) Decode(data []byte) (interface{}, error) {
	ret := req.codec.Instance()
	err := ret.Unmarshal(data)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func (req *Requester) Type() uint8 {
	return req.typ
}

func (req *Requester) WarpResponser() spdy.Responser {
	return NewResponser(req.Session(), req.Type(), req.Sequence())
}
