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
	"reflect"

	"github.com/openGemini/openGemini/lib/errno"
)

type Codec interface {
	Size() int
	Marshal([]byte) ([]byte, error)
	Unmarshal([]byte) error
	Instance() Codec
}

func ConvertToCodec(i interface{}) (Codec, error) {
	ret, ok := i.(Codec)
	if ret == nil || !ok {
		return nil, errno.NewError(errno.FailedConvertToCodec, reflect.TypeOf(i))
	}

	return ret, nil
}

func bufferResize(b []byte, size int) []byte {
	l := len(b)
	n := cap(b) - l

	if n >= size {
		return b
	}

	buf := make([]byte, l, size+l)
	copy(buf, b)
	return buf
}
