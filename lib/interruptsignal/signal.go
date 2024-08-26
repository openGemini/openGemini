// Copyright Huawei Cloud Computing Technologies Co., Ltd.
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

package interruptsignal

type InterruptSignal struct {
	sig chan struct{}
}

func NewInterruptSignal() *InterruptSignal {
	return &InterruptSignal{
		sig: make(chan struct{}),
	}
}

func (i *InterruptSignal) Closed() bool {
	select {
	case <-i.sig:
		return true
	default:
		return false
	}
}

func (i *InterruptSignal) Signal() <-chan struct{} {
	return i.sig
}

func (i *InterruptSignal) Close() {
	select {
	case <-i.sig:
	default:
		close(i.sig)
	}
}
