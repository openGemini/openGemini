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
package tokenClvc

type FreqList struct {
	token string
	freq  int
}

func (f *FreqList) Token() string {
	return f.token
}

func (f *FreqList) SetToken(token string) {
	f.token = token
}

func (f *FreqList) Freq() int {
	return f.freq
}

func (f *FreqList) SetFreq(freq int) {
	f.freq = freq
}

func NewFreqList(token string, freq int) *FreqList {
	return &FreqList{
		token: token,
		freq:  freq,
	}
}
