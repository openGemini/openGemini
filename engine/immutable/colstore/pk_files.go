/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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

package colstore

import (
	"sync"

	"github.com/openGemini/openGemini/lib/fragment"
	"github.com/openGemini/openGemini/lib/record"
)

type PKInfo struct {
	rec  *record.Record
	mark fragment.IndexFragment
}

func (p *PKInfo) GetRec() *record.Record {
	return p.rec
}

func (p *PKInfo) GetMark() fragment.IndexFragment {
	return p.mark
}

type PKInfos map[string]*PKInfo

type PKFiles struct {
	mutex   sync.RWMutex
	pkInfos PKInfos
}

func NewPKFiles() *PKFiles {
	return &PKFiles{
		pkInfos: make(map[string]*PKInfo),
	}
}

func (f *PKFiles) SetPKInfo(file string, rec *record.Record, mark fragment.IndexFragment) {
	f.mutex.Lock()
	f.pkInfos[file] = &PKInfo{
		rec:  rec,
		mark: mark,
	}
	f.mutex.Unlock()
}

func (f *PKFiles) GetPKInfo(file string) (*PKInfo, bool) {
	f.mutex.RLock()
	ret, ok := f.pkInfos[file]
	f.mutex.RUnlock()
	return ret, ok
}

func (f *PKFiles) DelPKInfo(file string) {
	f.mutex.Lock()
	delete(f.pkInfos, file)
	f.mutex.Unlock()
}

func (f *PKFiles) GetPKInfos() PKInfos {
	f.mutex.RLock()
	ret := f.pkInfos
	f.mutex.RUnlock()
	return ret
}
