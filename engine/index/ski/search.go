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

package ski

import (
	"io"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/mergeset"
)

type indexSearch struct {
	idx *ShardKeyIndex
	ts  mergeset.TableSearch
	kb  bytesutil.ByteBuffer
	mp  shardKeyToSeriesIdParser
}

func (is *indexSearch) hasItem(nsPrefix byte, key []byte) (bool, error) {
	ts := &is.ts
	kb := &is.kb

	kb.B = append(kb.B[:0], nsPrefix)
	kb.B = append(kb.B, key...)
	if err := ts.FirstItemWithPrefix(kb.B); err != nil {
		if err != io.EOF {
			return false, err
		}
		return false, nil
	}
	return len(ts.Item) == len(kb.B), nil
}
