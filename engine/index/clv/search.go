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

package clv

import (
	"bytes"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/mergeset"
)

type tokenSearch struct {
	tbs mergeset.TableSearch
	kb  bytesutil.ByteBuffer
}

func (ts *tokenSearch) searchDicVersion() uint32 {
	tbs := &ts.tbs
	kb := &ts.kb

	var version uint32
	kb.B = append(kb.B[:0], txPrefixDicVersion)
	tbs.Seek(kb.B)
	for tbs.NextItem() {
		if !bytes.HasPrefix(tbs.Item, kb.B) {
			// Nothing found.
			break
		}
		version = unmarshaDiclVersion(tbs.Item)
	}

	return version
}

// Only query items with the same token.
func (ts *tokenSearch) searchInvertIndexByVtoken(vtoken string, invert *InvertIndex) {
	tbs := &ts.tbs
	kb := &ts.kb

	// e.g., PrefixPos + "get" + " " + "token" + " "+ Suffix
	kb.B = append(kb.B[:0], txPrefixPos)
	kb.B = append(kb.B, []byte(vtoken)...)
	kb.B = append(kb.B, txSuffix)

	tbs.Seek(kb.B)
	for tbs.NextItem() {
		if !bytes.HasPrefix(tbs.Item, kb.B) {
			break
		}
		unmarshal(tbs.Item, invert)
	}
}

// Query items prefixed with token.
func (ts *tokenSearch) searchInvertIndexByPrefixVtoken(vtoken string, invert *InvertIndex) {
	tbs := &ts.tbs
	kb := &ts.kb

	// e.g., PPrefixPos + "get" + " " + "token" + " "
	kb.B = append(kb.B[:0], txPrefixPos)
	kb.B = append(kb.B, []byte(vtoken)...)

	tbs.Seek(kb.B)
	for tbs.NextItem() {
		if !bytes.HasPrefix(tbs.Item, kb.B) {
			break
		}
		unmarshal(tbs.Item, invert)
	}
}
