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

package clv

import (
	"bytes"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/mergeset"
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
func (ts *tokenSearch) searchInvertIndexByVtoken(vtoken string, invert *InvertIndex, offset uint16) {
	tbs := &ts.tbs
	kb := &ts.kb

	// e.g., PrefixPos + "get" + " " + "token" + " "+ Suffix
	kb.B = append(kb.B[:0], txPrefixPos)
	kb.B = append(kb.B, vtoken...)
	kb.B = append(kb.B, txSuffix)

	tbs.Seek(kb.B)
	for tbs.NextItem() {
		if !bytes.HasPrefix(tbs.Item, kb.B) {
			break
		}
		unmarshal(tbs.Item, invert, offset)
	}
}

// Query items prefixed with token.
func (ts *tokenSearch) searchInvertIndexByPrefixVtoken(vtoken string, invert *InvertIndex, offset uint16) {
	tbs := &ts.tbs
	kb := &ts.kb

	// e.g., PPrefixPos + "get" + " " + "token" + " "
	kb.B = append(kb.B[:0], txPrefixPos)
	kb.B = append(kb.B, vtoken...)

	tbs.Seek(kb.B)
	for tbs.NextItem() {
		if !bytes.HasPrefix(tbs.Item, kb.B) {
			break
		}
		unmarshal(tbs.Item, invert, offset)
	}
}

func (ts *tokenSearch) searchTermsIndex(term string, filter ...func(b []byte) bool) []string {
	var fn func(b []byte) bool
	if len(filter) == 1 {
		fn = filter[0]
	} else if len(filter) > 1 {
		logger.Errorf("only one apply function is supported in fulltext-index")
		return nil
	}

	tbs := &ts.tbs
	kb := &ts.kb

	kb.B = append(kb.B[:0], txPrefixTerm)
	kb.B = append(kb.B, term...)
	var terms []string
	tbs.Seek(kb.B)
	for tbs.NextItem() {
		if !bytes.HasPrefix(tbs.Item, kb.B) {
			break
		}

		t := tbs.Item[1:] // remove prefix
		if fn != nil && !fn(t) {
			continue
		}

		terms = append(terms, string(t))
	}

	return terms
}
