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

package tsi

import (
	"errors"
	"regexp"
	"strings"
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"github.com/openGemini/openGemini/open_src/vm/uint64set"
	"go.uber.org/zap"
)

type tagSets struct {
	tagsArray [][]influx.Tag
}

func (ts *tagSets) reset() {
	ts.tagsArray = ts.tagsArray[:0]
}

func (ts *tagSets) resize(rowCount int, colCount int) {
	ts.resizeRow(rowCount)
	ts.resizeColumn(colCount)
}

func (ts *tagSets) resizeRow(rowCount int) {
	if cap(ts.tagsArray) > rowCount {
		ts.tagsArray = ts.tagsArray[:rowCount]
	} else {
		delta := rowCount - cap(ts.tagsArray)
		ts.tagsArray = ts.tagsArray[:cap(ts.tagsArray)]
		ts.tagsArray = append(ts.tagsArray, make([][]influx.Tag, delta)...)
	}
}

func (ts *tagSets) resizeColumn(colCount int) {
	for i := range ts.tagsArray {
		if cap(ts.tagsArray[i]) > colCount {
			ts.tagsArray[i] = ts.tagsArray[i][:colCount]
		} else {
			delta := colCount - cap(ts.tagsArray[i])
			ts.tagsArray[i] = ts.tagsArray[i][:cap(ts.tagsArray[i])]
			ts.tagsArray[i] = append(ts.tagsArray[i], make([]influx.Tag, delta)...)
		}
	}
}

type TagSetsPool struct {
	p sync.Pool
}

func (pool *TagSetsPool) Get() *tagSets {
	sets := pool.p.Get()
	if sets == nil {
		return &tagSets{}
	}
	return sets.(*tagSets)
}

func (pool *TagSetsPool) Put(tags *tagSets) {
	tags.reset()
	pool.p.Put(tags)
}

/*
	eg, inputTags:{{Key: "tk1", Value: "[tv1,tv11]", IsArray: 0},
					{Key: "tk2", Value: "[tv2,tv22]", IsArray: 0},
					{Key: "tk3", Value: "[tv3,tv33]", IsArray: 0}}

	tagArray:[[{Key: "tk1", Value: "tv1", IsArray: 0},
             {Key: "tk2", Value: "tv2", IsArray: 0},
	         {Key: "tk3", Value: "tv3", IsArray: 0},]
			 [{Key: "tk1", Value: "tv11", IsArray: 0},
             {Key: "tk2", Value: "tv22", IsArray: 0},
	         {Key: "tk3", Value: "tv33", IsArray: 0},]]

*/
func analyzeTagSets(dstTagSets *tagSets, tags []influx.Tag) error {
	var arrayLen int
	var arrayCount int
	for i := range tags {
		if tags[i].IsArray {
			arrayLen = strings.Count(tags[i].Value, ",") + 1
			arrayCount++
		}
	}
	if arrayCount > 1 {
		return errno.NewError(errno.WriteMultiArray)
	}

	dstTagSets.resize(arrayLen, len(tags))

	for cIndex := range tags {
		if tags[cIndex].IsArray {
			values := strings.Split(tags[cIndex].Value[1:len(tags[cIndex].Value)-1], ",")
			for rIndex := range values {
				if len(values[rIndex]) == 0 {
					return errno.NewError(errno.WriteErrorArray)
				}
				dstTagSets.tagsArray[rIndex][cIndex].Key = tags[cIndex].Key
				dstTagSets.tagsArray[rIndex][cIndex].Value = values[rIndex]
			}
		} else {
			for rIndex := 0; rIndex < arrayLen; rIndex++ {
				dstTagSets.tagsArray[rIndex][cIndex].Key = tags[cIndex].Key
				dstTagSets.tagsArray[rIndex][cIndex].Value = tags[cIndex].Value
			}
		}
	}
	return nil
}

func getTagsSizeAndLen(tags []influx.Tag) (int, int) {
	var tagSize int
	var tagLen int
	for i := range tags {
		if len(tags[i].Value) != 0 {
			tagSize += tags[i].Size()
			tagLen++
		}
	}

	return tagLen, tagSize
}

func unmarshalIndexKeys(name []byte, tags []influx.Tag, indexkeypool []byte) ([]byte, int, bool) {
	indexKl := 4 + // total length of indexkey
		2 + // measurment name length
		len(name) + // measurment name with version
		2 // tag count
	tagLen, tagSize := getTagsSizeAndLen(tags)
	indexKl += 4 * tagLen // length of each tag key and value
	indexKl += tagSize    // size of tag keys/values
	start := len(indexkeypool)

	// marshal total len
	indexkeypool = encoding.MarshalUint32(indexkeypool, uint32(indexKl))
	// marshal measurement
	indexkeypool = encoding.MarshalUint16(indexkeypool, uint16(len(name)))
	indexkeypool = append(indexkeypool, name...)
	// marshal tags
	indexkeypool = encoding.MarshalUint16(indexkeypool, uint16(tagLen))

	// eg, series is mst,tk1=tv1,tk2=tv2,tk3=[,],
	emptyTagValue := true
	for i := range tags {
		// eg, tags[i] is tk3=[,]
		if len(tags[i].Value) == 0 {
			continue
		}
		kl := len(tags[i].Key)
		indexkeypool = encoding.MarshalUint16(indexkeypool, uint16(kl))
		indexkeypool = append(indexkeypool, tags[i].Key...)
		vl := len(tags[i].Value)
		indexkeypool = encoding.MarshalUint16(indexkeypool, uint16(vl))
		indexkeypool = append(indexkeypool, tags[i].Value...)
		emptyTagValue = false
	}

	end := len(indexkeypool)
	newLen := end - start
	return indexkeypool, newLen, emptyTagValue
}

/*
	format
	one key: 1 + key, 1 means one key
	two keys: 2 + len(key1) + key1 + len(key2) + key2
*/
func marshalCombineIndexKeys(name []byte, tagsArray [][]influx.Tag, dst []byte) ([]byte, error) {
	indexkey := kbPool.Get()
	defer kbPool.Put(indexkey)

	var tagIndex []int
	var keyLen int
	var emptyTagValue bool
	for i := range tagsArray {
		indexkey.B, keyLen, emptyTagValue = unmarshalIndexKeys(name, tagsArray[i], indexkey.B)
		if emptyTagValue {
			continue
		}
		tagIndex = append(tagIndex, keyLen)
	}

	if len(tagIndex) == 0 {
		return nil, errors.New("error row")
	}

	// fast path, only one indexkey
	if len(tagIndex) == 1 {
		// marshal count=1
		dst = append(dst, indexkey.B[:tagIndex[0]]...)
		return dst, nil
	}

	// slow path, multi indexkeys
	dst = encoding.MarshalUint32(dst, uint32(0))
	dst = encoding.MarshalUint16(dst, uint16(len(tagIndex)))
	start := 0
	for i := range tagIndex {
		dst = encoding.MarshalUint16(dst, uint16(tagIndex[i]))
		dst = append(dst, indexkey.B[start:start+tagIndex[i]]...)
		start += tagIndex[i]
	}
	return dst, nil
}

func unmarshalCombineIndexKeys(indexKeys [][]byte, src []byte) ([][]byte, int, error) {
	if len(src) < 4 {
		logger.GetLogger().Error("too small keycount")
		return nil, 0, errno.NewError(errno.ErrTooSmallKeyCount)
	}
	// fast path, only one indexkey
	if hasOneKey(src) {
		indexKeys = resizeSeriesKeys(indexKeys, 1)
		indexKeys[0] = src
		return indexKeys, 1, nil
	}

	// skip 0000
	src = src[4:]

	keyCount := int(encoding.UnmarshalUint16(src))
	src = src[2:]

	indexKeys = resizeSeriesKeys(indexKeys, keyCount)
	// slow path, multi indexkeys
	for i := 0; i < keyCount; i++ {
		keyLen := int(encoding.UnmarshalUint16(src))
		src = src[2:]
		if keyLen > len(src) {
			logger.GetLogger().Error("too small index keys")
			return nil, 0, errno.NewError(errno.ErrTooSmallIndexKey)
		}
		indexKeys[i] = src[:keyLen]
		src = src[keyLen:]
	}

	return indexKeys, keyCount, nil
}

func getKeyCount(combineKey []byte) int {
	return int(encoding.UnmarshalUint32(combineKey))
}

func hasOneKey(combineKey []byte) bool {
	if getKeyCount(combineKey) > 0 {
		return true
	}
	return false
}

func resizeSeriesKeys(indexKeys [][]byte, keyCount int) [][]byte {
	if cap(indexKeys) > keyCount {
		indexKeys = indexKeys[:keyCount]
	} else {
		delta := keyCount - cap(indexKeys)
		indexKeys = indexKeys[:cap(indexKeys)]
		indexKeys = append(indexKeys, make([][]byte, delta)...)
	}
	return indexKeys
}

func analyzeSeriesWithCondition(series [][]byte, condition influxql.Expr, isExpectSeries []bool) (int, []bool, error) {
	isExpectSeries = resizeExpectSeries(isExpectSeries, len(series))
	// no need to analyze one series
	if len(series) == 1 {
		isExpectSeries[0] = true
		return 1, isExpectSeries, nil
	}

	if condition == nil {
		for i := range isExpectSeries {
			isExpectSeries[i] = true
		}
		return len(series), isExpectSeries, nil
	}

	var expectCount int
	var tagsBuf influx.PointTags
	for i := range series {
		_, err := influx.IndexKeyToTags(series[i], true, &tagsBuf)
		if err != nil {
			return 0, nil, err
		}

		ok, err := hasExpectedTag(&tagsBuf, condition)
		if err != nil && err != ErrFieldExpr {
			return 0, nil, err
		}

		if ok {
			isExpectSeries[i] = true
			expectCount++
		}
	}
	return expectCount, isExpectSeries, nil
}

func hasExpectedTag(tagsBuf *influx.PointTags, expr influxql.Expr) (bool, error) {
	switch expr := expr.(type) {
	case *influxql.BinaryExpr:
		switch expr.Op {
		case influxql.AND, influxql.OR:
			// If the tagsBuf matches filter expressions for the LHS.
			lbool, lerr := hasExpectedTag(tagsBuf, expr.LHS)
			if lerr != nil && lerr != ErrFieldExpr {
				return false, lerr
			}

			// If the tagsBuf matches filter expressions for the RHS.
			rbool, rerr := hasExpectedTag(tagsBuf, expr.RHS)
			if rerr != nil && rerr != ErrFieldExpr {
				return false, rerr
			}

			// if expression is "AND".
			if expr.Op == influxql.AND {
				// eg, field_float1>1.0 AND tk1='value11'
				if lerr == ErrFieldExpr && rbool {
					return true, nil
				}

				// eg, tk1='value11' AND field_float1>1.0
				if lbool && rerr == ErrFieldExpr {
					return true, nil
				}

				// eg, field_float1>1.0 AND field_float2>1.0
				if lerr == ErrFieldExpr && rerr == ErrFieldExpr {
					return true, nil
				}

				// eg, tk1='value11' AND tk2='value11'
				if lbool && rbool {
					return true, nil
				}

				return false, nil
			}

			// if expression is "OR". eg, field_float1>1.0 OR tk1='value11'
			if lerr == ErrFieldExpr || rerr == ErrFieldExpr {
				return true, nil
			}

			if lbool || rbool {
				return true, nil
			}

			return false, nil

		default:
			return matchTagFilter(tagsBuf, expr)
		}

	case *influxql.ParenExpr:
		return hasExpectedTag(tagsBuf, expr.Expr)

	case *influxql.BooleanLiteral:
		return true, nil

	default:
		return false, nil
	}
}

func matchTagFilter(tagsBuf *influx.PointTags, n *influxql.BinaryExpr) (bool, error) {
	// Retrieve the variable reference from the correct side of the expression.
	key, ok := n.LHS.(*influxql.VarRef)
	value := n.RHS
	if !ok {
		key, ok = n.RHS.(*influxql.VarRef)
		if !ok {
			// This is an expression we do not know how to evaluate.eg, ((field_float1>1.0))
			// Refer to seriesByBinaryExpr()
			return true, nil
		}
		value = n.LHS
	}

	// Not tag, regard as field
	if key.Type != influxql.Tag {
		return false, ErrFieldExpr
	}

	tf := new(tagFilter)
	switch value := value.(type) {
	case *influxql.StringLiteral:
		if ok := tf.Contains(tagsBuf, key.Val, value.Val, n.Op == influxql.NEQ, false); ok {
			return true, nil
		}
	case *influxql.RegexLiteral:
		if ok := tf.Contains(tagsBuf, key.Val, value.Val.String(), n.Op == influxql.NEQREGEX, true); ok {
			return true, nil
		}
	case *influxql.VarRef:
		if ok := tf.Contains(tagsBuf, key.Val, value.Val, n.Op == influxql.NEQ, false); ok {
			return true, nil
		}
	default:
		return false, nil
	}

	return false, nil
}

/*
  regexValue: val.*1
  value: value
*/
func matchWithRegex(regexValue, value string) bool {
	match, err := regexp.MatchString(regexValue, value)
	if err != nil {
		return false
	}
	return match
}

/*
  expectValue: value
  value: value
*/
func matchWithNoRegex(expectValue, value string) bool {
	if expectValue == value {
		return true
	}

	return false
}

func resizeExpectSeries(expectSeries []bool, keyCount int) []bool {
	if cap(expectSeries) > keyCount {
		expectSeries = expectSeries[:keyCount]
	} else {
		delta := keyCount - cap(expectSeries)
		expectSeries = expectSeries[:cap(expectSeries)]
		expectSeries = append(expectSeries, make([]bool, delta)...)
	}
	return expectSeries
}

func (idx *MergeSetIndex) searchSeriesWithTagArray(tsid uint64, seriesKeys [][]byte, combineKey []byte,
	isExpectSeries []bool, condition influxql.Expr) ([][]byte, []bool, error) {
	combineKey = combineKey[:0]
	combineKey, err := idx.searchSeriesKey(combineKey, tsid)
	if err != nil {
		idx.logger.Error("searchSeriesKey fail", zap.Error(err))
		return nil, nil, err
	}

	// if config.EnableTagArray = false, then combineKey only has one key
	if !config.EnableTagArray {
		seriesKeys = resizeSeriesKeys(seriesKeys, 1)
		seriesKeys[0] = combineKey

		isExpectSeries = resizeExpectSeries(isExpectSeries, 1)
		isExpectSeries[0] = true
		return seriesKeys, isExpectSeries, nil
	}

	seriesKeys, _, err = unmarshalCombineIndexKeys(seriesKeys, combineKey)
	if err != nil {
		return nil, nil, err
	}

	_, isExpectSeries, err = analyzeSeriesWithCondition(seriesKeys, condition, isExpectSeries)
	if err != nil {
		logger.GetLogger().Error("analyzeSeriesWithCondition fail", zap.Error(err))
		return nil, nil, err
	}
	return seriesKeys, isExpectSeries, nil
}

func (is *indexSearch) subTSIDSWithTagArray(mstTSIDS, filterTSIDS *uint64set.Set) (*uint64set.Set, error) {
	if config.EnableTagArray {
		subIDs, err := is.getTSIDSWithOneKey(filterTSIDS)
		if err != nil {
			return nil, err
		}
		mstTSIDS.Subtract(subIDs)
	} else {
		mstTSIDS.Subtract(filterTSIDS)
	}
	return mstTSIDS, nil
}

func (is *indexSearch) getTSIDSWithOneKey(tsids *uint64set.Set) (*uint64set.Set, error) {
	subIDs := &uint64set.Set{}
	itr := tsids.Iterator()
	var tmpSeriesKey []byte // reused
	for itr.HasNext() {
		tsid := itr.Next()
		tmpSeriesKey = tmpSeriesKey[:0]
		tmpSeriesKey, err := is.idx.searchSeriesKey(tmpSeriesKey, tsid)
		if err != nil {
			return nil, err
		}
		// if tmpSeriesKey is a combineSeriesKey, can't sub this tsid,
		// because this tsid contains other serieskeys
		if hasOneKey(tmpSeriesKey) {
			subIDs.Add(tsid)
		}
	}
	return subIDs, nil
}
