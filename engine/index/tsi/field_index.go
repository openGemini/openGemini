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

package tsi

import (
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"sync"
	"unsafe"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/memory"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/workingsetcache"
	"github.com/openGemini/openGemini/lib/index"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/mergeset"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

const (
	FieldIndexDirName = "field"
)

type fieldIndex struct {
	tb           *mergeset.Table
	path         string
	lock         *string
	indexBuilder *IndexBuilder
	cache        *workingsetcache.Cache
	fieldKeys    map[string]string
	fieldKeyLock sync.RWMutex
	indexLock    sync.RWMutex

	isOpen bool
}

func NewFieldIndex(opts *Options) (*fieldIndex, error) {
	fi := &fieldIndex{
		path:      opts.path,
		lock:      opts.lock,
		fieldKeys: make(map[string]string),
	}

	fi.cache = LoadCache(TSIDToFieldCacheName, fi.path, memory.Allowed()/32)

	return fi, nil
}

func (idx *fieldIndex) Open() error {
	idx.indexLock.Lock()
	defer idx.indexLock.Unlock()
	if idx.isOpen {
		return nil
	}
	path := filepath.Join(idx.path, FieldIndexDirName)
	tb, err := mergeset.OpenTable(path, nil, nil, idx.lock)
	if err != nil {
		return fmt.Errorf("cannot open index:%s, err: %+v", path, err)
	}
	idx.tb = tb
	if err = idx.loadFieldKey(); err != nil {
		return err
	}
	idx.isOpen = true
	return nil
}

func (idx *fieldIndex) GetPidFromCache(id *uint64, key []byte) bool {
	if idx.cache == nil {
		return false
	}
	buf := (*[unsafe.Sizeof(*id)]byte)(unsafe.Pointer(id))[:]
	buf = idx.cache.Get(buf[:0], key)
	return uintptr(len(buf)) == unsafe.Sizeof(*id)
}

func (idx *fieldIndex) PutPidToCache(id *uint64, key []byte) {
	buf := (*[unsafe.Sizeof(*id)]byte)(unsafe.Pointer(id))[:]
	idx.cache.Set(key, buf)
}

func (idx *fieldIndex) SetIndexBuilder(builder *IndexBuilder) {
	idx.indexBuilder = builder
}

func (idx *fieldIndex) getIndexSearch() *indexSearch {
	v := indexSearchPool.Get()
	if v == nil {
		v = &indexSearch{}
	}

	is := v.(*indexSearch)
	is.ts.Init(idx.tb)

	return is
}

func (idx *fieldIndex) putIndexSearch(is *indexSearch) {
	is.kb.Reset()
	is.ts.MustClose()
	is.mp.Reset()
	is.vrp.Reset()
	is.idx = nil
	indexSearchPool.Put(is)
}

func (idx *fieldIndex) CreateIndexIfNotExists(primaryIndex PrimaryIndex, row *influx.Row) (uint64, error) {
	var field influx.Field
	// Find the field need to be created index.
	for _, opt := range row.IndexOptions {
		if opt.Oid == uint32(index.Field) {
			if len(opt.IndexList) != 1 {
				return 0, fmt.Errorf("just allow only one field to create FieldIndex")
			}
			if int(opt.IndexList[0]) < len(row.Tags) {
				return 0, fmt.Errorf("cannot create field index for tag: %s", row.Tags[opt.IndexList[0]].Key)
			}
			field = row.Fields[int(opt.IndexList[0])-len(row.Tags)]
			if field.Type != influx.Field_Type_String {
				return 0, fmt.Errorf("field type must be string for FieldIndex")
			}
			break
		}
	}

	vname := kbPool.Get()
	defer kbPool.Put(vname)

	vname.B = append(vname.B[:0], []byte(row.Name)...)

	if _, ok := idx.fieldKeys[string(vname.B)]; !ok {
		if err := idx.storeFieldKey(vname.B, []byte(field.Key)); err != nil {
			return 0, err
		}
		idx.fieldKeyLock.Lock()
		idx.fieldKeys[string(vname.B)] = field.Key
		idx.fieldKeyLock.Unlock()
	}

	key := encoding.MarshalUint64(nil, row.SeriesId)
	key = append(key, field.StrValue...)

	// First check no need to lock.
	pid, err := idx.getPidByPkey(key)
	if err != nil {
		return 0, err
	}

	if pid == 0 {
		idx.indexLock.Lock()
		defer idx.indexLock.Unlock()
		// Check again.
		pid, err = idx.getPidByPkey(key)
		if err != nil {
			return 0, err
		}

		if pid == 0 {
			if pid, err = idx.createIndex(row.SeriesId, field.StrValue); err != nil {
				return 0, err
			}
		}

		idx.PutPidToCache(&pid, key)
	}
	row.PrimaryId = pid
	return pid, nil
}

func (idx *fieldIndex) getFieldsByTSID(tsid uint64) ([][]byte, error) {
	is := idx.getIndexSearch()
	defer idx.putIndexSearch(is)

	return is.getFieldsByTSID(tsid)
}

func (idx *fieldIndex) getPidByPkey(key []byte) (uint64, error) {
	var pid uint64
	if idx.GetPidFromCache(&pid, key) {
		return pid, nil
	}

	is := idx.getIndexSearch()
	defer idx.putIndexSearch(is)

	pid, err := is.getPidByPkey(key)
	if pid != 0 {
		idx.PutPidToCache(&pid, key)
	}

	return pid, err
}

func (idx *fieldIndex) createIndex(sid uint64, fieldValue string, opt ...string) (uint64, error) {
	ii := idxItemsPool.Get()
	defer idxItemsPool.Put(ii)

	// Create tsid -> field index
	ii.B = append(ii.B[:0], nsPrefixTSIDToField)
	ii.B = encoding.MarshalUint64(ii.B, sid)
	ii.B = append(ii.B, byte(len(fieldValue)))
	ii.B = append(ii.B, fieldValue...)
	ii.Next()

	// Create pkey -> pid
	pid := GenerateUUID()
	ii.B = append(ii.B, nsPrefixFieldToPID)
	ii.B = encoding.MarshalUint64(ii.B, sid)
	ii.B = append(ii.B, fieldValue...)
	ii.B = append(ii.B, kvSeparatorChar)
	ii.B = encoding.MarshalUint64(ii.B, pid)
	ii.Next()

	if err := idx.tb.AddItems(ii.Items); err != nil {
		return 0, err
	}

	return pid, nil
}

func (idx *fieldIndex) storeFieldKey(name []byte, fieldKey []byte) error {
	ii := idxItemsPool.Get()
	defer idxItemsPool.Put(ii)

	// Create mst -> fieldKey
	ii.B = append(ii.B[:0], nsPrefixMstToFieldKey)
	ii.B = encoding.MarshalUint16(ii.B, uint16(len(name)))
	ii.B = append(ii.B, name...)
	ii.B = encoding.MarshalUint16(ii.B, uint16(len(fieldKey)))
	ii.B = append(ii.B, fieldKey...)
	ii.Next()

	if err := idx.tb.AddItems(ii.Items); err != nil {
		return err
	}
	return nil
}

func (idx *fieldIndex) loadFieldKey() error {
	var err error
	idx.fieldKeys, err = idx.getIndexSearch().getFieldKey()
	if err != io.EOF {
		return err
	}

	return nil
}

func (idx *fieldIndex) Search(primaryIndex PrimaryIndex, span *tracing.Span, name []byte, opt *query.ProcessorOptions, groups interface{}) (GroupSeries, error) {
	vname := kbPool.Get()
	defer kbPool.Put(vname)

	vname.B = append(vname.B[:0], name...)

	groupSeries, ok := groups.(GroupSeries)
	if !ok {
		return nil, fmt.Errorf("not a group series: %v", groups)
	}
	sortedTagsSets := make(GroupSeries, 0, len(groupSeries))

	var groupByField bool
	idx.fieldKeyLock.RLock()
	fieldKey, ok := idx.fieldKeys[string(vname.B)]
	idx.fieldKeyLock.RUnlock()
	if !ok {
		// The measurement doesn't contain field index.
		return groupSeries, nil
	}

	for _, dim := range opt.Dimensions {
		if dim == fieldKey {
			groupByField = true
			break
		}
	}

	var err error
	var hasFieldIndexFilter bool
	for _, group := range groupSeries {
		var tagSet TagSetEx = new(TagSetInfo)
		for i, tagSetElem := range group.TagSetItems() {
			var fieldValues [][]byte
			fieldValues, tagSetElem.Filter, err = idx.getAllFields(fieldKey, tagSetElem.Filter, tagSetElem.ID, &hasFieldIndexFilter)
			if err != nil {
				return nil, err
			}

			if groupByField && len(fieldValues) == 0 {
				continue
			}

			if !groupByField && len(fieldValues) == 0 {
				tagSet.AppendKey(group.GetKey()...)
				tagSet.Append(tagSetElem.ID, tagSetElem.SeriesKey, tagSetElem.Filter, tagSetElem.TagsVec, nil)
			}

			// Generate TagSet for each field value.
			for _, fieldValue := range fieldValues {
				if groupByField {
					tagSet = new(TagSetInfo)
					sortedTagsSets = append(sortedTagsSets, tagSet)
				}
				tagSet, err = idx.genTagSet(tagSet, tagSetElem.ID, fieldKey, string(fieldValue), group, i, groupByField, opt.Dimensions)
				if err != nil {
					return nil, err
				}
			}
		}

		if tagSet.Len() == 0 && !hasFieldIndexFilter {
			sortedTagsSets = append(sortedTagsSets, group)
		} else {
			if !groupByField {
				sortedTagsSets = append(sortedTagsSets, tagSet)
			}
		}
	}

	sort.Sort(sortedTagsSets)
	return sortedTagsSets, nil
}

func (idx *fieldIndex) getAllFields(fieldKey string, filter influxql.Expr, sid uint64, flag *bool) ([][]byte, influxql.Expr, error) {
	var fieldValues [][]byte
	if filter != nil {
		// Extract field value from value filter.
		fieldValue := idx.extractField(filter, fieldKey)
		if fieldValue != nil {
			fieldValues = append(fieldValues, fieldValue)
			*flag = true
			return fieldValues, nil, nil
		}
	}

	var err error
	fieldValues, err = idx.getFieldsByTSID(sid)
	if err != nil {
		return nil, filter, err
	}
	return fieldValues, filter, nil
}

func (idx *fieldIndex) addToTagSet(tagSet TagSetEx, fieldKey string, fieldValue []byte, index int, sid uint64, group TagSetEx) (*TagSetInfoItem, bool, error) {
	key := encoding.MarshalUint64(nil, sid)
	key = append(key, fieldValue...)
	pid, err := idx.getPidByPkey(key)
	if err != nil {
		return nil, false, err
	}
	if pid == 0 {
		return nil, false, nil
	}

	groupItem := group.GetTagSetItem(index)

	var seriesKey []byte
	seriesKey = append(seriesKey, groupItem.SeriesKey...)
	seriesKey = append(seriesKey, influx.StringSplit...)
	seriesKey = append(seriesKey, fmt.Sprintf("%s"+influx.StringSplit, fieldKey)...)
	seriesKey = append(seriesKey, fieldValue...)

	var tagVec influx.PointTags
	tagVec = append(tagVec, groupItem.TagsVec...)

	tagSet.Append(pid, seriesKey, groupItem.Filter, tagVec, nil)

	return tagSet.GetTagSetItem(tagSet.Len() - 1), true, nil
}

func (idx *fieldIndex) genTagSet(tagSet TagSetEx, sid uint64, fieldKey, fieldValue string, group TagSetEx, index int, groupByField bool, dims []string) (TagSetEx, error) {
	tagSetItem, ok, err := idx.addToTagSet(tagSet, fieldKey, []byte(fieldValue), index, sid, group)
	if err != nil {
		return nil, err
	}
	if !ok {
		return tagSet, nil
	}

	if groupByField {
		tagSetItem.TagsVec = append(tagSetItem.TagsVec, influx.Tag{
			Key:   fieldKey,
			Value: fieldValue,
		})
		sort.Sort(&tagSetItem.TagsVec)
		tagSet.SetKey(MakeGroupTagsKey(dims, tagSetItem.TagsVec, tagSet.GetKey()[:0]))
	} else {
		tagSet.SetKey(group.GetKey())
	}

	return tagSet, nil
}

func (idx *fieldIndex) extractField(expr influxql.Expr, fieldKey string) []byte {
	switch expr := expr.(type) {
	case *influxql.BinaryExpr:
		switch expr.Op {
		case influxql.EQ:
			key, ok := expr.LHS.(*influxql.VarRef)
			value := expr.RHS
			if !ok {
				key, ok = expr.RHS.(*influxql.VarRef)
				if !ok {
					return nil
				}
				value = expr.LHS
			}
			if key.Val != fieldKey {
				return nil
			}
			switch value := value.(type) {
			case *influxql.StringLiteral:
				return []byte(value.Val)
			case *influxql.VarRef:
				return []byte(value.Val)
			default:
				return nil
			}
		default:
			return nil
		}
	default:
		return nil
	}
}

func (idx *fieldIndex) Delete(primaryIndex PrimaryIndex, name []byte, condition influxql.Expr, tr TimeRange) error {
	// TODO
	return nil
}

func (idx *fieldIndex) Close() error {
	if !idx.isOpen {
		return nil
	}
	idx.tb.MustClose()
	idx.isOpen = false
	return nil
}

func (idx *fieldIndex) DebugFlush() {
	idx.tb.DebugFlush()
}

func FieldIndexHandler(opt *Options, primaryIndex PrimaryIndex) (*IndexAmRoutine, error) {
	fieldIndex, err := NewFieldIndex(opt)
	if err != nil {
		return nil, err
	}
	return &IndexAmRoutine{
		amKeyType:    index.Field,
		amOpen:       FieldOpen,
		amBuild:      FieldBuild,
		amInsert:     FieldInsert,
		amDelete:     FieldDelete,
		amScan:       FieldScan,
		amClose:      FieldClose,
		amFlush:      FieldFlush,
		amCacheClear: FieldCacheClear,
		index:        fieldIndex,
		primaryIndex: primaryIndex,
	}, nil
}

func FieldBuild(relation *IndexRelation) error {
	return nil
}

func FieldOpen(index interface{}) error {
	fi, ok := index.(*fieldIndex)
	if !ok {
		return fmt.Errorf("not a field index: %v", index)
	}
	return fi.Open()
}

func FieldInsert(index interface{}, primaryIndex PrimaryIndex, name []byte, row interface{}) (uint64, error) {
	fi, ok := index.(*fieldIndex)
	if !ok {
		return 0, fmt.Errorf("not a field index: %v", index)
	}
	insertRow := row.(*influx.Row)
	return fi.CreateIndexIfNotExists(primaryIndex, insertRow)
}

func FieldScan(index interface{}, primaryIndex PrimaryIndex, span *tracing.Span, name []byte, opt *query.ProcessorOptions, callBack func(num int64) error, groups interface{}) (interface{}, int64, error) {
	fi, ok := index.(*fieldIndex)
	if !ok {
		return nil, 0, fmt.Errorf("not a field index: %v", index)
	}
	re, err := fi.Search(primaryIndex, span, name, opt, groups)
	return re, 0, err
}

func FieldDelete(index interface{}, primaryIndex PrimaryIndex, name []byte, condition influxql.Expr, tr TimeRange) error {
	fi := index.(*fieldIndex)
	return fi.Delete(primaryIndex, name, condition, tr)
}

func FieldClose(index interface{}) error {
	fi := index.(*fieldIndex)
	return fi.Close()
}

func FieldFlush(index interface{}) {
	fi, ok := index.(*fieldIndex)
	if !ok {
		logger.GetLogger().Error(fmt.Sprintf("index %v is not a FieldIndex", index))
		return
	}
	fi.DebugFlush()
}

func FieldCacheClear(index interface{}) error {
	return nil
}
