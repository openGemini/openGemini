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
	"bytes"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"text/tabwriter"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/memory"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/index/mergeindex"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/syscontrol"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/open_src/github.com/VictoriaMetrics/VictoriaMetrics/lib/mergeset"
	"github.com/openGemini/openGemini/open_src/github.com/savsgio/dictpool"
	"github.com/openGemini/openGemini/open_src/influx/index"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/meta"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"github.com/openGemini/openGemini/open_src/vm/uint64set"
	"go.uber.org/zap"
)

const (
	nsPrefixKeyToTSID = iota
	nsPrefixTSIDToKey
	nsPrefixTagToTSIDs
	//	nsPrefixCellIDToTSID
	nsPrefixDeletedTSIDs
	nsPrefixTSIDToField
	nsPrefixFieldToPID
	nsPrefixMstToFieldKey

	//  Prefix for column store
	nsPrefixTagKeysToTagValues
)

const (
	escapeChar       = 0
	tagSeparatorChar = 1
	kvSeparatorChar  = 2

	compositeTagKeyPrefix = '\xfe'
	//maxTSIDsPerRow        = 64
	MergeSetDirName = "mergeset"
)

var tagFiltersKeyGen uint64

var (
	queueSize     uint64
	queueSizeMask uint64
)

func init() {
	queueSize = util.CeilToPower2(uint64(cpu.GetCpuNum() << 1))
	queueSizeMask = queueSize - 1
}

func invalidateTagCache() {
	// This function must be fast, since it is called each
	// time new timeseries is added.
	atomic.AddUint64(&tagFiltersKeyGen, 1)
}

var kbPool bytesutil.ByteBufferPool

var indexSearchPool sync.Pool
var dstTagSetsPool TagSetsPool

type StorageIndex interface {
	initQueues(idx *MergeSetIndex)
	run(idx *MergeSetIndex)
	SearchTagValues(name []byte, tagKeys [][]byte, condition influxql.Expr, is *indexSearch) ([][]string, error)
}

type tsIndexImpl struct {
}

func (tsIdx *tsIndexImpl) run(idx *MergeSetIndex) {
	for i := 0; i < len(idx.queues); i++ {
		go func(index int) {
			for row := range idx.queues[index] {
				row.Row.SeriesId, row.Err = idx.CreateIndexIfNotExistsByRow(row.Row)
				row.Row.PrimaryId = row.Row.SeriesId
				row.Wg.Done()
			}
		}(i)
	}
}

func (tsIdx *tsIndexImpl) initQueues(idx *MergeSetIndex) {
	idx.queues = make([]chan *indexRow, queueSize)
	for i := 0; i < len(idx.queues); i++ {
		idx.queues[i] = make(chan *indexRow, 1024)
	}
}

func (tsIdx *tsIndexImpl) SearchTagValues(name []byte, tagKeys [][]byte, condition influxql.Expr, is *indexSearch) ([][]string, error) {
	return is.searchTagValues(name, tagKeys, condition)
}

type CsIndexImpl struct {
	prev []byte
}

func (csIdx *CsIndexImpl) run(idx *MergeSetIndex) {
	for i := 0; i < len(idx.queues); i++ {
		go func(index int) {
			for row := range idx.queues[index] {
				row.Err = csIdx.CreateIndexIfNotExistsByRow(idx, row.Row)
				row.Wg.Done()
			}
		}(i)
	}

	for i := 0; i < len(idx.labelStoreQueues); i++ {
		go func(index int) {
			for col := range idx.labelStoreQueues[index] {
				col.Err = csIdx.CreateIndexIfNotExistsByCol(idx, col)
				col.Wg.Done()
			}
		}(i)
	}
}

func (csIdx *CsIndexImpl) initQueues(idx *MergeSetIndex) {
	idx.queues = make([]chan *indexRow, queueSize)
	for i := 0; i < len(idx.queues); i++ {
		idx.queues[i] = make(chan *indexRow, 1024)
	}

	idx.labelStoreQueues = make([]chan *TagCol, queueSize)
	for i := 0; i < len(idx.labelStoreQueues); i++ {
		idx.labelStoreQueues[i] = make(chan *TagCol, 2048)
	}
}

func (csIdx *CsIndexImpl) SearchTagValues(name []byte, tagKeys [][]byte, condition influxql.Expr, is *indexSearch) ([][]string, error) {
	return is.searchTagValuesForLabelStore(name, tagKeys)
}

func (csIdx *CsIndexImpl) CreateIndexIfNotExistsByRow(idx *MergeSetIndex, row *influx.Row) error {
	vkey := kbPool.Get()
	vname := kbPool.Get()

	ii := idxItemsPool.Get()
	compositeKey := kbPool.Get()

	defer func() {
		kbPool.Put(vkey)
		kbPool.Put(vname)
		idxItemsPool.Put(ii)
		kbPool.Put(compositeKey)
	}()

	var exist bool
	var err error
	vname.B = append(vname.B[:0], row.Name...)
	for i := range row.Tags {
		vkey.B = append(vkey.B[:0], row.Name...)
		vkey.B = append(vkey.B, row.Tags[i].Key...)
		vkey.B = append(vkey.B, row.Tags[i].Value...)
		exist, err = idx.isTagKeyExist(vkey.B, vname.B, row.Tags[i].Key, row.Tags[i].Value)
		if err != nil {
			return err
		}

		if !exist {
			ii.B = idx.marshalTagToTagValues(compositeKey.B, ii.B, vname.B, []byte(row.Tags[i].Key), []byte(row.Tags[i].Value))
			ii.Next()
			idx.cache.PutTagValuesToTagKeysCache([]byte{1}, vkey.B)
			err = idx.tb.AddItems(ii.Items)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (csIdx *CsIndexImpl) CreateIndexIfNotExistsByCol(idx *MergeSetIndex, col *TagCol) error {
	vkey := kbPool.Get()
	vname := kbPool.Get()

	defer func() {
		kbPool.Put(vkey)
		kbPool.Put(vname)
	}()

	var exist bool
	var err error
	vname.B = append(vname.B[:0], col.Mst...)
	vkey.B = append(vkey.B[:0], col.Mst...)
	vkey.B = append(vkey.B, col.Key...)
	vkey.B = append(vkey.B, col.Val...)
	if bytes.Equal(csIdx.prev, vkey.B) {
		return nil
	}
	csIdx.prev = vkey.B
	exist, err = idx.isTagKeyExistByCol(vkey.B, col.Key, col.Val, col.Mst)
	if err != nil {
		return err
	}

	if !exist {
		compositeKey := kbPool.Get()
		ii := idxItemsPool.Get()
		defer func() {
			kbPool.Put(compositeKey)
			idxItemsPool.Put(ii)
		}()
		ii.B = idx.marshalTagToTagValues(compositeKey.B, ii.B, vname.B, col.Key, col.Val)
		ii.Next()
		idx.cache.PutTagValuesToTagKeysCache([]byte{1}, vkey.B)
		return idx.tb.AddItems(ii.Items)
	}

	return nil
}

type MergeSetIndex struct {
	tb               *mergeset.Table
	logger           *logger.Logger
	path             string
	lock             *string
	queues           []chan *indexRow
	labelStoreQueues []chan *TagCol

	cache *IndexCache

	// Deleted tsids
	deletedTSIDs     atomic.Value
	deletedTSIDsLock sync.Mutex

	mu sync.RWMutex

	indexBuilder *IndexBuilder
	StorageIndex StorageIndex
}

func NewMergeSetIndex(opts *Options) (*MergeSetIndex, error) {
	ms := &MergeSetIndex{
		path:   opts.path,
		lock:   opts.lock,
		logger: logger.NewLogger(errno.ModuleIndex),
	}

	switch opts.engineType {
	case config.TSSTORE:
		ms.StorageIndex = &tsIndexImpl{}
	case config.COLUMNSTORE:
		ms.StorageIndex = &CsIndexImpl{}
	default:
		return nil, errors.New("NewMergeSetIndex: unknown engineType")
	}

	return ms, nil
}

func (idx *MergeSetIndex) Open() error {
	tablePath := filepath.Join(idx.path, MergeSetDirName)
	tb, err := mergeset.OpenTable(tablePath, invalidateTagCache, mergeIndexRows, idx.lock)
	if err != nil {
		return fmt.Errorf("cannot open index:%s, err: %+v", tablePath, err)
	}
	idx.tb = tb

	mem := memory.Allowed()
	idx.cache = NewIndexCache(mem/32, mem/32, mem/16, mem/128, idx.path)

	if err := idx.loadDeletedTSIDs(); err != nil {
		return err
	}

	idx.StorageIndex.initQueues(idx)
	idx.run()

	return nil
}

func (idx *MergeSetIndex) WriteRow(row *indexRow) {
	partId := meta.HashID(row.Row.IndexKey) & queueSizeMask
	idx.queues[partId] <- row
}

func (idx *MergeSetIndex) WriteTagCols(tagCol *TagCol) {
	key := append(tagCol.Mst, tagCol.Key...)
	key = append(key, tagCol.Val...)
	partId := meta.HashID(key) & queueSizeMask
	idx.labelStoreQueues[partId] <- tagCol
}

func (idx *MergeSetIndex) run() {
	idx.StorageIndex.run(idx)
}

func (idx *MergeSetIndex) SetIndexBuilder(builder *IndexBuilder) {
	idx.indexBuilder = builder
}

func (idx *MergeSetIndex) getIndexSearch() *indexSearch {
	v := indexSearchPool.Get()
	if v == nil {
		v = &indexSearch{
			idx: idx,
			tfs: make([]tagFilter, 0, 1),
		}
	}

	is := v.(*indexSearch)
	is.ts.Init(idx.tb)
	is.idx = idx

	return is
}

func (idx *MergeSetIndex) putIndexSearch(is *indexSearch) {
	is.kb.Reset()
	is.ts.MustClose()
	is.mp.Reset()
	is.vrp.Reset()
	is.idx = nil
	is.tfs = is.tfs[:0]
	indexSearchPool.Put(is)
}

func (idx *MergeSetIndex) GetSeriesIdBySeriesKey(seriesKey []byte, name []byte) (uint64, error) {
	vkey := kbPool.Get()
	defer kbPool.Put(vkey)
	vkey.B = append(vkey.B[:0], seriesKey...)
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.getSeriesIdBySeriesKey(vkey.B)
}

func (idx *MergeSetIndex) IsTagKeyExist(row influx.Row) (bool, error) {
	vkey := kbPool.Get()
	vname := kbPool.Get()
	idx.mu.RLock()

	defer func() {
		kbPool.Put(vkey)
		kbPool.Put(vname)
		idx.mu.RUnlock()
	}()

	var exist bool
	var err error
	for _, tag := range row.Tags {
		vname.B = append(vname.B[:0], row.Name...)
		vkey.B = append(vkey.B[:0], tag.Key...)
		vkey.B = append(vkey.B, tag.Value...)

		exist, err = idx.isTagKeyExist(vkey.B, vname.B, tag.Key, tag.Value)
		if err != nil {
			return false, err
		}
		if !exist {
			return false, nil
		}
	}

	return true, nil
}

func (idx *MergeSetIndex) IsTagKeyExistByArrowFlight(col *TagCol) (bool, error) {
	vkey := kbPool.Get()
	vname := kbPool.Get()

	compositeKey := kbPool.Get()
	defer func() {
		kbPool.Put(vkey)
		kbPool.Put(vname)
		kbPool.Put(compositeKey)
	}()

	var exist bool
	var err error
	vname.B = append(vname.B[:0], col.Mst...)
	vkey.B = append(vkey.B[:0], col.Mst...)
	vkey.B = append(vkey.B, col.Key...)
	vkey.B = append(vkey.B, col.Val...)
	exist, err = idx.isTagKeyExistByCol(vkey.B, col.Key, col.Val, col.Mst)
	return exist, err
}

func (idx *MergeSetIndex) isTagKeyExist(key, name []byte, tagKey, tagValue string) (bool, error) {
	exist := idx.cache.isTagKeyExist(key)
	if exist {
		return exist, nil
	}

	is := idx.getIndexSearch()
	defer idx.putIndexSearch(is)

	exist, err := is.isTagKeyExist([]byte(tagKey), []byte(tagValue), name)
	if exist {
		idx.cache.PutTagValuesToTagKeysCache([]byte{1}, key)
		return true, nil
	}

	if err != io.EOF {
		return false, err
	}
	return false, nil
}

func (idx *MergeSetIndex) isTagKeyExistByCol(key, tagKey, tagValue, name []byte) (bool, error) {
	exist := idx.cache.isTagKeyExist(key)
	if exist {
		return exist, nil
	}

	is := idx.getIndexSearch()
	defer idx.putIndexSearch(is)
	exist, err := is.isTagKeyExist(tagKey, tagValue, name)
	if exist {
		idx.cache.PutTagValuesToTagKeysCache([]byte{1}, key)
		return true, nil
	}

	if err != io.EOF {
		return false, err
	}
	return false, nil
}

func (idx *MergeSetIndex) getSeriesIdBySeriesKey(seriesKeyWithVersion []byte) (uint64, error) {
	var tsid uint64
	var err error
	if idx.cache.GetTSIDFromTSIDCache(&tsid, seriesKeyWithVersion) {
		return tsid, nil
	}
	defer func(id *uint64) {
		if *id != 0 {
			idx.cache.PutTSIDToTSIDCache(id, seriesKeyWithVersion)
		}
	}(&tsid)

	is := idx.getIndexSearch()
	defer idx.putIndexSearch(is)

	tsid, err = is.getTSIDBySeriesKey(seriesKeyWithVersion)
	if err == nil {
		return tsid, nil
	}

	if err != io.EOF {
		return 0, err
	}
	return 0, nil
}

func (idx *MergeSetIndex) CreateIndexIfNotExists(mmRows *dictpool.Dict) error {
	vkey := kbPool.Get()
	defer kbPool.Put(vkey)
	vname := kbPool.Get()
	defer kbPool.Put(vname)

	var err error
	idx.mu.Lock()
	defer idx.mu.Unlock()

	for mmIdx := range mmRows.D {
		rows, ok := mmRows.D[mmIdx].Value.(*[]influx.Row)
		if !ok {
			return fmt.Errorf("create index failed due to rows are not belong to type row")
		}

		vname.B = append(vname.B[:0], []byte(mmRows.D[mmIdx].Key)...)
		for rowIdx := range *rows {
			if (*rows)[rowIdx].SeriesId != 0 {
				continue
			}

			vkey.B = append(vkey.B[:0], (*rows)[rowIdx].IndexKey...)
			(*rows)[rowIdx].SeriesId, err = idx.createIndexesIfNotExists(vkey.B, vname.B, (*rows)[rowIdx].Tags)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

/*
for row with no tags, eg: insert foo foo.bar.baz=1
use old path
*/
func (idx *MergeSetIndex) CreateIndexIfNotExistsByRow(row *influx.Row) (uint64, error) {
	vkey := kbPool.Get()
	defer kbPool.Put(vkey)
	vname := kbPool.Get()
	defer kbPool.Put(vname)

	vname.B = append(vname.B[:0], []byte(row.Name)...)
	vkey.B = append(vkey.B[:0], row.IndexKey...)

	var hasTagArray bool
	if idx.indexBuilder.EnableTagArray {
		for i := 0; i < len(row.Tags); i++ {
			if strings.HasPrefix(row.Tags[i].Value, "[") && strings.HasSuffix(row.Tags[i].Value, "]") {
				row.Tags[i].IsArray = true
				hasTagArray = true
			}
		}
	}

	if hasTagArray {
		sid, err := idx.createIndexesIfNotExistsWithTagArray(vkey.B, vname.B, row.Tags)
		return sid, err
	}

	// original path
	sid, err := idx.createIndexesIfNotExists(vkey.B, vname.B, row.Tags)
	return sid, err
}

func (idx *MergeSetIndex) createIndexesIfNotExists(vkey, vname []byte, tags []influx.Tag) (uint64, error) {
	tsid, err := idx.getSeriesIdBySeriesKey(vkey)
	if err != nil {
		return 0, err
	}

	if tsid != 0 {
		return tsid, nil
	}

	if err := idx.indexBuilder.SeriesLimited(); err != nil {
		return 0, err
	}

	defer func(id *uint64) {
		if *id != 0 {
			idx.cache.PutTSIDToTSIDCache(id, vkey)
		}
	}(&tsid)

	tsid, err = idx.createIndexes(vkey, vname, tags, nil, false)
	return tsid, err
}

func (idx *MergeSetIndex) createIndexesIfNotExistsWithTagArray(vkey, vname []byte, tags []influx.Tag) (uint64, error) {
	dstTagSets := dstTagSetsPool.Get()
	defer dstTagSetsPool.Put(dstTagSets)
	err := analyzeTagSets(dstTagSets, tags)
	if err != nil {
		return 0, err
	}

	// combineIndexKey = indexkey1 + indexkey2 + ...
	combineIndexKey := kbPool.Get()
	defer kbPool.Put(combineIndexKey)

	combineIndexKey.B, err = marshalCombineIndexKeys(vname, dstTagSets.tagsArray, combineIndexKey.B)
	if err != nil {
		return 0, err
	}

	tsid, err := idx.getSeriesIdBySeriesKey(combineIndexKey.B)
	if err != nil {
		return 0, err
	}

	if tsid != 0 {
		return tsid, nil
	}

	if err := idx.indexBuilder.SeriesLimited(); err != nil {
		return 0, err
	}

	defer func(id *uint64) {
		if *id != 0 {
			idx.cache.PutTSIDToTSIDCache(id, combineIndexKey.B)
		}
	}(&tsid)

	tsid, err = idx.createIndexes(combineIndexKey.B, vname, tags, dstTagSets.tagsArray, true)
	return tsid, err
}

var idxItemsPool mergeindex.IndexItemsPool

func (idx *MergeSetIndex) createIndexes(seriesKey []byte, name []byte, tags []influx.Tag, tagArray [][]influx.Tag, enableTagArray bool) (uint64, error) {
	tsid := idx.indexBuilder.GenerateUUID()

	ii := idxItemsPool.Get()
	defer idxItemsPool.Put(ii)

	// Create Series key -> TSID index
	ii.B = append(ii.B, nsPrefixKeyToTSID)
	ii.B = append(ii.B, seriesKey...)
	ii.B = append(ii.B, kvSeparatorChar)
	ii.B = encoding.MarshalUint64(ii.B, tsid)
	ii.Next()

	// Create TSID -> Series key index
	ii.B = append(ii.B, nsPrefixTSIDToKey)
	ii.B = encoding.MarshalUint64(ii.B, tsid)
	ii.B = append(ii.B, seriesKey...)
	ii.Next()

	// Create Tag -> TSID index
	compositeKey := kbPool.Get()
	if enableTagArray {
		tagMap := make(map[string]map[string]struct{})
		for _, tags := range tagArray {
			for i := range tags {
				if len(tags[i].Value) == 0 {
					continue
				}
				if _, ok := tagMap[tags[i].Key]; !ok {
					tagMap[tags[i].Key] = make(map[string]struct{})
				}
				if _, ok := tagMap[tags[i].Key][tags[i].Value]; ok {
					continue
				} else {
					tagMap[tags[i].Key][tags[i].Value] = struct{}{}
				}
				ii.B = idx.marshalTagToTSIDs(compositeKey.B, ii.B, name, tags[i], tsid)
				ii.Next()
			}
		}
	} else {
		for i := range tags {
			ii.B = idx.marshalTagToTSIDs(compositeKey.B, ii.B, name, tags[i], tsid)
			ii.Next()
		}
	}

	compositeKey.B = marshalCompositeTagKey(compositeKey.B[:0], name, nil)
	ii.B = append(ii.B, nsPrefixTagToTSIDs)
	ii.B = marshalTagValue(ii.B, compositeKey.B)
	ii.B = marshalTagValue(ii.B, nil)
	ii.B = encoding.MarshalUint64(ii.B, tsid)
	ii.Next()

	kbPool.Put(compositeKey)

	if err := idx.tb.AddItems(ii.Items); err != nil {
		return 0, err
	}

	return tsid, nil
}

func (idx *MergeSetIndex) marshalTagToTagValues(tmpB []byte, dstB []byte, name []byte, key, value []byte) []byte {
	tmpB = marshalCompositeTagKey(tmpB[:0], name, key)
	dstB = append(dstB, nsPrefixTagKeysToTagValues)
	dstB = marshalTagValue(dstB, tmpB)
	dstB = marshalTagValue(dstB, value)
	return dstB
}

// Create Tag -> TSID index
func (idx *MergeSetIndex) marshalTagToTSIDs(tmpB []byte, dstB []byte, name []byte, tag influx.Tag, tsid uint64) []byte {
	tmpB = marshalCompositeTagKey(tmpB[:0], name, []byte(tag.Key))
	dstB = append(dstB, nsPrefixTagToTSIDs)
	dstB = marshalTagValue(dstB, tmpB)
	dstB = marshalTagValue(dstB, []byte(tag.Value))
	dstB = encoding.MarshalUint64(dstB, tsid)
	return dstB
}

func (idx *MergeSetIndex) SeriesCardinality(name []byte, condition influxql.Expr, tr TimeRange) (uint64, error) {
	if condition == nil {
		return idx.seriesCardinality(name)
	}

	tsids, err := idx.searchTSIDs(name, condition, tr)
	if err != nil {
		return 0, err
	}
	return uint64(len(tsids)), nil
}

func (idx *MergeSetIndex) SearchSeries(series [][]byte, name []byte, condition influxql.Expr, tr TimeRange) ([][]byte, error) {
	tsids, err := idx.searchTSIDs(name, condition, tr)
	if err != nil {
		return series, err
	}
	if cap(series) >= len(tsids) {
		series = series[:len(tsids)]
	} else {
		series = series[:cap(series)]
		series = append(series, make([][]byte, len(tsids)-cap(series))...)
	}

	var combineSeriesKey []byte
	var combineKeys [][]byte
	var isExpectSeries []bool
	sIndex := 0
	for i := range tsids {
		combineKeys, _, isExpectSeries, err = idx.searchSeriesWithTagArray(tsids[i], combineKeys, nil, combineSeriesKey, isExpectSeries, condition)
		if err != nil {
			idx.logger.Error("searchSeriesKey fail", zap.Error(err), zap.String("index", "mergeset"))
			return nil, err
		}

		for j := range combineKeys {
			if !isExpectSeries[j] {
				continue
			}

			if sIndex >= len(tsids) {
				bufSeries := influx.GetBytesBuffer()
				bufSeries = influx.Parse2SeriesKey(combineKeys[j], bufSeries, false)
				series = append(series, bufSeries)
			} else {
				if series[sIndex] == nil {
					series[sIndex] = influx.GetBytesBuffer()
				}
				series[sIndex] = influx.Parse2SeriesKey(combineKeys[j], series[sIndex][:0], false)
				sIndex++
			}
		}
	}

	return series, nil
}

func (idx *MergeSetIndex) SearchSeriesIterator(span *tracing.Span, name []byte, opt *query.ProcessorOptions) (index.SeriesIDIterator, error) {
	var search *tracing.Span

	// need add version for delete measurement safeguard
	if span != nil {
		search = span.StartSpan("tsid_search")
		search.StartPP()
	}

	var err error
	var tsid uint64

	singleSeries := opt.GetHintType() == hybridqp.FullSeriesQuery
	if singleSeries {
		tsid, err = idx.GetSeriesIdBySeriesKey(opt.SeriesKey, name)
		if err != nil {
			idx.logger.Error("getSeriesIdBySeriesKey fail", zap.Error(err), zap.String("index", "mergeset"))
			return nil, err
		}
	}

	is := idx.getIndexSearch()

	is.setDeleted(idx.getDeletedTSIDs())
	itr, err := is.measurementSeriesByExprIterator(name, opt.Condition, singleSeries, tsid)
	if search != nil {
		search.Finish()
	}

	idx.putIndexSearch(is)
	if err != nil {
		idx.logger.Error("measurementSeriesByExprIterator fail", zap.Error(err), zap.String("index", "mergeset"))
		return nil, err
	}
	if itr == nil {
		idx.logger.Warn("itr is nil", zap.String("index", "mergeset"))
		return nil, nil
	}
	return itr, nil
}

func (idx *MergeSetIndex) SearchSeriesWithOpts(span *tracing.Span, name []byte, opt *query.ProcessorOptions, callBack func(num int64) error, _ interface{}) (GroupSeries, int64, error) {
	var indexSpan, tsidIter, sortTs *tracing.Span
	if span != nil {
		indexSpan = span.StartSpan("index_stat").StartPP()
		defer indexSpan.Finish()
	}

	seriesNum := int64(0)
	itr, err := idx.SearchSeriesIterator(indexSpan, name, opt)
	if err != nil {
		return nil, seriesNum, err
	}
	if itr == nil {
		return nil, seriesNum, nil
	}

	seriesLen := int64(itr.Ids().Len())
	if e := callBack(seriesLen); e != nil {
		return nil, seriesNum, e
	}
	seriesNum = seriesLen

	var querySeriesUpperBound = syscontrol.GetQuerySeriesLimit()
	if querySeriesUpperBound > 0 && int(seriesNum) > querySeriesUpperBound && !syscontrol.GetQueryEnabledWhenExceedSeries() {
		return nil, 0, errno.NewError(errno.ErrQuerySeriesUpperBound, int(seriesNum), querySeriesUpperBound)
	}

	dims := make([]string, len(opt.Dimensions))
	copy(dims, opt.Dimensions)
	if len(dims) > 1 {
		sort.Strings(dims)
	}
	dimPos := genDimensionPosition(opt.Dimensions)

	var tagSetsMap map[string]*TagSetInfo
	var tagSetSlice []*TagSetInfo
	if opt.GroupByAllDims {
		tagSetSlice = make([]*TagSetInfo, 0, seriesNum)
	} else {
		tagSetsMap = make(map[string]*TagSetInfo)
	}

	if indexSpan != nil {
		tsidIter = indexSpan.StartSpan("tsid_iter")
		tsidIter.StartPP()
	}

	var seriesN int
	var ok bool
	var groupTagKey []byte // reused
	var totalSeriesKeyLen int64
	var tagSet *TagSetInfo
	var tagsBuf influx.PointTags
	var seriesKeys [][]byte
	var combineSeriesKey []byte
	var isExpectSeries []bool
	var exprs []*influxql.BinaryExpr

LOOP:
	for {
		se, err := itr.Next()
		if err != nil {
			idx.logger.Error("itr.Next() fail", zap.Error(err), zap.String("index", "mergeset"))
			return nil, seriesNum, err
		} else if se.SeriesID == 0 {
			break
		}

		seriesKeys, exprs, isExpectSeries, err = idx.searchSeriesWithTagArray(se.SeriesID, seriesKeys, exprs, combineSeriesKey, isExpectSeries, opt.Condition)
		if err != nil {
			idx.logger.Error("searchSeriesKey fail", zap.Error(err), zap.String("index", "mergeset"))
			return nil, seriesNum, err
		}

		for i := range seriesKeys {
			if !isExpectSeries[i] {
				continue
			}

			var seriesKey []byte
			seriesKey = append(seriesKey, seriesKeys[i]...)
			totalSeriesKeyLen += int64(len(seriesKey))
			var mLen int
			groupByAllSortedTag := false
			// when group by * or group by all sorted tag, groupByAllSortedTag is true
			// there is no need to sort tags since it is already sorted when writing point
			tagsBuf, seriesKey, mLen, groupByAllSortedTag, err = influx.Parse2SeriesGroupKey(seriesKey, seriesKey, opt.Dimensions)
			if err != nil {
				return nil, seriesNum, err
			}

			if len(dims) > 0 {
				if groupByAllSortedTag {
					// when group by all sorted tag, do not need to calculate groupKey again
					groupTagKey = append(groupTagKey, seriesKey[mLen+1:]...)
				} else {
					groupTagKey = MakeGroupTagsKey(dims, tagsBuf, groupTagKey, dimPos)
				}
			}

			if opt.GroupByAllDims {
				tagSet = NewSingleTagSetInfo()
				tagSet.key = append(tagSet.key, groupTagKey...)
				tagSetSlice = append(tagSetSlice, tagSet)
			} else {
				tagSet, ok = tagSetsMap[bytesutil.ToUnsafeString(groupTagKey)]
				if !ok {
					tagSet = NewTagSetInfo()
					tagSet.key = append(tagSet.key, groupTagKey...)
				}
				tagSetsMap[string(groupTagKey)] = tagSet
			}

			if exprs[i] != nil {
				tagSet.Append(se.SeriesID, seriesKey, exprs[i], tagsBuf, nil)
			} else {
				tagSet.Append(se.SeriesID, seriesKey, se.Expr, tagsBuf, nil)
			}
			groupTagKey = groupTagKey[:0]
			seriesN++

			if querySeriesUpperBound > 0 && seriesN >= querySeriesUpperBound {
				idx.logger.Error("", zap.Error(errno.NewError(errno.ErrQuerySeriesUpperBound)),
					zap.Int("querySeriesLimit", querySeriesUpperBound),
					zap.String("index_path", idx.Path()),
					zap.ByteString("measurement", name),
				)
				break LOOP
			}
		}
	}

	if tsidIter != nil {
		if !opt.GroupByAllDims {
			tsidIter.SetNameValue(fmt.Sprintf("tagset_count=%d, series_cnt=%d, serieskey_len=%d",
				len(tagSetsMap), seriesN, totalSeriesKeyLen))
		} else {
			tsidIter.SetNameValue(fmt.Sprintf("tagset_count=%d, series_cnt=%d, serieskey_len=%d",
				len(tagSetSlice), seriesN, totalSeriesKeyLen))
		}
		tsidIter.Finish()
	}

	if indexSpan != nil {
		sortTs = indexSpan.StartSpan("sort_tagset")
		sortTs.StartPP()
	}

	// The TagSets have been created, as a map of TagSets. Just send
	// the values back as a slice, sorting for consistency.
	if !opt.GroupByAllDims {
		tagSetSlice = make([]*TagSetInfo, 0, len(tagSetsMap))
		for _, v := range tagSetsMap {
			tagSetSlice = append(tagSetSlice, v)
		}
	}

	if len(tagSetSlice) > 1 {
		sgs := NewSortGroupSeries(tagSetSlice, opt.Ascending)
		sort.Sort(sgs)
	}

	if sortTs != nil {
		sortTs.Finish()
	}

	return tagSetSlice, seriesNum, nil
}

func (idx *MergeSetIndex) SearchSeriesKeys(series [][]byte, name []byte, condition influxql.Expr) ([][]byte, error) {
	return idx.SearchSeries(series, name, condition, DefaultTR)
}

func (idx *MergeSetIndex) SearchTagValues(name []byte, tagKeys [][]byte, condition influxql.Expr) ([][]string, error) {
	if len(tagKeys) == 0 {
		return nil, nil
	}

	is := idx.getIndexSearch()
	defer idx.putIndexSearch(is)

	return idx.StorageIndex.SearchTagValues(name, tagKeys, condition, is)
}

func (idx *MergeSetIndex) SearchTagValuesCardinality(name, tagKey []byte) (uint64, error) {
	is := idx.getIndexSearch()
	defer idx.putIndexSearch(is)

	tagValueMap, err := is.searchTagValuesBySingleKey(name, tagKey, nil, nil)
	if err != nil {
		return 0, err
	}
	return uint64(len(tagValueMap)), nil
}

func (idx *MergeSetIndex) searchSeriesKey(dst []byte, tsid uint64) ([]byte, error) {
	// fast path, get from cache
	seriesKey := idx.cache.getFromSeriesKeyCache(dst, tsid)
	if len(seriesKey) > 0 {
		return seriesKey, nil
	}

	// slow path, find from merge set
	var err error
	is := idx.getIndexSearch()
	dst, err = is.searchSeriesKey(dst, tsid)
	idx.putIndexSearch(is)
	if err == nil {
		if len(dst) == 0 {
			return nil, fmt.Errorf("invalid seriesKey: %q", dst)
		}
		idx.cache.putToSeriesKeyCache(tsid, dst)
		return dst, nil
	}

	return nil, err
}

func (idx *MergeSetIndex) seriesCardinality(name []byte) (uint64, error) {
	is := idx.getIndexSearch()
	defer idx.putIndexSearch(is)
	return is.seriesCount(name)
}

func (idx *MergeSetIndex) searchTSIDs(name []byte, expr influxql.Expr, tr TimeRange) ([]uint64, error) {
	is := idx.getIndexSearch()
	defer idx.putIndexSearch(is)
	return is.searchTSIDs(name, expr, tr)
}

func (idx *MergeSetIndex) DumpSeries(tw *tabwriter.Writer) error {
	is := idx.getIndexSearch()
	defer idx.putIndexSearch(is)
	ts := &is.ts
	kb := &is.kb
	seriesKey := influx.GetBytesBuffer()
	defer influx.PutBytesBuffer(seriesKey)
	kb.B = append(kb.B[:0], nsPrefixTSIDToKey)
	ts.Seek(kb.B)
	for ts.NextItem() {
		if !bytes.HasPrefix(ts.Item, kb.B) {
			// Nothing found.
			break
		}
		tail := ts.Item[len(kb.B):]
		tsid := encoding.UnmarshalUint64(tail)
		tail = tail[8:]
		seriesKey = influx.Parse2SeriesKey(tail, seriesKey[:0], true)
		fmt.Fprintf(tw, "%d\t%s\n", tsid, string(seriesKey))
	}

	return nil
}

func (idx *MergeSetIndex) DumpMeasurements(tw *tabwriter.Writer, dumpInfo *DumpInfo) error {
	is := idx.getIndexSearch()
	defer idx.putIndexSearch(is)
	ts := &is.ts
	kb := &is.kb
	mp := &is.mp

	var err error
	lastMeasurement := ""
	lastTagKey := ""
	lastTagValue := ""
	kb.B = append(kb.B[:0], nsPrefixTagToTSIDs)
	ts.Seek(kb.B)
	for ts.NextItem() {
		if !bytes.HasPrefix(ts.Item, kb.B) {
			break
		}

		mp.Reset()
		err = mp.Init(ts.Item, nsPrefixTagToTSIDs)
		if err != nil {
			return err
		}

		if !dumpInfo.MeasurementFilter.Match(mp.Name[:len(mp.Name)-2]) {
			continue
		}

		if string(mp.Name) != lastMeasurement {
			fmt.Fprintf(tw, "%s\tversion:%d\n", string(mp.Name[:len(mp.Name)-2]), encoding.UnmarshalUint16(mp.Name[len(mp.Name)-2:]))
			lastMeasurement = string(mp.Name)
		}

		if !dumpInfo.ShowTagKeys || !dumpInfo.TagKeyFilter.Match(mp.Tag.Key) {
			continue
		}

		if lastTagKey != string(mp.Tag.Key) {
			fmt.Fprintf(tw, "    %s\n", string(mp.Tag.Key))
			lastTagKey = string(mp.Tag.Key)
		}

		if !dumpInfo.ShowTagValues || !dumpInfo.TagValueFilter.Match(mp.Tag.Value) {
			continue
		}

		if lastTagValue != string(mp.Tag.Value) {
			fmt.Fprintf(tw, "        %s\n", string(mp.Tag.Value))
			lastTagValue = string(mp.Tag.Value)
		}

		if dumpInfo.ShowTagValueSeries {
			mp.ParseTSIDs()
			err = idx.dumpSeriesByTsids(tw, mp.TSIDs)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (idx *MergeSetIndex) dumpSeriesByTsids(tw *tabwriter.Writer, tsids []uint64) error {
	is := idx.getIndexSearch()
	defer idx.putIndexSearch(is)
	encodeSeriesKey := influx.GetBytesBuffer()
	seriesKey := influx.GetBytesBuffer()
	defer influx.PutBytesBuffer(encodeSeriesKey)
	defer influx.PutBytesBuffer(seriesKey)

	var err error
	for i := range tsids {
		encodeSeriesKey, err = is.searchSeriesKey(encodeSeriesKey[:0], tsids[i])
		if err != nil {
			return err
		}
		seriesKey = influx.Parse2SeriesKey(encodeSeriesKey, seriesKey[:0], true)
		fmt.Fprintf(tw, "            %s\n", string(seriesKey))
	}
	return nil
}

func (idx *MergeSetIndex) Close() error {
	idx.tb.MustClose()

	for i := 0; i < len(idx.queues); i++ {
		close(idx.queues[i])
	}

	for i := 0; i < len(idx.labelStoreQueues); i++ {
		close(idx.labelStoreQueues[i])
	}

	if err := idx.cache.close(); err != nil {
		return err
	}

	return nil
}

func (idx *MergeSetIndex) Path() string {
	return idx.path
}

func mergeIndexRows(data []byte, items []mergeset.Item) ([]byte, []mergeset.Item) {
	tmm := getTagToTSIDsRowsMerger()
	defer putTagToTSIDsRowsMerger(tmm)
	data, items = mergeindex.MergeItems(data, items, nsPrefixTagToTSIDs, tmm)
	return data, items
}

//lint:ignore U1000 keep this
func mergeIndexRowsForColumnStore(data []byte, items []mergeset.Item, isArrowFlight bool) ([]byte, []mergeset.Item) {
	tmm := getTagToValuesRowsMerger()
	defer putTagToValuesRowsMerger(tmm)
	data, items = mergeindex.MergeItemsForColumnStore(data, items, nsPrefixTagKeysToTagValues, tmm, isArrowFlight)
	return data, items
}

func (idx *MergeSetIndex) loadDeletedTSIDs() error {
	dmis := &uint64set.Set{}
	is := idx.getIndexSearch()
	defer idx.putIndexSearch(is)
	ts := &is.ts
	kb := &is.kb
	kb.B = append(kb.B[:0], nsPrefixDeletedTSIDs)
	ts.Seek(kb.B)
	for ts.NextItem() {
		item := ts.Item
		if !bytes.HasPrefix(item, kb.B) {
			break
		}
		item = item[len(kb.B):]
		if len(item) != 8 {
			return fmt.Errorf("unexpected item len; got %d bytes; want %d bytes", len(item), 8)
		}
		tsid := encoding.UnmarshalUint64(item)
		dmis.Add(tsid)
	}
	if err := ts.Error(); err != nil {
		return err
	}

	idx.deletedTSIDs.Store(dmis)
	return nil
}

func (idx *MergeSetIndex) DeleteTSIDs(name []byte, condition influxql.Expr, tr TimeRange) error {
	if tr.Min < 0 {
		tr.Min = 0
	}
	minDate := uint64(tr.Min) / nsPerDay
	maxDate := uint64(tr.Max) / nsPerDay
	if maxDate-minDate > maxDaysForSearch {
		// Too much dates to delete, it maybe affect other process' performance
		return fmt.Errorf("too much dates [%d] to delete, it must less or equal %d days", maxDate-minDate, maxDaysForSearch)
	}

	tsids, err := idx.searchTSIDs(name, condition, tr)
	if err != nil {
		return err
	}

	return idx.deleteTSIDs(tsids)
}

func (idx *MergeSetIndex) deleteTSIDs(tsids []uint64) error {
	ii := idxItemsPool.Get()
	defer idxItemsPool.Put(ii)

	// Lock to protect concurrent delete safety
	idx.deletedTSIDsLock.Lock()
	curDeleted := idx.deletedTSIDs.Load().(*uint64set.Set)
	newDeleted := curDeleted.Clone()
	newDeleted.AddMulti(tsids)
	idx.deletedTSIDs.Store(newDeleted)
	idx.deletedTSIDsLock.Unlock()

	for _, tsid := range tsids {
		ii.B = append(ii.B, nsPrefixDeletedTSIDs)
		ii.B = encoding.MarshalUint64(ii.B, tsid)
		ii.Next()
	}
	return idx.tb.AddItems(ii.Items)
}

func (idx *MergeSetIndex) getDeletedTSIDs() *uint64set.Set {
	return idx.deletedTSIDs.Load().(*uint64set.Set)
}

func (idx *MergeSetIndex) GetDeletePrimaryKeys(name []byte, condition influxql.Expr, tr TimeRange) ([]uint64, error) {
	return nil, nil
}

func (idx *MergeSetIndex) GetPrimaryKeys(name []byte, opt *query.ProcessorOptions) ([]uint64, error) {
	return nil, nil
}

func (idx *MergeSetIndex) DebugFlush() {
	idx.tb.DebugFlush()
}

func MergeSetIndexHandler(opt *Options, primaryIndex PrimaryIndex) (*IndexAmRoutine, error) {
	return &IndexAmRoutine{
		amKeyType:    MergeSet,
		amOpen:       MergeSetOpen,
		amBuild:      MergeSetBuild,
		amInsert:     MergeSetInsert,
		amDelete:     MergeSetDelete,
		amScan:       MergeSetScan,
		amClose:      MergeSetClose,
		amFlush:      MergeSetFlush,
		index:        primaryIndex,
		primaryIndex: nil,
	}, nil
}

// indexRelation contains MergeSetIndex
// meta store IndexBuild
func MergeSetBuild(relation *IndexRelation) error {
	return nil
}

func MergeSetOpen(index interface{}) error {
	mergeIndex, ok := index.(*MergeSetIndex)
	if !ok {
		return fmt.Errorf("index %v is not a MergeSetIndex", index)
	}
	return mergeIndex.Open()
}

func MergeSetInsert(index interface{}, primaryIndex PrimaryIndex, name []byte, row interface{}) (uint64, error) {
	mergeIndex, ok := index.(*MergeSetIndex)
	if !ok {
		return 0, fmt.Errorf("index %v is not a MergeSetIndex", index)
	}
	insertPoint := row.(*influx.Row)
	return mergeIndex.CreateIndexIfNotExistsByRow(insertPoint)
}

// upper function call should analyze result
func MergeSetScan(index interface{}, primaryIndex PrimaryIndex, span *tracing.Span, name []byte, opt *query.ProcessorOptions, callBack func(num int64) error, _ interface{}) (interface{}, int64, error) {
	mergeIndex, ok := index.(*MergeSetIndex)
	if !ok {
		return nil, 0, fmt.Errorf("index %v is not a MergeSetIndex", index)
	}
	return mergeIndex.SearchSeriesWithOpts(span, name, opt, callBack, nil)
}

func MergeSetDelete(index interface{}, primaryIndex PrimaryIndex, name []byte, condition influxql.Expr, tr TimeRange) error {
	// TODO
	return nil
}

func MergeSetClose(index interface{}) error {
	mergeIndex, ok := index.(*MergeSetIndex)
	if !ok {
		return fmt.Errorf("index %v is not a MergeSetIndex", index)
	}
	return mergeIndex.Close()
}

func MergeSetFlush(index interface{}) {
	mergeIndex, ok := index.(*MergeSetIndex)
	if !ok {
		logger.GetLogger().Error(fmt.Sprintf("index %v is not a FieldIndex", index))
		return
	}
	mergeIndex.DebugFlush()
}

type tagKeyReflection struct {
	order []int
	buf   [][]byte
}

func genDimensionPosition(dims []string) map[string]int {
	dimPos := make(map[string]int, len(dims))
	for i, dim := range dims {
		dimPos[dim] = i
	}
	return dimPos
}

func NewTagKeyReflection(src, curr []string) *tagKeyReflection {
	if len(src) != len(curr) {
		panic("src len doesn't equal to curr len")
	}
	t := &tagKeyReflection{
		order: make([]int, 0, len(src)),
		buf:   make([][]byte, len(src)),
	}
	for i := range curr {
		t.buf[i] = make([]byte, 0)
		for j := range src {
			if src[j] == curr[i] {
				t.order = append(t.order, j)
			}
		}
	}
	return t
}

func MakeGroupTagsKey(dims []string, tags influx.PointTags, dst []byte, dimPos map[string]int) []byte {
	if len(dims) == 0 || len(tags) == 0 {
		return nil
	}

	result := make([]string, len(dims))
	i, j := 0, 0
	for i < len(dims) && j < len(tags) {
		if dims[i] < tags[j].Key {
			result[dimPos[dims[i]]] = dims[i] + influx.StringSplit + influx.StringSplit
			i++
		} else if dims[i] > tags[j].Key {
			j++
		} else {
			result[dimPos[dims[i]]] = dims[i] + influx.StringSplit + tags[j].Value + influx.StringSplit

			i++
			j++
		}
	}
	for k := range result {
		dst = append(dst, bytesutil.ToUnsafeBytes(result[k])...)
	}

	// skip last '\x00'
	if len(dst) > 1 {
		return dst[:len(dst)-1]
	}
	return dst
}
