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
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"text/tabwriter"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/memory"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/mergeset"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/index/mergeindex"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/stringinterner"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/open_src/github.com/savsgio/dictpool"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
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

func invalidateTagCache() {
	// This function must be fast, since it is called each
	// time new timeseries is added.
	atomic.AddUint64(&tagFiltersKeyGen, 1)
}

var kbPool bytesutil.ByteBufferPool

var indexSearchPool sync.Pool

type MergeSetIndex struct {
	tb     *mergeset.Table
	logger *zap.Logger
	path   string

	cache *IndexCache

	// Deleted tsids
	deletedTSIDs     atomic.Value
	deletedTSIDsLock sync.Mutex

	mu sync.RWMutex

	indexBuilder *IndexBuilder
}

func NewMergeSetIndex(opts *Options) (*MergeSetIndex, error) {
	ms := &MergeSetIndex{
		path: opts.path,
	}

	return ms, nil
}

func (idx *MergeSetIndex) Open() error {
	path := idx.path + "/" + MergeSetDirName
	tb, err := mergeset.OpenTable(path, invalidateTagCache, mergeIndexRows)
	if err != nil {
		return fmt.Errorf("cannot open index:%s, err: %+v", path, err)
	}
	idx.tb = tb
	if logger.GetLogger() == nil {
		idx.logger = zap.NewNop()
	} else {
		idx.logger = logger.GetLogger().With(zap.String("index", "mergeset"))
	}

	mem := memory.Allowed()
	idx.cache = NewIndexCache(mem/32, mem/32, mem/16, idx.path)

	if err := idx.loadDeletedTSIDs(); err != nil {
		return err
	}

	return nil
}

func (idx *MergeSetIndex) SetIndexBuilder(builder *IndexBuilder) {
	idx.indexBuilder = builder
}

func (idx *MergeSetIndex) getIndexSearch() *indexSearch {
	v := indexSearchPool.Get()
	if v == nil {
		v = &indexSearch{
			idx: idx,
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
	is.idx = nil
	indexSearchPool.Put(is)
}

func (idx *MergeSetIndex) GetSeriesIdBySeriesKey(seriesKey []byte, name []byte) (uint64, error) {
	var err error
	version, loaded := idx.indexBuilder.loadOrStore(stringinterner.InternSafe(record.Bytes2str(name)))
	if !loaded {
		if err = idx.indexBuilder.saveVersion(name, version); err != nil {
			return 0, err
		}
	}
	vkey := kbPool.Get()
	defer kbPool.Put(vkey)
	vkey.B = append(vkey.B[:0], seriesKey...)
	vkey.B = encoding.MarshalUint16(vkey.B, version)
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.getSeriesIdBySeriesKey(vkey.B)
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
		version, loaded := idx.indexBuilder.loadOrStore(stringinterner.InternSafe(mmRows.D[mmIdx].Key))
		if !loaded {
			if err = idx.indexBuilder.saveVersion([]byte(mmRows.D[mmIdx].Key), version); err != nil {
				return err
			}
		}
		vname.B = append(vname.B[:0], []byte(mmRows.D[mmIdx].Key)...)
		vname.B = encoding.MarshalUint16(vname.B, version)
		for rowIdx := range *rows {
			if (*rows)[rowIdx].SeriesId != 0 {
				continue
			}

			// retry get seriesId in Lock
			vkey.B = append(vkey.B[:0], (*rows)[rowIdx].IndexKey...)
			vkey.B = encoding.MarshalUint16(vkey.B, version)
			(*rows)[rowIdx].SeriesId, err = idx.createIndexesIfNotExists(vkey.B, vname.B, (*rows)[rowIdx].Tags, (*rows)[rowIdx].ShardKey)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (idx *MergeSetIndex) CreateIndexIfNotExistsByRow(row *influx.Row, version uint16) (uint64, error) {
	vkey := kbPool.Get()
	defer kbPool.Put(vkey)
	vname := kbPool.Get()
	defer kbPool.Put(vname)

	var sid uint64
	vname.B = append(vname.B[:0], []byte(row.Name)...)
	vname.B = encoding.MarshalUint16(vname.B, version)
	// retry get seriesId in Lock
	vkey.B = append(vkey.B[:0], row.IndexKey...)
	vkey.B = encoding.MarshalUint16(vkey.B, version)
	sid, err := idx.createIndexesIfNotExists(vkey.B, vname.B, row.Tags, row.ShardKey)
	return sid, err
}

func (idx *MergeSetIndex) createIndexesIfNotExists(vkey, vname []byte, tags []influx.Tag, shardKey []byte) (uint64, error) {
	tsid, err := idx.getSeriesIdBySeriesKey(vkey)
	if err != nil {
		return 0, err
	}

	if tsid != 0 {
		return tsid, nil
	}

	defer func(id *uint64) {
		if *id != 0 {
			idx.cache.PutTSIDToTSIDCache(id, vkey)
		}
	}(&tsid)

	tsid, err = idx.createIndexes(vkey, vname, tags)
	return tsid, err
}

var idxItemsPool mergeindex.IndexItemsPool

func (idx *MergeSetIndex) createIndexes(seriesKey []byte, name []byte, tags []influx.Tag) (uint64, error) {
	tsid := GenerateUUID()

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

	for i := range tags {
		compositeKey.B = marshalCompositeTagKey(compositeKey.B[:0], name, []byte(tags[i].Key))
		ii.B = append(ii.B, nsPrefixTagToTSIDs)
		ii.B = marshalTagValue(ii.B, compositeKey.B)
		ii.B = marshalTagValue(ii.B, []byte(tags[i].Value))
		ii.B = encoding.MarshalUint64(ii.B, tsid)
		ii.Next()
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

func (idx *MergeSetIndex) SeriesCardinality(name []byte, condition influxql.Expr, tr TimeRange) (uint64, error) {
	version, ok := idx.indexBuilder.getVersion(record.Bytes2str(name))

	if !ok {
		return 0, nil
	}
	name = encoding.MarshalUint16(name, version)
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
	version, ok := idx.indexBuilder.getVersion(record.Bytes2str(name))
	if !ok {
		return series, nil
	}
	name = encoding.MarshalUint16(name, version)
	tsids, err := idx.searchTSIDs(name, condition, tr)
	if err != nil {
		return series, err
	}

	var tmpSeriesKey []byte // reused
	if cap(series) >= len(tsids) {
		series = series[:len(tsids)]
	} else {
		series = series[:cap(series)]
		series = append(series, make([][]byte, len(tsids)-cap(series))...)
	}
	for i := range tsids {
		tmpSeriesKey, err = idx.searchSeriesKey(tmpSeriesKey, tsids[i])
		if err != nil {
			return nil, err
		}
		if series[i] == nil {
			series[i] = influx.GetBytesBuffer()
		}
		series[i] = influx.Parse2SeriesKey(tmpSeriesKey, series[i][:0])
		tmpSeriesKey = tmpSeriesKey[:0]
	}
	return series, nil
}

func (idx *MergeSetIndex) SearchSeriesWithOpts(span *tracing.Span, name []byte, opt *query.ProcessorOptions) (GroupSeries, error) {
	var indexSpan, search, tsid_iter, sortTs *tracing.Span
	if span != nil {
		indexSpan = span.StartSpan("index_stat").StartPP()
		defer indexSpan.Finish()
	}
	version, ok := idx.indexBuilder.getVersion(record.Bytes2str(name))
	if !ok {
		return nil, nil
	}
	// need add version for delete measurement safeguard
	if indexSpan != nil {
		search = indexSpan.StartSpan("tsid_search")
		search.StartPP()
	}

	nameWithVer := encoding.MarshalUint16(name, version)

	var tsid uint64
	var err error
	tsid, err = idx.GetSeriesIdBySeriesKey(opt.SeriesKey, name)
	if err != nil {
		return nil, err
	}

	is := idx.getIndexSearch()
	defer idx.putIndexSearch(is)

	itr, err := is.measurementSeriesByExprIterator(nameWithVer, opt.Condition, TimeRange{Min: opt.StartTime, Max: opt.EndTime}, opt.GetHintType() == hybridqp.FullSeriesQuery, tsid)
	if search != nil {
		search.Finish()
	}

	if err != nil {
		return nil, err
	}
	if itr == nil {
		return nil, nil
	}

	var dims []string
	var totalSeriesKeyLen int64
	if len(opt.Dimensions) > 0 {
		dims = make([]string, len(opt.Dimensions))
		copy(dims, opt.Dimensions)
		sort.Strings(dims)
	}

	kvReflection := NewTagKeyReflection(opt.Dimensions, dims)
	tagSets := make(map[string]*TagSetInfo)
	releaseSeriesInfo := func() {
		for _, v := range tagSets {
			v.release()
		}
	}

	if indexSpan != nil {
		tsid_iter = indexSpan.StartSpan("tsid_iter")
		tsid_iter.StartPP()
	}

	deletedTSIDs := idx.getDeletedTSIDs()
	var seriesN int
	var indexKeyBuf []byte // reused
	var groupTagKey []byte // reused

	for {
		var tagsBuf influx.PointTags
		se, err := itr.Next()
		if err != nil {
			releaseSeriesInfo()
			return nil, err
		} else if se.SeriesID == 0 {
			break
		}

		if deletedTSIDs.Has(se.SeriesID) {
			continue
		}

		indexKeyBuf, err = idx.searchSeriesKey(indexKeyBuf, se.SeriesID)
		if err != nil {
			releaseSeriesInfo()
			return nil, err
		}

		// there is no need to sort tags since it is already sorted when writing point
		_, err = influx.IndexKeyToTags(indexKeyBuf, true, &tagsBuf)
		if err != nil {
			releaseSeriesInfo()
			return nil, err
		}

		if len(dims) > 0 {
			kvReflection.Reset()
			groupTagKey = MakeGroupTagsKey(dims, tagsBuf, groupTagKey, kvReflection)
		}

		tagSet, ok := tagSets[string(groupTagKey)]
		if !ok {
			tagSet = NewTagSetInfo()
			tagSet.key = append(tagSet.key, groupTagKey...)
		}

		// seriesKey buf will be released when the cursor closed
		seriesKey := getSeriesKeyBuf()
		seriesKey = influx.Parse2SeriesKey(indexKeyBuf, seriesKey)

		totalSeriesKeyLen += int64(len(indexKeyBuf))
		tagSet.Append(se.SeriesID, seriesKey, se.Expr, tagsBuf)
		tagSets[string(groupTagKey)] = tagSet
		indexKeyBuf = indexKeyBuf[:0]
		groupTagKey = groupTagKey[:0]
		seriesN++
	}

	if tsid_iter != nil {
		tsid_iter.SetNameValue(fmt.Sprintf("tagset_count=%d, series_cnt=%d, serieskey_len=%d",
			len(tagSets), seriesN, totalSeriesKeyLen))
		tsid_iter.Finish()
	}

	if indexSpan != nil {
		sortTs = indexSpan.StartSpan("sort_tagset")
		sortTs.StartPP()
	}
	// Sort the series in each tag set.
	for _, t := range tagSets {
		sort.Sort(t)
	}

	// The TagSets have been created, as a map of TagSets. Just send
	// the values back as a slice, sorting for consistency.
	sortedTagsSets := make([]*TagSetInfo, 0, len(tagSets))
	for _, v := range tagSets {
		sortedTagsSets = append(sortedTagsSets, v)
	}
	sort.Sort(GroupSeries(sortedTagsSets))

	if sortTs != nil {
		sortTs.Finish()
	}

	return sortedTagsSets, nil
}

func (idx *MergeSetIndex) SearchSeriesKeys(series [][]byte, name []byte, condition influxql.Expr) ([][]byte, error) {
	return idx.SearchSeries(series, name, condition, DefaultTR)
}

func (idx *MergeSetIndex) SearchAllSeriesKeys() ([][]byte, error) {
	is := idx.getIndexSearch()
	keys, err := is.getAllSeriesKeys()
	idx.putIndexSearch(is)
	return keys, err
}

func (idx *MergeSetIndex) SearchTagValues(name []byte, tagKeys [][]byte, condition influxql.Expr) ([][]string, error) {
	if len(tagKeys) == 0 {
		return nil, nil
	}

	is := idx.getIndexSearch()
	defer idx.putIndexSearch(is)

	version, ok := idx.indexBuilder.getVersion(record.Bytes2str(name))
	if !ok {
		return nil, nil
	}

	name = encoding.MarshalUint16(name, version)
	return is.searchTagValues(name, tagKeys, condition)
}

func (idx *MergeSetIndex) SearchAllTagValues(tagKey []byte) (map[string]map[string]struct{}, error) {
	if len(tagKey) == 0 {
		return nil, nil
	}

	is := idx.getIndexSearch()
	defer idx.putIndexSearch(is)

	result := make(map[string]map[string]struct{}, 16)
	var err error
	err = idx.indexBuilder.walkVersions(func(key string, value uint16) error {
		var tvs map[string]struct{}
		name := key
		version, ok := idx.indexBuilder.mVersion[key]
		if !ok {
			err = fmt.Errorf("measurement %s does not exist", name)
			return err
		}

		tvs, err = is.searchTagValuesBySingleKey(encoding.MarshalUint16([]byte(name), version), tagKey, nil)
		if err != nil {
			return err
		}
		result[name] = tvs
		return nil
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (idx *MergeSetIndex) SearchTagValuesCardinality(name, tagKey []byte) (uint64, error) {
	is := idx.getIndexSearch()
	defer idx.putIndexSearch(is)

	version, ok := idx.indexBuilder.getVersion(record.Bytes2str(name))
	if !ok {
		return 0, fmt.Errorf("measurement %s does not exist", name)
	}

	name = encoding.MarshalUint16(name, version)
	tagValueMap, err := is.searchTagValuesBySingleKey(name, tagKey, nil)
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
		if len(dst) < 3 {
			return nil, fmt.Errorf("invalid seriesKey: %q", dst)
		}
		seriesKey := dst[:len(dst)-2]
		idx.cache.putToSeriesKeyCache(tsid, seriesKey)
		return seriesKey, nil
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
		seriesKey = influx.Parse2SeriesKey(tail, seriesKey[:0])
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
		seriesKey = influx.Parse2SeriesKey(encodeSeriesKey, seriesKey[:0])
		fmt.Fprintf(tw, "            %s\n", string(seriesKey))
	}
	return nil
}

func (idx *MergeSetIndex) Close() error {
	idx.tb.MustClose()

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

	version, ok := idx.indexBuilder.getVersion(record.Bytes2str(name))
	if !ok {
		return fmt.Errorf("measurement %s doesn't exist", name)
	}
	name = encoding.MarshalUint16(name, version)

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

func MergeSetIndexHandler(opt *Options, primaryIndex PrimaryIndex) *IndexAmRoutine {
	return &IndexAmRoutine{
		amKeyType:    MergeSet,
		amOpen:       MergeSetOpen,
		amBuild:      MergeSetBuild,
		amInsert:     MergeSetInsert,
		amDelete:     MergeSetDelete,
		amScan:       MergeSetScan,
		amClose:      MergeSetClose,
		index:        primaryIndex,
		primaryIndex: nil,
	}
}

// indexRelation contains MergeSetIndex
// meta store IndexBuild
func MergeSetBuild(relation *IndexRelation) error {
	return nil
}

func MergeSetOpen(index interface{}) error {
	mergeindex := index.(*MergeSetIndex)
	return mergeindex.Open()
}

func MergeSetInsert(index interface{}, primaryIndex PrimaryIndex, name []byte, row interface{}, version uint16) (uint64, error) {
	mergeindex := index.(*MergeSetIndex)
	insertPoints := row.(*influx.Row)
	return mergeindex.CreateIndexIfNotExistsByRow(insertPoints, version)
}

// upper function call should analyze result
func MergeSetScan(index interface{}, primaryIndex PrimaryIndex, span *tracing.Span, name []byte, opt *query.ProcessorOptions) (interface{}, error) {
	mergeindex := index.(*MergeSetIndex)
	return mergeindex.SearchSeriesWithOpts(span, name, opt)
}

func MergeSetDelete(index interface{}, primaryIndex PrimaryIndex, name []byte, condition influxql.Expr, tr TimeRange) error {
	// TODO
	return nil
}

func MergeSetClose(index interface{}) error {
	mergeindex := index.(*MergeSetIndex)
	return mergeindex.Close()
}

type tagKeyReflection struct {
	order []int
	buf   [][]byte
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

func (t *tagKeyReflection) Reset() {
	for i := range t.buf {
		t.buf[i] = t.buf[i][:0]
	}
}

func MakeGroupTagsKey(dims []string, tags influx.PointTags, dst []byte, reflect *tagKeyReflection) []byte {
	if len(dims) == 0 || len(tags) == 0 {
		return nil
	}

	i, j := 0, 0
	for i < len(dims) && j < len(tags) {
		if dims[i] < tags[j].Key {
			reflect.buf[reflect.order[i]] = append(reflect.buf[reflect.order[i]], bytesutil.ToUnsafeBytes(dims[i])...)
			reflect.buf[reflect.order[i]] = append(reflect.buf[reflect.order[i]], []byte{'=', ','}...)
			i++
		} else if dims[i] > tags[j].Key {
			j++
		} else {
			reflect.buf[reflect.order[i]] = append(reflect.buf[reflect.order[i]], bytesutil.ToUnsafeBytes(dims[i])...)
			reflect.buf[reflect.order[i]] = append(reflect.buf[reflect.order[i]], '=')
			reflect.buf[reflect.order[i]] = append(reflect.buf[reflect.order[i]], bytesutil.ToUnsafeBytes(tags[j].Value)...)
			reflect.buf[reflect.order[i]] = append(reflect.buf[reflect.order[i]], ',')

			i++
			j++
		}
	}
	for k := range reflect.order {
		dst = append(dst, reflect.buf[k]...)
	}

	// skip last ','
	if len(dst) > 1 {
		return dst[:len(dst)-1]
	}
	return dst
}
