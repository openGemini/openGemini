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

//nolint

package tsi

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/index/clv"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/index"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/savsgio/dictpool"
)

var (
	sequenceID = uint64(time.Now().Unix())
)

var (
	_ TagSet   = &TagSetMergeInfo{}
	_ TagSetEx = &TagSetInfo{}
)

type TagSet interface {
	Ref()
	Unref()
	Len() int
	Less(i, j int) bool
	Swap(i, j int)
	GetSid(int, int) uint64
	GetShardId(int, int) uint64
	GetShardNum(int) int
	GetSeriesKeys(int) []byte
	GetFilters(int) influxql.Expr
	GetKey() []byte
	GetTagsVec(int) *influx.PointTags
	GetRowFilter(int) *[]clv.RowFilter
}

type TagSetEx interface {
	TagSet
	Cut(idx int)
	GetTagSetItem(int) *TagSetInfoItem
	SetKey([]byte)
	AppendKey(...byte)
	Append(uint64, []byte, influxql.Expr, influx.PointTags, []clv.RowFilter)
	AppendWithOpt(uint64, []byte, influxql.Expr, influx.PointTags, []clv.RowFilter, *query.ProcessorOptions)
	GetTagsWithQuerySchema(i int, schema *executor.QuerySchema) *influx.PointTags
	Sort(schema *executor.QuerySchema)
	TagSetItems() []TagSetInfoItem
}

type TagSetMergeInfo struct {
	ref int64

	ShardIds   [][]uint64
	IDs        [][]uint64
	Filters    []influxql.Expr
	SeriesKeys [][]byte           // encoded series key
	TagsVec    []influx.PointTags // tags of all series
	key        []byte             // group by tag sets key
	RowFilters *clv.RowFilters    // uesed in full-text index for row filtering
}

func (t *TagSetMergeInfo) GetSid(sidIdx, shardIdx int) uint64 {
	return t.IDs[sidIdx][shardIdx]
}

func (t *TagSetMergeInfo) GetShardNum(sidIdx int) int {
	return len(t.ShardIds[sidIdx])
}

func (t *TagSetMergeInfo) GetShardId(sidIdx, shardIdx int) uint64 {
	return t.ShardIds[sidIdx][shardIdx]
}

func (t *TagSetMergeInfo) GetSeriesKeys(sidIdx int) []byte {
	return t.SeriesKeys[sidIdx]
}

func (t *TagSetMergeInfo) GetFilters(sidIdx int) influxql.Expr {
	return t.Filters[sidIdx]
}

func (t *TagSetMergeInfo) GetKey() []byte {
	return t.key
}

func (t *TagSetMergeInfo) GetTagsVec(sidIdx int) *influx.PointTags {
	return &t.TagsVec[sidIdx]
}

func (t *TagSetMergeInfo) GetRowFilter(idx int) *[]clv.RowFilter {
	if t.RowFilters != nil {
		return t.RowFilters.GetRowFilter(idx)
	}
	return nil
}

func (t *TagSetMergeInfo) Len() int {
	return len(t.IDs)
}

func (t *TagSetMergeInfo) Less(i, j int) bool {
	return bytes.Compare(t.SeriesKeys[i], t.SeriesKeys[j]) < 0
}

func (t *TagSetMergeInfo) Swap(i, j int) {
	t.SeriesKeys[i], t.SeriesKeys[j] = t.SeriesKeys[j], t.SeriesKeys[i]
	t.IDs[i], t.IDs[j] = t.IDs[j], t.IDs[i]
	t.ShardIds[i], t.ShardIds[j] = t.ShardIds[j], t.ShardIds[i]
	t.TagsVec[i], t.TagsVec[j] = t.TagsVec[j], t.TagsVec[i]
	t.Filters[i], t.Filters[j] = t.Filters[j], t.Filters[i]
	if t.RowFilters != nil {
		t.RowFilters.Swap(i, j)
	}
}

func (t *TagSetMergeInfo) Ref() {
	atomic.AddInt64(&t.ref, 1)
}

func (t *TagSetMergeInfo) Unref() {
	if atomic.AddInt64(&t.ref, -1) == 0 {
		t.reset()
		tagSetPool.put(t)
	}
}

func (t *TagSetMergeInfo) reset() {
	t.ref = 0
	t.key = t.key[:0]
	t.IDs = t.IDs[:0]
	t.ShardIds = t.ShardIds[:0]
	t.Filters = t.Filters[:0]
	t.TagsVec = t.TagsVec[:0]
	t.SeriesKeys = t.SeriesKeys[:0]
	if t.RowFilters != nil {
		t.RowFilters.Reset()
	}
}

var (
	tagSetPool = newTagSetMergePool()
)

type tagSetMergeInfoPool struct {
	cache chan *TagSetMergeInfo
}

func newTagSetMergePool() *tagSetMergeInfoPool {
	n := cpu.GetCpuNum() * 2
	if n < 8 {
		n = 8
	}
	if n > 128 {
		n = 128
	}
	return &tagSetMergeInfoPool{cache: make(chan *TagSetMergeInfo, n)}
}

func (p *tagSetMergeInfoPool) put(set *TagSetMergeInfo) {
	select {
	case p.cache <- set:
	default:
	}
}

func (p *tagSetMergeInfoPool) get(size int) (set *TagSetMergeInfo) {
	select {
	case set = <-p.cache:
		return
	default:
		return &TagSetMergeInfo{
			ref:        0,
			IDs:        make([][]uint64, 0, size),
			ShardIds:   make([][]uint64, 0, size),
			Filters:    make([]influxql.Expr, 0, size),
			SeriesKeys: make([][]byte, 0, size),
			TagsVec:    make([]influx.PointTags, 0, size),
		}
	}
}

func TransTagSet2Merged(t1 TagSetEx, shardId uint64, shardNum int) *TagSetMergeInfo {
	t := tagSetPool.get(t1.Len())
	shardIdSlices := make([]uint64, t1.Len()*shardNum)
	idSlices := make([]uint64, t1.Len()*shardNum)
	for i, item := range t1.TagSetItems() {
		t.Filters = append(t.Filters, item.Filter)
		t.SeriesKeys = append(t.SeriesKeys, item.SeriesKey)
		t.TagsVec = append(t.TagsVec, item.TagsVec)
		t.IDs = append(t.IDs, append(idSlices[i*shardNum:i*shardNum:(i+1)*shardNum], item.ID))
		t.ShardIds = append(t.ShardIds, append(shardIdSlices[i*shardNum:i*shardNum:(i+1)*shardNum], shardId))
	}
	t.key = append(t.key, t1.GetKey()...)
	return t
}

func ExtendMergeTagSetInfos(ts []*TagSetMergeInfo, shardId uint64) []*TagSetMergeInfo {
	for _, t := range ts {
		for i := range t.IDs {
			if sidIdx := len(t.IDs[i]) - 1; sidIdx >= 0 {
				t.IDs[i] = append(t.IDs[i], t.IDs[i][sidIdx])
				t.ShardIds[i] = append(t.ShardIds[i], shardId)
			}
		}
	}
	return ts
}

func SortMergeTagSetInfos(t1 []*TagSetMergeInfo, t2 []TagSetEx, shardId uint64, shardNum int) []*TagSetMergeInfo {
	if len(t1) == 0 {
		res := make([]*TagSetMergeInfo, len(t2))
		for i := range t2 {
			t2[i].Ref()
			sort.Sort(t2[i])
			res[i] = TransTagSet2Merged(t2[i], shardId, shardNum)
			t2[i].Unref()
		}
		return res
	}

	res := make([]*TagSetMergeInfo, 0, util.Max(len(t1), len(t2)))
	i, j := 0, 0
	for i < len(t1) && j < len(t2) {
		cmp := bytes.Compare(t1[i].key, t2[j].GetKey())
		if cmp == 0 {
			t2[j].Ref()
			sort.Sort(t2[j])
			res = append(res, sortMergeTagSetInfo(t1[i], t2[j], shardId, shardNum))
			t2[j].Unref()
			i++
			j++
		} else if cmp < 0 {
			res = append(res, t1[i])
			i++
		} else {
			t2[j].Ref()
			sort.Sort(t2[j])
			res = append(res, TransTagSet2Merged(t2[j], shardId, shardNum))
			t2[j].Unref()
			j++
		}
	}
	res = append(res, t1[i:]...)
	for ; j < len(t2); j++ {
		t2[j].Ref()
		sort.Sort(t2[j])
		res = append(res, TransTagSet2Merged(t2[j], shardId, shardNum))
		t2[j].Unref()
	}
	return res
}

func sortMergeTagSetInfo(t1 *TagSetMergeInfo, t2 TagSetEx, shardId uint64, shardNum int) *TagSetMergeInfo {
	res := tagSetPool.get(util.Max(len(t1.IDs), t2.Len()))
	res.key = append(res.key, t1.key...)
	i, j := 0, 0
	for i < len(t1.IDs) && j < t2.Len() {
		t2Item := t2.GetTagSetItem(j)
		cmp := bytes.Compare(t1.SeriesKeys[i], t2Item.SeriesKey)
		if cmp == 0 {
			res.SeriesKeys = append(res.SeriesKeys, t1.SeriesKeys[i])
			res.TagsVec = append(res.TagsVec, t1.TagsVec[i])
			res.IDs = append(res.IDs, append(t1.IDs[i], t2Item.ID))
			res.ShardIds = append(res.ShardIds, append(t1.ShardIds[i], shardId))
			res.Filters = append(res.Filters, t1.Filters[i])
			i++
			j++
		} else if cmp < 0 {
			res.SeriesKeys = append(res.SeriesKeys, t1.SeriesKeys[i])
			res.TagsVec = append(res.TagsVec, t1.TagsVec[i])
			res.IDs = append(res.IDs, t1.IDs[i])
			res.ShardIds = append(res.ShardIds, t1.ShardIds[i])
			res.Filters = append(res.Filters, t1.Filters[i])
			i++
		} else {
			res.SeriesKeys = append(res.SeriesKeys, t2Item.SeriesKey)
			res.TagsVec = append(res.TagsVec, t2Item.TagsVec)
			res.IDs = append(res.IDs, append(make([]uint64, 0, shardNum), t2Item.ID))
			res.ShardIds = append(res.ShardIds, append(make([]uint64, 0, shardNum), shardId))
			res.Filters = append(res.Filters, t2Item.Filter)
			j++
		}
	}
	res.SeriesKeys = append(res.SeriesKeys, t1.SeriesKeys[i:]...)
	res.TagsVec = append(res.TagsVec, t1.TagsVec[i:]...)
	res.IDs = append(res.IDs, t1.IDs[i:]...)
	res.ShardIds = append(res.ShardIds, t1.ShardIds[i:]...)
	res.Filters = append(res.Filters, t1.Filters[i:]...)

	for ; j < t2.Len(); j++ {
		t2Item := t2.GetTagSetItem(j)
		res.SeriesKeys = append(res.SeriesKeys, t2Item.SeriesKey)
		res.TagsVec = append(res.TagsVec, t2Item.TagsVec)
		res.IDs = append(res.IDs, append(make([]uint64, 0, shardNum), t2Item.ID))
		res.ShardIds = append(res.ShardIds, append(make([]uint64, 0, shardNum), shardId))
		res.Filters = append(res.Filters, t2Item.Filter)
	}
	return res
}

type TagSetInfoItem struct {
	ID        uint64
	Filter    influxql.Expr
	SeriesKey []byte           // encoded series key
	TagsVec   influx.PointTags // tags of series
}

type TagSetInfo struct {
	ref int64

	TagSetInfoItems []TagSetInfoItem
	key             []byte          // group by tag sets key
	RowFilters      *clv.RowFilters // only uesed in full-text index for row filtering
}

func (t *TagSetInfo) SetKey(key []byte) {
	t.key = key
}

func (t *TagSetInfo) TagSetItems() []TagSetInfoItem {
	return t.TagSetInfoItems
}

func (t *TagSetInfo) GetTagSetItem(idx int) *TagSetInfoItem {
	return &t.TagSetInfoItems[idx]
}

func (t *TagSetInfo) AppendKey(key ...byte) {
	t.key = append(t.key, key...)
}

func (t *TagSetInfo) String() string {
	n := len(t.TagSetInfoItems)
	var builder strings.Builder
	for i := 0; i < n; i++ {
		builder.WriteString(fmt.Sprintf("%d -> %s\n", t.TagSetInfoItems[i].ID, t.TagSetInfoItems[i].SeriesKey))
	}
	return builder.String()
}

func (t *TagSetInfo) Len() int { return len(t.TagSetInfoItems) }
func (t *TagSetInfo) Less(i, j int) bool {
	return bytes.Compare(t.TagSetInfoItems[i].SeriesKey, t.TagSetInfoItems[j].SeriesKey) < 0
}
func (t *TagSetInfo) Swap(i, j int) {
	t.TagSetInfoItems[i], t.TagSetInfoItems[j] = t.TagSetInfoItems[j], t.TagSetInfoItems[i]
	if t.RowFilters != nil {
		t.RowFilters.Swap(i, j)
	}
}

func (t *TagSetInfo) Cut(idx int) {
	if idx >= len(t.TagSetInfoItems) {
		return
	}
	t.TagSetInfoItems = t.TagSetInfoItems[:idx]
}

func NewTagSetInfo() *TagSetInfo {
	return setPool.getInit(32)
}

func NewSingleTagSetInfo() *TagSetInfo {
	return setPool.getInit(1)
}

func (t *TagSetInfo) reset() {
	t.ref = 0
	t.key = t.key[:0]
	t.TagSetInfoItems = t.TagSetInfoItems[:0]
	if t.RowFilters != nil {
		t.RowFilters.Reset()
	}
}

func (t *TagSetInfo) AppendWithOpt(id uint64, seriesKey []byte, filter influxql.Expr, tags influx.PointTags,
	rowFilter []clv.RowFilter, opt *query.ProcessorOptions) {
	item := TagSetInfoItem{ID: id, Filter: filter}
	if opt.SimpleTagset {
		if len(t.TagSetInfoItems) == 0 {
			item.TagsVec = tags
		}
	} else {
		item.TagsVec = tags
		item.SeriesKey = seriesKey
	}
	t.TagSetInfoItems = append(t.TagSetInfoItems, item)
	if t.RowFilters != nil {
		t.RowFilters.Append(rowFilter)
	}
}

func (t *TagSetInfo) Append(id uint64, seriesKey []byte, filter influxql.Expr, tags influx.PointTags, rowFilter []clv.RowFilter) {
	item := TagSetInfoItem{ID: id, Filter: filter, TagsVec: tags, SeriesKey: seriesKey}
	t.TagSetInfoItems = append(t.TagSetInfoItems, item)
	if t.RowFilters != nil {
		t.RowFilters.Append(rowFilter)
	}
}

func (t *TagSetInfo) GetTagsWithQuerySchema(i int, s *executor.QuerySchema) *influx.PointTags {
	if s.Options().GetSimpleTagset() {
		return &t.TagSetInfoItems[0].TagsVec
	}
	return &t.TagSetInfoItems[i].TagsVec
}

func (t *TagSetInfo) Ref() {
	atomic.AddInt64(&t.ref, 1)
}

func (t *TagSetInfo) Unref() {
	if atomic.AddInt64(&t.ref, -1) == 0 {
		t.release()
	}
}

func (t *TagSetInfo) release() {
	t.reset()
	setPool.put(t)
}

func (t *TagSetInfo) Sort(schema *executor.QuerySchema) {
	if schema.HasExcatLimit() {
		sort.Sort(t)
	}
}

func (t *TagSetInfo) GetSid(sidIdx, _ int) uint64 {
	return t.TagSetInfoItems[sidIdx].ID
}

func (t *TagSetInfo) GetShardId(_, _ int) uint64 {
	return 0
}

func (t *TagSetInfo) GetShardNum(_ int) int {
	return 1
}

func (t *TagSetInfo) GetSeriesKeys(sidIdx int) []byte {
	return t.TagSetInfoItems[sidIdx].SeriesKey
}

func (t *TagSetInfo) GetFilters(sidIdx int) influxql.Expr {
	return t.TagSetInfoItems[sidIdx].Filter
}

func (t *TagSetInfo) GetKey() []byte {
	return t.key
}

func (t *TagSetInfo) GetTagsVec(sidIdx int) *influx.PointTags {
	return &t.TagSetInfoItems[sidIdx].TagsVec
}

func (t *TagSetInfo) GetRowFilter(idx int) *[]clv.RowFilter {
	if t.RowFilters != nil {
		return t.RowFilters.GetRowFilter(idx)
	}
	return nil
}

type tagSetInfoPool struct {
	cache chan *TagSetInfo
}

var (
	setPool = NewTagSetPool()
)

func NewTagSetPool() *tagSetInfoPool {
	n := cpu.GetCpuNum() * 2
	if n < 8 {
		n = 8
	}
	if n > 128 {
		n = 128
	}

	return &tagSetInfoPool{
		cache: make(chan *TagSetInfo, n),
	}
}

func (p *tagSetInfoPool) put(set *TagSetInfo) {
	select {
	case p.cache <- set:
	default:
	}
}

func (p *tagSetInfoPool) getInit(initNum int) (set *TagSetInfo) {
	return p.GetBySize(initNum)
}

func (p *tagSetInfoPool) GetBySize(size int) (set *TagSetInfo) {
	select {
	case set = <-p.cache:
		return
	default:
		return &TagSetInfo{
			ref:             0,
			TagSetInfoItems: make([]TagSetInfoItem, 0, size),
		}
	}
}

type SortGroupSeries struct {
	groupSeries []TagSetEx
	ascending   bool
}

func (s SortGroupSeries) Len() int {
	return len(s.groupSeries)
}

func (s SortGroupSeries) Less(i, j int) bool {
	if s.ascending {
		return bytes.Compare(s.groupSeries[i].GetKey(), s.groupSeries[j].GetKey()) < 0
	}
	return bytes.Compare(s.groupSeries[i].GetKey(), s.groupSeries[j].GetKey()) > 0
}

func (s SortGroupSeries) Swap(i, j int) {
	s.groupSeries[i], s.groupSeries[j] = s.groupSeries[j], s.groupSeries[i]
}

func NewSortGroupSeries(groupSeries []TagSetEx, ascending bool) *SortGroupSeries {
	return &SortGroupSeries{
		groupSeries: groupSeries,
		ascending:   ascending,
	}
}

type GroupSeries []TagSetEx

func (gs GroupSeries) Len() int           { return len(gs) }
func (gs GroupSeries) Less(i, j int) bool { return bytes.Compare(gs[i].GetKey(), gs[j].GetKey()) < 0 }
func (gs GroupSeries) Swap(i, j int) {
	gs[i], gs[j] = gs[j], gs[i]
}
func (gs GroupSeries) Reverse() {
	sort.Sort(sort.Reverse(gs))
	for index := range gs {
		tt := gs[index]
		for i, j := 0, tt.Len()-1; i < j; i, j = i+1, j-1 {
			tt.Swap(i, j)
		}
	}
}
func (gs GroupSeries) SeriesCnt() int {
	var cnt int
	for i := range gs {
		cnt += gs[i].Len()
	}
	return cnt
}

type Index interface {
	CreateIndexIfNotExists(mmRows *dictpool.Dict) error
	GetSeriesIdBySeriesKey(key []byte) (uint64, error)
	SearchSeries(series [][]byte, name []byte, condition influxql.Expr, tr TimeRange) ([][]byte, error)
	SearchSeriesWithOpts(span *tracing.Span, name []byte, opt *query.ProcessorOptions, callBack func(num int64) error, _ interface{}) (GroupSeries, int64, error)
	SeriesCardinality(name []byte, condition influxql.Expr, tr TimeRange) (uint64, error)
	SearchSeriesKeys(series [][]byte, name []byte, condition influxql.Expr) ([][]byte, error)
	SearchSeriesByTableAndCond(name []byte, expr influxql.Expr, tr TimeRange) ([]uint64, error)
	SearchTagValues(name []byte, tagKeys [][]byte, condition influxql.Expr) ([][]string, error)
	SearchTagValuesCardinality(name, tagKey []byte) (uint64, error)
	GetSeries(sid uint64, buf []byte, condition influxql.Expr, callback func(key *influx.SeriesKey)) error

	// search
	GetPrimaryKeys(name []byte, opt *query.ProcessorOptions) ([]uint64, error)
	// delete
	GetDeletePrimaryKeys(name []byte, condition influxql.Expr, tr TimeRange) ([]uint64, error)

	SetIndexBuilder(builder *IndexBuilder)

	DeleteTSIDs(name []byte, condition influxql.Expr, tr TimeRange) error
	LoadDeletedTSIDs() error

	Path() string

	DebugFlush()
	Open() error
	Close() error
}

type Options struct {
	opId          uint64 // assign task id
	ident         *meta.IndexIdentifier
	path          string
	lock          *string
	indexType     index.IndexType
	engineType    config.EngineType
	startTime     time.Time
	endTime       time.Time
	duration      time.Duration
	cacheDuration time.Duration
	logicalClock  uint64
	sequenceID    *uint64
	mergeDuration time.Duration
	obsOpt        *obs.ObsOptions
}

func (opts *Options) ObsOpt(obsOpt *obs.ObsOptions) *Options {
	opts.obsOpt = obsOpt
	return opts
}

func (opts *Options) OpId(opId uint64) *Options {
	opts.opId = opId
	return opts
}

func (opts *Options) LogicalClock(clock uint64) *Options {
	opts.logicalClock = clock
	return opts
}

func (opts *Options) SequenceId(id *uint64) *Options {
	opts.sequenceID = id
	return opts
}

func (opts *Options) Ident(ident *meta.IndexIdentifier) *Options {
	opts.ident = ident
	return opts
}

func (opts *Options) Path(path string) *Options {
	opts.path = path
	return opts
}

func (opts *Options) Lock(lock *string) *Options {
	opts.lock = lock
	return opts
}

func (opts *Options) IndexType(indexType index.IndexType) *Options {
	opts.indexType = indexType
	return opts
}

func (opts *Options) EngineType(engineType config.EngineType) *Options {
	opts.engineType = engineType
	return opts
}

func (opts *Options) StartTime(startTime time.Time) *Options {
	opts.startTime = startTime
	return opts
}

func (opts *Options) EndTime(endTime time.Time) *Options {
	opts.endTime = endTime
	return opts
}

func (opts *Options) Duration(duration time.Duration) *Options {
	opts.duration = duration
	return opts
}

func (opts *Options) CacheDuration(cacheDuration time.Duration) *Options {
	opts.cacheDuration = cacheDuration
	return opts
}

func (opts *Options) MergeDuration(duration time.Duration) *Options {
	opts.mergeDuration = duration
	return opts
}

func NewIndex(opts *Options) (Index, error) {
	switch opts.indexType {
	case index.MergeSet:
		return NewMergeSetIndex(opts)
	default:
		return NewMergeSetIndex(opts)
	}
}

func GenerateUUID() uint64 {
	b := kbPool.Get()
	// first three bytes is big endian of logicClock
	b.B = append(b.B, byte(metaclient.LogicClock>>16))
	b.B = append(b.B, byte(metaclient.LogicClock>>8))
	b.B = append(b.B, byte(metaclient.LogicClock))

	// last five bytes is big endian of sequenceID
	id := atomic.AddUint64(&sequenceID, 1)
	b.B = append(b.B, byte(id>>32))
	b.B = append(b.B, byte(id>>24))
	b.B = append(b.B, byte(id>>16))
	b.B = append(b.B, byte(id>>8))
	b.B = append(b.B, byte(id))

	pid := binary.BigEndian.Uint64(b.B)
	kbPool.Put(b)

	return pid
}

type DumpInfo struct {
	ShowTagKeys        bool
	ShowTagValues      bool
	ShowTagValueSeries bool
	MeasurementFilter  *regexp.Regexp
	TagKeyFilter       *regexp.Regexp
	TagValueFilter     *regexp.Regexp
}
