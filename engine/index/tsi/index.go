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
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/open_src/github.com/savsgio/dictpool"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/meta"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
)

const (
	defaultTSIDCacheSize = 128 << 20
	defaultSKeyCacheSize = 128 << 20
	defaultTagCacheSize  = 512 << 20
)

type IndexType int

const (
	MergeSet IndexType = iota
	Text
	Field
	TimeCluster
	IndexTypeAll
)

var (
	sequenceID = uint64(time.Now().Unix())
)

type TagSetInfo struct {
	ref int64

	IDs        []uint64
	Filters    []influxql.Expr
	SeriesKeys [][]byte           // encoded series key
	TagsVec    []influx.PointTags // tags of all series
	key        []byte             // group by tag sets key
	RowFilters *clv.RowFilters    // only uesed in full-text index for row filtering
}

func (t *TagSetInfo) String() string {
	n := len(t.IDs)
	var builder strings.Builder
	for i := 0; i < n; i++ {
		builder.WriteString(fmt.Sprintf("%d -> %s\n", t.IDs[i], t.SeriesKeys[i]))
	}
	return builder.String()
}

func (t *TagSetInfo) Len() int { return len(t.IDs) }
func (t *TagSetInfo) Less(i, j int) bool {
	return bytes.Compare(t.SeriesKeys[i], t.SeriesKeys[j]) < 0
}
func (t *TagSetInfo) Swap(i, j int) {
	t.SeriesKeys[i], t.SeriesKeys[j] = t.SeriesKeys[j], t.SeriesKeys[i]
	t.IDs[i], t.IDs[j] = t.IDs[j], t.IDs[i]
	t.TagsVec[i], t.TagsVec[j] = t.TagsVec[j], t.TagsVec[i]
	t.Filters[i], t.Filters[j] = t.Filters[j], t.Filters[i]
	if t.RowFilters != nil {
		t.RowFilters.Swap(i, j)
	}
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
	t.IDs = t.IDs[:0]
	t.Filters = t.Filters[:0]
	t.TagsVec = t.TagsVec[:0]
	t.SeriesKeys = t.SeriesKeys[:0]
	if t.RowFilters != nil {
		t.RowFilters.Reset()
	}
}

func (t *TagSetInfo) Append(id uint64, seriesKey []byte, filter influxql.Expr, tags influx.PointTags, rowFilter []clv.RowFilter) {
	t.IDs = append(t.IDs, id)
	t.Filters = append(t.Filters, filter)
	t.TagsVec = append(t.TagsVec, tags)
	t.SeriesKeys = append(t.SeriesKeys, seriesKey)
	if t.RowFilters != nil {
		t.RowFilters.Append(rowFilter)
	}
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
			ref:        0,
			IDs:        make([]uint64, 0, size),
			Filters:    make([]influxql.Expr, 0, size),
			SeriesKeys: make([][]byte, 0, size),
			TagsVec:    make([]influx.PointTags, 0, size),
		}
	}
}

type SortGroupSeries struct {
	groupSeries []*TagSetInfo
	ascending   bool
}

func (s SortGroupSeries) Len() int {
	return len(s.groupSeries)
}

func (s SortGroupSeries) Less(i, j int) bool {
	if s.ascending {
		return bytes.Compare(s.groupSeries[i].key, s.groupSeries[j].key) < 0
	}
	return bytes.Compare(s.groupSeries[i].key, s.groupSeries[j].key) > 0
}

func (s SortGroupSeries) Swap(i, j int) {
	s.groupSeries[i], s.groupSeries[j] = s.groupSeries[j], s.groupSeries[i]
}

func NewSortGroupSeries(groupSeries []*TagSetInfo, ascending bool) *SortGroupSeries {
	return &SortGroupSeries{
		groupSeries: groupSeries,
		ascending:   ascending,
	}
}

type GroupSeries []*TagSetInfo

func (gs GroupSeries) Len() int           { return len(gs) }
func (gs GroupSeries) Less(i, j int) bool { return bytes.Compare(gs[i].key, gs[j].key) < 0 }
func (gs GroupSeries) Swap(i, j int) {
	gs[i], gs[j] = gs[j], gs[i]
}
func (gs GroupSeries) Reverse() {
	sort.Sort(sort.Reverse(gs))
	for index := range gs {
		tt := gs[index]
		for i, j := 0, tt.Len()-1; i < j; i, j = i+1, j-1 {
			tt.IDs[i], tt.IDs[j] = tt.IDs[j], tt.IDs[i]
			tt.Filters[i], tt.Filters[j] = tt.Filters[j], tt.Filters[i]
			tt.SeriesKeys[i], tt.SeriesKeys[j] = tt.SeriesKeys[j], tt.SeriesKeys[i]
			tt.TagsVec[i], tt.TagsVec[j] = tt.TagsVec[j], tt.TagsVec[i]
			if tt.RowFilters != nil {
				tt.RowFilters.Swap(i, j)
			}
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
	GetSeriesIdBySeriesKey(key, name []byte) (uint64, error)
	SearchSeries(series [][]byte, name []byte, condition influxql.Expr, tr TimeRange) ([][]byte, error)
	SearchSeriesWithOpts(span *tracing.Span, name []byte, opt *query.ProcessorOptions, callBack func(num int64) error, _ interface{}) (GroupSeries, int64, error)
	SeriesCardinality(name []byte, condition influxql.Expr, tr TimeRange) (uint64, error)
	SearchSeriesKeys(series [][]byte, name []byte, condition influxql.Expr) ([][]byte, error)
	SearchTagValues(name []byte, tagKeys [][]byte, condition influxql.Expr) ([][]string, error)
	SearchTagValuesCardinality(name, tagKey []byte) (uint64, error)

	// search
	GetPrimaryKeys(name []byte, opt *query.ProcessorOptions) ([]uint64, error)
	// delete
	GetDeletePrimaryKeys(name []byte, condition influxql.Expr, tr TimeRange) ([]uint64, error)

	SetIndexBuilder(builder *IndexBuilder)

	DeleteTSIDs(name []byte, condition influxql.Expr, tr TimeRange) error

	Path() string

	DebugFlush()
	Open() error
	Close() error
}

type Options struct {
	opId         uint64 // assign task id
	ident        *meta.IndexIdentifier
	path         string
	lock         *string
	indexType    IndexType
	engineType   config.EngineType
	startTime    time.Time
	endTime      time.Time
	duration     time.Duration
	logicalClock uint64
	sequenceID   *uint64
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

func (opts *Options) IndexType(indexType IndexType) *Options {
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

func NewIndex(opts *Options) (Index, error) {
	switch opts.indexType {
	case MergeSet:
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
