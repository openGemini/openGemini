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
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/open_src/github.com/savsgio/dictpool"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/meta"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"go.uber.org/zap"
)

// IndexBuilder is a collection of all indexes
type IndexBuilder struct {
	opId uint64 // assign task id

	path      string           // eg, data/db/pt/rp/index/indexid
	Relations []*IndexRelation // <oid, indexRelation>
	ident     *meta.IndexIdentifier
	endTime   time.Time
	duration  time.Duration

	mu             sync.RWMutex
	logicalClock   uint64
	sequenceID     *uint64
	lock           *string
	EnableTagArray bool

	seriesLimiter func() error
}

func NewIndexBuilder(opt *Options) *IndexBuilder {
	iBuilder := &IndexBuilder{
		opId:         opt.opId,
		path:         opt.path,
		ident:        opt.ident,
		duration:     opt.duration,
		endTime:      opt.endTime,
		logicalClock: opt.logicalClock,
		sequenceID:   opt.sequenceID,
		lock:         opt.lock,
		Relations:    make([]*IndexRelation, IndexTypeAll),
	}
	return iBuilder
}

type indexRow struct {
	Row *influx.Row
	Wg  *sync.WaitGroup
	Err error
}

type indexRows []indexRow

func (rows *indexRows) reset() {
	for i := range *rows {
		(*rows)[i].Wg = nil
		(*rows)[i].Row = nil
		(*rows)[i].Err = nil
	}
}

var indexRowsPool sync.Pool

func getIndexRows() *indexRows {
	rows := indexRowsPool.Get()
	if rows == nil {
		return &indexRows{}
	}
	return rows.(*indexRows)
}

func putIndexRows(rows *indexRows) {
	rows.reset()
	*rows = (*rows)[:0]
	indexRowsPool.Put(rows)
}

func (iBuilder *IndexBuilder) SetSeriesLimiter(limiter func() error) {
	iBuilder.seriesLimiter = limiter
}

func (iBuilder *IndexBuilder) SeriesLimited() error {
	if iBuilder.seriesLimiter == nil {
		return nil
	}

	return iBuilder.seriesLimiter()
}

func (iBuilder *IndexBuilder) GenerateUUID() uint64 {
	b := kbPool.Get()
	// first three bytes is big endian of logicClock
	b.B = append(b.B, byte(iBuilder.logicalClock>>16))
	b.B = append(b.B, byte(iBuilder.logicalClock>>8))
	b.B = append(b.B, byte(iBuilder.logicalClock))

	// last five bytes is big endian of sequenceID
	id := atomic.AddUint64(iBuilder.sequenceID, 1)
	b.B = append(b.B, byte(id>>32))
	b.B = append(b.B, byte(id>>24))
	b.B = append(b.B, byte(id>>16))
	b.B = append(b.B, byte(id>>8))
	b.B = append(b.B, byte(id))

	pid := binary.BigEndian.Uint64(b.B)
	kbPool.Put(b)

	return pid
}

func (iBuilder *IndexBuilder) Flush() {
	for i := range iBuilder.Relations {
		if iBuilder.isRelationInited(uint32(i)) {
			iBuilder.Relations[i].IndexFlush()
		}
	}
}

func (iBuilder *IndexBuilder) Open() error {
	start := time.Now()

	// Open all indexes
	for i := range iBuilder.Relations {
		if iBuilder.isRelationInited(uint32(i)) {
			if err := iBuilder.Relations[i].IndexOpen(); err != nil {
				logger.GetLogger().Error("Index open fail", zap.Error(err))
				return err
			}
		}
	}
	statistics.IndexTaskInit(iBuilder.GetIndexID(), iBuilder.opId, iBuilder.ident.OwnerDb, iBuilder.ident.OwnerPt, iBuilder.RPName())
	statistics.IndexStepDuration(iBuilder.GetIndexID(), iBuilder.opId, "OpenIndexDone", time.Since(start).Nanoseconds(), true)
	return nil
}

func (iBuilder *IndexBuilder) Path() string {
	return iBuilder.path
}

func (iBuilder *IndexBuilder) GetPrimaryIndex() PrimaryIndex {
	return iBuilder.Relations[0].indexAmRoutine.index.(PrimaryIndex)
}

func (iBuilder *IndexBuilder) Ident() *meta.IndexIdentifier {
	return iBuilder.ident
}

func (iBuilder *IndexBuilder) RPName() string {
	return iBuilder.ident.Policy
}

func (iBuilder *IndexBuilder) GetIndexID() uint64 {
	return iBuilder.ident.Index.IndexID
}

func (iBuilder *IndexBuilder) SetDuration(duration time.Duration) {
	iBuilder.duration = duration
}

func (iBuilder *IndexBuilder) GetDuration() time.Duration {
	return iBuilder.duration
}

func (iBuilder *IndexBuilder) Expired() bool {
	// duration == 0 means INF.
	now := time.Now().UTC()
	if iBuilder.duration != 0 && iBuilder.endTime.Add(iBuilder.duration).Before(now) {
		return true
	}

	return false
}

func (iBuilder *IndexBuilder) GetEndTime() time.Time {
	return iBuilder.endTime
}

func (iBuilder *IndexBuilder) CreateIndexIfNotExists(mmRows *dictpool.Dict, needSecondaryIndex bool) error {
	primaryIndex := iBuilder.GetPrimaryIndex()
	var wg sync.WaitGroup
	// 1st, create primary index.
	iRows := getIndexRows()
	for mmIdx := range mmRows.D {
		rows, ok := mmRows.D[mmIdx].Value.(*[]influx.Row)
		if !ok {
			putIndexRows(iRows)
			return errno.NewError(errno.CreateIndexFailPointRowType)
		}

		for rowIdx := range *rows {
			row := &(*rows)[rowIdx]
			if row.SeriesId != 0 {
				continue
			}
			if cap(*iRows) > len(*iRows) {
				*iRows = (*iRows)[:len(*iRows)+1]
			} else {
				*iRows = append(*iRows, indexRow{})
			}
			iRow := &(*iRows)[len(*iRows)-1]
			iRow.Row = row
			iRow.Wg = &wg

			wg.Add(1)
			idx := primaryIndex.(*MergeSetIndex)
			idx.WriteRow(iRow)
		}
	}
	// Wait all rows in the batch finished.
	wg.Wait()

	// Check Err.
	for _, row := range *iRows {
		if row.Err != nil {
			putIndexRows(iRows)
			return row.Err
		}
	}
	putIndexRows(iRows)

	// 2nd, create secondary index.
	if !needSecondaryIndex {
		return nil
	}

	for mmIdx := range mmRows.D {
		rows, _ := mmRows.D[mmIdx].Value.(*[]influx.Row)
		for rowIdx := range *rows {
			row := &(*rows)[rowIdx]
			if err := iBuilder.createSecondaryIndex(row, primaryIndex); err != nil {
				return err
			}
		}
	}

	return nil
}

func (iBuilder *IndexBuilder) CreateIndexIfNotExistsForColumnStore(rec *record.Record, tagIndex []int, mst string) error {
	primaryIndex := iBuilder.GetPrimaryIndex()
	idx, ok := primaryIndex.(*MergeSetIndex)
	if !ok {
		return errors.New("get mergeSetIndex failed")
	}
	tagCols := GetIndexTagCols()
	defer PutIndexTagCols(tagCols)
	mstBinary := []byte(mst)
	tagsByteSlice := make([][]byte, len(tagIndex))
	for i := range tagsByteSlice {
		tagsByteSlice[i] = []byte(rec.Schema[tagIndex[i]].Name)
	}
	var wg sync.WaitGroup
	for index := 0; index < rec.RowNums(); index++ {
		for i := range tagIndex {
			// hash by single mst,tag key,tag value
			if cap(*tagCols) > len(*tagCols) {
				*tagCols = (*tagCols)[:len(*tagCols)+1]
			} else {
				*tagCols = append(*tagCols, TagCol{})
			}
			tagCol := &(*tagCols)[len(*tagCols)-1]
			tagCol.Mst = mstBinary
			tagCol.Key = tagsByteSlice[i]
			tagCol.Val, _ = rec.ColVals[tagIndex[i]].StringValue(index)
			tagCol.Wg = &wg
			wg.Add(1)
			idx.WriteTagCols(tagCol)
		}
	}
	wg.Wait()
	return nil
}

func (iBuilder *IndexBuilder) CreateSecondaryIndexIfNotExist(mmRows *dictpool.Dict) error {
	primaryIndex := iBuilder.GetPrimaryIndex()
	for mmIdx := range mmRows.D {
		rows, _ := mmRows.D[mmIdx].Value.(*[]influx.Row)
		for rowIdx := range *rows {
			row := &(*rows)[rowIdx]
			if err := iBuilder.createSecondaryIndex(row, primaryIndex); err != nil {
				return err
			}
		}
	}
	return nil
}

func (iBuilder *IndexBuilder) createSecondaryIndex(row *influx.Row, primaryIndex PrimaryIndex) error {
	for _, indexOpt := range row.IndexOptions {
		relation := iBuilder.Relations[indexOpt.Oid]
		if relation == nil {
			opt := &Options{
				indexType: GetIndexTypeById(indexOpt.Oid),
				path:      primaryIndex.Path(),
				lock:      iBuilder.lock,
			}
			if err := iBuilder.initRelation(indexOpt.Oid, opt, primaryIndex); err != nil {
				return err
			}

			relation = iBuilder.Relations[indexOpt.Oid]
		}
		if err := relation.IndexInsert([]byte(row.Name), row); err != nil {
			return err
		}
	}
	return nil
}

func (iBuilder *IndexBuilder) isRelationInited(oid uint32) bool {
	return iBuilder.Relations[oid] != nil
}

func (iBuilder *IndexBuilder) initRelation(oid uint32, opt *Options, primaryIndex PrimaryIndex) error {
	if iBuilder.isRelationInited(oid) {
		return nil
	}

	iBuilder.mu.Lock()
	defer iBuilder.mu.Unlock()
	if iBuilder.isRelationInited(oid) {
		return nil
	}

	var err error
	relation, err := NewIndexRelation(opt, primaryIndex, iBuilder)
	if err != nil {
		return err
	}
	if err = relation.IndexOpen(); err != nil {
		return err
	}

	iBuilder.Relations[oid] = relation
	return nil
}

func (iBuilder *IndexBuilder) Scan(span *tracing.Span, name []byte, opt *query.ProcessorOptions, callBack func(num int64) error) (interface{}, int64, error) {
	// 1st, use primary index to scan.
	relation := iBuilder.Relations[uint32(MergeSet)]
	if relation == nil {
		return nil, 0, fmt.Errorf("not exist index for %s", name)
	}
	groups, seriesNum, err := relation.IndexScan(span, name, opt, nil, callBack)
	if err != nil {
		return nil, seriesNum, err
	}

	// 2nd, use secondary index to scan.
	for i := range iBuilder.Relations {
		indexSeriesNum := int64(0)
		if !iBuilder.isRelationInited(uint32(i)) {
			continue
		}

		if iBuilder.Relations[i].oid == uint32(MergeSet) {
			continue
		}
		groups, indexSeriesNum, err = iBuilder.Relations[i].IndexScan(span, name, opt, groups, callBack)
		if err != nil {
			return nil, seriesNum, err
		}
		seriesNum += indexSeriesNum
	}

	return groups, seriesNum, nil
}

func (iBuilder *IndexBuilder) Delete(name []byte, condition influxql.Expr, tr TimeRange) error {
	var err error
	var index int
	for i := range iBuilder.Relations {
		if !iBuilder.isRelationInited(uint32(i)) {
			continue
		}

		if iBuilder.Relations[i].oid == uint32(MergeSet) {
			index = i
			continue
		}
		err = iBuilder.Relations[i].IndexDelete(name, condition, tr)
		if err != nil {
			return err
		}
	}

	//delete primary index last
	return iBuilder.Relations[index].IndexDelete(name, condition, tr)
}

func (iBuilder *IndexBuilder) Close() error {
	for i := range iBuilder.Relations {
		if !iBuilder.isRelationInited(uint32(i)) {
			continue
		}

		if err := iBuilder.Relations[i].IndexClose(); err != nil {
			return err
		}
	}
	return nil
}
