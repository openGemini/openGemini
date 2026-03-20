// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package shelf

import (
	"io"
	"sort"
	"strings"
	"sync"

	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"go.uber.org/zap"
)

// WalIterators iterates multiple WAL files
type WalIterators struct {
	sync.RWMutex
	iterators []*WalIterator
	snapshots []*WalIterator
	dead      map[int]struct{}

	idx         Index
	tableFilter string
	rowLimit    int

	// Columns to read from disk (SELECT tag/field + filtered)
	readCols record.Schemas

	// SELECT field names that should survive projection
	fieldsSelected map[string]struct{}

	// Final schema returned to client after projection
	resultCols record.Schemas
}

func NewWalIterators(walSlice []*Wal, mst string, schema record.Schemas, tagSelected record.Schemas,
	fieldSelected map[string]struct{}, rowLimit int, idx Index) *WalIterators {
	readCols := make(record.Schemas, 0, len(schema)+len(tagSelected))
	readCols = append(readCols, schema...)
	readCols = append(readCols, tagSelected...)
	sort.Sort(record.CustomSchemas(readCols))

	resultCols := make(record.Schemas, 0, len(fieldSelected)+len(tagSelected)+1)
	for _, field := range schema {
		if _, ok := fieldSelected[field.Name]; !ok {
			continue
		}
		resultCols = append(resultCols, record.Field{Type: field.Type, Name: field.Name})
	}
	resultCols = append(resultCols, tagSelected...)
	sort.Sort(record.CustomSchemas(resultCols))

	wis := &WalIterators{
		iterators:      make([]*WalIterator, 0, len(walSlice)),
		dead:           make(map[int]struct{}),
		idx:            idx,
		tableFilter:    mst,
		readCols:       readCols,
		fieldsSelected: fieldSelected,
		resultCols:     resultCols,
		rowLimit:       rowLimit,
	}

	if len(walSlice) == 0 {
		return wis
	}
	for _, wal := range walSlice {
		it := NewWalIterator(wal, mst, idx)
		wis.iterators = append(wis.iterators, it)
	}
	return wis
}

// Next return ConsumeRecord from files of wal
func (its *WalIterators) Next() (*record.ConsumeRecord, error) {
	its.removeIterator()

	// including the fields selected by SELECT, the tag columns, and the field columns used for filtering in WHERE
	resultRecord := record.NewRecord(its.readCols, false)

	snapshot, release := its.snapshot()
	defer release()

	var err error
	// traverse each walIterator to obtain record until the number of rows meets the requirement or io.EOF
	for idx, currentItr := range snapshot {
		for {
			if resultRecord.RowNums() >= its.rowLimit {
				resultRecord, err = its.RemoveFilterColumns(resultRecord)
				if err != nil {
					logger.GetLogger().Error("consume failed to remove filter columns", zap.Error(err))
					return nil, err
				}
				return &record.ConsumeRecord{Rec: resultRecord}, nil
			}
			currentRecord, err := currentItr.Next()
			if err != nil {
				if err == io.EOF {
					if currentItr.walReader.wal.StateSwitching {
						its.markDead(idx)
					}
					break
				}
				logger.GetLogger().Error("iterator returned error, abort iteration",
					zap.String("component", "WalIterators"),
					zap.Error(err))
				return nil, err
			}
			resultRecord.AppendRec(currentRecord, 0, 1)
		}
	}

	if resultRecord.RowNums() == 0 {
		return nil, io.EOF
	}
	resultRecord, err = its.RemoveFilterColumns(resultRecord)
	if err != nil {
		logger.GetLogger().Error("consume failed to remove filter columns")
		return nil, err
	}
	return &record.ConsumeRecord{Rec: resultRecord}, nil
}

func (its *WalIterators) AddIterator(itr record.RecIterator) {
	its.Lock()
	defer its.Unlock()
	if it, ok := itr.(*WalIterator); ok {
		its.iterators = append(its.iterators, it)
	}
}

func (its *WalIterators) removeIterator() {
	if len(its.dead) == 0 {
		return
	}
	its.Lock()
	defer its.Unlock()
	k := 0
	for i, itr := range its.iterators {
		if _, hasDropped := its.dead[i]; !hasDropped {
			its.iterators[k] = itr
			k++
		} else {
			its.iterators[i].Release()
			its.iterators[i] = nil
		}
	}
	its.iterators = its.iterators[:k]
	clear(its.dead)
}

func (its *WalIterators) snapshot() ([]*WalIterator, func()) {
	n := len(its.iterators)
	if cap(its.snapshots) < n {
		its.snapshots = make([]*WalIterator, n)
	} else {
		its.snapshots = its.snapshots[:n]
	}

	its.RLock()
	defer its.RUnlock()

	copy(its.snapshots, its.iterators)

	return its.snapshots, func() {
		clear(its.snapshots)
		its.snapshots = its.snapshots[:0]
	}
}

func (its *WalIterators) markDead(idx int) {
	its.dead[idx] = struct{}{}
}

// RemoveFilterColumns Remove the redundant columns used for filtering
func (its *WalIterators) RemoveFilterColumns(rec *record.Record) (*record.Record, error) {
	k := 0
	err := util.FindIntersectionIndex(rec.Schema[:rec.Len()-1], its.resultCols[:its.resultCols.Len()-1],
		func(f1 record.Field, f2 record.Field) int {
			return strings.Compare(f1.Name, f2.Name)
		},
		func(i, j int) error {
			if rec.Schema[i].Type == its.resultCols[j].Type {
				rec.ColVals[k] = rec.ColVals[i]
				rec.Schema[k] = rec.Schema[i]
				k++
			}
			return nil
		})
	if err != nil {
		return nil, err
	}
	rec.ColVals[k] = rec.ColVals[rec.Len()-1]
	rec.Schema[k] = rec.Schema[rec.Len()-1]
	rec.ColVals = rec.ColVals[:k+1]
	rec.Schema = rec.Schema[:k+1]
	return rec, nil
}

func (its *WalIterators) Release() {
	for _, it := range its.iterators {
		it.Release()
	}
}

func (its *WalIterators) SidCnt() int {
	return 0
}

// WalIterator iterator single wal file
type WalIterator struct {
	ctx            *WalCtx
	ctxReleaseFunc func()
	walReader      *WalConsumeReader
	idx            Index
}

func NewWalIterator(wal *Wal, mst string, idx Index) *WalIterator {
	walCtx, f := NewWalCtx()
	reader := NewWalConsumeReader(wal, walCtx, mst, 0)
	return &WalIterator{
		ctx:            walCtx,
		ctxReleaseFunc: f,
		walReader:      reader,
		idx:            idx,
	}
}

func (it *WalIterator) Release() {
	it.ctxReleaseFunc()
	it.walReader.Release()
}

func (it *WalIterator) Next() (*record.Record, error) {
	// WAL might not be opened yet or closed by the convert-to-tssp task.
	if !it.walReader.wal.opened || it.walReader.wal.file.IsClosed() {
		return nil, io.EOF
	}
	sid, seriesKey, recData, err := it.walReader.ReadBlock()
	if err != nil {
		return nil, err
	}

	decoder := &WalRecordDecoder{}
	rec := it.ctx.Record()
	rec.Reset()
	err = decoder.Decode(rec, recData)
	if err != nil {
		logger.GetLogger().Error("consume failed to decode record from byte array")
		return nil, err
	}

	// Add the tag column to the record
	pointTags := make(influx.PointTags, 0, record.DefaultTagsNumPerSeriesKey)
	if sid > 0 && it.idx != nil {
		if err = it.idx.GetSeries(sid, []byte{}, nil, func(key *influx.SeriesKey) {
			for _, tag := range key.TagSet {
				pointTags = append(pointTags, influx.Tag{Key: string(tag.Key), Value: string(tag.Value)})
			}
		}); err != nil {
			return nil, err
		}
	} else if sid <= 0 {
		_, err = influx.IndexKeyToTags(seriesKey, false, &pointTags)
		if err != nil {
			return nil, err
		}
	}
	for _, t := range pointTags {
		var col record.ColVal
		col.AppendString(t.Value)
		rec.ColVals = append(rec.ColVals, col)
		rec.Schema = append(rec.Schema, record.Field{
			Type: influx.Field_Type_Tag,
			Name: t.Key,
		})
	}

	sort.Sort(rec)
	return rec, nil
}
