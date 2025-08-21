// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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

package tsreader

import (
	"sort"

	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/index"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

const noContentSID = 0

type Index interface {
	GetSeries(sid uint64, buf []byte, condition influxql.Expr, callback func(key *influx.SeriesKey)) error
}

type CombinedIterator interface {
	Next() (sid uint64, rec *record.Record, err error)
	Close()
}

type ConsumeIterator struct {
	idx       Index
	opts      *ConsumeOptions
	order     []immutable.TSSPFile
	unordered []immutable.TSSPFile

	combinedIterator CombinedIterator
	signal           chan struct{}
	logger           *logger.Logger
	sortHelper       *record.ColumnSortHelper
}

func NewConsumeIterator(opts *ConsumeOptions) *ConsumeIterator {
	return &ConsumeIterator{
		opts:       opts,
		signal:     make(chan struct{}),
		logger:     logger.NewLogger(errno.ModuleUnknown),
		sortHelper: record.NewColumnSortHelper(),
	}
}

func (itr *ConsumeIterator) SetIndex(idx Index) {
	itr.idx = idx
}

func (itr *ConsumeIterator) Release() {
	itr.sortHelper.Release()
	UnrefTSSPFile(itr.order...)
	UnrefTSSPFile(itr.unordered...)
	itr.combinedIterator.Close()

	itr.order = nil
	itr.unordered = nil
	itr.sortHelper = nil
}

func (itr *ConsumeIterator) initIterator() {
	if itr.opts.seriesItr == nil {
		itr.combinedIterator = immutable.NewChunkIterators(append(itr.order, itr.unordered...), 0, itr.signal, itr.logger, itr.opts.schema)
	} else {
		itr.combinedIterator = NewSeriesIdIterator(itr.order, itr.unordered, itr.opts.seriesItr, itr.opts.tr, itr.opts.schema)
	}
}

func (itr *ConsumeIterator) AddReader(order bool, reader immutable.TSSPFile) {
	reader.RefFileReader()
	if order {
		itr.order = append(itr.order, reader)
	} else {
		itr.unordered = append(itr.unordered, reader)
	}
}

func (itr *ConsumeIterator) Next() (uint64, *record.ConsumeRecord, error) {
	if itr.combinedIterator == nil {
		itr.initIterator()
	}

	for {
		sid, rec, err := itr.combinedIterator.Next()
		if err != nil {
			return 0, nil, err
		}
		if sid == noContentSID {
			return 0, nil, nil
		}

		rec = itr.filterByTime(rec)
		rec = itr.removeFilterColumns(rec)
		if rec == nil {
			continue
		}

		res, err := itr.appendTags(rec, sid)
		if err != nil {
			return 0, nil, err
		}
		return sid, res, nil
	}
}

func (itr *ConsumeIterator) filterByTime(rec *record.Record) *record.Record {
	if itr.opts.seriesItr != nil {
		return rec
	}

	if rec.RowNums() == 0 {
		return nil
	}
	return immutable.FilterByTime(rec, itr.opts.tr)
}

func (itr *ConsumeIterator) removeFilterColumns(rec *record.Record) *record.Record {
	if rec.RowNums() == 0 {
		return nil
	}

	resRec := &record.Record{}
	recLen := len(itr.opts.fieldSelected)
	resRec.Schema = make(record.Schemas, 0, recLen)
	resRec.ColVals = make([]record.ColVal, 0, recLen)

	for i := range rec.Schema {
		if _, ok := itr.opts.fieldSelected[rec.Schema[i].Name]; !ok {
			continue
		}
		resRec.Schema = append(resRec.Schema, rec.Schema[i])
		val := record.ColVal{}
		val.AppendAll(&rec.ColVals[i])
		resRec.ColVals = append(resRec.ColVals, val)
	}
	return resRec
}

func (itr *ConsumeIterator) appendTags(rec *record.Record, sid uint64) (*record.ConsumeRecord, error) {
	// fast fail
	if itr.opts.tagSelected.Len() == 0 {
		return &record.ConsumeRecord{Rec: rec}, nil
	}
	series := make(map[string]string, 16)
	if err := itr.idx.GetSeries(sid, []byte{}, nil, func(key *influx.SeriesKey) {
		for _, tag := range key.TagSet {
			series[string(tag.Key)] = string(tag.Value)
		}
	}); err != nil {
		return nil, err
	}

	tags := make([]*record.Tag, itr.opts.tagSelected.Len())
	for i, field := range itr.opts.tagSelected {
		tagValue, ok := series[field.Name]
		if !ok {
			continue
		}
		tags[i] = &record.Tag{Key: field.Name, Value: tagValue}
	}
	res := &record.ConsumeRecord{Rec: rec, Tags: tags}
	return res, nil
}

type ConsumeOptions struct {
	seriesItr     index.SeriesIDIterator
	tr            util.TimeRange
	schema        record.Schemas
	tagSelected   record.Schemas
	fieldSelected map[string]struct{}
}

func NewConsumeOptions(opts *query.ProcessorOptions, seriesItr index.SeriesIDIterator) (*ConsumeOptions, error) {
	co := &ConsumeOptions{
		seriesItr:     seriesItr,
		tr:            util.TimeRange{Min: opts.StartTime, Max: opts.EndTime},
		schema:        make(record.Schemas, len(opts.FieldAux)+len(opts.Aux)),
		tagSelected:   make(record.Schemas, len(opts.TagAux)),
		fieldSelected: make(map[string]struct{}),
	}

	schemas := make(map[string]record.Field)

	// Iterate through the fields in condition to determine which fields need to be deleted after querying.
	for _, varRef := range opts.Aux {
		if varRef.Type != influxql.Tag {
			schemas[varRef.Val] = record.Field{Type: record.ToModelTypes(varRef.Type), Name: varRef.Val}
		}
	}

	// Iterate through the fields in select to determine which fields need to be queried from the file and retained.
	for _, varRef := range opts.FieldAux {
		if _, ok := schemas[varRef.Val]; !ok {
			schemas[varRef.Val] = record.Field{Type: record.ToModelTypes(varRef.Type), Name: varRef.Val}
		}
		co.fieldSelected[varRef.Val] = struct{}{}
	}
	co.fieldSelected[record.TimeField] = struct{}{}

	i := 0
	for _, field := range schemas {
		co.schema[i].Name = field.Name
		co.schema[i].Type = field.Type
		i++
	}
	co.schema = co.schema[:i]
	sort.Sort(co.schema)
	co.schema = append(co.schema, record.Field{Type: influx.Field_Type_Int, Name: record.TimeField})

	// The file does not contain the tag column, which needs to be additionally supplemented after the query.
	for j := 0; j < len(opts.TagAux); j++ {
		co.tagSelected[j].Name = opts.TagAux[j].Val
		co.tagSelected[j].Type = record.ToModelTypes(opts.TagAux[j].Type)
	}

	return co, nil
}
