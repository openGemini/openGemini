// Copyright Huawei Cloud Computing Technologies Co., Ltd.
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

package immutable_test

import (
	"bytes"
	"fmt"
	"reflect"
	"sort"
	"time"

	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/rand"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

func getDefaultSchemas() record.Schemas {
	return record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
	}
}

var defaultInterval = time.Second.Nanoseconds()
var saveDir = ""
var defaultTier uint64 = util.Warm

type MergeTestHelper struct {
	mst     string
	store   *immutable.MmsTables
	records map[uint64]*record.Record
	expect  map[uint64]*record.Record
	saveDir string

	generator *recordGenerator

	seq     uint64
	compare bool
}

func NewMergeTestHelper(conf *immutable.Config) *MergeTestHelper {
	lockPath := ""
	mth := &MergeTestHelper{
		store:     immutable.NewTableStore(saveDir, &lockPath, &defaultTier, false, conf),
		records:   make(map[uint64]*record.Record),
		expect:    make(map[uint64]*record.Record),
		generator: newRecordGenerator(0, defaultInterval, true),
		compare:   true,
	}
	mth.store.SetImmTableType(config.TSSTORE)
	return mth
}

func (h *MergeTestHelper) disableCompare() {
	h.compare = false
}

func (h *MergeTestHelper) resetRecords() {
	h.records = make(map[uint64]*record.Record)
}

func (h *MergeTestHelper) addRecord(sid uint64, rec *record.Record) {
	h.mergeRec(sid, rec.Copy(true, nil, rec.Schema), h.records)

	if h.compare {
		h.appendRec(sid, rec, h.expect)
	}
}

func (h *MergeTestHelper) appendRec(sid uint64, rec *record.Record, dst map[uint64]*record.Record) {
	item, ok := dst[sid]
	if !ok {
		dst[sid] = rec
	} else {
		item.PadRecord(rec)
		rec.PadRecord(item)

		for i := range item.ColVals {
			item.ColVals[i].AppendColVal(&rec.ColVals[i], item.Schema[i].Type, 0, rec.ColVals[i].Len)
		}
	}
}

func (h *MergeTestHelper) mergeRec(sid uint64, rec *record.Record, dst map[uint64]*record.Record) {
	item, ok := dst[sid]
	if !ok {
		dst[sid] = rec
	} else {
		item.Merge(rec)
	}
}

func (h *MergeTestHelper) saveToOrder() error {
	return h.save(true)
}

func (h *MergeTestHelper) saveToUnordered() error {
	return h.save(false)
}

func (h *MergeTestHelper) save(order bool) error {
	h.seq++
	fo, err := saveRecordToFile(h.seq, h.records, order, h.store.Conf)
	if err != nil {
		return err
	}
	h.store.AddTSSPFiles(fo.Name(), order, fo)
	h.resetRecords()
	return nil
}

func (h *MergeTestHelper) mergeAndCompact(force bool) error {
	if err := h.store.MergeOutOfOrder(1, false, force); err != nil {
		return err
	}
	h.store.Wait()

	h.store.CompactionEnable()
	if err := h.store.LevelCompact(0, 1); err != nil {
		return err
	}
	h.store.Wait()
	return nil
}

func (h *MergeTestHelper) readMergedRecord() map[uint64]*record.Record {
	res := make(map[uint64]*record.Record)

	for _, f := range h.store.Order["mst"].Files() {
		fi := immutable.NewFileIterator(f, immutable.CLog)
		itr := immutable.NewChunkIterator(fi)

		for {
			if !itr.Next() {
				break
			}

			sid := itr.GetSeriesID()
			if sid == 0 {
				panic("series ID is zero")
			}
			rec := itr.GetRecord()
			record.CheckRecord(rec)

			item, ok := res[sid]
			if !ok {
				item = &record.Record{}
				item.Schema = append(item.Schema[:0], rec.Schema...)
				item.ReserveColVal(item.Len())
				res[sid] = item
			}

			item.Merge(rec)
		}
	}

	return res
}

func (h *MergeTestHelper) readExpectRecord() map[uint64]*record.Record {
	for sid, item := range h.expect {
		sh := record.NewSortHelper()
		h.expect[sid] = sh.Sort(item)
	}
	return h.expect
}

func newSortAux(times []int64, schema record.Schemas) *record.SortAux {
	aux := &record.SortAux{
		RowIds:  nil,
		Times:   nil,
		SortRec: nil,
	}
	aux.InitRecord(schema)
	aux.Init(times)

	return aux
}

func compareRecords(expect, got map[uint64]*record.Record) error {
	for sid, exp := range expect {
		item, ok := got[sid]
		if !ok {
			return fmt.Errorf("not series %d", sid)
		}

		if err := compareRecordSchema(exp.Schema, item.Schema); err != nil {
			return err
		}

		if err := compareRecordCols(exp, item); err != nil {
			return err
		}

		delete(expect, sid)
		delete(got, sid)
	}

	return nil
}

func compareRecordCols(expect, got *record.Record) error {
	if expect.RowNums() != got.RowNums() {
		return fmt.Errorf("different row nums")
	}

	for i := 0; i < expect.Len(); i++ {
		if err := compareColVal(&expect.ColVals[i], &got.ColVals[i], &expect.Schema[i]); err != nil {
			return err
		}
	}

	return nil
}

func compareColVal(expect, got *record.ColVal, ref *record.Field) error {
	if expect.Len != got.Len {
		return fmt.Errorf("different col.Len. exp: %d, got %d, ref: %s",
			expect.Len, got.Len, ref.String())
	}

	if expect.NilCount != got.NilCount {
		return fmt.Errorf("different col.NilCount. exp: %d, got %d, ref: %s",
			expect.NilCount, got.NilCount, ref.String())
	}

	if !bytes.Equal(expect.Val, got.Val) {
		return fmt.Errorf("different col.Val: %s", ref.String())
	}

	expect.RepairBitmap()
	got.RepairBitmap()
	if !reflect.DeepEqual(expect.Bitmap, got.Bitmap) {
		return fmt.Errorf("different col.Bitmap: %s", ref.String())
	}

	if !reflect.DeepEqual(expect.Offset, got.Offset) {
		return fmt.Errorf("different col.Offset: %s", ref.String())
	}

	return nil
}

func compareRecordSchema(expect, got record.Schemas) error {
	if expect.Len() != got.Len() {
		return fmt.Errorf("different schema length")
	}

	for i, exp := range expect {
		if exp.Name != got[i].Name {
			return fmt.Errorf("different schema name")
		}

		if exp.Type != got[i].Type {
			return fmt.Errorf("different schema type")
		}
	}

	return nil
}

type recordGenerator struct {
	begin     int64
	interval  int64
	withEmpty bool
}

func newRecordGenerator(begin, interval int64, withEmpty bool) *recordGenerator {
	return &recordGenerator{
		begin:     begin,
		interval:  interval,
		withEmpty: withEmpty,
	}
}

func (rg *recordGenerator) setBegin(v int64) *recordGenerator {
	rg.begin = v
	return rg
}

func (rg *recordGenerator) incrBegin(n int) *recordGenerator {
	rg.begin += int64(n) * rg.interval
	return rg
}

func (rg *recordGenerator) generate(schema []record.Field, rowCount int) *record.Record {
	rec := &record.Record{
		RecMeta: nil,
		ColVals: nil,
		Schema:  schema,
	}

	for i := 0; i < len(schema); i++ {
		col := record.ColVal{}

		for k := 0; k < rowCount; k++ {
			switch schema[i].Type {
			case influx.Field_Type_String:
				rg.generateString(&col, rg.withEmpty)
			case influx.Field_Type_Int:
				rg.generateInt(&col, rg.withEmpty)
			case influx.Field_Type_Boolean:
				rg.generateBool(&col, rg.withEmpty)
			case influx.Field_Type_Float:
				rg.generateFloat(&col, rg.withEmpty)
			}
		}

		rec.ColVals = append(rec.ColVals, col)
	}

	rec.Schema = append(rec.Schema, record.Field{
		Type: influx.Field_Type_Int,
		Name: record.TimeField,
	})

	rec.ColVals = append(rec.ColVals, rg.generateTimeCol(rowCount))
	sort.Sort(rec)
	return rec
}

func (rg *recordGenerator) generateInt(col *record.ColVal, withEmpty bool) {
	if withEmpty && rand.Float64() > 0.5 {
		col.AppendIntegerNull()
		return
	}

	col.AppendInteger(int64(rand.Float64() * 1000))
}

func (rg *recordGenerator) generateFloat(col *record.ColVal, withEmpty bool) {
	if withEmpty && rand.Float64() > 0.5 {
		col.AppendFloatNull()
		return
	}

	col.AppendFloat(rand.Float64() * 10)
}

func (rg *recordGenerator) generateString(col *record.ColVal, withEmpty bool) {
	if withEmpty && rand.Float64() > 0.5 {
		col.AppendStringNull()
		return
	}

	col.AppendString(fmt.Sprintf("s_%03d", int64(rand.Float64()*1000)))
}

func (rg *recordGenerator) generateBool(col *record.ColVal, withEmpty bool) {
	if withEmpty && rand.Float64() > 0.5 {
		col.AppendBooleanNull()
		return
	}

	col.AppendBoolean(rand.Float64() > 0.5)
}

func (rg *recordGenerator) generateTimeCol(rowCount int) record.ColVal {
	col := record.ColVal{}

	for i := 0; i < rowCount; i++ {
		col.AppendInteger(rg.begin + int64(i)*rg.interval)
	}

	return col
}

func saveRecordToFile(seq uint64, records map[uint64]*record.Record, order bool, conf *immutable.Config) (immutable.TSSPFile, error) {
	var sidList []uint64
	sidMap := make(map[uint64]struct{})
	for sid := range records {
		sidMap[sid] = struct{}{}
		sidList = append(sidList, sid)
	}
	lockPath := ""
	fileName := immutable.NewTSSPFileName(seq, 0, 0, 0, order, &lockPath)
	builder := immutable.NewMsBuilder(saveDir, "mst", &lockPath, conf, 0, fileName, 1, nil, 2, config.TSSTORE, nil, 0)
	sort.Slice(sidList, func(i, j int) bool {
		return sidList[i] < sidList[j]
	})
	for _, sid := range sidList {
		rec := records[sid]
		sort.Sort(rec)
		if err := builder.WriteData(sid, rec); err != nil {
			return nil, err
		}
	}

	f, err := builder.NewTSSPFile(false)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func itrTSSPFile(f immutable.TSSPFile, hook func(sid uint64, rec *record.Record)) {
	fi := immutable.NewFileIterator(f, immutable.CLog)
	itr := immutable.NewChunkIterator(fi)

	for {
		if !itr.Next() {
			break
		}

		sid := itr.GetSeriesID()
		if sid == 0 {
			panic("series ID is zero")
		}
		rec := itr.GetRecord()
		record.CheckRecord(rec)

		hook(sid, rec)
	}
}
