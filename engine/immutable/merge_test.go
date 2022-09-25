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

package immutable_test

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/immutable/readcache"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/rand"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/open_src/influx/meta"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"github.com/stretchr/testify/assert"
)

var defaultSchemas = record.Schemas{
	record.Field{Type: influx.Field_Type_Int, Name: "int"},
	record.Field{Type: influx.Field_Type_Float, Name: "float"},
	record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
	record.Field{Type: influx.Field_Type_String, Name: "string"},
	record.Field{Type: influx.Field_Type_Int, Name: "time"},
}

var defaultInterval = time.Second.Nanoseconds()
var timeBegin = time.Date(2022, 7, 22, 17, 39, 0, 0, time.UTC).UnixNano()
var saveDir = "/tmp/test_merge/"
var defaultTier uint64 = meta.Warm

func init() {
	immutable.MaxNumOfFileToMerge = 20
	timeBegin = timeBegin - timeBegin%100000
}

type MergeTestHelper struct {
	store   *immutable.MmsTables
	records map[uint64]*record.Record
	rvMap   RecordValuesMap
}

func removeDataDir() {
	_ = os.RemoveAll(saveDir + "/mst")
}

func NewMergeTestHelper(conf *immutable.Config) *MergeTestHelper {
	return &MergeTestHelper{
		store:   immutable.NewTableStore(saveDir, &defaultTier, true, conf),
		records: make(map[uint64]*record.Record),
		rvMap:   make(RecordValuesMap),
	}
}

func (h *MergeTestHelper) dedupe(sid uint64) {
	val, ok := h.rvMap[sid]
	if !ok {
		return
	}

	l := len(val.times)
	for i := 0; i < l-1; i++ {
		if val.times[i] == val.times[i+1] {

			if val.int64Val[i+1] == 0 {
				val.int64Val[i+1] = val.int64Val[i]
			}
			if val.float64Val[i+1] == 0 {
				val.float64Val[i+1] = val.float64Val[i]
			}
			if val.nil[i+1] == true {
				val.boolVal[i+1] = val.boolVal[i]
			}
			if val.stringVal[i+1] == "" {
				val.stringVal[i+1] = val.stringVal[i]
			}

			val.int64Val = append(val.int64Val[:i], val.int64Val[i+1:]...)
			val.float64Val = append(val.float64Val[:i], val.float64Val[i+1:]...)
			val.boolVal = append(val.boolVal[:i], val.boolVal[i+1:]...)
			val.stringVal = append(val.stringVal[:i], val.stringVal[i+1:]...)
			val.times = append(val.times[:i], val.times[i+1:]...)
			val.nil = append(val.nil[:i], val.nil[i+1:]...)
			l--
			i--
		}
	}
	h.rvMap[sid] = val
}

func (h *MergeTestHelper) resetRecords() {
	h.records = make(map[uint64]*record.Record)
}

func (h *MergeTestHelper) genRecord(sid uint64, begin int64, limit int, randData bool) {
	rec, values := genRecByTimeBegin(defaultSchemas, begin, defaultInterval, limit, randData)
	h.rvMap.add(sid, values)
	old, ok := h.records[sid]
	if !ok {
		h.records[sid] = rec
	} else {
		old.Merge(rec)
	}
}

func (h *MergeTestHelper) genRecordRandomEmpty(sid uint64, begin int64, limit int) {
	rec, values := genRecByTimeBeginRandomEmpty(defaultSchemas, begin, defaultInterval, limit)
	h.rvMap.add(sid, values)
	h.records[sid] = rec
}

func (h *MergeTestHelper) save(seq uint64, order bool) error {
	fo, err := saveRecordToFile(seq, h.records, order, h.store.Conf)
	if err != nil {
		return err
	}
	h.store.AddTSSPFiles(fo.Name(), order, fo)
	h.resetRecords()
	return nil
}

/*
模型1: 前置多个无序，相互交叉， 按时间戳最小值开始合并，合并顺序：3，1，2
               -------有序-------
1      -----------
2         -------------
3------------
*/
func TestMergeHelper_Merge_mod1(t *testing.T) {
	cacheIns := readcache.GetReadCacheIns()
	cacheIns.Purge()
	removeDataDir()
	mh := NewMergeTestHelper(immutable.NewConfig())
	mh.genRecord(100, timeBegin-defaultInterval*10, 10, false)
	if !assert.NoError(t, mh.save(1, true)) {
		return
	}

	for i, n := range []int64{16, 14, 22} {
		begin := timeBegin - defaultInterval*n + int64(i+1)*1000
		mh.genRecord(100, begin, 10, false)
		if !assert.NoError(t, mh.save(uint64(i+2), false)) {
			return
		}
	}

	assert.NoError(t, mergeAndCompare(mh.store, mh.rvMap, []uint64{1}))
}

// 模型2: 有序 10 个时间线 一个文件，乱序20个时间线，20个文件
func TestMergeHelper_Merge_mod2(t *testing.T) {
	cacheIns := readcache.GetReadCacheIns()
	cacheIns.Purge()
	removeDataDir()
	limit := 50
	mh := NewMergeTestHelper(immutable.NewConfig())

	// 10个时间线，每个时间线 5000 条数据
	for i := 0; i < 10; i++ {
		sid := uint64(i*2 + 100)
		begin := timeBegin - defaultInterval*int64(limit) + int64(i+1)*10000
		mh.genRecord(sid, begin, limit, false)
	}

	if !assert.NoError(t, mh.save(1, true)) {
		return
	}

	// 20个乱序文件
	for i := 0; i < 20; i++ {
		sid := uint64(i) + 100
		begin := timeBegin - defaultInterval*(rand.Int63()%7000) + int64(i+1)*1000
		mh.genRecord(sid, begin, limit, false)

		if !assert.NoError(t, mh.save(uint64(i+2), false)) {
			return
		}
	}

	assert.NoError(t, mergeAndCompare(mh.store, mh.rvMap, []uint64{1}))
}

/*
模型3: 剩余数据合并
SeriesID=3 合并到 第二个文件
SeriesID=1 部分合入第一个文件，部分合入第二个文件，
合并剩余数据时，先合并 SeriesID=1， 然后 合并 SeriesID=3， 按SeriesID 从小到大依次合并

------SeriesID=1-------             ------SeriesID=2-------       ------SeriesID=3-------     ------SeriesID=1-------
        -----SeriesID=3-----
                              -----SeriesID=1-----
*/
func TestMergeHelper_Merge_mod3(t *testing.T) {
	cacheIns := readcache.GetReadCacheIns()
	cacheIns.Purge()
	removeDataDir()
	limit := 100
	mh := NewMergeTestHelper(immutable.NewConfig())

	for i, sid := range []uint64{1, 2, 3, 1} {
		begin := timeBegin - defaultInterval*int64(limit*(4-i)) - 100*int64(4-i)*defaultInterval
		fmt.Println(begin, " --> ", begin+defaultInterval*int64(limit))

		mh.genRecord(sid, begin, limit, false)
		if !assert.NoError(t, mh.save(uint64(i+1), true)) {
			return
		}
	}

	begin := timeBegin - defaultInterval*int64(limit*8) + 30*defaultInterval + 10000
	fmt.Println(begin, " --> ", begin+defaultInterval*int64(limit))

	mh.genRecord(3, begin, limit, false)
	if !assert.NoError(t, mh.save(5, false)) {
		return
	}

	begin = timeBegin - defaultInterval*int64(limit*7) + 50*defaultInterval + 20000
	fmt.Println(begin, " --> ", begin+defaultInterval*int64(limit))

	mh.genRecord(1, begin, limit, false)
	if !assert.NoError(t, mh.save(6, false)) {
		return
	}

	assert.NoError(t, mergeAndCompare(mh.store, mh.rvMap, []uint64{1, 2, 3, 4}))
}

/*
模型4: 剩余数据合并
第二个有序文件 中包含两个 SeriesID： 1 和 2
乱序文件SeriesID=1， 与两个有序文件时间重叠
乱序数据最大时间超过第二个文件在 SeriesID=1 的数据最大时间

------SeriesID=1-------     ------SeriesID=1-----|-----SeriesID=2-------
             -----------------SeriesID=1-----------------
*/
func TestMergeHelper_Merge_mod4(t *testing.T) {
	cacheIns := readcache.GetReadCacheIns()
	cacheIns.Purge()
	removeDataDir()
	limit := 10
	mh := NewMergeTestHelper(immutable.NewConfig())

	mh.genRecord(1, getBegin(limit, 4, 0), limit, false)
	if !assert.NoError(t, mh.save(1, true)) {
		return
	}

	mh.genRecord(1, getBegin(limit, 2.5, 0), limit, false)
	mh.genRecord(2, getBegin(limit, 1.5, 0), limit, false)
	if !assert.NoError(t, mh.save(2, true)) {
		return
	}

	mh.genRecord(1, getBegin(limit, 3.5, 10000), 2*limit+limit/2, false)
	if !assert.NoError(t, mh.save(3, false)) {
		return
	}

	assert.NoError(t, mergeAndCompare(mh.store, mh.rvMap, []uint64{1, 2}))
}

/*
模型5: 重叠数据合并， 乱序数据中有部分数据与 正常数据时间戳重叠，使用乱序数据替换
      ------SeriesID=1-------      ------SeriesID=1-------
-----SeriesID=1------      --
*/
func TestMergeHelper_Merge_mod5(t *testing.T) {
	cacheIns := readcache.GetReadCacheIns()
	cacheIns.Purge()
	removeDataDir()
	limit := 10
	mh := NewMergeTestHelper(immutable.NewConfig())

	mh.genRecord(1, getBegin(limit, 3, 0), limit, false)
	if !assert.NoError(t, mh.save(1, true)) {
		return
	}

	mh.genRecord(1, getBegin(limit, 1, 0), limit, false)
	if !assert.NoError(t, mh.save(2, true)) {
		return
	}

	mh.genRecord(1, getBegin(limit, 3.5, 0), limit, false)
	if !assert.NoError(t, mh.save(3, false)) {
		return
	}

	mh.genRecord(1, getBegin(limit, 3, 0)-defaultInterval*2, 2, false)
	if !assert.NoError(t, mh.save(4, false)) {
		return
	}

	mh.genRecordRandomEmpty(1, getBegin(limit, 2, 0)-defaultInterval*2, 2)
	if !assert.NoError(t, mh.save(5, false)) {
		return
	}

	val := mh.rvMap[1]
	l := len(val.times)
	for i := 0; i < l-1; i++ {
		if val.times[i] == val.times[i+1] {

			if val.int64Val[i+1] == 0 {
				val.int64Val[i+1] = val.int64Val[i]
			}
			if val.float64Val[i+1] == 0 {
				val.float64Val[i+1] = val.float64Val[i]
			}
			if val.nil[i+1] == true {
				val.boolVal[i+1] = val.boolVal[i]
			}
			if val.stringVal[i+1] == "" {
				val.stringVal[i+1] = val.stringVal[i]
			}

			val.int64Val = append(val.int64Val[:i], val.int64Val[i+1:]...)
			val.float64Val = append(val.float64Val[:i], val.float64Val[i+1:]...)
			val.boolVal = append(val.boolVal[:i], val.boolVal[i+1:]...)
			val.stringVal = append(val.stringVal[:i], val.stringVal[i+1:]...)
			val.times = append(val.times[:i], val.times[i+1:]...)
			val.nil = append(val.nil[:i], val.nil[i+1:]...)
			l--
			i--
		}
	}

	assert.NoError(t, mergeAndCompare(mh.store, mh.rvMap, []uint64{1, 2}))
}

// 模型6: 有序 10 个时间线 10个文件，乱序20个时间线，20个文件
// merge 后 LevelCompact为 3个文件
func TestMergeHelper_Merge_mod6(t *testing.T) {
	cacheIns := readcache.GetReadCacheIns()
	cacheIns.Purge()
	removeDataDir()
	limit := 100
	mh := NewMergeTestHelper(immutable.NewConfig())

	// 10个时间线，每个时间线 10000 条数据
	seqList := make([]uint64, 0, 10)
	for i := 0; i < 10; i++ {
		sid := uint64(i*2 + 100)
		begin := timeBegin - defaultInterval*int64(limit) + int64(i+1)*10000
		mh.genRecord(sid, begin, limit, false)

		if !assert.NoError(t, mh.save(uint64(i+1), true)) {
			return
		}
		seqList = append(seqList, uint64(i+1))
	}

	// 20个乱序文件
	for i := 0; i < 20; i++ {
		sid := uint64(i) + 100
		begin := timeBegin - defaultInterval*(rand.Int63()%6000) + int64(i+1)*1000
		mh.genRecord(sid, begin, limit, false)

		if !assert.NoError(t, mh.save(uint64(i+11), false)) {
			return
		}
	}

	assert.NoError(t, mergeAndCompare(mh.store, mh.rvMap, seqList))
}

// 模型7: 乱序数据最大时间，小于有序数据最小时间
// 有序                          ------SeriesID=1-------    ------SeriesID=1-------|------SeriesID=2-------
// 乱序 -----SeriesID=1------                                               -----SeriesID=2------
func TestMergeHelper_Merge_mod7(t *testing.T) {
	cacheIns := readcache.GetReadCacheIns()
	cacheIns.Purge()
	removeDataDir()
	limit := 10
	mh := NewMergeTestHelper(immutable.NewConfig())

	var sid uint64 = 100
	begin := 100 * time.Second.Nanoseconds()
	duration := defaultInterval * int64(limit)

	mh.genRecord(sid, begin-3*duration, limit, false)
	if !assert.NoError(t, mh.save(1, true)) {
		return
	}

	mh.genRecord(sid, begin-2*duration, limit, false)
	mh.genRecord(sid+1, begin-duration, limit, false)
	if !assert.NoError(t, mh.save(2, true)) {
		return
	}

	for i := 0; i < 6; i++ {
		mh.genRecord(sid+100+uint64(i), begin-duration, limit, false)
		if !assert.NoError(t, mh.save(10+uint64(i), true)) {
			return
		}
	}

	mh.genRecord(sid, begin-10*duration, limit, false)
	mh.genRecord(sid+1, begin-2*duration, limit, false)
	if !assert.NoError(t, mh.save(sid, false)) {
		return
	}

	assert.NoError(t, mergeAndCompare(mh.store, mh.rvMap, []uint64{sid, sid + 1}))
}

func getBegin(limit int, factor float64, mantissa int64) int64 {
	x := int64(float64(limit) * factor)

	return timeBegin - defaultInterval*x + mantissa
}

// 合并乱序数据，并读取合并后的 record 和 预期数据进行对比
func mergeAndCompare(store *immutable.MmsTables, expValues RecordValuesMap, seqs []uint64) error {
	if err := store.MergeOutOfOrder(1); err != nil {
		return err
	}
	store.Wait()

	store.CompactionEnable()
	if err := store.LevelCompact(0, 1); err != nil {
		return err
	}
	store.Wait()

	files := store.Order["mst"]

	hlp := immutable.NewMergeHelper(nil, defaultTier, "mst", saveDir, func() bool {
		return false
	})
	var records = make(map[uint64]*record.Record)
	sort.Slice(seqs, func(i, j int) bool {
		return seqs[i] < seqs[j]
	})

	for _, f := range files.Files() {
		fi := immutable.NewFileIterator(f, immutable.CLog)
		itr := immutable.NewChunkIterator(fi)

		err2 := hlp.ReadSeriesRecord(itr, func(sid uint64, rec *record.Record) error {
			tmp, ok := records[sid]
			if !ok {
				records[sid] = rec
				hlp.RenewSeriesRecord()
				return nil
			}

			tmp.Merge(rec)
			return nil
		})

		if err2 != nil {
			return err2
		}
	}

	for sid, rec := range records {
		val, ok := expValues[sid]
		if !ok {
			return fmt.Errorf("invalid series ID: %d", sid)
		}

		if !checkTime(rec.TimeColumn()) {
			fmt.Println(rec.Times())
			return fmt.Errorf("unordered time column")
		}

		if err := compareRecord(rec, val); err != nil {
			return fmt.Errorf("the record does not meet expectations. series id: %d. \n %v", sid, err)
		}
	}

	return nil
}

func checkTime(col *record.ColVal) bool {
	times := col.IntegerValues()
	if len(times) <= 1 {
		return true
	}

	for i := 1; i < len(times); i++ {
		if times[i] <= times[i-1] {
			return false
		}
	}

	return true
}

// 对比 record 和 预期数据
func compareRecord(rec *record.Record, val *RecordValues) error {
	times := rec.Times()
	dst := &RecordValues{
		stringVal:  make([]string, len(times)),
		int64Val:   make([]int64, len(times)),
		boolVal:    make([]bool, len(times)),
		float64Val: make([]float64, len(times)),
		nil:        make([]bool, len(times)),
	}
	dst.times = times
	for i := 0; i < len(rec.Schema)-1; i++ {
		col := rec.ColVals[i]
		for j := 0; j < len(dst.times); j++ {
			if !col.ValidAt(j) {
				continue
			}

			switch rec.Schema[i].Type {
			case influx.Field_Type_Float:
				v, _ := col.FloatValue(j)
				dst.float64Val[j] = v
			case influx.Field_Type_Int:
				v, _ := col.IntegerValue(j)
				dst.int64Val[j] = v
			case influx.Field_Type_Boolean:
				v, _ := col.BooleanValue(j)
				dst.boolVal[j] = v
			case influx.Field_Type_String:
				v, _ := col.StringValueUnsafe(j)
				dst.stringVal[j] = v
			}
		}
	}
	sort.Sort(dst)

	val.idx = nil
	if !reflect.DeepEqual(dst.int64Val, val.int64Val) {
		return fmt.Errorf("int64Val not equal. \n exp: %+v; \n got: %+v \n", val.int64Val, dst.int64Val)
	}
	if !reflect.DeepEqual(dst.boolVal, val.boolVal) {
		return fmt.Errorf("boolVal not equal. \n exp: %+v; \n got: %+v \n", val.boolVal, dst.boolVal)
	}
	if !reflect.DeepEqual(dst.stringVal, val.stringVal) {
		return fmt.Errorf("stringVal not equal. \n exp: %+v; \n got: %+v \n", val.stringVal, dst.stringVal)
	}
	if !reflect.DeepEqual(dst.float64Val, val.float64Val) {
		return fmt.Errorf("float64Val not equal. \n exp: %+v; \n got: %+v \n", val.float64Val, dst.float64Val)
	}
	if !reflect.DeepEqual(dst.times, val.times) {
		return fmt.Errorf("times not equal. \n exp: %+v; \n got: %+v \n", val.times, dst.times)
	}

	return nil
}

type RecordValuesMap map[uint64]*RecordValues

func (rm RecordValuesMap) add(sid uint64, val *RecordValues) {
	dst, ok := rm[sid]
	if !ok {
		rm[sid] = val
		return
	}
	dst.merge(val)
}

type RecordValues struct {
	int64Val   []int64
	float64Val []float64
	stringVal  []string
	boolVal    []bool
	times      []int64
	idx        []int
	nil        []bool
}

func (r *RecordValues) Len() int {
	return len(r.times)
}

func (r *RecordValues) Less(i, j int) bool {
	if r.times[i] == r.times[j] && r.idx != nil {
		return r.idx[i] < r.idx[j]
	}

	return r.times[i] < r.times[j]
}

func (r *RecordValues) Swap(i, j int) {
	r.boolVal[i], r.boolVal[j] = r.boolVal[j], r.boolVal[i]
	r.float64Val[i], r.float64Val[j] = r.float64Val[j], r.float64Val[i]
	r.stringVal[i], r.stringVal[j] = r.stringVal[j], r.stringVal[i]
	r.int64Val[i], r.int64Val[j] = r.int64Val[j], r.int64Val[i]
	r.times[i], r.times[j] = r.times[j], r.times[i]
	r.nil[i], r.nil[j] = r.nil[j], r.nil[i]

	if r.idx != nil {
		r.idx[i], r.idx[j] = r.idx[j], r.idx[i]
	}
}

func (r *RecordValues) merge(src *RecordValues) {
	for i := 0; i < len(src.idx); i++ {
		src.idx[i] += len(r.times)
	}

	r.int64Val = append(r.int64Val, src.int64Val...)
	r.float64Val = append(r.float64Val, src.float64Val...)
	r.boolVal = append(r.boolVal, src.boolVal...)
	r.stringVal = append(r.stringVal, src.stringVal...)
	r.times = append(r.times, src.times...)
	r.idx = append(r.idx, src.idx...)
	r.nil = append(r.nil, src.nil...)

	sort.Sort(r)
}

func saveRecordToFile(seq uint64, records map[uint64]*record.Record, order bool, conf *immutable.Config) (immutable.TSSPFile, error) {
	var sidList []uint64
	sidMap := make(map[uint64]struct{})
	for sid := range records {
		sidMap[sid] = struct{}{}
		sidList = append(sidList, sid)
	}

	fileName := immutable.NewTSSPFileName(seq, 0, 0, 0, order)
	builder := immutable.GetMsBuilder(saveDir, "mst", fileName,
		conf, nil, 1, 2)

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

func genRecByTimeBegin(schema record.Schemas, begin int64, interval int64, limit int, randData bool) (*record.Record, *RecordValues) {
	val := &RecordValues{
		stringVal:  make([]string, limit),
		int64Val:   make([]int64, limit),
		boolVal:    make([]bool, limit),
		float64Val: make([]float64, limit),
		nil:        make([]bool, limit),
	}
	bitMaps := make([][]int, 4)

	k := int((begin / 723) % 100)
	var n, mod1, mod2, mod3 int
	if randData {
		mod1, mod2, mod3 = 20, 30, 40
	} else {
		mod1, mod2, mod3 = 2, 3, 4
	}
	for i := 0; i < limit; i++ {
		if randData {
			n = rand.Intn(1 << 60)
		} else {
			n = k + i
		}

		bitMaps[0] = append(bitMaps[0], 1)
		bitMaps[1] = append(bitMaps[1], i%mod1)
		bitMaps[2] = append(bitMaps[2], i%mod2)
		bitMaps[3] = append(bitMaps[3], i%mod3)

		val.stringVal[i] = fmt.Sprintf("s_%d", n)
		if bitMaps[1][i] == 1 {
			val.int64Val[i] = int64(n)
		}
		if bitMaps[2][i] == 1 {
			val.float64Val[i] = float64(n)
		}
		if bitMaps[3][i] == 1 {
			val.boolVal[i] = n%2 == 0
		} else {
			val.nil[i] = true
		}
		val.times = append(val.times, begin+int64(i)*interval)
		val.idx = append(val.idx, i)
	}

	return genRowRec(schema,
		bitMaps[1], val.int64Val,
		bitMaps[2], val.float64Val,
		bitMaps[0], val.stringVal,
		bitMaps[3], val.boolVal,
		val.times), val
}

func genRecByTimeBeginRandomEmpty(schema record.Schemas, begin int64, interval int64, limit int) (*record.Record, *RecordValues) {
	val := &RecordValues{
		stringVal:  make([]string, limit),
		int64Val:   make([]int64, limit),
		boolVal:    make([]bool, limit),
		float64Val: make([]float64, limit),
		nil:        make([]bool, limit),
	}
	bitMaps := make([][]int, 4)

	k := int((begin / 323) % 100)

	for i := 0; i < limit; i++ {
		n := k + i

		bitMaps[0] = append(bitMaps[0], n%2)
		bitMaps[1] = append(bitMaps[1], n%3)
		bitMaps[2] = append(bitMaps[2], n%5)
		bitMaps[3] = append(bitMaps[3], 1)

		if bitMaps[0][i] == 1 {
			val.stringVal[i] = fmt.Sprintf("sr_%d", n)
		}
		if bitMaps[1][i] == 1 {
			val.int64Val[i] = int64(n) + 1000
		}
		if bitMaps[2][i] == 1 {
			val.float64Val[i] = float64(n) + 1000
		}
		if bitMaps[3][i] == 1 {
			val.boolVal[i] = n%2 == 0
		} else {
			val.nil[i] = true
		}

		val.times = append(val.times, begin+int64(i)*interval)
		val.idx = append(val.idx, i)
	}

	return genRowRec(schema,
		bitMaps[1], val.int64Val,
		bitMaps[2], val.float64Val,
		bitMaps[0], val.stringVal,
		bitMaps[3], val.boolVal,
		val.times), val
}

func genRowRec(schema []record.Field, intValBitmap []int, intVal []int64, floatValBitmap []int, floatVal []float64,
	stringValBitmap []int, stringVal []string, booleanValBitmap []int, boolVal []bool, time []int64) *record.Record {
	var rec record.Record

	rec.Schema = append(rec.Schema, schema...)
	for i, v := range rec.Schema {
		var colVal record.ColVal
		if v.Type == influx.Field_Type_Int {
			if i == len(rec.Schema)-1 {
				// time col
				for index := range time {
					colVal.AppendInteger(time[index])
				}
			} else {
				for index := range time {
					if intValBitmap[index] == 1 {
						colVal.AppendInteger(intVal[index])
					} else {
						colVal.AppendIntegerNull()
					}
				}
			}
		} else if v.Type == influx.Field_Type_Boolean {
			for index := range time {
				if booleanValBitmap[index] == 1 {
					colVal.AppendBoolean(boolVal[index])
				} else {
					colVal.AppendBooleanNull()
				}
			}
		} else if v.Type == influx.Field_Type_Float {
			for index := range time {
				if floatValBitmap[index] == 1 {
					colVal.AppendFloat(floatVal[index])
				} else {
					colVal.AppendFloatNull()
				}
			}
		} else if v.Type == influx.Field_Type_String {
			for index := range time {
				if stringValBitmap[index] == 1 {
					colVal.AppendString(stringVal[index])
				} else {
					colVal.AppendStringNull()
				}
			}
		} else {
			panic("error type")
		}
		rec.ColVals = append(rec.ColVals, colVal)
	}
	return &rec
}

func TestMergeHelper_Merge_zeroSeriesID(t *testing.T) {
	cacheIns := readcache.GetReadCacheIns()
	cacheIns.Purge()
	removeDataDir()
	limit := 10
	mh := NewMergeTestHelper(immutable.NewConfig())

	begin := timeBegin - defaultInterval*int64(limit) + 10000
	mh.genRecord(0, begin, limit, false)

	if err := mh.save(1, true); err != nil {
		t.Errorf("%v", err)
	}

	hlp := immutable.NewMergeHelper(nil, defaultTier, "mst", saveDir, func() bool {
		return false
	})
	for _, f := range mh.store.Order["mst"].Files() {
		fi := immutable.NewFileIterator(f, immutable.CLog)
		itr := immutable.NewChunkIterator(fi)
		err := hlp.ReadSeriesRecord(itr, func(sid uint64, rec *record.Record) error {
			return nil
		})
		assert.Regexp(t, regexp.MustCompile("invalid record, series id is 0\\. file: (.*?)\\.tssp"), err.Error())
	}
}

func TestMergeHelper_Merge_diffSchema(t *testing.T) {
	cacheIns := readcache.GetReadCacheIns()
	cacheIns.Purge()
	removeDataDir()
	limit := 10
	conf := immutable.NewConfig()
	mh := NewMergeTestHelper(conf)

	begin := timeBegin - defaultInterval*int64(limit) + 10000
	mh.genRecord(1, begin, limit, false)
	if !assert.NoError(t, mh.save(1, false)) {
		return
	}

	mh.genRecord(1, begin, limit, false)
	schema := &mh.records[1].Schema[0]
	exp := errno.NewError(errno.DiffSchemaType, schema.Name, 3, schema.Type)
	schema.Type = 3
	if !assert.NoError(t, mh.save(2, true)) {
		return
	}

	hlp := immutable.NewMergeHelper(logger.NewLogger(errno.ModuleMerge), defaultTier, "mst", saveDir, func() bool {
		return false
	})
	hlp.Conf = conf
	hlp.ReadWaitMergedRecords(mh.store.OutOfOrder["mst"])
	_, err := hlp.MergeTo(mh.store.Order["mst"])

	assert.EqualError(t, err, exp.Error())
}

func TestMergeHelper_Merge_files(t *testing.T) {
	t.Skip()
	names := strings.Split("CPU,Disk,Diskio,Fhcount,Load,Memory,Monitor,Nic,Procs,Swap,Tcp,Tcpstatus", ",")
	names = strings.Split("Diskio", ",")

	for _, name := range names {
		readWaitMergedRecords(t, name)
	}
}

func readWaitMergedRecords(t *testing.T, name string) {
	mh := NewMergeTestHelper(immutable.NewConfig())
	err := openFilesFromDir("E:/tmp/merge/"+name+"/out-of-order/", mh.store, false)
	if !assert.NoError(t, err) {
		return
	}

	err = openFilesFromDir("E:/tmp/merge/"+name+"/", mh.store, true)
	if !assert.NoError(t, err) {
		return
	}

	assert.NoError(t, mh.store.MergeOutOfOrder(1))
}

func openFilesFromDir(dir string, store *immutable.MmsTables, order bool) error {
	glob, err := fileops.Glob(dir + "*")
	if err != nil {
		return err
	}
	fmt.Println(glob)

	for _, s := range glob {
		if s[len(s)-4:] != "tssp" {
			continue
		}

		f, err := immutable.OpenTSSPFile(s, order, false)
		if err != nil {
			return err
		}

		_ = f.LoadIndex()
		store.AddTSSPFiles(f.Name(), order, f)
	}
	return nil
}

/*
模型7: preset one order file, one disorder file.
      the number of rows exceed the segment limit after merging
1      -----------------------  : sequence file 120 rows
                    |<--10-->|
2                   --------------------
      |<---------------140------------->|
*/
func TestMergeHelper_Merge_SegmentLimit1(t *testing.T) {
	cacheIns := readcache.GetReadCacheIns()
	cacheIns.Purge()
	removeDataDir()

	conf := immutable.NewConfig()
	conf.SetMaxRowsPerSegment(40)
	conf.SetMaxSegmentLimit(3)
	mh := NewMergeTestHelper(conf)

	rows := 40 * 3
	mh.genRecord(100, timeBegin, rows, false)
	if !assert.NoError(t, mh.save(1, true)) {
		return
	}

	tm := timeBegin + int64(rows-10)*defaultInterval
	mh.genRecord(100, tm, rows-10, false)
	if !assert.NoError(t, mh.save(2, false)) {
		return
	}

	mh.dedupe(100)
	assert.NoError(t, mergeAndCompare(mh.store, mh.rvMap, []uint64{1}))
}

/*
模型8: preset 5 order files, 6 disorder file.
      the number of rows exceed the segment limit after merging
1      -----------------------  : sequence file 120 rows
                    |<--10-->| overlap:10 rows
2                   --------------------
      |<---------------140------------->|
....
*/
func TestMergeHelper_Merge_SegmentLimit2(t *testing.T) {
	cacheIns := readcache.GetReadCacheIns()
	cacheIns.Purge()
	removeDataDir()
	conf := immutable.NewConfig()
	conf.SetMaxRowsPerSegment(40)
	conf.SetMaxSegmentLimit(3)
	mh := NewMergeTestHelper(conf)

	rows := 40 * 3

	var seq uint64 = 1
	var sid uint64 = 1
	// 5个时间线，每个时间线 120 条数据
	begin := timeBegin
	for i := 0; i < 5; i++ {
		mh.genRecord(sid, begin, rows, false)
		begin += int64(rows) * defaultInterval
		sid++
	}

	if !assert.NoError(t, mh.save(seq, true)) {
		return
	}
	seq++

	// 6个乱序文件
	begin = timeBegin
	sid = 1
	for i := 0; i < 6; i++ {
		st := begin - 10*defaultInterval
		mh.genRecord(sid, st, rows, false)
		begin += int64(rows) * defaultInterval
		if !assert.NoError(t, mh.save(seq, false)) {
			return
		}
		seq++
		sid++
	}

	for i := uint64(1); i < 6; i++ {
		mh.dedupe(i)
	}
	assert.NoError(t, mergeAndCompare(mh.store, mh.rvMap, []uint64{1}))
}

/*
模型8: preset 1 order files, 8 out-of-order file; compact after merge.
      the number of rows exceed the segment limit after merging
1      -----------------------  : sequence file 120 rows
                    |<--10-->| overlap:10 rows
2                   --------------------------- : out-of-order file, 120 rows
      |<-------------------230---------------->|
....
*/
func TestMergeHelper_Merge_SegmentLimit3(t *testing.T) {
	cacheIns := readcache.GetReadCacheIns()
	cacheIns.Purge()
	removeDataDir()
	rowPerSeg := 8
	conf := immutable.NewConfig()
	conf.SetMaxRowsPerSegment(rowPerSeg)
	conf.SetMaxSegmentLimit(3)
	mh := NewMergeTestHelper(conf)

	rows := rowPerSeg * 3

	var seq uint64 = 1
	var sid uint64 = 1
	// 8个时间线，每个时间线 120 条数据
	begin := timeBegin
	for i := 0; i < 8; i++ {
		mh.genRecord(sid, begin, rows, false)
		sid++
		seq++
	}

	if !assert.NoError(t, mh.save(seq, true)) {
		return
	}

	// 8个乱序文件
	begin = timeBegin
	sid = 1
	st := begin - 10*defaultInterval
	for i := 0; i < 8; i++ {
		mh.genRecord(sid, st, rows, false)
		if !assert.NoError(t, mh.save(seq, false)) {
			return
		}
		seq++
		sid++
	}

	for i := uint64(1); i <= 8; i++ {
		mh.dedupe(i)
	}
	assert.NoError(t, mergeAndCompare(mh.store, mh.rvMap, []uint64{1}))
}

func TestMergeHelper_Merge_First(t *testing.T) {
	cacheIns := readcache.GetReadCacheIns()
	cacheIns.Purge()
	removeDataDir()

	conf := immutable.NewConfig()
	conf.SetMaxRowsPerSegment(40000)
	conf.SetMaxSegmentLimit(10000)
	mh := NewMergeTestHelper(conf)

	for i := 0; i < 100; i++ {
		mh.genRecord(uint64(100+i), timeBegin-defaultInterval*10, 10000, false)
	}
	if !assert.NoError(t, mh.save(1, true)) {
		return
	}

	for i, n := range []int64{16, 14, 22} {
		begin := timeBegin - defaultInterval*n + int64(i+1)*1000
		mh.genRecord(100, begin, 10, false)
		if !assert.NoError(t, mh.save(uint64(i+2), false)) {
			return
		}
	}

	if !assert.NoError(t, mh.store.MergeOutOfOrder(1)) {
		return
	}
	mh.store.Wait()

	dir := saveDir + "/mst/out-of-order/*"
	glob, err := fileops.Glob(dir)
	if err != nil {
		return
	}
	if assert.Equal(t, 1, len(glob), "merge failed") {
		return
	}
}

func TestMerge_SplitFile(t *testing.T) {
	cacheIns := readcache.GetReadCacheIns()
	cacheIns.Purge()
	removeDataDir()

	conf := immutable.NewConfig()
	conf.SetFilesLimit(10)
	mh := NewMergeTestHelper(conf)

	var seq uint64 = 1
	var sid uint64 = 1

	// gen 4 order files
	rows := 20000
	begin := timeBegin
	interval := 10000
	for i := 0; i < 4; i++ {
		mh.genRecord(sid, begin, rows-2, true)
		begin += int64(rows-2) * defaultInterval
		begin += int64(interval) * defaultInterval
		mh.genRecord(sid, begin, 2, true)
		begin += 2 * defaultInterval

		sid++
		mh.genRecord(sid, begin, interval, true)
		begin += int64(interval) * defaultInterval
		sid++
		mh.genRecord(sid, begin, interval, true)
		begin += int64(interval) * defaultInterval
		if !assert.NoError(t, mh.save(seq, true)) {
			return
		}
		sid++
		seq++
	}

	files := mh.store.GetFilesRef("mst", true)
	newNames := []string{
		"00000001-0000-00000000.tssp", "00000001-0000-00000001.tssp",
		"00000001-0000-00000002.tssp", "00000001-0000-00000003.tssp",
	}
	if len(files) != 4 {
		t.Fatal("gen file fail")
	}
	var names []string
	for _, f := range files {
		name := f.Path()
		names = append(names, name)
	}
	immutable.UnrefFiles(files...)
	if err := mh.store.Close(); err != nil {
		t.Fatal(err)
	}
	for i, old := range names {
		dir := filepath.Dir(old)
		nn := filepath.Join(dir, newNames[i])
		_ = fileops.RenameFile(old, nn)
	}
	mh.store = immutable.NewTableStore(saveDir, &defaultTier, true, conf)
	if _, _, err := mh.store.Open(); err != nil {
		t.Fatal(err)
	}

	// gen 4 out-of-order-files
	sids := []uint64{1, 4, 7, 10}
	for _, id := range sids {
		v := mh.rvMap[id]
		st := v.times[rows-3] + defaultInterval
		mh.genRecord(id, st, interval, true)
		if !assert.NoError(t, mh.save(seq, false)) {
			return
		}
		seq++
	}

	// gen 1 out-of-order-files
	for id := uint64(11); id < 14; id++ {
		st := begin
		if id <= 12 {
			v := mh.rvMap[12]
			st = v.times[interval-4]
		} else {
			begin += int64(rows) * defaultInterval
		}

		mh.genRecord(id, st, rows, true)
	}
	if !assert.NoError(t, mh.save(seq, false)) {
		return
	}

	for i := uint64(1); i <= 13; i++ {
		mh.dedupe(i)
	}

	assert.NoError(t, mergeAndCompare(mh.store, mh.rvMap, []uint64{1}))
}
