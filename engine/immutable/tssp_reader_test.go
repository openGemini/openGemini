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

package immutable

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/influxdata/influxdb/pkg/bloom"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/interruptsignal"
	"github.com/openGemini/openGemini/lib/rand"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/open_src/influx/meta"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"github.com/stretchr/testify/assert"
)

const (
	testDir = "/tmp/data1"
)

func genMemTableData(id uint64, idCount int, rows int, idr *MinMax, tr *MinMax) ([]uint64, map[uint64]*record.Record) {
	tm := time.Now().Truncate(time.Minute).UnixNano()

	idr.min = id
	tr.min = uint64(tm)

	schema := []record.Field{
		{Name: "field1_int64", Type: influx.Field_Type_Int},
		{Name: "field2_float", Type: influx.Field_Type_Float},
		{Name: "field3_string", Type: influx.Field_Type_String},
		{Name: "field4_bool", Type: influx.Field_Type_Boolean},
		{Name: "time", Type: influx.Field_Type_Int},
	}

	genRecFn := func() *record.Record {
		b := record.NewRecordBuilder(schema)

		f1 := rand.Int63n(10)
		f2 := 1.2 * float64(f1)
		f4 := true

		f1Builder := b.Column(0) // int64
		f2Builder := b.Column(1) // float
		f3Builder := b.Column(2) // string
		f4Builder := b.Column(3) // bool
		tmBuilder := b.Column(4) // timestamp
		for i := 1; i <= rows; i++ {
			if i%21 == 0 {
				f1Builder.AppendIntegerNull()
			} else {
				f1Builder.AppendInteger(f1)
			}

			f2Builder.AppendFloat(f2)

			if i%25 == 0 {
				f3Builder.AppendStringNull()
			} else {
				f3 := fmt.Sprintf("test_%d", f1)
				f3Builder.AppendString(f3)
			}

			if i%30 == 0 {
				f4Builder.AppendBooleanNull()
			} else {
				f4Builder.AppendBoolean(f4)
			}

			tmBuilder.AppendInteger(tm)
			f4 = !f4
			tm += time.Millisecond.Milliseconds()
			f1++
			f2 += 1.1
		}

		return b
	}

	ids := make([]uint64, 0, idCount)
	data := make(map[uint64]*record.Record, idCount)
	for i := 0; i < idCount; i++ {
		rec := genRecFn()
		data[id] = rec
		ids = append(ids, id)
		id++
	}

	idr.max = id - 1
	tr.max = uint64(tm - time.Millisecond.Milliseconds())

	return ids, data
}

func TestTableStoreOpen(t *testing.T) {
	sig := interruptsignal.NewInterruptSignal()
	defer func() {
		sig.Close()
		fileops.RemoveAll(testDir)
	}()
	_ = fileops.RemoveAll(testDir)
	conf := NewConfig()
	tier := uint64(meta.Hot)
	store := NewTableStore(testDir, &tier, false, conf)

	write := func(ids []uint64, data map[uint64]*record.Record, msb *MsBuilder) {
		for _, id := range ids {
			rec := data[id]
			err := msb.WriteData(id, rec)
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	fileSeq := uint64(1)
	msts := []string{"mst", "mst1", "cpu"}
	var idMinMax, tmMinMax MinMax
	for _, mst := range msts {
		ids, data := genMemTableData(1, 10, 100, &idMinMax, &tmMinMax)
		fileName := NewTSSPFileName(fileSeq, 0, 0, 0, true)
		msb := AllocMsBuilder(testDir, mst, conf, 10, fileName, 0, store.Sequencer(), 2)
		write(ids, data, msb)
		fileSeq++
		store.AddTable(msb, true, false)
	}

	sid := uint64(10)
	for i := 0; i < 3; i++ {
		ids, data := genMemTableData(sid, 10, 100, &idMinMax, &tmMinMax)
		isOrder := !(i == 2)
		fileName := NewTSSPFileName(fileSeq, 0, 0, 0, isOrder)
		msb := AllocMsBuilder(testDir, "mst", conf, 10, fileName, 0, store.Sequencer(), 2)
		write(ids, data, msb)
		sid += 5
		store.AddTable(msb, isOrder, false)
		fileSeq++
	}

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	tier1 := uint64(meta.Hot)
	store = NewTableStore(testDir, &tier1, false, conf)
	if _, _, err := store.Open(); err != nil {
		t.Fatal(err)
	}
}

func TestParseFileName(t *testing.T) {
	type TestCase struct {
		fName   string
		valid   bool
		seq     uint64
		level   uint16
		merge   uint16
		extent  uint16
		fmtName string
	}

	tsts := []TestCase{
		{
			fName: "00000001-0001-00010001.tssp",
			valid: true, seq: 1, level: 1, merge: 1, extent: 1,
			fmtName: "00000001-0001-00010001",
		},
		{
			fName: "100000001-0001-00010001.tssp",
			valid: true, seq: 0x100000001, level: 1, merge: 1, extent: 1,
			fmtName: "100000001-0001-00010001",
		},
		{
			fName: "000100001-0001-00010001.tssp",
			valid: true, seq: 0x100000001, level: 1, merge: 1, extent: 1,
			fmtName: "00100001-0001-00010001",
		},
		{
			fName: "0000000100000001-0001-00010001.tssp",
			valid: true, seq: 0x100000001, level: 1, merge: 1, extent: 1,
			fmtName: "100000001-0001-00010001",
		},
		{
			fName: "0000000100000001-0001-00010001.tssp.init",
			valid: true, seq: 0x100000001, level: 1, merge: 1, extent: 1,
			fmtName: "100000001-0001-00010001",
		},
		{
			fName: "00000000000000001-0001-00010001.tssp",
			valid: false, seq: 1, level: 1, merge: 1, extent: 1,
			fmtName: "00000001-0001-00010001",
		},
		{
			fName: "00000001-0001-00010001.tssp.init",
			valid: true, seq: 1, level: 1, merge: 1, extent: 1,
			fmtName: "00000001-0001-00010001",
		},
		{
			fName: "0000001a-0002-000b000f.tssp",
			valid: true, seq: 26, level: 2, merge: 11, extent: 15,
			fmtName: "0000001a-0002-000b000f",
		},
		{
			fName: "0000001a-0012-000b000f.tssp.init",
			valid: true, seq: 26, level: 18, merge: 11, extent: 15,
			fmtName: "0000001a-0012-000b000f",
		},
		{
			fName: "0000001a-0002-000b000f.tssx",
			valid: false, seq: 26, level: 2, merge: 11, extent: 15,
		},
		{
			fName: "0000001a-0002-000b000f.tssp.ini",
			valid: false, seq: 26, level: 2, merge: 11, extent: 15,
		},
		{
			fName: "00000001-0001-00010001.tssp.initt",
			valid: false, seq: 1, level: 1, merge: 1, extent: 1,
		},
	}
	dir := "/data/test/"
	for _, tst := range tsts {
		name := filepath.Join(dir, tst.fName)
		var fileName TSSPFileName
		err := fileName.ParseFileName(name)
		if !tst.valid {
			if err == nil {
				t.Fatalf("%v is a invalid file name, but check true", tst.fName)
			}
			continue
		}

		if err != nil {
			t.Fatalf("%v is a valid file name, but check false", tst.fName)
		}

		fileName.SetOrder(true)
		str := fileName.String()
		if str != tst.fmtName {
			t.Fatalf("exp:%v, get:%v", tst.fmtName, str)
		}
	}
}

func TestTsspReader(t *testing.T) {
	fName := NewTSSPFileName(1, 0, 0, 0, false)
	msb := &MsBuilder{
		Conf:     NewConfig(),
		trailer:  &Trailer{},
		FileName: fName,
	}

	fd := &mockFile{
		CloseFn: func() error {
			return fmt.Errorf("close file fail")
		},
		NameFn: func() string {
			return "/tmp/0000001a-0012-000b000f.tssp.init"
		},
		StatFn: func() (os.FileInfo, error) {
			return nil, fmt.Errorf("stat fail")
		},
	}
	msb.fd = fd
	msb.trailer.bloomK, msb.trailer.bloomM = bloom.Estimate(1, falsePositive)
	msb.fileSize = 4096
	msb.bloomFilter = make([]byte, 8)

	_, err := CreateTSSPFileReader(msb.fileSize, msb.fd, msb.trailer, &msb.TableData, msb.FileVersion(), false)
	if err == nil || !strings.Contains(err.Error(), "table store create file failed") {
		t.Fatal("create tssp file should be fail")
	}

	fd.CloseFn = func() error { return nil }
	_, err = CreateTSSPFileReader(msb.fileSize, msb.fd, msb.trailer, &msb.TableData, msb.FileVersion(), false)
	if err == nil || !strings.Contains(err.Error(), "table store create file failed") {
		t.Fatal("create tssp file should be fail")
	}
}

func TestFullCompacted(t *testing.T) {
	type nameInfo struct {
		seq    uint64
		level  uint16
		extent uint16
	}
	type TestCase struct {
		exp   bool
		files []nameInfo
	}

	cases := []TestCase{
		{
			files: []nameInfo{{seq: 1, level: 3, extent: 0}},
			exp:   true,
		},
		{
			files: []nameInfo{{seq: 1, level: 0, extent: 0}},
			exp:   true,
		},

		{
			files: []nameInfo{{seq: 1, level: 3, extent: 0}, {seq: 1, level: 3, extent: 1}, {seq: 1, level: 3, extent: 2}, {seq: 1, level: 3, extent: 3}},
			exp:   true,
		},

		{
			files: []nameInfo{{seq: 1, level: 3, extent: 0}, {seq: 1, level: 3, extent: 1}, {seq: 2, level: 3, extent: 0}, {seq: 2, level: 3, extent: 1}},
			exp:   false,
		},

		{
			files: []nameInfo{{seq: 1, level: 1}, {seq: 2, level: 1}, {seq: 3, level: 1}, {seq: 4, level: 1}, {seq: 5, level: 1}},
			exp:   false,
		},
		{
			files: []nameInfo{{seq: 1, level: 0}, {seq: 2, level: 0}, {seq: 3, level: 0}, {seq: 4, level: 0}},
			exp:   false,
		},
		{
			files: []nameInfo{{seq: 1, level: 3}, {seq: 2, level: 1}, {seq: 3, level: 0}, {seq: 4, level: 0}},
			exp:   false,
		},
	}

	fs := &TSSPFiles{}

	for _, tsc := range cases {
		fs.files = fs.files[:0]
		for _, ni := range tsc.files {
			fs.files = append(fs.files, &tsspFile{name: NewTSSPFileName(ni.seq, ni.level, 0, ni.extent, true)})
		}

		got := fs.fullCompacted()
		if got != tsc.exp {
			t.Fatalf("check full compacted fail, exp:%v, get:%v", tsc.exp, got)
		}
	}

}

func TestDropMeasurement(t *testing.T) {
	sig := interruptsignal.NewInterruptSignal()
	defer func() {
		sig.Close()
		fileops.RemoveAll(testDir)
	}()
	_ = fileops.RemoveAll(testDir)
	conf := NewConfig()
	tier := uint64(meta.Hot)
	store := NewTableStore(testDir, &tier, false, conf)

	write := func(ids []uint64, data map[uint64]*record.Record, msb *MsBuilder) {
		for _, id := range ids {
			rec := data[id]
			err := msb.WriteData(id, rec)
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	fileSeq := uint64(1)
	msts := []string{"mst", "mst1", "cpu"}
	var idMinMax, tmMinMax MinMax
	for _, mst := range msts {
		ids, data := genMemTableData(1, 10, 100, &idMinMax, &tmMinMax)
		fileName := NewTSSPFileName(fileSeq, 0, 0, 0, true)
		msb := AllocMsBuilder(testDir, mst, conf, 10, fileName, 0, store.Sequencer(), 2)
		write(ids, data, msb)
		fileSeq++
		store.AddTable(msb, true, false)
	}

	files := 8 * 8
	sid := uint64(10)
	for i := 0; i < files; i++ {
		ids, data := genMemTableData(sid, 10, 100, &idMinMax, &tmMinMax)
		fileName := NewTSSPFileName(fileSeq, 0, 0, 0, true)
		msb := AllocMsBuilder(testDir, "mst", conf, 10, fileName, 0, store.Sequencer(), 2)
		write(ids, data, msb)
		sid += 5
		store.AddTable(msb, true, false)
		fileSeq++
	}

	if err := store.LevelCompact(0, 1); err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Millisecond * 20)

	var wg sync.WaitGroup
	errs := make(chan error, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			if err := store.DropMeasurement(context.Background(), "mst"); err != nil {
				errs <- err
			}
			wg.Done()
		}()
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}

	if store.tableFiles("mst", true) != nil {
		t.Fatal("drop measurement fail")
	}

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestClosedTsspFile(t *testing.T) {
	sig := interruptsignal.NewInterruptSignal()
	defer func() {
		sig.Close()
		fileops.RemoveAll(testDir)
	}()
	_ = fileops.RemoveAll(testDir)
	conf := NewConfig()
	tier := uint64(meta.Hot)
	store := NewTableStore(testDir, &tier, false, conf)

	write := func(ids []uint64, data map[uint64]*record.Record, msb *MsBuilder) {
		for _, id := range ids {
			rec := data[id]
			err := msb.WriteData(id, rec)
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	fileSeq := uint64(1)
	var idMinMax, tmMinMax MinMax
	ids, data := genMemTableData(1, 10, 100, &idMinMax, &tmMinMax)
	fileName := NewTSSPFileName(fileSeq, 0, 0, 0, true)
	msb := AllocMsBuilder(testDir, "mst", conf, 10, fileName, 0, store.Sequencer(), 2)
	write(ids, data, msb)
	fileSeq++
	store.AddTable(msb, true, false)

	fs := store.tableFiles("mst", true)
	if fs == nil {
		t.Fatal("get mst files fail")
	}

	fs.StopFiles()

	f := store.File("mst", fs.Files()[0].Path(), true)
	if f == nil {
		t.Fatal("get file fail")
	}

	_, _, err := f.MetaIndex(ids[0], record.TimeRange{Min: math.MinInt64, Max: math.MaxInt64})
	if err != errFileClosed {
		t.Fatal("stop fail fail")
	}

	_, err = f.MetaIndexAt(0)
	if err != errFileClosed {
		t.Fatal("stop fail fail")
	}

	var cm ChunkMeta
	_, err = f.ChunkMeta(ids[0], 0, 0, 0, 0, &cm)
	if err != errFileClosed {
		t.Fatal("stop fail fail")
	}

	_, err = f.ReadData(0, 16, nil)
	if err != errFileClosed {
		t.Fatal("stop fail fail")
	}

	var mi MetaIndex
	_, err = f.ReadChunkMetaData(0, &mi, nil)
	if err != errFileClosed {
		t.Fatal("stop fail fail")
	}

	_, err = f.ReadAt(&cm, 0, nil, nil)
	if err != errFileClosed {
		t.Fatal("stop fail fail")
	}

	_, err = f.ChunkAt(0)
	if err != errFileClosed {
		t.Fatal("stop fail fail")
	}

	_, err = f.Contains(ids[0])
	if err != errFileClosed {
		t.Fatal("stop fail fail")
	}
	_, err = f.ContainsValue(ids[0], record.TimeRange{Min: math.MinInt64, Max: math.MaxInt64})
	if err != errFileClosed {
		t.Fatal("stop fail fail")
	}

	_, _, err = f.MinMaxTime()
	if err != errFileClosed {
		t.Fatal("stop fail fail")
	}
}

func TestReadTimeColumn(t *testing.T) {
	dir := t.TempDir()
	conf := NewConfig()
	tier := uint64(meta.Hot)
	store := NewTableStore(dir, &tier, false, conf)
	defer store.Close()

	write := func(ids []uint64, data map[uint64]*record.Record, msb *MsBuilder) {
		for _, id := range ids {
			rec := data[id]
			err := msb.WriteData(id, rec)
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	fileSeq := uint64(1)
	var idMinMax, tmMinMax MinMax
	ids, data := genMemTableData(1, 10, 100, &idMinMax, &tmMinMax)
	fileName := NewTSSPFileName(fileSeq, 0, 0, 0, true)
	msb := AllocMsBuilder(dir, "mst", conf, 10, fileName, 0, store.Sequencer(), 2)
	write(ids, data, msb)
	fileSeq++
	store.AddTable(msb, true, false)

	fs := store.tableFiles("mst", true)
	if !assert.NotEmpty(t, fs, "get mst files fail") {
		return
	}

	f := store.File("mst", fs.Files()[0].Path(), true)
	if !assert.NotEmpty(t, f, "get file failed") {
		return
	}

	var cm = &ChunkMeta{}
	var err error

	midx, _ := f.MetaIndexAt(0)
	if !assert.NotEmpty(t, midx) {
		return
	}

	cm, err = f.ChunkMeta(midx.id, midx.offset, midx.size, midx.count, 0, nil)
	if !assert.NoError(t, err) {
		return
	}

	dst := &record.Record{}
	cm.colMeta[0] = cm.colMeta[len(cm.colMeta)-1]
	for _, item := range cm.colMeta {
		dst.Schema = append(dst.Schema, record.Field{
			Type: int(item.ty), Name: item.name,
		})
	}
	dst.ColVals = make([]record.ColVal, len(dst.Schema))

	_, err = f.ReadAt(cm, 0, dst, &ReadContext{coderCtx: &CoderContext{}})
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, dst.Times(), dst.Column(0).IntegerValues())
}

func TestCompactionPlan(t *testing.T) {
	tier := uint64(meta.Hot)
	store := NewTableStore("", &tier, false, NewConfig())

	type TestCase struct {
		name         string
		tsspFileName []string
		expGroups    [][]string
	}

	cases := []TestCase{
		TestCase{
			name: "compPlan1",
			tsspFileName: []string{
				"00000001-0000-00000000.tssp",
				"00000002-0000-00000000.tssp",
				"00000003-0000-00000000.tssp",
				"00000004-0000-00000000.tssp",
				"00000005-0000-00000000.tssp",
				"00000006-0000-00000000.tssp",
				"00000007-0000-00000000.tssp",
				"00000008-0000-00000000.tssp",
				"00000009-0002-00000000.tssp",
				"00000011-0002-00000000.tssp",
				"00000019-0002-00000000.tssp",
				"00000021-0001-00000000.tssp",
			},
			expGroups: [][]string{},
		},
		TestCase{
			name: "compPlan2",
			tsspFileName: []string{
				"00000000-0000-00000000.tssp",
				"00000001-0002-00000000.tssp",
				"00000009-0002-00000000.tssp",
				"00000011-0002-00000000.tssp",
				"00000019-0002-00000000.tssp",
				"00000021-0001-00000000.tssp",
			},
			expGroups: [][]string{
				[]string{
					"00000001-0002-00000000.tssp",
					"00000009-0002-00000000.tssp",
					"00000011-0002-00000000.tssp",
					"00000019-0002-00000000.tssp",
				},
			},
		},
		TestCase{
			name: "compPlan3",
			tsspFileName: []string{
				"00000001-0002-00000000.tssp",
				"00000009-0002-00000000.tssp",
				"00000011-0002-00000000.tssp",
				"00000019-0002-00000000.tssp",
			},
			expGroups: [][]string{
				[]string{
					"00000001-0002-00000000.tssp",
					"00000009-0002-00000000.tssp",
					"00000011-0002-00000000.tssp",
					"00000019-0002-00000000.tssp",
				},
			},
		},
		TestCase{
			name: "compPlan4",
			tsspFileName: []string{
				"00000001-0002-00000000.tssp",
				"00000001-0002-00000001.tssp",
				"00000001-0002-00000002.tssp",
				"00000021-0002-00000000.tssp",
				"00000021-0002-00000001.tssp",
				"00000041-0002-00000000.tssp",
				"00000049-0002-00000000.tssp",
				"00000051-0002-00000000.tssp",
			},
			expGroups: [][]string{},
		},

		TestCase{
			name: "compPlan5",
			tsspFileName: []string{
				"00000001-0002-00000000.tssp",
				"00000001-0002-00000001.tssp",
				"00000001-0002-00000002.tssp",
				"00000021-0002-00000000.tssp",
				"00000021-0002-00000001.tssp",
				"00000041-0002-00000000.tssp",
				"00000049-0002-00000000.tssp",
				"00000051-0002-00000000.tssp",
				"00000059-0002-00000000.tssp",
			},
			expGroups: [][]string{
				[]string{
					"00000041-0002-00000000.tssp",
					"00000049-0002-00000000.tssp",
					"00000051-0002-00000000.tssp",
					"00000059-0002-00000000.tssp",
				},
			},
		},

		TestCase{
			name: "compPlan6",
			tsspFileName: []string{
				"00000001-0002-00000000.tssp",
				"00000001-0002-00000001.tssp",
				"00000001-0002-00000002.tssp",
				"00000021-0002-00000000.tssp",
				"00000021-0002-00000001.tssp",
				"00000041-0002-00000000.tssp",
				"00000049-0002-00000000.tssp",
				"00000051-0002-00000000.tssp",
				"00000059-0002-00000000.tssp",
				"00000059-0002-00000001.tssp",
			},
			expGroups: [][]string{},
		},
		TestCase{
			name: "compPlan7",
			tsspFileName: []string{
				"00000001-0002-00000000.tssp",
				"00000001-0002-00000001.tssp",
				"00000001-0002-00000002.tssp",
				"00000021-0002-00000000.tssp",
				"00000021-0002-00000001.tssp",
				"00000041-0002-00000000.tssp",
				"00000049-0002-00000000.tssp",
				"00000051-0002-00000000.tssp",
			},
			expGroups: [][]string{},
		},
		TestCase{
			name: "compPlan7",
			tsspFileName: []string{
				"00000001-0002-00000000.tssp",
				"00000001-0002-00000001.tssp",
				"00000001-0002-00000002.tssp",
				"00000021-0002-00000000.tssp",
				"00000021-0002-00000001.tssp",
				"00000041-0002-00000000.tssp",
				"00000049-0002-00000000.tssp",
				"00000049-0002-00000001.tssp",
				"00000049-0002-00000002.tssp",
				"00000051-0002-00000000.tssp",
				"00000059-0002-00000000.tssp",
				"00000061-0002-00000000.tssp",
				"00000069-0002-00000000.tssp",
				"00000071-0002-00000000.tssp",
			},
			expGroups: [][]string{
				[]string{
					"00000051-0002-00000000.tssp",
					"00000059-0002-00000000.tssp",
					"00000061-0002-00000000.tssp",
					"00000069-0002-00000000.tssp",
				},
			},
		},

		TestCase{
			name: "compPlan8",
			tsspFileName: []string{
				"00000000-0000-00000000.tssp",
				"00000001-0002-00000000.tssp",
				"00000009-0002-00000000.tssp",
				"00000011-0002-00000000.tssp",
				"00000019-0002-00000000.tssp",
				"00000021-0002-00000000.tssp",
				"00000029-0002-00000000.tssp",
				"00000029-0002-00000001.tssp",
				"00000031-0002-00000000.tssp",
				"00000039-0002-00000000.tssp",
				"00000039-0002-00000001.tssp",
				"00000041-0002-00000000.tssp",
				"00000049-0002-00000000.tssp",
				"00000051-0002-00000000.tssp",
				"00000059-0002-00000000.tssp",
				"00000061-0002-00000000.tssp",
			},
			expGroups: [][]string{
				[]string{
					"00000001-0002-00000000.tssp",
					"00000009-0002-00000000.tssp",
					"00000011-0002-00000000.tssp",
					"00000019-0002-00000000.tssp",
				},
				[]string{
					"00000041-0002-00000000.tssp",
					"00000049-0002-00000000.tssp",
					"00000051-0002-00000000.tssp",
					"00000059-0002-00000000.tssp",
				},
			},
		},
	}

	for _, c := range cases {
		for _, fn := range c.tsspFileName {
			f := genTsspFile(fn)
			if f == nil {
				t.Fatalf("parse file name (%v) fail", fn)
			}

			store.addTSSPFile(true, f)
		}

		fs := store.tableFiles("mst", true)
		plans := store.mmsPlan("mst", fs, 2, LeveLMinGroupFiles[2], nil)
		if len(plans) != len(c.expGroups) {
			t.Fatalf("exp groups :%v, get:%v", len(c.expGroups), len(plans))
		}
		for i, group := range c.expGroups {
			if !reflect.DeepEqual(group, plans[i].group) {
				t.Fatalf("exp groups :%v, get:%v", c.expGroups, plans)
			}
			store.CompactDone(group)
		}

		delete(store.Order, "mst")
	}
}

func genTsspFile(name string) TSSPFile {
	var fn TSSPFileName
	if err := fn.ParseFileName(name); err != nil {
		return nil
	}
	mr := &mockTableReader{name: name}
	mr.FileNameFn = func() string { return mr.name }
	mr.NameFn = func() string { return "mst" }
	return &tsspFile{
		ref:    1,
		name:   fn,
		reader: mr,
	}
}

type mockTableReader struct {
	name          string
	OpenFn        func() error
	CloseFn       func() error
	ReadDataFn    func(cm *ChunkMeta, segment int, dst *record.Record, decs *ReadContext) (*record.Record, error)
	MetaIndexAtFn func(idx int) (*MetaIndex, error)
	MetaIndexFn   func(id uint64, tr record.TimeRange) (int, *MetaIndex, error)
	ChunkMetaFn   func(id uint64, offset int64, size, itemCount uint32, metaIdx int, dst *ChunkMeta) (*ChunkMeta, error)
	ChunkMetaAtFn func(index int) (*ChunkMeta, error)

	ReadMetaBlockFn     func(metaIdx int, id uint64, offset int64, size uint32, count uint32, dst *[]byte) ([]byte, error)
	ReadDataBlockFn     func(offset int64, size uint32, dst *[]byte) ([]byte, error)
	ReadFn              func(offset int64, size uint32, dst *[]byte) ([]byte, error)
	ReadChunkMetaDataFn func(metaIdx int, m *MetaIndex, dst []ChunkMeta) ([]ChunkMeta, error)
	BlockHeaderFn       func(meta *ChunkMeta, dst []record.Field) ([]record.Field, error)

	StatFn             func() *Trailer
	MinMaxSeriesIDFn   func() (min, max uint64, err error)
	MinMaxTimeFn       func() (min, max int64, err error)
	ContainsFn         func(id uint64, tm record.TimeRange) bool
	ContainsTimeFn     func(tm record.TimeRange) bool
	ContainsIdFn       func(id uint64) bool
	CreateTimeFn       func() int64
	NameFn             func() string
	FileNameFn         func() string
	RenameFn           func(newName string) error
	FileSizeFn         func() int64
	InMemSizeFn        func() int64
	VersionFn          func() uint64
	FreeMemoryFn       func() int64
	LoadIntoMemoryFn   func() error
	LoadIndexFn        func() error
	AverageChunkRowsFn func() int
	MaxChunkRowsFn     func() int
}

func (r *mockTableReader) Open() error  { return r.OpenFn() }
func (r *mockTableReader) Close() error { return r.CloseFn() }
func (r *mockTableReader) ReadData(cm *ChunkMeta, segment int, dst *record.Record, decs *ReadContext) (*record.Record, error) {
	return r.ReadDataFn(cm, segment, dst, decs)
}
func (r *mockTableReader) MetaIndexAt(idx int) (*MetaIndex, error) { return r.MetaIndexAtFn(idx) }
func (r *mockTableReader) MetaIndex(id uint64, tr record.TimeRange) (int, *MetaIndex, error) {
	return r.MetaIndexFn(id, tr)
}
func (r *mockTableReader) ChunkMeta(id uint64, offset int64, size, itemCount uint32, metaIdx int, dst *ChunkMeta) (*ChunkMeta, error) {
	return r.ChunkMetaFn(id, offset, size, itemCount, metaIdx, dst)
}
func (r *mockTableReader) ChunkMetaAt(index int) (*ChunkMeta, error) { return r.ChunkMetaAtFn(index) }

func (r *mockTableReader) ReadMetaBlock(metaIdx int, id uint64, offset int64, size uint32, count uint32, dst *[]byte) ([]byte, error) {
	return r.ReadMetaBlockFn(metaIdx, id, offset, size, count, dst)
}
func (r *mockTableReader) ReadDataBlock(offset int64, size uint32, dst *[]byte) ([]byte, error) {
	return r.ReadDataBlockFn(offset, size, dst)
}
func (r *mockTableReader) Read(offset int64, size uint32, dst *[]byte) ([]byte, error) {
	return r.ReadFn(offset, size, dst)
}
func (r *mockTableReader) ReadChunkMetaData(metaIdx int, m *MetaIndex, dst []ChunkMeta) ([]ChunkMeta, error) {
	return r.ReadChunkMetaDataFn(metaIdx, m, dst)
}
func (r *mockTableReader) BlockHeader(meta *ChunkMeta, dst []record.Field) ([]record.Field, error) {
	return r.BlockHeaderFn(meta, dst)
}

func (r *mockTableReader) Stat() *Trailer                               { return r.StatFn() }
func (r *mockTableReader) MinMaxSeriesID() (min, max uint64, err error) { return r.MinMaxSeriesIDFn() }
func (r *mockTableReader) MinMaxTime() (min, max int64, err error)      { return r.MinMaxTimeFn() }
func (r *mockTableReader) Contains(id uint64, tm record.TimeRange) bool { return r.ContainsFn(id, tm) }
func (r *mockTableReader) ContainsTime(tm record.TimeRange) bool        { return r.ContainsTimeFn(tm) }
func (r *mockTableReader) ContainsId(id uint64) bool                    { return r.ContainsIdFn(id) }
func (r *mockTableReader) CreateTime() int64                            { return r.CreateTimeFn() }
func (r *mockTableReader) Name() string                                 { return r.NameFn() }
func (r *mockTableReader) FileName() string                             { return r.FileNameFn() }
func (r *mockTableReader) Rename(newName string) error                  { return r.RenameFn(newName) }
func (r *mockTableReader) FileSize() int64                              { return r.FileSizeFn() }
func (r *mockTableReader) InMemSize() int64                             { return r.InMemSizeFn() }
func (r *mockTableReader) Version() uint64                              { return r.VersionFn() }
func (r *mockTableReader) FreeMemory() int64                            { return r.FreeMemoryFn() }
func (r *mockTableReader) LoadIntoMemory() error                        { return r.LoadIntoMemoryFn() }
func (r *mockTableReader) LoadIndex() error                             { return r.LoadIndexFn() }
func (r *mockTableReader) AverageChunkRows() int                        { return r.AverageChunkRowsFn() }
func (r *mockTableReader) MaxChunkRows() int                            { return r.MaxChunkRowsFn() }
