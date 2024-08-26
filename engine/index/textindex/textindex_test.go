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

package textindex

import (
	"fmt"
	"testing"

	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

func NewRecordForTextIndex() *record.Record {
	rec := record.NewRecord([]record.Field{{Name: "_log", Type: influx.Field_Type_String}, {Name: record.TimeField, Type: influx.Field_Type_Int}}, false)
	rawData := []string{
		"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/534.24 (KHTML, like Gecko) Chrome/11.0.696.12 Safari/534.24",
		"this is a 中文日志",
		"client ip is 10.20.30.15",
	}
	for i := 0; i < len(rawData); i++ {
		rec.ColVals[0].AppendString(rawData[i])
		rec.ColVals[1].AppendInteger(int64(i))
	}
	return rec
}

func TestCreateTextIndex(t *testing.T) {
	dir := t.TempDir()
	defer fileops.RemoveAll(dir)
	lockPath := "lock"
	repeat := 5
	for i := 0; i < repeat; i++ {
		fileName := fmt.Sprintf("00000000-00000000-0000000%d", i)
		indexWriter := NewTextIndexWriter(dir, "", fileName, lockPath, " ？‘;.<>{}[],")
		if indexWriter == nil {
			t.Fatal("NewTextIndexWriter failed")
		}

		if err := indexWriter.Open(); err != nil {
			t.Fatalf("TextIndexWriter open Failed, err:%+v", err)
		}

		rec := NewRecordForTextIndex()
		if err := indexWriter.CreateAttachIndex(rec, []int{0}, []int{0}); err != nil {
			t.Fatalf("TextIndexWriter CreateAttachIndex Failed, err:%+v", err)
		}

		if err := indexWriter.Close(); err != nil {
			t.Fatalf("TextIndexWriter close Failed, err:%+v", err)
		}
	}
}

func TestGrowPackedBuf(t *testing.T) {
	bd := NewBlockData()
	keyDstLen := dataBufSize + 1024
	dataDstLen := dataBufSize + 2048
	bd.GrowPackedBuf(keyDstLen, dataDstLen)
	if cap(bd.PackedKeys) < keyDstLen {
		t.Fatal("PackedKeys cap grow failed")
	}
	if cap(bd.PackedData) < dataDstLen {
		t.Fatal("PackedData cap grow failed")
	}
}

func NewBigRecordForTextIndex() *record.Record {
	rec := record.NewRecord([]record.Field{{Name: "_log", Type: influx.Field_Type_String}, {Name: record.TimeField, Type: influx.Field_Type_Int}}, false)
	rawData := []string{
		"MozillaMozilla WindowsWindows WindowsNT Windows6 WindowsWOW64 AppleWebKitAppleWebKitAppleWebKit ChromeSafariChromeSafariChromeSafari",
		"MozillaMozillaWindowsWOW64 WindowsNT Windows6WindowsWindows WindowsWOW64 AppleWebKitAppleWebKitAppleWebKit ChromeSafariChromeSafariChromeSafari",
		"MozillaMozillaWindowsWindowsWindowsNTWindows6WindowsWOW64AppleWebKitAppleWebKitAppleWebKitChromeSafariChromeSafariChromeSafari",
	}
	for i := 0; i < len(rawData); i++ {
		rec.ColVals[0].AppendString(rawData[i])
		rec.ColVals[1].AppendInteger(int64(i))
	}
	return rec
}

func TestBigRecordForTextIndex(t *testing.T) {
	dir := t.TempDir()
	defer fileops.RemoveAll(dir)
	lockPath := "lock"
	indexWriter := NewTextIndexWriter(dir, "", "00000000-00000000-0000000", lockPath, " ？‘;.<>{}[],")
	if indexWriter == nil {
		t.Fatal("NewTextIndexWriter failed")
	}

	if err := indexWriter.Open(); err != nil {
		t.Fatalf("TextIndexWriter open Failed, err:%+v", err)
	}

	rec := NewBigRecordForTextIndex()
	if err := indexWriter.CreateAttachIndex(rec, []int{0}, []int{0}); err != nil {
		t.Fatalf("TextIndexWriter CreateAttachIndex Failed, err:%+v", err)
	}

	if err := indexWriter.Close(); err != nil {
		t.Fatalf("TextIndexWriter close Failed, err:%+v", err)
	}
}

func NewRecordSplitForTextIndex() (*record.Record, []int) {
	rec := record.NewRecord([]record.Field{{Name: "_log", Type: influx.Field_Type_String}, {Name: record.TimeField, Type: influx.Field_Type_Int}}, false)
	rawData := []string{
		"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/534.24 (KHTML, like Gecko) Chrome/11.0.696.12 Safari/534.22",
		"Mozilla/5.1 (Windows NT 6.2; WOW64) AppleWebKit/534.25 (KHTML, like Gecko) Chrome/11.1.696.12 Safari/534.23",
		"Mozilla/5.2 (Windows NT 6.3; WOW64) AppleWebKit/534.26 (KHTML, like Gecko) Chrome/11.2.696.12 Safari/534.24",
		"Mozilla/5.3 (Windows NT 6.4; WOW64) AppleWebKit/534.27 (KHTML, like Gecko) Chrome/11.3.696.12 Safari/534.25",
		"Mozilla/5.4 (Windows NT 6.5; WOW64) AppleWebKit/534.28 (KHTML, like Gecko) Chrome/11.4.696.12 Safari/534.26",
	}
	totalRows := 16500
	rowsInSeg := 512
	rowsPerSegment := make([]int, 0)

	for i := 0; i < totalRows; i++ {
		if i != 0 && i%rowsInSeg == 0 {
			rowsPerSegment = append(rowsPerSegment, i)
		}
		rec.ColVals[0].AppendString(rawData[i%len(rawData)])
		rec.ColVals[1].AppendInteger(int64(i))
	}
	rowsPerSegment = append(rowsPerSegment, totalRows-1)
	return rec, rowsPerSegment
}

func TestRecordSplitForTextIndex(t *testing.T) {
	dir := t.TempDir()
	defer fileops.RemoveAll(dir)
	lockPath := "lock"
	indexWriter := NewTextIndexWriter(dir, "", "00000000-00000000-0000001", lockPath, " ？‘;.<>{}[],")
	if indexWriter == nil {
		t.Fatal("NewTextIndexWriter failed")
	}

	if err := indexWriter.Open(); err != nil {
		t.Fatalf("TextIndexWriter open Failed, err:%+v", err)
	}
	rec, rowsPerSegment := NewRecordSplitForTextIndex()
	if err := indexWriter.CreateAttachIndex(rec, []int{0}, rowsPerSegment); err != nil {
		t.Fatalf("TextIndexWriter CreateAttachIndex Failed, err:%+v", err)
	}

	if err := indexWriter.Close(); err != nil {
		t.Fatalf("TextIndexWriter close Failed, err:%+v", err)
	}
}
