//go:build linux || amd64

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

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/influxdata/influxdb/pkg/testing/assert"
	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/index"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/rpn"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"go.uber.org/zap"
)

func TestContainerHead(t *testing.T) {
	src := make([]byte, 0, 5)
	var head ContainerHead
	src = append(src, 1)                  // type
	src = encoding.MarshalUint16(src, 2)  // key
	src = encoding.MarshalUint16(src, 16) // cardinality

	_, err := head.Unmarshal(src[:3])
	assert.NotEqual(t, err, nil)
	_, err = head.Unmarshal(src)
	assert.Equal(t, err, nil)
	assert.Equal(t, head.ContainerType, uint8(1))
	assert.Equal(t, head.ContainerKey, uint16(2))
	assert.Equal(t, head.Cardinality, uint32(16+1))
}

func GenBitsetContainerRowsId(rowIds []uint32, key uint32) []uint32 {
	base := key * 65536
	for i := 0; i < 65536; i++ {
		if i%5 == 0 {
			continue
		}
		rowIds = append(rowIds, uint32(i)+base)
	}
	return rowIds
}

func GenArrayContainerRowsId(rowIds []uint32, key uint32) []uint32 {
	base := key * 65536
	for i := 0; i < 600; i++ {
		if i%2 == 0 {
			continue
		}
		rowIds = append(rowIds, uint32(i)+base)
	}
	return rowIds
}

func GenRunContainerRowsId(rowIds []uint32, key uint32) []uint32 {
	base := key * 65536
	for i := 0; i < 300; i++ {
		if i%100 == 0 {
			continue
		}
		rowIds = append(rowIds, uint32(i)+base)
	}
	return rowIds
}

func TestContainerUnmarshal(t *testing.T) {
	memEle := GetMemElement(16)
	if memEle == nil {
		t.Error("GetMemElement failed")
	}
	keys := []byte("test")
	rowIds := make([]uint32, 0, 65536)
	// add bitset container
	rowIds = GenBitsetContainerRowsId(rowIds, 0)
	// add array container
	rowIds = GenArrayContainerRowsId(rowIds, 1)
	// add run container
	rowIds = GenRunContainerRowsId(rowIds, 2)
	for i := 0; i < len(rowIds); i++ {
		AddPostingToMem(memEle, keys, rowIds[i])
	}
	// marshal
	data := NewBlockData()
	ph := NewBlockHeader()
	next := RetrievePostingList(memEle, data, ph)
	if next != false {
		t.Error("retrieve posting list failed")
	}
	// containers0
	containers0, err := UnmarshalContainer(data.Data)
	if err != nil {
		t.Errorf("unmarshal container failed, error: %+v", err)
	}
	containers1, err := UnmarshalContainer(data.Data)
	if err != nil {
		t.Errorf("unmarshal container failed, error: %+v", err)
	}

	container := IntersectContainers(containers0, containers1)
	newRowIds := make([]uint32, 0, 65536)
	for i := 0; i < len(container); i++ {
		tmpRowsId := container[i].Serialize()
		newRowIds = append(newRowIds, tmpRowsId...)
	}
	if len(newRowIds) != len(rowIds) {
		t.Errorf("posting list mismatch, old=%d, now=%d", len(rowIds), len(newRowIds))
	}
	for i := 0; i < len(rowIds); i++ {
		if newRowIds[i] != rowIds[i] {
			t.Errorf("posting list mismatch, id=%d, old=%d, new=%d", i, newRowIds[i], rowIds[i])
		}
	}
}

func TransRowidsToContainer(keys []byte, rowIds []uint32) ([]Container, error) {
	memEle := GetMemElement(16)
	if memEle == nil {
		return nil, fmt.Errorf("GetMemElement failed")
	}
	defer PutInvertMemElement(memEle)
	for i := 0; i < len(rowIds); i++ {
		AddPostingToMem(memEle, keys, rowIds[i])
	}
	// marshal
	data := NewBlockData()
	ph := NewBlockHeader()
	next := RetrievePostingList(memEle, data, ph)
	if next != false {
		return nil, fmt.Errorf("retrieve posting list failed")
	}
	// containers0
	containers, err := UnmarshalContainer(data.Data)
	if err != nil {
		return nil, fmt.Errorf("unmarshal container failed, error: %+v", err)
	}
	return containers, nil
}

func IntersectRowIds(rowIds0, rowIds1 []uint32) []uint32 {
	rowIds := make([]uint32, 0)
	var i, j int
	for (i < len(rowIds0)) && (j < len(rowIds1)) {
		if rowIds0[i] < rowIds1[j] {
			i++
		} else if rowIds0[i] > rowIds1[j] {
			j++
		} else {
			rowIds = append(rowIds, rowIds0[i])
			i++
			j++
		}
	}
	return rowIds
}

func TestContainerIntersect(t *testing.T) {
	memEle := GetMemElement(16)
	if memEle == nil {
		t.Error("GetMemElement failed")
	}
	defer PutInvertMemElement(memEle)
	keys := []byte("test")
	rowIds0 := make([]uint32, 0, 65536)
	rowIds0 = GenBitsetContainerRowsId(rowIds0, 1)
	rowIds0 = GenRunContainerRowsId(rowIds0, 2)
	rowIds0 = GenArrayContainerRowsId(rowIds0, 5)
	rowIds0 = GenRunContainerRowsId(rowIds0, 6)
	rowIds0 = GenRunContainerRowsId(rowIds0, 9)
	containers0, err := TransRowidsToContainer(keys, rowIds0)
	if err != nil {
		t.Error(err)
	}
	rowIds1 := make([]uint32, 0, 65536)
	rowIds1 = GenArrayContainerRowsId(rowIds1, 1)
	rowIds1 = GenRunContainerRowsId(rowIds1, 3)
	rowIds1 = GenRunContainerRowsId(rowIds1, 5)
	rowIds1 = GenRunContainerRowsId(rowIds1, 7)
	rowIds1 = GenBitsetContainerRowsId(rowIds1, 9)
	containers1, err := TransRowidsToContainer(keys, rowIds1)
	if err != nil {
		t.Error(err)
	}
	rowIds2 := make([]uint32, 0, 65536)
	rowIds2 = GenRunContainerRowsId(rowIds2, 1)
	rowIds2 = GenRunContainerRowsId(rowIds2, 4)
	rowIds2 = GenBitsetContainerRowsId(rowIds2, 5)
	rowIds2 = GenRunContainerRowsId(rowIds2, 8)
	rowIds2 = GenArrayContainerRowsId(rowIds2, 9)
	containers2, err := TransRowidsToContainer(keys, rowIds2)
	if err != nil {
		t.Error(err)
	}
	// intersect rowIds
	rowIds := IntersectRowIds(rowIds0, rowIds1)
	rowIds = IntersectRowIds(rowIds2, rowIds)
	// intersect containers
	container := IntersectContainers(containers0, containers1)
	container = IntersectContainers(containers2, container)
	newRowIds := make([]uint32, 0, 65536)
	for i := 0; i < len(container); i++ {
		tmpRowsId := container[i].Serialize()
		newRowIds = append(newRowIds, tmpRowsId...)
	}
	if len(newRowIds) != len(rowIds) {
		t.Errorf("posting list mismatch, old=%d, now=%d", len(rowIds), len(newRowIds))
	}
	for i := 0; i < len(rowIds); i++ {
		if newRowIds[i] != rowIds[i] {
			t.Errorf("posting list mismatch, id=%d, old=%d, new=%d", i, newRowIds[i], rowIds[i])
		}
	}
}

type MockTssp struct {
	path string
}

func (f *MockTssp) Path() string {
	return f.path
}

func (f *MockTssp) Name() string {
	return ""
}

func NewRecordForTextIndexReader() (*record.Record, []int) {
	rec := record.NewRecord([]record.Field{
		{Name: "content", Type: influx.Field_Type_String},
		{Name: record.TimeField, Type: influx.Field_Type_Int},
	}, false)
	rawData := []string{
		"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/534.24 (KHTML, like Gecko) Chrome/11.0.696.12 Safari/534.22",
		"n/5.1 (MacOs Mojave 10.13; WOW64) HuaweiWebKit/534.25 (KHTML, like Gecko) Chrome/11.1.696.12 Safari/534.23",
		"Mozilla/5.2 (Windows NT 6.3; WOW64) AppleWebKit/534.26 (KHTML, like Gecko) Chrome/11.2.696.12 Safari/534.24",
		"Mozilla/5.3 (MacOs Mojave 10.13; WOW64) HuaweiWebKit/534.27 (KHTML, like Gecko) Chrome/11.3.696.12 Safari/534.25",
		"Mozilla/5.4 (Linux 7.6; WOW64) AppleWebKit/534.28 (KHTML, like Gecko) Chrome/11.4.696.12 Safari/534.26",
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

func ReFileName(fileName string, fieldName string) error {
	lock := fileops.FileLockOption("")
	for j := 0; j < colstore.TextIndexMax; j++ {
		newName := colstore.AppendSecondaryIndexSuffix(fileName, fieldName, index.Text, j)
		oldName := newName + tmpFileSuffix
		if err := fileops.RenameFile(oldName, newName, lock); err != nil {
			err = errno.NewError(errno.RenameFileFailed, zap.String("old", oldName), zap.String("new", newName), err)
			return err
		}
	}
	return nil
}

func TestCreateTextIndexFilterReaders(t *testing.T) {
	field := "content"
	schema := record.Schemas{{Name: field, Type: influx.Field_Type_String}}
	ir := &influxql.IndexRelation{
		IndexNames: []string{index.TextIndex},
		Oids:       []uint32{uint32(index.Text)},
		IndexList:  []*influxql.IndexList{&influxql.IndexList{IList: []string{field}}},
		IndexOptions: []*influxql.IndexOptions{
			&influxql.IndexOptions{Options: []*influxql.IndexOption{
				&influxql.IndexOption{Tokens: " /?';.<>{}[],"},
			}}},
	}
	reader := NewTextIndexFilterReaders("/tmp", "00000000-00000000-0000001.tssp", schema, ir)
	assert.NotEqual(t, reader, nil)
	// elem is invalid
	_, err := reader.IsExist(0, nil)
	assert.NotEqual(t, err, nil)
	// elem.Key is invalid
	rpnExpr := &rpn.SKRPNElement{
		RPNOp: rpn.MATCHPHRASE,
		Key:   "field",
		Value: 100,
	}
	_, err = reader.IsExist(0, rpnExpr)
	assert.NotEqual(t, err, nil)
	// filterReader open failed
	rpnExpr.Key = field
	_, err = reader.IsExist(0, rpnExpr)
	assert.NotEqual(t, err, nil)
	// elem.Value is invalid
	reader.readers[0] = &TextIndexFilterReader{}
	_, err = reader.IsExist(0, rpnExpr)
	assert.NotEqual(t, err, nil)
	err = reader.Close()
	assert.Equal(t, err, nil)
}

func TestCreateTextIndexReader(t *testing.T) {
	option := &query.ProcessorOptions{
		Condition: &influxql.BinaryExpr{
			Op:  influxql.MATCHPHRASE,
			LHS: &influxql.VarRef{Val: "content"},
			RHS: &influxql.StringLiteral{Val: "AppleWebKit"},
		}}
	schema := record.Schemas{{Name: "content", Type: influx.Field_Type_String}}
	rpnExpr := rpn.ConvertToRPNExpr(option.GetCondition())
	reader, err := NewTextIndexReader(rpnExpr, schema, option, false)
	assert.Equal(t, err, nil)
	err = reader.ReInit("index.pos")
	assert.NotEqual(t, err, nil)
	err = reader.Close()
	assert.Equal(t, err, nil)
}

//skip
/*
func TestTextIndexReader(t *testing.T) {
	tmpDir := t.TempDir()
	//defer fileops.RemoveAll(tmpDir)
	os.Mkdir(tmpDir+"/logmst", os.ModeDir)

	// construct test data
	field := "content"
	tokens := " /?';.<>{}[],"
	indexWriter := NewTextIndexWriter(tmpDir, "logmst", "00000000-00000000-0000001", "lock", tokens)
	if indexWriter == nil {
		t.Fatal("NewTextIndexWriter failed")
	}
	rec, rowsPerSegment := NewRecordForTextIndexReader()
	if err := indexWriter.CreateAttachIndex(rec, []int{0}, rowsPerSegment); err != nil {
		t.Fatalf("TextIndexWriter CreateAttachIndex Failed, err:%+v", err)
	}
	if err := indexWriter.Close(); err != nil {
		t.Fatalf("TextIndexWriter close Failed, err:%+v", err)
	}
	ReFileName(tmpDir+"/logmst/00000000-00000000-0000001", field)
	dataFile := &MockTssp{path: tmpDir + "/logmst/00000000-00000000-0000001.tssp"}
	// construct the reader
	schema := record.Schemas{{Name: field, Type: influx.Field_Type_String}}
	option := &query.ProcessorOptions{
		Condition: &influxql.BinaryExpr{
			Op:  influxql.MATCHPHRASE,
			LHS: &influxql.VarRef{Val: field},
			RHS: &influxql.StringLiteral{Val: "AppleWebKit"},
		},
		Sources: []influxql.Source{
			&influxql.Measurement{
				Name: "logmst",
				IndexRelation: &influxql.IndexRelation{
					IndexNames: []string{index.TextIndex},
					Oids:       []uint32{uint32(index.Text)},
					IndexList:  []*influxql.IndexList{&influxql.IndexList{IList: []string{"content"}}},
					IndexOptions: []*influxql.IndexOptions{
						&influxql.IndexOptions{Options: []*influxql.IndexOption{
							&influxql.IndexOption{Tokens: tokens},
						}}},
				},
			},
		},
	}
	rpnExpr := rpn.ConvertToRPNExpr(option.GetCondition())
	reader, err := NewTextIndexReader(rpnExpr, schema, option, false)
	assert.Equal(t, err, nil)
	err = reader.ReInit(dataFile)
	assert.Equal(t, err, nil)
	ok, err := reader.MayBeInFragment(0)
	assert.Equal(t, err, nil)
	assert.Equal(t, ok, true)
	reader.Close()

	// test hit 0
	option.Condition = &influxql.BinaryExpr{
		Op:  influxql.MATCHPHRASE,
		LHS: &influxql.VarRef{Val: field},
		RHS: &influxql.StringLiteral{Val: "Mozilla/6"},
	}
	rpnExpr = rpn.ConvertToRPNExpr(option.GetCondition())
	reader, err = NewTextIndexReader(rpnExpr, schema, option, false)
	assert.Equal(t, err, nil)
	err = reader.ReInit(dataFile)
	assert.Equal(t, err, nil)
	ok, err = reader.MayBeInFragment(0)
	assert.Equal(t, err, nil)
	assert.Equal(t, ok, true)
	ok, err = reader.MayBeInFragment(1)
	assert.Equal(t, err, nil)
	assert.Equal(t, ok, true)
	ok, err = reader.MayBeInFragment(10000)
	assert.Equal(t, err, nil)
	assert.Equal(t, ok, false)

	// test miss 0
	option.Condition = &influxql.BinaryExpr{
		Op:  influxql.MATCHPHRASE,
		LHS: &influxql.VarRef{Val: field},
		RHS: &influxql.StringLiteral{Val: "Mozilla/Unix"},
	}
	rpnExpr = rpn.ConvertToRPNExpr(option.GetCondition())
	reader, err = NewTextIndexReader(rpnExpr, schema, option, false)
	assert.Equal(t, err, nil)
	err = reader.ReInit(dataFile)
	assert.Equal(t, err, nil)
	ok, err = reader.MayBeInFragment(0)
	assert.Equal(t, err, nil)
	assert.Equal(t, ok, false)

	// test miss 1
	option.Condition = &influxql.BinaryExpr{
		Op:  influxql.MATCHPHRASE,
		LHS: &influxql.VarRef{Val: field},
		RHS: &influxql.StringLiteral{Val: "/?';.<>{}[]"},
	}
	rpnExpr = rpn.ConvertToRPNExpr(option.GetCondition())
	reader, err = NewTextIndexReader(rpnExpr, schema, option, false)
	assert.Equal(t, err, nil)
	err = reader.ReInit(dataFile)
	assert.Equal(t, err, nil)
	ok, err = reader.MayBeInFragment(0)
	assert.Equal(t, err, nil)
	assert.Equal(t, ok, false)

	// test miss 2
	option.Condition = &influxql.BinaryExpr{
		Op:  influxql.MATCHPHRASE,
		LHS: &influxql.VarRef{Val: field},
		RHS: &influxql.StringLiteral{Val: "Zoo"},
	}
	rpnExpr = rpn.ConvertToRPNExpr(option.GetCondition())
	reader, err = NewTextIndexReader(rpnExpr, schema, option, false)
	assert.Equal(t, err, nil)
	err = reader.ReInit(dataFile)
	assert.Equal(t, err, nil)
	ok, err = reader.MayBeInFragment(0)
	assert.Equal(t, err, nil)
	assert.Equal(t, ok, false)

	reader.Close()
}
*/
