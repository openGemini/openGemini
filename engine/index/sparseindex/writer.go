/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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

package sparseindex

import (
	"errors"
	"fmt"

	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/lib/fragment"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
)

var (
	// InitIndexFragmentFixedSize means that each fragment is fixed in size except the last fragment.
	InitIndexFragmentFixedSize = true
)

type IndexWriter interface {
	CreatePrimaryIndex(
		srcRec *record.Record,
		pkSchema record.Schemas,
		rowsNumPerFragment int,
		tcLocation int8,
	) (
		*record.Record, fragment.IndexFragment, error,
	)
	Close() error
}

type IndexWriterImpl struct {
}

func NewIndexWriter() *IndexWriterImpl {
	return &IndexWriterImpl{}
}

// CretePrimaryIndex generates sparse primary index based on sorted data to be flushed to disks.
func (w *IndexWriterImpl) CreatePrimaryIndex(
	srcRec *record.Record,
	pkSchema record.Schemas,
	rowsNumPerFragment int,
	tcLocation int8,
) (
	*record.Record,
	fragment.IndexFragment,
	error,
) {
	rowNum := srcRec.RowNums()
	numFragment, remainFragment := rowNum/rowsNumPerFragment, rowNum%rowsNumPerFragment
	if remainFragment > 0 {
		numFragment += 1
	}

	dstRec, err := w.createPrimaryIndexData(srcRec, pkSchema, rowsNumPerFragment, numFragment, tcLocation)
	if err != nil {
		return nil, nil, err
	}

	indexFragment := w.createPrimaryIndexFragment(rowsNumPerFragment, numFragment, remainFragment)
	return dstRec, indexFragment, nil
}

// createPrimaryIndexRecord generates sparse primary index data based on sorted data to be flushed to disks.
func (w *IndexWriterImpl) createPrimaryIndexData(
	srcRec *record.Record,
	pkSchema record.Schemas,
	rowsNumPerFragment int,
	numFragment int,
	tcLocation int8,
) (
	*record.Record,
	error,
) {
	if tcLocation > colstore.DefaultTCLocation {
		field := record.Field{Type: influx.Field_Type_Int, Name: record.TimeClusterCol}
		pkSchema = append([]record.Field{field}, pkSchema...)
	}
	dstRec := record.NewRecord(pkSchema, false)
	for i := 0; i < pkSchema.Len(); i++ {
		if idx := srcRec.Schema.FieldIndex(pkSchema.Field(i).Name); idx >= 0 {
			switch pkSchema.Field(i).Type {
			case influx.Field_Type_String, influx.Field_Type_Tag:
				w.generatePrimaryIndexData(srcRec, dstRec, rowsNumPerFragment, numFragment, pkSchema.Field(i).Type, idx, i)
			case influx.Field_Type_Int:
				w.generatePrimaryIndexData(srcRec, dstRec, rowsNumPerFragment, numFragment, influx.Field_Type_Int, idx, i)
			case influx.Field_Type_Float:
				w.generatePrimaryIndexData(srcRec, dstRec, rowsNumPerFragment, numFragment, influx.Field_Type_Float, idx, i)
			case influx.Field_Type_Boolean:
				w.generatePrimaryIndexData(srcRec, dstRec, rowsNumPerFragment, numFragment, influx.Field_Type_Boolean, idx, i)
			default:
				return nil, errors.New("unsupported data type")
			}
		} else {
			return nil, fmt.Errorf("the table does not have a primary key field, %s", pkSchema.Field(i).Name)
		}
	}
	return dstRec, nil
}

func (w *IndexWriterImpl) generatePrimaryIndexData(
	srcRec *record.Record,
	dstRec *record.Record,
	rowsNumPerFragment int,
	numFragment int,
	dataType int,
	srcColIdx int,
	dstColIdx int,
) {
	for j := 0; j < numFragment; j++ {
		dstRec.ColVals[dstColIdx].AppendColVal(&srcRec.ColVals[srcColIdx], dataType, j*rowsNumPerFragment, j*rowsNumPerFragment+1)
	}
	// The last row of data is added to the index by default that is used to determine the infinite boundary of the last fragment.
	rowNum := srcRec.RowNums()
	dstRec.ColVals[dstColIdx].AppendColVal(&srcRec.ColVals[srcColIdx], dataType, rowNum-1, rowNum)
}

// createPrimaryIndexFragment generates sparse primary index fragment based on sorted data to be flushed to disks.
func (w *IndexWriterImpl) createPrimaryIndexFragment(
	rowsNumPerFragment int,
	numFragment int,
	remainFragment int,
) fragment.IndexFragment {
	if InitIndexFragmentFixedSize {
		return fragment.NewIndexFragmentFixedSize(uint32(numFragment), uint64(rowsNumPerFragment))
	}

	accumulateRowCount := make([]uint64, 0, numFragment)
	for i := 1; i <= numFragment; i++ {
		accumulateRowCount = append(accumulateRowCount, uint64(i*rowsNumPerFragment))
	}
	if remainFragment > 0 {
		accumulateRowCount = append(accumulateRowCount, accumulateRowCount[len(accumulateRowCount)-1]+uint64(remainFragment))
	}
	return fragment.NewIndexFragmentVariable(accumulateRowCount)
}

func (w *IndexWriterImpl) Close() error {
	return nil
}
