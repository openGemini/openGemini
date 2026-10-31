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

package sparseindex

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/index"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/rpn"
	"github.com/openGemini/openGemini/lib/tokenizer"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

var fieldMetadataCache = expirable.NewLRU[string, map[string]FieldMetadata](1000, nil, 5*time.Minute)

const DefaultFalseRate = 0.001
const StartOffset = 0
const StartSegmentId = -1
const BFMetadataStart = "BFMD"
const WriteBufferSize = 1024 * 1024

var _ = RegistrySKFileReaderCreator(uint32(index.BloomFilterUniversal), &UniversalBFCreator{})

var fileId atomic.Int64

type UniversalBFCreator struct {
}

func (creator *UniversalBFCreator) CreateSKFileReader(rpnExpr *rpn.RPNExpr, schema record.Schemas, option hybridqp.Options, isCache bool) (SKFileReader, error) {
	return NewUniversalBloomFilterReader(rpnExpr, schema, option)
}

type UniversalBloomFilter struct {
	Filter *bloom.BloomFilter
}

// FieldMetadata stores the offset and length information for each field's bloom filter data
type FieldMetadata struct {
	Offset uint64 // Starting position of this field's data in the merged file
	Length uint64 // Total length of this field's bloom filter data
	EndPos uint64 // Ending position of this field's data in the merged file
}

// UniversalBloomFilterWriter implements stream-based bloom filter writing to avoid OOM
type UniversalBloomFilterWriter struct {
	*skipIndexWriter
	falseRate      float64
	mu             sync.RWMutex
	fieldWriters   map[string]*bufio.Writer // Buffered writers for each field
	fieldTempFiles map[string]fileops.File  // Temporary files for each field
	fieldOffsets   map[string]FieldMetadata // Track field positions for final merge
	indexFilePath  string
	bf             *UniversalBloomFilter
}

func (idx *UniversalBloomFilterWriter) IndexFilePath() string {
	return idx.indexFilePath
}

func (idx *UniversalBloomFilterWriter) SetIndexFilePath(indexFilePath string) {
	idx.indexFilePath = indexFilePath
}

func (idx *UniversalBloomFilterWriter) CreateAttachIndex(writeRec *record.Record, schemaIdx, rowsPerSegment []int) error {
	for _, id := range schemaIdx {
		field := writeRec.Schema[id]
		if err := idx.BuildUniversalBloomFilter(writeRec, id, field.Name); err != nil {
			return err
		}
	}
	return nil
}

func (idx *UniversalBloomFilterWriter) CreateDetachIndex(writeRec *record.Record, schemaIdx, rowsPerSegment []int, dataBuf [][]byte) ([][]byte, []string) {
	return nil, nil
}

// Flush merges all field bloom filter files into a single indexed file with metadata
func (idx *UniversalBloomFilterWriter) Flush() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Flush all field writers but don't close the underlying files yet
	if err := idx.closeFieldWriters(); err != nil {
		return fmt.Errorf("failed to flush field writers: %v", err)
	}

	// Merge all field files into the final index file
	if err := idx.mergeFieldFiles(); err != nil {
		return fmt.Errorf("failed to merge field files: %v", err)
	}

	// Clean up temporary files
	if err := idx.cleanupTempFiles(); err != nil {
		return fmt.Errorf("failed to remove temp files: %v", err)
	}

	return nil
}

// mergeFieldFiles merges all field temporary files into the final index file using streaming approach
func (idx *UniversalBloomFilterWriter) mergeFieldFiles() error {
	// Open the final index file
	file, err := fileops.Create(idx.indexFilePath)
	if err != nil {
		return err
	}
	defer file.Close()

	// Create buffered writer for the final file
	finalWriter := bufio.NewWriterSize(file, WriteBufferSize)
	defer finalWriter.Flush()

	// Track current offset for field metadata
	currentOffset := uint64(0)

	// Process each field file in streaming fashion
	for fieldName, oldFile := range idx.fieldTempFiles {
		fieldLength, err2 := copyTempFile(oldFile, fieldName, finalWriter)
		if err2 != nil {
			return err2
		}

		// Store field metadata
		idx.fieldOffsets[fieldName] = FieldMetadata{
			Offset: currentOffset,
			Length: fieldLength,
		}

		currentOffset += fieldLength
	}

	// Create and write metadata bytes at the end of the file
	metadata := idx.createMetadataBytes(currentOffset)
	if _, err := finalWriter.Write(metadata); err != nil {
		return fmt.Errorf("failed to write metadata: %v", err)
	}

	return nil
}

func copyTempFile(oldFile fileops.File, fieldName string, finalWriter *bufio.Writer) (uint64, error) {
	// Get field file
	tempFile, err := fileops.Open(oldFile.Name())
	if err != nil {
		return 0, err
	}
	defer tempFile.Close()

	// Get file size
	fileInfo, err := tempFile.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to get file info for field %s: %v", fieldName, err)
	}
	fieldLength := uint64(fileInfo.Size())

	// Copy field data directly from temp file to final file using streaming
	if _, err := io.Copy(finalWriter, tempFile); err != nil {
		return 0, fmt.Errorf("failed to copy field data for %s: %v", fieldName, err)
	}
	return fieldLength, nil
}

// createMetadataBytes creates the metadata bytes for field offsets and lengths
func (idx *UniversalBloomFilterWriter) createMetadataBytes(metadataOffset uint64) []byte {
	var metadata []byte

	// Write metadata header
	metadata = append(metadata, BFMetadataStart...)

	// Write number of fields
	metadata = binary.LittleEndian.AppendUint32(metadata, uint32(len(idx.fieldOffsets)))

	// Write metadata offset (this points to the start of this metadata block)
	metadata = binary.LittleEndian.AppendUint64(metadata, metadataOffset)

	// Write each field's metadata
	for fieldName, fieldMeta := range idx.fieldOffsets {
		// Write field name length
		metadata = binary.LittleEndian.AppendUint32(metadata, uint32(len(fieldName)))

		// Write field name
		metadata = append(metadata, fieldName...)

		// Write field offset and length
		metadata = binary.LittleEndian.AppendUint64(metadata, fieldMeta.Offset)
		metadata = binary.LittleEndian.AppendUint64(metadata, fieldMeta.Length)
	}

	metadata = binary.LittleEndian.AppendUint32(metadata, uint32(len(metadata)))
	return metadata
}

// NewUniversalGeneralBloomFilterWriter creates a new UniversalBloomFilterWriter with stream-based writing capabilities
func NewUniversalGeneralBloomFilterWriter(dir, msName, dataFilePath, lockPath, tokens string, params *influxql.IndexParam) *UniversalBloomFilterWriter {
	var falseRate float64
	if len(params.IList) == 0 {
		falseRate = DefaultFalseRate
	} else {
		if v, ok := params.IList[0].(*influxql.NumberLiteral); ok {
			falseRate = v.Val
		}
	}
	bfWriter := &UniversalBloomFilterWriter{
		falseRate:       falseRate,
		fieldWriters:    make(map[string]*bufio.Writer),
		fieldTempFiles:  make(map[string]fileops.File),
		fieldOffsets:    make(map[string]FieldMetadata),
		skipIndexWriter: newSkipIndexWriter(dir, msName, dataFilePath, lockPath, tokens),
	}

	bfWriter.indexFilePath = path.Join(bfWriter.dir, bfWriter.msName, colstore.AppendSecondaryIndexSuffix(bfWriter.dataFilePath, "", index.BloomFilter, 0)+tmpFileSuffix)
	return bfWriter
}

// getFieldTempFilePath returns the temporary file path for a given field name
func (idx *UniversalBloomFilterWriter) getFieldTempFilePath(fieldName string) string {
	curFileId := fileId.Add(1)
	return path.Join(idx.dir,
		idx.msName,
		idx.dataFilePath+"."+fmt.Sprintf("%d", curFileId)+".bf"+tmpFileSuffix)
}

func (idx *UniversalBloomFilterWriter) Files() []string {
	return []string{idx.indexFilePath}
}

// createFieldWriter creates a buffered writer for the specified field if it doesn't exist
func (idx *UniversalBloomFilterWriter) createFieldWriter(fieldName string) error {
	if _, exists := idx.fieldWriters[fieldName]; exists {
		return nil // Writer already exists
	}

	// Create temporary file for this field
	tempFilePath := idx.getFieldTempFilePath(fieldName)
	file, err := fileops.Create(tempFilePath)
	if err != nil {
		return fmt.Errorf("failed to create temp file for field %s: %v", fieldName, err)
	}

	// Create buffered writer
	writer := bufio.NewWriterSize(file, WriteBufferSize)

	idx.fieldWriters[fieldName] = writer
	idx.fieldTempFiles[fieldName] = file

	return nil
}

// flushFieldWriters flushes all field writers but keeps the underlying files open
func (idx *UniversalBloomFilterWriter) flushFieldWriters() error {
	for _, writer := range idx.fieldWriters {
		if err := writer.Flush(); err != nil {
			return err
		}
	}

	return nil
}

// closeFieldFiles closes all underlying field files after merging is complete
func (idx *UniversalBloomFilterWriter) closeFieldFiles() error {
	for _, file := range idx.fieldTempFiles {
		if err := file.Close(); err != nil {
			return err
		}
	}

	return nil
}

// closeFieldWriters closes all field writers and their underlying files (kept for backward compatibility)
func (idx *UniversalBloomFilterWriter) closeFieldWriters() error {
	// First flush all writers
	if err := idx.flushFieldWriters(); err != nil {
		return err
	}

	// Then close all files
	return idx.closeFieldFiles()
}

// cleanupTempFiles removes all temporary files
func (idx *UniversalBloomFilterWriter) cleanupTempFiles() error {
	for _, file := range idx.fieldTempFiles {
		if err := fileops.Remove(file.Name()); err != nil {
			return err
		}
	}
	return nil
}

func (idx *UniversalBloomFilterWriter) NewUniversalBloomFilter(elemNums int) *UniversalBloomFilter {
	filter := bloom.NewWithEstimates(uint(elemNums), idx.falseRate)

	idx.bf = &UniversalBloomFilter{
		Filter: filter,
	}
	return idx.bf
}

func (bf *UniversalBloomFilter) Test(value []byte) bool {
	return bf.Filter.Test(value)
}

func (bf *UniversalBloomFilter) Serialize() ([]byte, error) {
	data, err := bf.Filter.GobEncode()
	if err != nil {
		return nil, err
	}

	length := uint32(len(data))
	lengthBytes := make([]byte, util.Uint32SizeBytes)
	binary.LittleEndian.PutUint32(lengthBytes, length)

	result := append(lengthBytes, data...)
	return result, nil
}

func DeserializeBloomFilter(data []byte) (*UniversalBloomFilter, error) {
	filter := &bloom.BloomFilter{}
	if err := filter.GobDecode(data); err != nil {
		return nil, fmt.Errorf("failed to deserialize bloomFilter: %v", err)
	}

	return &UniversalBloomFilter{
		Filter: filter,
	}, nil
}

func (idx *UniversalBloomFilterWriter) BuildBloomFilterFromRecord(record *record.Record, colIdx int) (*UniversalBloomFilter, error) {
	if record == nil || colIdx < 0 || colIdx >= len(record.Schema) {
		return nil, fmt.Errorf("invalid params")
	}

	if err := idx.addElementToBloomFilter(record, colIdx); err != nil {
		return nil, err
	}

	return idx.bf, nil
}

func (idx *UniversalBloomFilterWriter) addElementToBloomFilter(record *record.Record, colIdx int) error {
	colVal := &record.ColVals[colIdx]

	switch record.Schema[colIdx].Type {
	case influx.Field_Type_Int:
		idx.addBytesToBfByFixLen(util.Int64SizeBytes, colVal.Val, colVal.Len)
	case influx.Field_Type_Float:
		idx.addBytesToBfByFixLen(util.Float64SizeBytes, colVal.Val, colVal.Len)
	case influx.Field_Type_String:
		offs, lens := colVal.GetOffsAndLens()
		return idx.addBytesToBfByArray(offs, lens, colVal.Val)
	default:
		return fmt.Errorf("unsupported column type: %v", influx.FieldTypeName[record.Schema[colIdx].Type])
	}

	return nil
}

func (idx *UniversalBloomFilterWriter) addBytesToBfByArray(offs, lens []int32, bytes []byte) error {
	if len(lens) == 0 {
		return errors.New("expect at least one string parameter")
	}
	simpleTokenizer := tokenizer.GetTokenizerFromPool()
	defer tokenizer.PutTokenizerToPool(simpleTokenizer)
	wordBytes, hasEmptyString := simpleTokenizer.AllWordBytes(bytes, offs, lens)
	wordsNumbers := len(wordBytes)
	var bf *UniversalBloomFilter
	if hasEmptyString {
		bf = idx.NewUniversalBloomFilter(wordsNumbers + 1)
		bf.Filter.AddString("")
	} else {
		bf = idx.NewUniversalBloomFilter(wordsNumbers)
	}

	for i := 0; i < wordsNumbers; i++ {
		bf.Filter.Add(wordBytes[i])
	}
	return nil
}

func (idx *UniversalBloomFilterWriter) addBytesToBfByFixLen(fixLen int, bytes []byte, rowNumbers int) {
	bf := idx.NewUniversalBloomFilter(rowNumbers)
	for i := 0; i+fixLen <= len(bytes); i += fixLen {
		bf.Filter.Add(bytes[i : i+fixLen])
	}
}

// BuildUniversalBloomFilter builds and immediately writes bloom filter data to stream for the specified field
func (idx *UniversalBloomFilterWriter) BuildUniversalBloomFilter(record *record.Record, colIdx int, fieldName string) error {
	if record == nil {
		return errors.New("record cannot be nil")
	}

	bf, err := idx.BuildBloomFilterFromRecord(record, colIdx)
	if err != nil {
		return fmt.Errorf("failed to build bloomFilter %v", err)
	}

	data, err := bf.Serialize()
	if err != nil {
		return fmt.Errorf("failed to serialize bloomFilter %v", err)
	}

	// Write bloom filter data directly to field's buffered writer
	return idx.writeBloomFilterToStream(fieldName, data)
}

// writeBloomFilterToStream writes serialized bloom filter data to the field's buffered writer
func (idx *UniversalBloomFilterWriter) writeBloomFilterToStream(fieldName string, data []byte) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Create field writer if it doesn't exist
	if err := idx.createFieldWriter(fieldName); err != nil {
		return err
	}

	// Write data to the field's buffered writer
	writer := idx.fieldWriters[fieldName]
	if _, err := writer.Write(data); err != nil {
		return fmt.Errorf("failed to write bloom filter data for field %s: %v", fieldName, err)
	}

	return nil
}

// BloomFilterFieldData stores bloom filter data with partial loading support
type BloomFilterFieldData struct {
	lastReadOffset  uint64                        // Last read position in the field data
	maxBFIndex      int                           // Maximum loaded BF index
	lastRemainBytes []byte                        // Last Remain Bytes
	loadedBFs       map[int]*UniversalBloomFilter // Map of loaded BF by index
	fieldMetadata   FieldMetadata                 // Field metadata for this field
}

// Default read threshold for partial loading (10MB)
const DefaultReadThreshold = 100 * 1024 * 1024

type UniversalBloomFilterReader struct {
	indexFilePath string
	bfsMap        map[string]*BloomFilterFieldData // Changed to use BloomFilterFieldData
	sk            SKCondition
	option        hybridqp.Options
	wordBytes     [][]byte
}

func (r *UniversalBloomFilterReader) MayBeInFragment(fragId uint32) (bool, error) {
	return r.sk.IsExist(int64(fragId), r)
}

func (r *UniversalBloomFilterReader) GetFragmentRowCount(fragId uint32) (int64, error) {
	return 0, nil
}

func (r *UniversalBloomFilterReader) ReInit(file interface{}) error {
	// Initialize readers with file pathName information
	if f1, ok := file.(TsspFile); ok {
		filePath := f1.Path()
		if filePath == "" {
			return errors.New("file path cannot be empty")
		}

		dir, fileName := path.Split(filePath)

		fileName = strings.TrimSuffix(fileName, path.Ext(fileName))
		r.indexFilePath = colstore.AppendSecondaryIndexSuffix(path.Join(dir, fileName), "", index.BloomFilter, colstore.MinRowsForSeek)
		clear(r.bfsMap)
		r.wordBytes = nil
	} else {
		return errors.New("file need be tssp type")
	}

	return nil
}

func (r *UniversalBloomFilterReader) IsExist(blockId int64, elem *rpn.SKRPNElement) (bool, error) {
	switch elem.Op {
	case influxql.EQ, influxql.MATCHPHRASE:
		return isInBF(blockId, elem, r)
	default:
		return true, nil
	}
}

func isInBF(blockId int64, elem *rpn.SKRPNElement, r *UniversalBloomFilterReader) (bool, error) {
	bfKey := r.indexFilePath + ":" + elem.Key
	fieldName := elem.Key
	fragId := int(blockId)

	fieldData, ok := r.bfsMap[bfKey]
	if !ok {
		metadata, err := r.loadMetadata()
		if err != nil {
			return false, err
		}

		if v, ok := metadata[fieldName]; ok {
			// Initialize BloomFilterFieldData if not exists
			r.bfsMap[bfKey] = &BloomFilterFieldData{
				lastReadOffset:  StartOffset,
				maxBFIndex:      StartSegmentId,
				lastRemainBytes: make([]byte, 0),
				loadedBFs:       make(map[int]*UniversalBloomFilter),
				fieldMetadata:   v,
			}
			fieldData = r.bfsMap[bfKey]
		} else {
			return false, fmt.Errorf("no such column name of: %s", fieldName)
		}
	}

	// If the requested BF index is beyond what we've loaded, try to loadMetadata more data
	if fragId > fieldData.maxBFIndex {
		// Load more data to reach the requested BF
		for fieldData.maxBFIndex < fragId && fieldData.lastReadOffset < fieldData.fieldMetadata.EndPos {
			err := r.loadFieldData(elem.Key)
			if err != nil {
				return false, fmt.Errorf("failed to loadMetadata more field data: %v", err)
			}
		}
	}

	// Check if we now have the requested BF
	if bf, exists := fieldData.loadedBFs[fragId]; exists {
		var bytes []byte
		switch elem.Value.(type) {
		case int64:
			bytes = make([]byte, util.Int64SizeBytes)
			binary.LittleEndian.PutUint64(bytes, uint64(elem.Value.(int64)))
		case float64:
			bytes = make([]byte, util.Float64SizeBytes)
			binary.LittleEndian.PutUint64(bytes, math.Float64bits(elem.Value.(float64)))
		case string:
			if len(r.wordBytes) == 0 {
				bytes = []byte(elem.Value.(string))
				if len(bytes) == 0 {
					return bf.Filter.TestString(""), nil
				}
				simpleTokenizer := tokenizer.GetTokenizerFromPool()
				defer tokenizer.PutTokenizerToPool(simpleTokenizer)
				r.wordBytes, _ = simpleTokenizer.AllWordBytes(bytes, []int32{0}, []int32{int32(len(bytes))})
			}
			for i := 0; i < len(r.wordBytes); i++ {
				if !bf.Test(r.wordBytes[i]) {
					return false, nil
				}
			}
			return true, nil
		default:
			return false, errors.New("error type of bf query")
		}
		return bf.Test(bytes), nil
	}
	// If we have data but not the specific BF, it means there's a gap or error
	return false, errors.New("bloom filter not found in loaded data")
}

func (r *UniversalBloomFilterReader) GetRowCount(blockId int64, elem *rpn.SKRPNElement) (int64, error) {
	return 0, nil
}

func (r *UniversalBloomFilterReader) StartSpan(span *tracing.Span) {

}

func NewUniversalBloomFilterReader(rpnExpr *rpn.RPNExpr, schema record.Schemas, option hybridqp.Options) (*UniversalBloomFilterReader, error) {
	sk, err := NewSKCondition(rpnExpr, schema)
	if err != nil {
		return nil, err
	}

	return &UniversalBloomFilterReader{
		sk:     sk,
		option: option,
		bfsMap: make(map[string]*BloomFilterFieldData),
	}, nil
}

// Close closes the UniversalBloomFilterReader
func (r *UniversalBloomFilterReader) Close() error {
	// Clear the bloom filter map to free memory
	clear(r.bfsMap)
	return nil
}

// loadFieldMetadataFromDisk reads field metadata from the end of the index file
func (r *UniversalBloomFilterReader) loadFieldMetadataFromDisk() (map[string]FieldMetadata, error) {
	file, err := fileops.Open(r.indexFilePath)
	if err != nil {
		return nil, fmt.Errorf("cannot open index file: %v", err)
	}
	defer file.Close()

	// Get file size to find metadata position
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("cannot get file info: %v", err)
	}
	fileSize := fileInfo.Size()

	// Minimum metadata size: magic number (4) + field count (4) + metadata offset (8)
	minMetadataSize := 16
	if fileSize < int64(minMetadataSize) {
		return nil, errors.New("file too small to contain metadata")
	}

	// Read metadata from the end of the file
	metadataLengthBuffer := make([]byte, 4)
	if _, err := file.ReadAt(metadataLengthBuffer, fileSize-int64(4)); err != nil {
		return nil, fmt.Errorf("failed to read metadataLength: %v", err)
	}
	metadataLength := binary.LittleEndian.Uint32(metadataLengthBuffer)

	// Read metadata from the end of the file
	metadataBuffer := make([]byte, metadataLength)
	if _, err := file.ReadAt(metadataBuffer, fileSize-int64(metadataLength+4)); err != nil {
		return nil, fmt.Errorf("failed to read metadata header: %v", err)
	}

	// Check magic number
	magicNumber := string(metadataBuffer[0:4])
	if magicNumber != BFMetadataStart {
		return nil, errors.New("invalid metadata magic number")
	}

	fieldCount := binary.LittleEndian.Uint32(metadataBuffer[4:8])
	metadataOffset := binary.LittleEndian.Uint64(metadataBuffer[8:16])

	// Read complete metadata
	metadataSize := int64(fileSize) - int64(metadataOffset)
	if metadataSize < 0 {
		return nil, errors.New("invalid metadata offset")
	}

	metadataData := metadataBuffer[16:]

	// Parse field metadata
	fieldMetadata := make(map[string]FieldMetadata)
	pos := 0

	for i := uint32(0); i < fieldCount; i++ {
		if pos+4 > len(metadataData) {
			return nil, errors.New("invalid metadata: field name length out of bounds")
		}

		fieldNameLength := binary.LittleEndian.Uint32(metadataData[pos : pos+4])
		pos += 4

		if pos+int(fieldNameLength) > len(metadataData) {
			return nil, errors.New("invalid metadata: field name out of bounds")
		}

		fieldName := string(metadataData[pos : pos+int(fieldNameLength)])
		pos += int(fieldNameLength)

		if pos+16 > len(metadataData) {
			return nil, errors.New("invalid metadata: field metadata out of bounds")
		}

		offset := binary.LittleEndian.Uint64(metadataData[pos : pos+8])
		length := binary.LittleEndian.Uint64(metadataData[pos+8 : pos+16])
		pos += 16

		fieldMetadata[fieldName] = FieldMetadata{
			Offset: offset,
			Length: length,
			EndPos: offset + length,
		}
	}

	return fieldMetadata, nil
}

// loadFieldData loads bloom filter data for a specific field from the index file with partial loading support
func (r *UniversalBloomFilterReader) loadFieldData(fieldName string) error {
	fieldKey := r.indexFilePath + ":" + fieldName
	fieldData := r.bfsMap[fieldKey]

	// Calculate how much data to read (use threshold or remaining data)
	readLength := DefaultReadThreshold
	fieldMetadata := fieldData.fieldMetadata
	if fieldMetadata.Length-fieldData.lastReadOffset < uint64(readLength) {
		readLength = int(fieldMetadata.Length - fieldData.lastReadOffset)
	}

	clear(fieldData.loadedBFs)

	// Open file for reading
	file, err := fileops.Open(r.indexFilePath)
	if err != nil {
		return fmt.Errorf("cannot open index file: %v", err)
	}
	defer file.Close()

	// Read partial data
	bfsData := make([]byte, readLength)
	if _, err := file.ReadAt(bfsData, int64(fieldMetadata.Offset)+int64(fieldData.lastReadOffset)); err != nil {
		return fmt.Errorf("failed to read field data for %s: %v", fieldName, err)
	}

	if len(fieldData.lastRemainBytes) > 0 {
		bfsData = append(fieldData.lastRemainBytes, bfsData...)
		fieldData.lastRemainBytes = fieldData.lastRemainBytes[:0]
	}

	// Parse individual bloom filters from the partial data
	dataOffset := 0
	for dataOffset+util.Uint32SizeBytes < len(bfsData) {
		bfLength := binary.LittleEndian.Uint32(bfsData[dataOffset : dataOffset+util.Uint32SizeBytes])
		if dataOffset+int(bfLength)+util.Uint32SizeBytes > len(bfsData) {
			fieldData.lastRemainBytes = append(fieldData.lastRemainBytes, bfsData[dataOffset:]...)
			break // Not enough data for complete BF
		}

		bfData := bfsData[dataOffset+util.Uint32SizeBytes : dataOffset+util.Uint32SizeBytes+int(bfLength)]
		bf, err := DeserializeBloomFilter(bfData)
		if err != nil {
			return fmt.Errorf("failed to deserialize bloom filter for field %s: %v", fieldName, err)
		}

		// Store the BF with its index
		bfIndex := fieldData.maxBFIndex + 1
		fieldData.loadedBFs[bfIndex] = bf
		fieldData.maxBFIndex = bfIndex

		dataOffset += util.Uint32SizeBytes + int(bfLength)
	}

	// Update the last read offset
	fieldData.lastReadOffset += uint64(readLength)

	return nil
}

func (r *UniversalBloomFilterReader) loadMetadata() (map[string]FieldMetadata, error) {
	// Try to get field metadata from cache first
	cacheKey := r.indexFilePath
	if metadata, found := fieldMetadataCache.Get(cacheKey); found {
		return metadata, nil
	} else {
		// Load field metadata from file if not in cache
		var err error
		metadata, err = r.loadFieldMetadataFromDisk()
		if err != nil {
			return nil, fmt.Errorf("failed to loadMetadata field metadata: %v", err)
		}

		// Store in cache for future use
		fieldMetadataCache.Add(cacheKey, metadata)
		return metadata, nil
	}
}
