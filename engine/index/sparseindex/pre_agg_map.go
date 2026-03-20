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
	"math"
	"path"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/fragment"
	"github.com/openGemini/openGemini/lib/index"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

var preAggMapMetadataCache = expirable.NewLRU[string, map[string]EnhancedPreAggMapFieldMetadata](1000, nil, 5*time.Minute)

const PreAggMapWriteBufferSize = 1024 * 1024
const PreAggMapDefaultReadThreshold = 100 * 1024 * 1024

const (
	MetaLengthBytes int64 = 4

	MetaValidBytes      int64 = 4
	MetaFieldCountBytes int64 = 4

	FieldNameLengthBytes int64 = 4
	FieldAggOffsetBytes  int64 = 8
	FieldAggLengthBytes  int64 = 4
	FieldTypeBytes       int64 = 1
	FieldMetaAggBytes    int64 = 8
)

var preAggMapFileId atomic.Int64

// FieldType defines the type of field for aggregation purposes
type FieldType byte

const (
	FieldTypeTime  FieldType = 0 // time[count]
	FieldTypeInt   FieldType = 1 // int[count/sum/min/max]
	FieldTypeFloat FieldType = 2 // float[count/sum/min/max]
	FieldTypeOther FieldType = 3 // not int/float[count]
)

// EnhancedPreAggMapFieldMetadata stores enhanced field metadata with aggregation info
type EnhancedPreAggMapFieldMetadata struct {
	FieldName string
	Offset    uint64      // Starting position of this field's data in the merged file
	Length    uint64      // Total length of this field's pre agg map data
	EndPos    uint64      // Ending position of this field's data in the merged file
	Type      FieldType   // Field type for aggregation
	Count     uint64      // Overall count for the field
	Sum       interface{} // Overall sum for the field (int64 or float64)
	Min       interface{} // Overall min for the field (int64 or float64)
	Max       interface{} // Overall max for the field (int64 or float64)
}

// PreAggMapRecord represents aggregation data for a segment
type PreAggMapRecord struct {
	Count uint64
	Sum   interface{} // int64 or float64
	Min   interface{} // int64 or float64
	Max   interface{} // int64 or float64
}

// PreAggMapWriter implements stream-based pre agg map writing to avoid OOM
type PreAggMapWriter struct {
	*skipIndexWriter
	fieldWriters   map[string]*bufio.Writer                   // Buffered writers for each field
	fieldTempFiles map[string]fileops.File                    // Temporary files for each field
	fieldMetaInfos map[string]*EnhancedPreAggMapFieldMetadata // Track field meta infos for final merge
	indexFilePath  string
}

// NewPreAggMapWriter creates a new PreAggMapWriter with stream-based writing capabilities
func NewPreAggMapWriter(dir, msName, dataFilePath, lockPath, tokens string) *PreAggMapWriter {
	preAggMapWriter := &PreAggMapWriter{
		fieldWriters:    make(map[string]*bufio.Writer),
		fieldTempFiles:  make(map[string]fileops.File),
		fieldMetaInfos:  make(map[string]*EnhancedPreAggMapFieldMetadata),
		skipIndexWriter: newSkipIndexWriter(dir, msName, dataFilePath, lockPath, tokens),
	}

	preAggMapWriter.indexFilePath = path.Join(preAggMapWriter.dir, preAggMapWriter.msName, colstore.AppendSecondaryIndexSuffix(preAggMapWriter.dataFilePath, "", index.AggMap, 0)+tmpFileSuffix)
	return preAggMapWriter
}

// CreateAttachIndex builds and writes pre agg map data for all segments of the specified field
func (idx *PreAggMapWriter) CreateAttachIndex(writeRec *record.Record, schemaIdx, rowsPerSegment []int) error {
	for _, id := range schemaIdx {
		field := writeRec.Schema[id]
		col := &writeRec.ColVals[id]
		if err := idx.BuildPreAggMapFromRecord(col, field); err != nil {
			return err
		}
	}
	return nil
}

// BuildPreAggMapFromRecord builds and immediately writes pre agg map data to stream for the specified field
func (idx *PreAggMapWriter) BuildPreAggMapFromRecord(col *record.ColVal, field record.Field) error {
	if col == nil {
		return errors.New("build pre-agg error,because the record is nil")
	}

	fieldName := field.Name
	fieldType := field.Type

	segmentAgg := calculateSegmentAgg(col, fieldName, fieldType)

	// Calculate and store field metadata
	err := idx.calculateFieldMetadata(fieldName, fieldType, segmentAgg)
	if err != nil {
		return fmt.Errorf("failed to calculate field metadata: %v", err)
	}

	// Write pre agg map data for all segments to field's buffered writer
	return idx.writePreAggMapToStream(fieldName, segmentAgg)
}

func calculateSegmentAgg(col *record.ColVal, fieldName string, fieldType int) PreAggMapRecord {
	if fieldName == record.TimeField {
		return PreAggMapRecord{
			Count: uint64(col.Len),
		}
	}
	var aggMap PreAggMapRecord
	switch fieldType {
	case influx.Field_Type_Int:
		values := col.IntegerValues()
		count, sum, _min, _max := calculateAggIndices[int64](values, math.MaxInt64, math.MinInt64)
		aggMap = PreAggMapRecord{
			Count: count,
			Sum:   sum,
			Min:   _min,
			Max:   _max,
		}
	case influx.Field_Type_Float:
		values := col.FloatValues()
		count, sum, _min, _max := calculateAggIndices[float64](values, math.MaxFloat64, -math.MaxFloat64)
		aggMap = PreAggMapRecord{
			Count: count,
			Sum:   sum,
			Min:   _min,
			Max:   _max,
		}
	default:
		// For non-numeric fields, only calculate count
		aggMap = PreAggMapRecord{
			Count: uint64(col.Len - col.NilCount),
		}
	}
	return aggMap
}

func calculateAggIndices[T Number](values []T, initialMin, initialMax T) (uint64, T, T, T) {
	count := len(values)

	if count == 0 {
		return 0, 0, initialMin, initialMax
	}

	var sum, minValue, maxValue T
	minValue = values[0]
	maxValue = values[0]
	sum = values[0]

	for i := 1; i < count; i++ {
		sum += values[i]
		if values[i] < minValue {
			minValue = values[i]
		}
		if values[i] > maxValue {
			maxValue = values[i]
		}
	}

	return uint64(count), sum, minValue, maxValue
}

// calculateFieldMetadata calculates field metadata with aggregation info
func (idx *PreAggMapWriter) calculateFieldMetadata(fieldName string, fieldType int, segmentAgg PreAggMapRecord) error {
	aggMapFieldMetadata, exists := idx.fieldMetaInfos[fieldName]
	if !exists {
		aggMapFieldMetadata = &EnhancedPreAggMapFieldMetadata{
			FieldName: fieldName,
			Count:     segmentAgg.Count,
		}
		// Determine field type based on schema and field name
		// Check if it's a time column based on field name
		if fieldName == record.TimeField {
			aggMapFieldMetadata.Type = FieldTypeTime
		} else {
			// Determine field type based on schema type
			switch fieldType {
			case influx.Field_Type_Int:
				aggMapFieldMetadata.Type = FieldTypeInt
				aggMapFieldMetadata.Sum = segmentAgg.Sum.(int64)
				aggMapFieldMetadata.Min = segmentAgg.Min.(int64)
				aggMapFieldMetadata.Max = segmentAgg.Max.(int64)
			case influx.Field_Type_Float:
				aggMapFieldMetadata.Type = FieldTypeFloat
				aggMapFieldMetadata.Sum = segmentAgg.Sum.(float64)
				aggMapFieldMetadata.Min = segmentAgg.Min.(float64)
				aggMapFieldMetadata.Max = segmentAgg.Max.(float64)
			default:
				aggMapFieldMetadata.Type = FieldTypeOther
			}
		}
	} else {
		aggMapFieldMetadata.Count = aggMapFieldMetadata.Count + segmentAgg.Count

		switch aggMapFieldMetadata.Type {
		case FieldTypeInt:
			aggMapFieldMetadata.Sum = aggMapFieldMetadata.Sum.(int64) + segmentAgg.Sum.(int64)
			aggMapFieldMetadata.Min = min(aggMapFieldMetadata.Min.(int64), segmentAgg.Min.(int64))
			aggMapFieldMetadata.Max = max(aggMapFieldMetadata.Max.(int64), segmentAgg.Max.(int64))
		case FieldTypeFloat:
			aggMapFieldMetadata.Sum = aggMapFieldMetadata.Sum.(float64) + segmentAgg.Sum.(float64)
			aggMapFieldMetadata.Min = min(aggMapFieldMetadata.Min.(float64), segmentAgg.Min.(float64))
			aggMapFieldMetadata.Max = max(aggMapFieldMetadata.Max.(float64), segmentAgg.Max.(float64))
		default:
		}

	}
	idx.fieldMetaInfos[fieldName] = aggMapFieldMetadata

	return nil
}

// writePreAggMapToStream writes serialized pre agg map data to the field's buffered writer for all segments
func (idx *PreAggMapWriter) writePreAggMapToStream(fieldName string, segmentAgg PreAggMapRecord) error {
	// Create field writer if it doesn't exist
	if err := idx.createFieldWriter(fieldName); err != nil {
		return err
	}

	// Write data to the field's buffered writer
	writer := idx.fieldWriters[fieldName]

	fieldMeta := idx.fieldMetaInfos[fieldName]

	// Write count
	countBytes := make([]byte, util.Int64SizeBytes)
	binary.LittleEndian.PutUint64(countBytes, segmentAgg.Count)
	if _, err := writer.Write(countBytes); err != nil {
		return fmt.Errorf("failed to write count for field %s: %v", fieldName, err)
	}

	// Write sum, min, max if they exist based on field type
	if fieldMeta.Type == FieldTypeInt || fieldMeta.Type == FieldTypeFloat {
		// Write sum, min, max in consistent format (8 bytes each)
		var sumVal, minVal, maxVal uint64
		if fieldMeta.Type == FieldTypeInt {
			sumVal = uint64(segmentAgg.Sum.(int64))
			minVal = uint64(segmentAgg.Min.(int64))
			maxVal = uint64(segmentAgg.Max.(int64))
		} else {
			sumVal = math.Float64bits(segmentAgg.Sum.(float64))
			minVal = math.Float64bits(segmentAgg.Min.(float64))
			maxVal = math.Float64bits(segmentAgg.Max.(float64))
		}

		sumBytes := make([]byte, util.Int64SizeBytes)
		binary.LittleEndian.PutUint64(sumBytes, sumVal)
		if _, err := writer.Write(sumBytes); err != nil {
			return fmt.Errorf("failed to write sum for field %s: %v", fieldName, err)
		}

		minBytes := make([]byte, util.Int64SizeBytes)
		binary.LittleEndian.PutUint64(minBytes, minVal)
		if _, err := writer.Write(minBytes); err != nil {
			return fmt.Errorf("failed to write min for field %s: %v", fieldName, err)
		}

		maxBytes := make([]byte, util.Int64SizeBytes)
		binary.LittleEndian.PutUint64(maxBytes, maxVal)
		if _, err := writer.Write(maxBytes); err != nil {
			return fmt.Errorf("failed to write max for field %s: %v", fieldName, err)
		}
	}

	return nil
}

// createFieldWriter creates a buffered writer for the specified field if it doesn't exist
func (idx *PreAggMapWriter) createFieldWriter(fieldName string) error {
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
	writer := bufio.NewWriterSize(file, PreAggMapWriteBufferSize)

	idx.fieldWriters[fieldName] = writer
	idx.fieldTempFiles[fieldName] = file

	return nil
}

// getFieldTempFilePath returns the temporary file path for a given field name
func (idx *PreAggMapWriter) getFieldTempFilePath(fieldName string) string {
	curFileId := preAggMapFileId.Add(1)
	return path.Join(idx.dir,
		idx.msName,
		idx.dataFilePath+"."+fmt.Sprintf("%d", curFileId)+".agg"+tmpFileSuffix)
}

func (idx *PreAggMapWriter) CreateDetachIndex(writeRec *record.Record, schemaIdx, rowsPerSegment []int, dataBuf [][]byte) ([][]byte, []string) {
	return nil, nil
}

// Flush merges all field pre agg map files into a single indexed file with metadata
func (idx *PreAggMapWriter) Flush() error {
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

// closeFieldWriters closes all field writers and their underlying files (kept for backward compatibility)
func (idx *PreAggMapWriter) closeFieldWriters() error {
	// First flush all writers
	if err := idx.flushFieldWriters(); err != nil {
		return err
	}

	// Then close all files
	return idx.closeFieldFiles()
}

// flushFieldWriters flushes all field writers but keeps the underlying files open
func (idx *PreAggMapWriter) flushFieldWriters() error {
	for _, writer := range idx.fieldWriters {
		if err := writer.Flush(); err != nil {
			return err
		}
	}

	return nil
}

// closeFieldFiles closes all underlying field files after merging is complete
func (idx *PreAggMapWriter) closeFieldFiles() error {
	for _, file := range idx.fieldTempFiles {
		if err := file.Close(); err != nil {
			return err
		}
	}

	return nil
}

// mergeFieldFiles merges all field temporary files into the final index file using streaming approach
func (idx *PreAggMapWriter) mergeFieldFiles() error {
	// Open the final index file
	file, err := fileops.Create(idx.indexFilePath)
	if err != nil {
		return err
	}
	defer file.Close()

	// Create buffered writer for the final file
	finalWriter := bufio.NewWriterSize(file, PreAggMapWriteBufferSize)

	// Track current offset for field metadata
	currentOffset := uint64(0)

	// Process each field file in streaming fashion
	for fieldName, oldFile := range idx.fieldTempFiles {
		fieldAggSize, err2 := copyTempFile(oldFile, fieldName, finalWriter)
		if err2 != nil {
			return err2
		}

		// Store field metadata
		if fieldMeta, ok := idx.fieldMetaInfos[fieldName]; ok {
			fieldMeta.Offset = currentOffset
			fieldMeta.Length = fieldAggSize
			fieldMeta.EndPos = currentOffset + fieldAggSize
			idx.fieldMetaInfos[fieldName] = fieldMeta
		}

		currentOffset += fieldAggSize
	}

	// Create and write metadata bytes at the end of the file
	metadata := idx.createMetadataBytes()
	if _, err := finalWriter.Write(metadata); err != nil {
		return fmt.Errorf("failed to write metadata: %v", err)
	}

	if err := finalWriter.Flush(); err != nil {
		return fmt.Errorf("failed to flush final writer: %v", err)
	}

	return nil
}

// createMetadataBytes creates the metadata bytes for field offsets and lengths
func (idx *PreAggMapWriter) createMetadataBytes() []byte {
	var metadata []byte

	// Write 4 bytes blank space for future validation
	metadata = append(metadata, make([]byte, MetaValidBytes)...)

	// Write number of fields(4 bytes)
	metadata = binary.LittleEndian.AppendUint32(metadata, uint32(len(idx.fieldMetaInfos)))

	// Write each field's metadata
	for fieldName, fieldMeta := range idx.fieldMetaInfos {
		// Write field name length(4 bytes)
		metadata = binary.LittleEndian.AppendUint32(metadata, uint32(len(fieldName)))

		// Write field name
		metadata = append(metadata, fieldName...)

		// Write field offset(8 bytes) and length (4 bytes)
		metadata = binary.LittleEndian.AppendUint64(metadata, fieldMeta.Offset)
		metadata = binary.LittleEndian.AppendUint32(metadata, uint32(fieldMeta.Length))

		// Write field type(1 byte)
		metadata = append(metadata, byte(fieldMeta.Type))

		// Write count, sum, min, max based on field type
		// For FieldTypeTime and FieldTypeOther, only count is written
		metadata = binary.LittleEndian.AppendUint64(metadata, uint64(fieldMeta.Count))

		if fieldMeta.Type == FieldTypeInt {
			// Write int64 sum, min, max
			metadata = binary.LittleEndian.AppendUint64(metadata, uint64(fieldMeta.Sum.(int64)))
			metadata = binary.LittleEndian.AppendUint64(metadata, uint64(fieldMeta.Min.(int64)))
			metadata = binary.LittleEndian.AppendUint64(metadata, uint64(fieldMeta.Max.(int64)))
		} else if fieldMeta.Type == FieldTypeFloat {
			// Write float64 sum, min, max (as bit patterns)
			metadata = binary.LittleEndian.AppendUint64(metadata, math.Float64bits(fieldMeta.Sum.(float64)))
			metadata = binary.LittleEndian.AppendUint64(metadata, math.Float64bits(fieldMeta.Min.(float64)))
			metadata = binary.LittleEndian.AppendUint64(metadata, math.Float64bits(fieldMeta.Max.(float64)))
		}
	}

	metadata = binary.LittleEndian.AppendUint32(metadata, uint32(len(metadata)))
	return metadata
}

// cleanupTempFiles removes all temporary files
func (idx *PreAggMapWriter) cleanupTempFiles() error {
	for _, file := range idx.fieldTempFiles {
		if err := fileops.Remove(file.Name()); err != nil {
			return err
		}
	}
	return nil
}

func (idx *PreAggMapWriter) Files() []string {
	return []string{idx.indexFilePath}
}

// PreAggMapReader implements reading pre agg map data with field-level access
type PreAggMapReader struct {
	fullFilePath string
	preAgMaps    map[string]*PreAggMapFieldData
}

// ReInit initializes the reader with a file path
func (r *PreAggMapReader) ReInit(filePath string) {
	// Initialize readers with file path information
	_path, fileName := filepath.Split(filePath)

	fileName = strings.Split(fileName, ".")[0]
	fileName = colstore.AppendSecondaryIndexSuffix(fileName, "", index.AggMap, colstore.MinRowsForSeek)
	r.fullFilePath = filepath.Join(_path, fileName)
	r.preAgMaps = make(map[string]*PreAggMapFieldData)
}

// AggData represents aggregated data for a field that can be merged and filtered
type AggData struct {
	FieldName string      // Field name
	Type      FieldType   // Field type
	Count     uint64      // Total count
	Sum       interface{} // Total sum (int64 or float64)
	Min       interface{} // Total min (int64 or float64)
	Max       interface{} // Total max (int64 or float64)
}

// ReadAgg reads aggregation data for specified fields
func (r *PreAggMapReader) ReadAgg(fieldNames []string) ([]*AggData, error) {
	// First, load metadata for all requested fields
	metadata, err := r.loadMetadata()
	if err != nil {
		return nil, fmt.Errorf("failed to load metadata: %v", err)
	}

	// Prepare AggData for each requested field
	var aggDataList []*AggData

	for _, fieldName := range fieldNames {
		fieldMeta, exists := metadata[fieldName]
		if !exists {
			aggDataList = append(aggDataList, &AggData{})
			continue
		}

		aggData := &AggData{
			FieldName: fieldName,
			Type:      fieldMeta.Type,
			Count:     fieldMeta.Count,
			Sum:       fieldMeta.Sum,
			Min:       fieldMeta.Min,
			Max:       fieldMeta.Max,
		}

		aggDataList = append(aggDataList, aggData)
	}

	return aggDataList, nil
}

func (r *PreAggMapReader) loadMetadata() (map[string]EnhancedPreAggMapFieldMetadata, error) {
	// Try to get field metadata from cache first
	cacheKey := r.fullFilePath
	if metadata, found := preAggMapMetadataCache.Get(cacheKey); found {
		return metadata, nil
	} else {
		// Load field metadata from file if not in cache
		var err error
		metadata, err := r.loadFieldMetadataFromDisk()
		if err != nil {
			return nil, fmt.Errorf("failed to load field metadata: %v", err)
		}

		// Store in cache for future use
		preAggMapMetadataCache.Add(cacheKey, metadata)
		return metadata, nil
	}
}

// loadFieldMetadataFromDisk reads field metadata from the end of the index file
func (r *PreAggMapReader) loadFieldMetadataFromDisk() (map[string]EnhancedPreAggMapFieldMetadata, error) {
	file, err := fileops.Open(r.fullFilePath)
	if err != nil {
		return nil, fmt.Errorf("cannot open index file: %v", err)
	}
	defer file.Close()

	// Get file size and validate metadata availability
	fileSize, err := r.getFileSizeAndValidate(file)
	if err != nil {
		return nil, err
	}

	// Read metadata length and metadata buffer
	metadataBuffer, err := r.readMetadataBuffer(file, fileSize)
	if err != nil {
		return nil, err
	}

	// Extract field count and metadata data
	fieldCount, metadataData := r.extractFieldCountAndData(metadataBuffer)

	// Parse field metadata from the data
	return r.parseFieldMetadata(metadataData, fieldCount)
}

// getFileSizeAndValidate opens the file and gets its size, then validates if it contains metadata
func (r *PreAggMapReader) getFileSizeAndValidate(file fileops.File) (int64, error) {
	fileInfo, err := file.Stat()
	if err != nil {
		return 0, fmt.Errorf("cannot get file info: %v", err)
	}
	fileSize := fileInfo.Size()

	// Minimum metadata size: validation (4) + field count (4) + metadata_length (4)
	minMetadataSize := MetaValidBytes + MetaFieldCountBytes + MetaLengthBytes
	if fileSize < minMetadataSize {
		return 0, errors.New("file too small to contain metadata")
	}

	return fileSize, nil
}

// readMetadataBuffer reads the metadata length and metadata buffer from the end of the file
func (r *PreAggMapReader) readMetadataBuffer(file fileops.File, fileSize int64) ([]byte, error) {
	// Read metadata length from the end of the file
	metadataLengthBuffer := make([]byte, MetaLengthBytes)
	if _, err := file.ReadAt(metadataLengthBuffer, fileSize-MetaLengthBytes); err != nil {
		return nil, fmt.Errorf("failed to read metadataLength: %v", err)
	}
	metadataLength := binary.LittleEndian.Uint32(metadataLengthBuffer)

	if int64(metadataLength)+MetaLengthBytes > fileSize {
		return nil, errors.New("the metadata length exceeds the file size")
	}
	// Read metadata from the end of the file
	metadataBuffer := make([]byte, metadataLength)
	if _, err := file.ReadAt(metadataBuffer, fileSize-int64(metadataLength)-MetaLengthBytes); err != nil {
		return nil, fmt.Errorf("failed to read metadata header: %v", err)
	}

	return metadataBuffer, nil
}

// extractFieldCountAndData extracts the field count and metadata data from the buffer
func (r *PreAggMapReader) extractFieldCountAndData(metadataBuffer []byte) (uint32, []byte) {
	// Skip 4 bytes blank space
	fieldCount := binary.LittleEndian.Uint32(metadataBuffer[MetaValidBytes : MetaValidBytes+MetaFieldCountBytes])
	metadataData := metadataBuffer[MetaValidBytes+MetaFieldCountBytes:]
	return fieldCount, metadataData
}

// validateMetadataSize validates if the metadata data has enough space for the field count
func (r *PreAggMapReader) validateMetadataSize(metadataData []byte, fieldCount uint32) error {
	if fieldCount > 0 && FieldNameLengthBytes+FieldAggOffsetBytes+FieldAggLengthBytes+FieldTypeBytes+FieldMetaAggBytes > int64(len(metadataData)) {
		return errors.New("file too small to contain metadata")
	}
	return nil
}

// parseFieldMetadata parses field metadata from the metadata data
func (r *PreAggMapReader) parseFieldMetadata(metadataData []byte, fieldCount uint32) (map[string]EnhancedPreAggMapFieldMetadata, error) {
	// Validate metadata size
	if err := r.validateMetadataSize(metadataData, fieldCount); err != nil {
		return nil, err
	}

	// Parse field metadata
	fieldMetadata := make(map[string]EnhancedPreAggMapFieldMetadata)
	pos := int64(0)

	for i := uint32(0); i < fieldCount; i++ {
		fieldMeta, newPos, err := r.parseSingleFieldMetadata(metadataData, pos)
		if err != nil {
			return nil, err
		}
		fieldMetadata[fieldMeta.FieldName] = fieldMeta
		pos = newPos
	}

	return fieldMetadata, nil
}

// parseSingleFieldMetadata parses metadata for a single field
func (r *PreAggMapReader) parseSingleFieldMetadata(metadataData []byte, pos int64) (EnhancedPreAggMapFieldMetadata, int64, error) {
	// Read field name
	if pos+FieldNameLengthBytes > int64(len(metadataData)) {
		return EnhancedPreAggMapFieldMetadata{}, 0, errors.New("invalid metadata: field name length out of bounds")
	}
	fieldNameLength := int64(binary.LittleEndian.Uint32(metadataData[pos : pos+FieldNameLengthBytes]))
	pos += FieldNameLengthBytes

	if pos+fieldNameLength > int64(len(metadataData)) {
		return EnhancedPreAggMapFieldMetadata{}, 0, errors.New("invalid metadata: field name out of bounds")
	}
	fieldName := string(metadataData[pos : pos+fieldNameLength])
	pos += fieldNameLength

	// Read field offset, length, type and agg and get updated position
	fieldMeta, newPos, err := r.readFieldMetadataDetailsWithPos(metadataData, pos, fieldName)
	if err != nil {
		return EnhancedPreAggMapFieldMetadata{}, 0, err
	}
	return fieldMeta, newPos, nil
}

// readFieldMetadataWithPos reads field metadata details (offset, length, type, count, aggregates) and returns the updated position
func (r *PreAggMapReader) readFieldMetadataDetailsWithPos(metadataData []byte, pos int64, fieldName string) (EnhancedPreAggMapFieldMetadata, int64, error) {
	if pos+FieldAggOffsetBytes+FieldAggLengthBytes+FieldTypeBytes+FieldMetaAggBytes > int64(len(metadataData)) {
		return EnhancedPreAggMapFieldMetadata{}, 0, errors.New("invalid metadata: invalid field metadata")
	}

	// Read field offset and length
	offset := binary.LittleEndian.Uint64(metadataData[pos : pos+FieldAggOffsetBytes])
	length := binary.LittleEndian.Uint32(metadataData[pos+FieldAggOffsetBytes : pos+FieldAggOffsetBytes+FieldAggLengthBytes])
	newPos := pos + FieldAggOffsetBytes + FieldAggLengthBytes

	// Read field type
	fieldType := FieldType(metadataData[newPos])
	newPos += FieldTypeBytes

	// Read count
	count := binary.LittleEndian.Uint64(metadataData[newPos : newPos+FieldMetaAggBytes])
	newPos += FieldMetaAggBytes

	fieldMeta := EnhancedPreAggMapFieldMetadata{
		FieldName: fieldName,
		Offset:    offset,
		Length:    uint64(length),
		EndPos:    offset + uint64(length),
		Type:      fieldType,
		Count:     count,
	}

	// Read sum, min, max based on field type
	if fieldType == FieldTypeInt || fieldType == FieldTypeFloat {
		if newPos+FieldMetaAggBytes*3 > int64(len(metadataData)) {
			return fieldMeta, newPos, errors.New("invalid metadata: aggregates out of bounds")
		}

		sum := binary.LittleEndian.Uint64(metadataData[newPos : newPos+FieldMetaAggBytes])
		_min := binary.LittleEndian.Uint64(metadataData[newPos+FieldMetaAggBytes : newPos+FieldMetaAggBytes*2])
		_max := binary.LittleEndian.Uint64(metadataData[newPos+FieldMetaAggBytes*2 : newPos+FieldMetaAggBytes*3])

		if fieldType == FieldTypeInt {
			fieldMeta.Sum = int64(sum)
			fieldMeta.Min = int64(_min)
			fieldMeta.Max = int64(_max)
		} else {
			fieldMeta.Sum = math.Float64frombits(sum)
			fieldMeta.Min = math.Float64frombits(_min)
			fieldMeta.Max = math.Float64frombits(_max)
		}
		newPos += FieldMetaAggBytes * 3
	}

	return fieldMeta, newPos, nil
}

// PreAggMapFieldData stores pre agg map data with partial loading support
type PreAggMapFieldData struct {
	lastReadOffset    uint64                         // Last read position in the field data
	maxPreAggMapIndex int                            // Maximum loaded PreAggMap index
	lastRemainBytes   []byte                         // Last Remain Bytes
	loadedPreAgMaps   map[int]*PreAggMapRecord       // Map of loaded PreAggMap by index
	fieldMetadata     EnhancedPreAggMapFieldMetadata // Field metadata for this field
}

// ReadSegmentsAgg reads aggregation data for specified fields and specific segments
func (r *PreAggMapReader) ReadSegmentsAgg(fieldNames []string, fragmentRanges fragment.FragmentRanges) ([]*AggData, error) {
	// First, load metadata for all requested fields
	metadata, err := r.loadMetadata()
	if err != nil {
		return nil, fmt.Errorf("failed to load metadata: %v", err)
	}

	// Process each requested field
	return r.processFieldsForSegments(fieldNames, fragmentRanges, metadata)
}

// processFieldsForSegments processes each requested field to create aggregated data
func (r *PreAggMapReader) processFieldsForSegments(fieldNames []string, fragmentRanges fragment.FragmentRanges, metadata map[string]EnhancedPreAggMapFieldMetadata) ([]*AggData, error) {
	var aggDataList []*AggData

	for _, fieldName := range fieldNames {
		aggData, err := r.processSingleField(fieldName, fragmentRanges, metadata)
		if err != nil {
			return nil, err
		}
		aggDataList = append(aggDataList, aggData)
	}

	return aggDataList, nil
}

// processSingleField processes a single field to create aggregated data
func (r *PreAggMapReader) processSingleField(fieldName string, fragmentRanges fragment.FragmentRanges, metadata map[string]EnhancedPreAggMapFieldMetadata) (*AggData, error) {
	fieldMeta, exists := metadata[fieldName]
	if !exists {
		return &AggData{}, nil
	}

	// Get or create field data
	fieldData, err := r.getFieldData(fieldName, fieldMeta)
	if err != nil {
		return nil, err
	}

	// Filter segments based on fragmentRanges
	filteredSegments := r.filterSegmentsByRange(fieldData.loadedPreAgMaps, fragmentRanges)

	// Create aggregated data based on field type
	return r.createAggregatedData(fieldName, fieldMeta.Type, filteredSegments)
}

// getFieldData gets or initializes field data for the given field
func (r *PreAggMapReader) getFieldData(fieldName string, fieldMeta EnhancedPreAggMapFieldMetadata) (*PreAggMapFieldData, error) {
	fieldKey := r.fullFilePath + ":" + fieldName
	fieldData, ok := r.preAgMaps[fieldKey]
	if !ok {
		// Initialize PreAggMapFieldData if not exists
		fieldData = &PreAggMapFieldData{
			lastReadOffset:    StartOffset,
			maxPreAggMapIndex: StartSegmentId,
			lastRemainBytes:   make([]byte, 0),
			loadedPreAgMaps:   make(map[int]*PreAggMapRecord),
			fieldMetadata:     fieldMeta,
		}
		r.preAgMaps[fieldKey] = fieldData

		// Load all segments for this field
		if err := r.loadAllSegments(fieldName); err != nil {
			return nil, fmt.Errorf("failed to load segments for field %s: %v", fieldName, err)
		}
	}

	return fieldData, nil
}

// loadAllSegments loads all segments for a specific field
func (r *PreAggMapReader) loadAllSegments(fieldName string) error {
	fieldKey := r.fullFilePath + ":" + fieldName
	fieldData := r.preAgMaps[fieldKey]

	// Load all segments until we reach the end of the field data
	for fieldData.lastReadOffset < fieldData.fieldMetadata.Length {
		// Load more data
		if err := r.loadFieldData(fieldName); err != nil {
			return fmt.Errorf("failed to load field data: %v", err)
		}
	}

	return nil
}

// loadFieldData loads pre agg map data for a specific field from the index file with partial loading support
func (r *PreAggMapReader) loadFieldData(fieldName string) error {
	fieldKey := r.fullFilePath + ":" + fieldName
	fieldData := r.preAgMaps[fieldKey]

	// Calculate how much data to read
	readLength := PreAggMapDefaultReadThreshold
	if fieldData.fieldMetadata.Length-fieldData.lastReadOffset < uint64(readLength) {
		readLength = int(fieldData.fieldMetadata.Length - fieldData.lastReadOffset)
	}

	// Prepare for data loading
	if len(fieldData.loadedPreAgMaps) == 0 && fieldData.lastReadOffset == 0 {
		fieldData.loadedPreAgMaps = make(map[int]*PreAggMapRecord)
	}

	// Read file data
	preAggMapData, err := r.readFieldData(fieldName, fieldData, readLength)
	if err != nil {
		return err
	}

	// Parse and store the segments
	return r.parseAndStoreSegments(fieldData, preAggMapData, readLength)
}

// readFieldData reads the field data from the file
func (r *PreAggMapReader) readFieldData(fieldName string, fieldData *PreAggMapFieldData, readLength int) ([]byte, error) {
	// Open file for reading
	file, err := fileops.Open(r.fullFilePath)
	if err != nil {
		return nil, fmt.Errorf("cannot open index file: %v", err)
	}
	defer file.Close()

	// Read partial data
	preAggMapData := make([]byte, readLength)
	if _, err := file.ReadAt(preAggMapData, int64(fieldData.fieldMetadata.Offset)+int64(fieldData.lastReadOffset)); err != nil {
		return nil, fmt.Errorf("failed to read field data for %s: %v", fieldName, err)
	}

	// Append any remaining bytes from previous read
	if len(fieldData.lastRemainBytes) > 0 {
		preAggMapData = append(fieldData.lastRemainBytes, preAggMapData...)
		fieldData.lastRemainBytes = fieldData.lastRemainBytes[:0]
	}

	return preAggMapData, nil
}

// parseAndStoreSegments parses the preAggMapData and stores the segments
func (r *PreAggMapReader) parseAndStoreSegments(fieldData *PreAggMapFieldData, preAggMapData []byte, readLength int) error {
	// Parse individual pre agg maps from the partial data
	dataOffset := 0
	for dataOffset < len(preAggMapData) {
		preAggMap, newDataOffset, err := r.parsePreAggMapSegment(preAggMapData, dataOffset, fieldData.fieldMetadata.Type)
		if err != nil {
			// Store remaining bytes for next read
			fieldData.lastRemainBytes = append(fieldData.lastRemainBytes, preAggMapData[dataOffset:]...)
			break
		}

		// Add the loaded segment to the field's loaded maps
		fieldData.maxPreAggMapIndex++
		fieldData.loadedPreAgMaps[fieldData.maxPreAggMapIndex] = preAggMap
		dataOffset = newDataOffset
	}

	// Update the last read offset
	fieldData.lastReadOffset += uint64(readLength)

	return nil
}

// parsePreAggMapSegment parses a single preAggMap segment from the data
func (r *PreAggMapReader) parsePreAggMapSegment(preAggMapData []byte, dataOffset int, fieldType FieldType) (*PreAggMapRecord, int, error) {
	// Read count
	if dataOffset+util.Int64SizeBytes > len(preAggMapData) {
		return nil, dataOffset, errors.New("not enough data for complete count")
	}

	count := binary.LittleEndian.Uint64(preAggMapData[dataOffset : dataOffset+util.Int64SizeBytes])
	dataOffset += util.Int64SizeBytes

	preAggMap := &PreAggMapRecord{
		Count: count,
	}

	// Read sum, min, max if they exist based on field type
	if fieldType == FieldTypeInt || fieldType == FieldTypeFloat {
		newDataOffset, err := r.readAggValues(preAggMap, preAggMapData, dataOffset, fieldType)
		if err != nil {
			return nil, dataOffset, err
		}
		dataOffset = newDataOffset
	}

	return preAggMap, dataOffset, nil
}

// readAggValues reads the aggregation values (sum, min, max) based on field type
func (r *PreAggMapReader) readAggValues(preAggMap *PreAggMapRecord, preAggMapData []byte, dataOffset int, fieldType FieldType) (int, error) {
	// Read sum, min, max in consistent format (8 bytes each)
	if dataOffset+util.Int64SizeBytes*3 > len(preAggMapData) {
		return dataOffset, errors.New("not enough data for complete sum, min, max")
	}

	// Read sum value
	sumBits := binary.LittleEndian.Uint64(preAggMapData[dataOffset : dataOffset+util.Int64SizeBytes])
	dataOffset += util.Int64SizeBytes
	if fieldType == FieldTypeInt {
		preAggMap.Sum = int64(sumBits)
	} else {
		preAggMap.Sum = math.Float64frombits(sumBits)
	}

	// Read min value
	minBits := binary.LittleEndian.Uint64(preAggMapData[dataOffset : dataOffset+util.Int64SizeBytes])
	dataOffset += util.Int64SizeBytes
	if fieldType == FieldTypeInt {
		preAggMap.Min = int64(minBits)
	} else {
		preAggMap.Min = math.Float64frombits(minBits)
	}

	// Read max value
	maxBits := binary.LittleEndian.Uint64(preAggMapData[dataOffset : dataOffset+util.Int64SizeBytes])
	dataOffset += util.Int64SizeBytes
	if fieldType == FieldTypeInt {
		preAggMap.Max = int64(maxBits)
	} else {
		preAggMap.Max = math.Float64frombits(maxBits)
	}

	return dataOffset, nil
}

// filterSegmentsByRange filters segments based on fragment ranges
func (r *PreAggMapReader) filterSegmentsByRange(loadedPreAgMaps map[int]*PreAggMapRecord, fragmentRanges fragment.FragmentRanges) []*PreAggMapRecord {
	var filteredSegments []*PreAggMapRecord

	for segId, preAggMap := range loadedPreAgMaps {
		// Check if this segment falls within any of the fragment ranges
		for _, fr := range fragmentRanges {
			if uint32(segId) >= fr.Start && uint32(segId) < fr.End {
				filteredSegments = append(filteredSegments, preAggMap)
				break
			}
		}
	}

	return filteredSegments
}

// createAggregatedData creates aggregated data based on field type
func (r *PreAggMapReader) createAggregatedData(fieldName string, fieldType FieldType, filteredSegments []*PreAggMapRecord) (*AggData, error) {
	aggData := &AggData{
		FieldName: fieldName,
		Type:      fieldType,
	}

	switch fieldType {
	case FieldTypeTime:
		// For time fields, only count is aggregated
		var totalCount uint64 = 0
		for _, seg := range filteredSegments {
			totalCount += seg.Count
		}
		aggData.Count = totalCount

	case FieldTypeInt:
		// For int fields, aggregate count, sum, min, max
		aggData.Count, aggData.Sum, aggData.Min, aggData.Max = r.aggregateIntField(filteredSegments)

	case FieldTypeFloat:
		// For float fields, aggregate count, sum, min, max
		aggData.Count, aggData.Sum, aggData.Min, aggData.Max = r.aggregateFloatField(filteredSegments)

	case FieldTypeOther:
		// For other fields, only count is aggregated
		var totalCount uint64 = 0
		for _, seg := range filteredSegments {
			totalCount += seg.Count
		}
		aggData.Count = totalCount
	}

	return aggData, nil
}

// aggregateIntField aggregates int field data (count, sum, min, max)
func (r *PreAggMapReader) aggregateIntField(filteredSegments []*PreAggMapRecord) (uint64, interface{}, interface{}, interface{}) {
	var totalCount uint64 = 0
	var totalSum, globalMin, globalMax int64 = 0, math.MaxInt64, math.MinInt64

	if len(filteredSegments) > 0 {
		globalMin = filteredSegments[0].Min.(int64)
		globalMax = filteredSegments[0].Max.(int64)

		for _, seg := range filteredSegments {
			totalCount += seg.Count
			totalSum += seg.Sum.(int64)

			if seg.Min.(int64) < globalMin {
				globalMin = seg.Min.(int64)
			}
			if seg.Max.(int64) > globalMax {
				globalMax = seg.Max.(int64)
			}
		}
	}

	return totalCount, totalSum, globalMin, globalMax
}

// aggregateFloatField aggregates float field data (count, sum, min, max)
func (r *PreAggMapReader) aggregateFloatField(filteredSegments []*PreAggMapRecord) (uint64, interface{}, interface{}, interface{}) {
	var totalCount uint64 = 0
	var totalSum, globalMin, globalMax float64 = 0, math.MaxFloat64, -math.MaxFloat64

	if len(filteredSegments) > 0 {
		globalMin = filteredSegments[0].Min.(float64)
		globalMax = filteredSegments[0].Max.(float64)

		for _, seg := range filteredSegments {
			totalCount += seg.Count
			totalSum += seg.Sum.(float64)

			if seg.Min.(float64) < globalMin {
				globalMin = seg.Min.(float64)
			}
			if seg.Max.(float64) > globalMax {
				globalMax = seg.Max.(float64)
			}
		}
	}

	return totalCount, totalSum, globalMin, globalMax
}
