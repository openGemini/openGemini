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
	"fmt"
	"math"
	"path"
	"slices"

	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/index"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"go.uber.org/zap"
)

const (
	// SimpleSpatialIndexThreshold Simple spatial indexing is used when the number of data rows is less than this value
	SimpleSpatialIndexThreshold       = 500
	SpatialBlockNull            uint8 = 0x00
	SpatialBlockMBR             uint8 = 0x01 // Minimum Bounding Rectangle
	SpatialBlockNineMBR         uint8 = 0x02 // 1 big MBR + 9 small MBR( Minimum Bounding Circle)
)

type SpatialWriter struct {
	skipIndexWriter

	filePath string
	latName  string
	lonName  string
	lat      []float64
	lon      []float64

	written        int
	segmentOffsets []int64
	swap           []float64
}

func NewSpatialWriter(dir, name, lockPath string, fields []string) *SpatialWriter {
	w := &SpatialWriter{}
	w.lockPath = lockPath
	w.Open()

	if len(fields) >= 2 {
		w.latName, w.lonName = fields[0], fields[1]
	}

	w.filePath = path.Join(dir, colstore.AppendSecondaryIndexSuffix(name, "", index.Spatial, 0))
	return w
}

func (w *SpatialWriter) Flush() error {
	n := len(w.segmentOffsets)
	writer, err := w.GetWriter(w.filePath)
	if err != nil {
		return err
	}

	w.segmentOffsets = append(w.segmentOffsets, int64(n*util.Int64SizeBytes))
	err = writer.WriteData(util.Int64Slice2byte(w.segmentOffsets))
	return err
}

func (w *SpatialWriter) Len() int {
	return len(w.lat)
}

func (w *SpatialWriter) Swap(i, j int) {
	w.lat[i], w.lat[j] = w.lat[j], w.lat[i]
	w.lon[i], w.lon[j] = w.lon[j], w.lon[i]
}

func (w *SpatialWriter) Less(i, j int) bool {
	if w.lat[i] < w.lat[j] {
		return true
	}
	return w.lon[i] < w.lon[j]
}

func (w *SpatialWriter) FlushSegment() error {
	if len(w.lat) != len(w.lon) {
		return fmt.Errorf("lat/lon mismatch")
	}

	writer, err := w.GetWriter(w.filePath)
	if err != nil {
		return err
	}
	defer func() {
		w.segmentOffsets = append(w.segmentOffsets, int64(w.written))
		w.lat = w.lat[:0]
		w.lon = w.lon[:0]
	}()

	n := len(w.lat)
	if n == 0 {
		w.written++
		return writer.WriteData([]byte{SpatialBlockNull})
	}

	typ := SpatialBlockMBR
	if len(w.lat) >= SimpleSpatialIndexThreshold {
		typ = SpatialBlockNineMBR
	}

	err = writer.WriteData([]byte{typ})
	if err != nil {
		return err
	}
	w.written++

	mbr := w.buildRectBound()
	err = writer.WriteData(util.Float64Slice2byte(mbr))
	if err != nil {
		return err
	}
	w.written += util.Float64SizeBytes * len(mbr)

	if typ == SpatialBlockMBR {
		return nil
	}

	mbc := w.buildSubRectBound(mbr)
	err = writer.WriteData(util.Float64Slice2byte(mbc))
	if err != nil {
		return err
	}
	w.written += util.Float64SizeBytes * len(mbc)

	return nil
}

func (w *SpatialWriter) buildRectBound() []float64 {
	n := len(w.lat)
	w.swap = append(w.swap[:0], w.lat[n-1], w.lon[n-1], w.lat[n-1], w.lon[n-1])

	for i := range n - 1 {
		updateMinBoundRect(w.swap, w.lat[i], w.lon[i])
	}

	return w.swap
}

func (w *SpatialWriter) buildSubRectBound(rectBound []float64) []float64 {
	rects := [9][]float64{}
	latMin, lonMin := rectBound[0], rectBound[1]
	latMax, lonMax := rectBound[2], rectBound[3]

	var delta = func(vMax, vMin float64) float64 {
		return (vMax - vMin) * 1e6
	}

	// Nine-square grid step length
	latSep := uint64(math.Ceil(delta(latMax, latMin) / 3))
	lonSep := uint64(math.Ceil(delta(lonMax, lonMin) / 3))

	for i := range len(w.lat) {
		lat, lon := w.lat[i], w.lon[i]

		// calculate the grid cell indices of the coordinate point in the 3x3 grid
		k := uint64(delta(lat, latMin))/latSep*3 + uint64(delta(lon, lonMin))/lonSep

		rect := rects[k]
		if len(rect) == 0 {
			rects[k] = []float64{lat, lon, lat, lon}
			continue
		}

		updateMinBoundRect(rect, lat, lon)
	}

	for _, rect := range rects {
		if len(rect) > 0 {
			w.swap = append(w.swap[:0], rect...)
		}
	}

	return w.swap
}

func updateMinBoundRect(mbr []float64, lat, lon float64) {
	if mbr[0] > lat {
		mbr[0] = lat
	}
	if mbr[1] > lon {
		mbr[1] = lon
	}
	if mbr[2] < lat {
		mbr[2] = lat
	}
	if mbr[3] < lon {
		mbr[3] = lon
	}
}

func (w *SpatialWriter) CreateAttachIndex(writeRec *record.Record, schemaIdx, _ []int) error {
	if len(schemaIdx) == 0 || writeRec.Len() != len(schemaIdx) {
		return nil
	}

	for _, idx := range schemaIdx {
		ref := &writeRec.Schema[idx]
		if ref.Type != influx.Field_Type_Float {
			return fmt.Errorf("invalid field type: %v, want: %v", ref.Type, influx.Field_Type_Float)
		}

		switch ref.Name {
		case w.latName:
			w.lat = append(w.lat, writeRec.ColVals[idx].FloatValues()...)
		case w.lonName:
			w.lon = append(w.lon, writeRec.ColVals[idx].FloatValues()...)
		default:
			logger.GetLogger().Warn("invalid column name",
				zap.String("got", ref.Name),
				zap.Strings("want", []string{w.lonName, w.latName}))
		}
	}

	return nil
}

func (w *SpatialWriter) CreateDetachIndex(writeRec *record.Record, schemaIdx, rowsPerSegment []int, dataBuf [][]byte) ([][]byte, []string) {
	return nil, nil
}

type SpatialReader struct {
	fileops.Reader

	file string
	lock string

	tailSize   int64
	segOffsets []int64
}

func NewSpatialReader(file string, lock string) *SpatialReader {
	return &SpatialReader{
		file: file,
		lock: lock,
	}
}

func (r *SpatialReader) Open() error {
	err := r.Reader.Open(r.file, r.lock)
	if err != nil {
		return err
	}

	tailSize := int64(8) // Number of bytes of r.segCount
	if r.Size() < tailSize {
		return fmt.Errorf("spatial index size too small: %d, want: %d", r.Size(), tailSize)
	}

	var buf [8]byte
	_, err = r.File().ReadAt(buf[:], r.Size()-8)
	if err != nil {
		return err
	}

	offsetSize := int64(0)
	util.Bytes2Value(buf[:], &offsetSize)
	tailSize += offsetSize
	if r.Size() < tailSize {
		return fmt.Errorf("spatial index too small: %d, want: %d", r.Size(), tailSize)
	}

	r.tailSize = tailSize
	segBuf := make([]byte, offsetSize)
	_, err = r.File().ReadAt(segBuf, r.Size()-tailSize)

	r.segOffsets = util.Bytes2Int64Slice(segBuf)
	return err
}

func (r *SpatialReader) SegmentOffsets() []int64 {
	return r.segOffsets
}

func (r *SpatialReader) ReadSegment(dst []byte, idx int) ([]byte, error) {
	var size int64
	if idx < len(r.segOffsets)-1 {
		size = r.segOffsets[idx+1] - r.segOffsets[idx]
	} else {
		size = r.Size() - r.tailSize
	}

	dst = slices.Grow(dst, int(size))[:len(dst)+int(size)]
	_, err := r.File().ReadAt(dst[len(dst):], size)
	return dst, err
}
