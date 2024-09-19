// Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.
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

package immutable

import (
	"errors"
	"io"
	"reflect"
	"testing"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"go.uber.org/zap"
)

func TestTagSets_Add(t *testing.T) {
	tagSets := NewTagSets()
	tagSets.Add("testKey", "testValue")
	if tagSets.TagKVCount() != 1 {
		t.Fatalf("")
	}
	if !reflect.DeepEqual(tagSets.sets, map[string]map[string]struct{}{"testKey": {"testValue": struct{}{}}}) {
		t.Fatalf("")
	}

	tagSets.Add("testKey", "testValue2")
	tagSets.Add("testKey2", "testValue3")
	if tagSets.TagKVCount() != 3 {
		t.Fatalf("TagKVCount expected: %v, actual: %v", 3, tagSets.TagKVCount())
	}
	want := map[string]map[string]struct{}{
		"testKey":  {"testValue": struct{}{}, "testValue2": struct{}{}},
		"testKey2": {"testValue3": struct{}{}},
	}
	if !reflect.DeepEqual(tagSets.sets, want) {
		t.Fatalf("TagSets sets expected: %v, actual: %v", want, tagSets)
	}
}

func TestTagSets_ForEach(t *testing.T) {
	tagSets := NewTagSets()
	tagSets.Add("testKey", "testValue")
	tagSets.Add("testKey", "testValue2")
	tagSets.Add("testKey2", "testValue3")

	want := map[string]map[string]struct{}{
		"testKey":  {"testValue": struct{}{}, "testValue2": struct{}{}},
		"testKey2": {"testValue3": struct{}{}},
	}
	tagSets.ForEach(func(tagKey, tagValue string) {
		if values, tagKeyExist := want[tagKey]; !tagKeyExist {
			t.Fatalf("TagSets.ForEach failed, actual:%+v, expected:%+v", tagSets.sets, want)
		} else {
			if _, valueExist := values[tagValue]; !valueExist {
				t.Fatalf("TagSets.ForEach failed, actual:%+v, expected:%+v", tagSets.sets, want)
			}
		}
	})
}

func TestSequenceIterator_Run(t *testing.T) {
	tsspFiles := []TSSPFile{
		&MockTSSPFileSeqIterator{
			MetaIndexItemNumFn: func() int64 { return 3 },
			RefFileReaderFn:    func() {},
			UnrefFileReaderFn:  func() {},
		},
	}

	metaReader := &MockChunkMetasReader{
		ReadChunkMetasFn: func(f TSSPFile, idx int) ([]byte, []uint32, error) { return make([]byte, 10), make([]uint32, 10), nil },
	}

	errSearchSeriesKey := errno.NewError(errno.ErrSearchSeriesKey)
	mockErr := errors.New("mock error")

	for _, testcase := range []struct {
		Name    string
		Handler SequenceIteratorHandler
		Err     error
	}{
		{
			Name: "normal",
			Handler: &MockSequenceIteratorHandler{
				BeginFn:         func() {},
				FinishFn:        func() {},
				NextFileFn:      func(file TSSPFile) {},
				NextChunkMetaFn: func(bytes []byte) error { return nil },
				LimitedFn:       func() bool { return false },
			},
			Err: nil,
		},
		{
			Name: "handler limited",
			Handler: &MockSequenceIteratorHandler{
				BeginFn:         func() {},
				FinishFn:        func() {},
				NextFileFn:      func(file TSSPFile) {},
				NextChunkMetaFn: func(bytes []byte) error { return nil },
				LimitedFn:       func() bool { return true },
			},
			Err: io.EOF,
		},
		{
			Name: "ErrSearchSeriesKey",
			Handler: &MockSequenceIteratorHandler{
				BeginFn:         func() {},
				FinishFn:        func() {},
				NextFileFn:      func(file TSSPFile) {},
				NextChunkMetaFn: func(bytes []byte) error { return errSearchSeriesKey },
				LimitedFn:       func() bool { return false },
			},
			Err: nil,
		},
		{
			Name: "Unknown Error",
			Handler: &MockSequenceIteratorHandler{
				BeginFn:         func() {},
				FinishFn:        func() {},
				NextFileFn:      func(file TSSPFile) {},
				NextChunkMetaFn: func(bytes []byte) error { return mockErr },
				LimitedFn:       func() bool { return false },
			},
			Err: mockErr,
		},
	} {
		t.Run(testcase.Name, func(t *testing.T) {
			iterator := NewSequenceIterator(testcase.Handler, logger.NewLogger(errno.ModuleUnknown).SetZapLogger(zap.NewNop()))
			iterator.SetChunkMetasReader(metaReader)
			iterator.AddFiles(tsspFiles)
			err := iterator.Run()
			if !errors.Is(err, testcase.Err) {
				t.Fatalf("SequenceIterator.Run failed, actual %v, expected: %v", err, testcase.Err)
			}
		})
	}

}

type MockSequenceIteratorHandler struct {
	InitFn          func(map[string]interface{}) error
	BeginFn         func()
	NextFileFn      func(TSSPFile)
	NextChunkMetaFn func([]byte) error
	LimitedFn       func() bool
	FinishFn        func()
}

func (h *MockSequenceIteratorHandler) Init(param map[string]interface{}) error {
	return h.InitFn(param)
}

func (h *MockSequenceIteratorHandler) Begin() {
	h.BeginFn()
}

func (h *MockSequenceIteratorHandler) NextFile(f TSSPFile) {
	h.NextFileFn(f)
}

func (h *MockSequenceIteratorHandler) NextChunkMeta(buf []byte) error {
	return h.NextChunkMetaFn(buf)
}

func (h *MockSequenceIteratorHandler) Limited() bool {
	return h.LimitedFn()
}

func (h *MockSequenceIteratorHandler) Finish() {
	h.FinishFn()
}

type MockTSSPFileSeqIterator struct {
	TSSPFile
	PathFn                  func() string
	NameFn                  func() string
	FileNameFn              func() TSSPFileName
	LevelAndSequenceFn      func() (uint16, uint64)
	FileNameMergeFn         func() uint16
	FileNameExtendFn        func() uint16
	IsOrderFn               func() bool
	RefFn                   func()
	UnrefFn                 func()
	RefFileReaderFn         func()
	UnrefFileReaderFn       func()
	StopFn                  func()
	InuseFn                 func() bool
	MetaIndexAtFn           func(idx int) (*MetaIndex, error)
	MetaIndexFn             func(id uint64, tr util.TimeRange) (int, *MetaIndex, error)
	ChunkMetaFn             func(id uint64, offset int64, size, itemCount uint32, metaIdx int, ctx *ChunkMetaContext, ioPriority int) (*ChunkMeta, error)
	ReadAtFn                func(cm *ChunkMeta, segment int, dst *record.Record, decs *ReadContext, ioPriority int) (*record.Record, error)
	ReadDataFn              func(offset int64, size uint32, dst *[]byte, ioPriority int) ([]byte, error)
	ReadChunkMetaDataFn     func(metaIdx int, m *MetaIndex, dst []ChunkMeta, ioPriority int) ([]ChunkMeta, error)
	FileStatFn              func() *Trailer
	FileSizeFn              func() int64
	InMemSizeFn             func() int64
	ContainsFn              func(id uint64) (bool, error)
	ContainsByTimeFn        func(tr util.TimeRange) (bool, error)
	ContainsValueFn         func(id uint64, tr util.TimeRange) (bool, error)
	MinMaxTimeFn            func() (int64, int64, error)
	OpenFn                  func() error
	CloseFn                 func() error
	LoadIntoMemoryFn        func() error
	LoadComponentsFn        func() error
	LoadIdTimesFn           func(p *IdTimePairs) error
	RenameFn                func(newName string) error
	RemoveFn                func() error
	FreeMemoryFn            func(evictLock bool) int64
	FreeFileHandleFn        func() error
	VersionFn               func() uint64
	AverageChunkRowsFn      func() int
	MaxChunkRowsFn          func() int
	MetaIndexItemNumFn      func() int64
	AddToEvictListFn        func(level uint16)
	RemoveFromEvictListFn   func(level uint16)
	GetFileReaderRefFn      func() int64
	RenameOnObsFn           func(obsName string, tmp bool, opt *obs.ObsOptions) error
	ChunkMetaCompressModeFn func() uint8
}

func (file *MockTSSPFileSeqIterator) Path() string           { return file.PathFn() }
func (file *MockTSSPFileSeqIterator) Name() string           { return file.NameFn() }
func (file *MockTSSPFileSeqIterator) FileName() TSSPFileName { return file.FileNameFn() }
func (file *MockTSSPFileSeqIterator) LevelAndSequence() (uint16, uint64) {
	return file.LevelAndSequenceFn()
}
func (file *MockTSSPFileSeqIterator) FileNameMerge() uint16  { return file.FileNameMergeFn() }
func (file *MockTSSPFileSeqIterator) FileNameExtend() uint16 { return file.FileNameExtendFn() }
func (file *MockTSSPFileSeqIterator) IsOrder() bool          { return file.IsOrderFn() }
func (file *MockTSSPFileSeqIterator) Ref()                   { file.RefFn() }
func (file *MockTSSPFileSeqIterator) Unref()                 { file.UnrefFn() }
func (file *MockTSSPFileSeqIterator) RefFileReader()         { file.RefFileReaderFn() }
func (file *MockTSSPFileSeqIterator) UnrefFileReader()       { file.UnrefFileReaderFn() }
func (file *MockTSSPFileSeqIterator) Stop()                  { file.StopFn() }
func (file *MockTSSPFileSeqIterator) Inuse() bool            { return file.InuseFn() }
func (file *MockTSSPFileSeqIterator) MetaIndexAt(idx int) (*MetaIndex, error) {
	return file.MetaIndexAtFn(idx)
}
func (file *MockTSSPFileSeqIterator) MetaIndex(id uint64, tr util.TimeRange) (int, *MetaIndex, error) {
	return file.MetaIndexFn(id, tr)
}
func (file *MockTSSPFileSeqIterator) ChunkMeta(id uint64, offset int64, size, itemCount uint32, metaIdx int, ctx *ChunkMetaContext, ioPriority int) (*ChunkMeta, error) {
	return file.ChunkMetaFn(id, offset, size, itemCount, metaIdx, ctx, ioPriority)
}
func (file *MockTSSPFileSeqIterator) ReadAt(cm *ChunkMeta, segment int, dst *record.Record, decs *ReadContext, ioPriority int) (*record.Record, error) {
	return file.ReadAtFn(cm, segment, dst, decs, ioPriority)
}
func (file *MockTSSPFileSeqIterator) ReadData(offset int64, size uint32, dst *[]byte, ioPriority int) ([]byte, error) {
	return file.ReadDataFn(offset, size, dst, ioPriority)
}
func (file *MockTSSPFileSeqIterator) ReadChunkMetaData(metaIdx int, m *MetaIndex, dst []ChunkMeta, ioPriority int) ([]ChunkMeta, error) {
	return file.ReadChunkMetaDataFn(metaIdx, m, dst, ioPriority)
}
func (file *MockTSSPFileSeqIterator) FileStat() *Trailer               { return file.FileStatFn() }
func (file *MockTSSPFileSeqIterator) FileSize() int64                  { return file.FileSizeFn() }
func (file *MockTSSPFileSeqIterator) InMemSize() int64                 { return file.InMemSizeFn() }
func (file *MockTSSPFileSeqIterator) Contains(id uint64) (bool, error) { return file.ContainsFn(id) }
func (file *MockTSSPFileSeqIterator) ContainsByTime(tr util.TimeRange) (bool, error) {
	return file.ContainsByTimeFn(tr)
}
func (file *MockTSSPFileSeqIterator) ContainsValue(id uint64, tr util.TimeRange) (bool, error) {
	return file.ContainsValueFn(id, tr)
}
func (file *MockTSSPFileSeqIterator) MinMaxTime() (int64, int64, error) { return file.MinMaxTimeFn() }
func (file *MockTSSPFileSeqIterator) Open() error                       { return file.OpenFn() }
func (file *MockTSSPFileSeqIterator) Close() error                      { return file.CloseFn() }
func (file *MockTSSPFileSeqIterator) LoadIntoMemory() error             { return file.LoadIntoMemoryFn() }
func (file *MockTSSPFileSeqIterator) LoadComponents() error             { return file.LoadComponentsFn() }
func (file *MockTSSPFileSeqIterator) LoadIdTimes(p *IdTimePairs) error  { return file.LoadIdTimesFn(p) }
func (file *MockTSSPFileSeqIterator) Rename(newName string) error       { return file.RenameFn(newName) }
func (file *MockTSSPFileSeqIterator) Remove() error                     { return file.RemoveFn() }
func (file *MockTSSPFileSeqIterator) FreeMemory(evictLock bool) int64 {
	return file.FreeMemoryFn(evictLock)
}
func (file *MockTSSPFileSeqIterator) FreeFileHandle() error       { return file.FreeFileHandleFn() }
func (file *MockTSSPFileSeqIterator) Version() uint64             { return file.VersionFn() }
func (file *MockTSSPFileSeqIterator) AverageChunkRows() int       { return file.AverageChunkRowsFn() }
func (file *MockTSSPFileSeqIterator) MaxChunkRows() int           { return file.MaxChunkRowsFn() }
func (file *MockTSSPFileSeqIterator) MetaIndexItemNum() int64     { return file.MetaIndexItemNumFn() }
func (file *MockTSSPFileSeqIterator) AddToEvictList(level uint16) { file.AddToEvictListFn(level) }
func (file *MockTSSPFileSeqIterator) RemoveFromEvictList(level uint16) {
	file.RemoveFromEvictListFn(level)
}
func (file *MockTSSPFileSeqIterator) GetFileReaderRef() int64 { return file.GetFileReaderRefFn() }
func (file *MockTSSPFileSeqIterator) RenameOnObs(obsName string, tmp bool, opt *obs.ObsOptions) error {
	return file.RenameOnObsFn(obsName, tmp, opt)
}
func (file *MockTSSPFileSeqIterator) ChunkMetaCompressMode() uint8 {
	return file.ChunkMetaCompressModeFn()
}

type MockChunkMetasReader struct {
	ReadChunkMetasFn func(f TSSPFile, idx int) ([]byte, []uint32, error)
}

func (r *MockChunkMetasReader) ReadChunkMetas(f TSSPFile, idx int) ([]byte, []uint32, error) {
	return r.ReadChunkMetasFn(f, idx)
}
