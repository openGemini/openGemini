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

package executor

import (
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/lib/fragment"
)

type FileMode uint8

const (
	Detached FileMode = iota
	Attached
)

var (
	_ IndexFrags = &DetachedFrags{}
	_ IndexFrags = &AttachedFrags{}
)

type IndexFrags interface {
	BasePath() string
	FragCount() int64
	IndexCount() int
	Indexes() interface{}
	AppendIndexes(...interface{})
	FragRanges() []fragment.FragmentRanges
	AppendFragRanges(...fragment.FragmentRanges)
	AddFragCount(int64)
	FileMode() FileMode
	SetErr(error)
	GetErr() error
}

type BaseFrags struct {
	fileMode   FileMode
	fragCount  int64
	basePath   string
	err        error
	fragRanges []fragment.FragmentRanges
}

func NewBaseFrags(basePath string, fileMode FileMode) *BaseFrags {
	return &BaseFrags{basePath: basePath, fileMode: fileMode}
}

func (s *BaseFrags) BasePath() string {
	return s.basePath
}

func (s *BaseFrags) FragCount() int64 {
	return s.fragCount
}

func (s *BaseFrags) FragRanges() []fragment.FragmentRanges {
	return s.fragRanges
}

func (s *BaseFrags) AppendFragRanges(frs ...fragment.FragmentRanges) {
	s.fragRanges = append(s.fragRanges, frs...)
}

func (s *BaseFrags) AddFragCount(fragCount int64) {
	s.fragCount += fragCount
}

func (s *BaseFrags) SetErr(err error) {
	s.err = err
}

func (s *BaseFrags) GetErr() error {
	return s.err
}

func (s *BaseFrags) FileMode() FileMode {
	return s.fileMode
}

type DetachedFrags struct {
	BaseFrags
	metaIndexes []*immutable.MetaIndex
}

func NewDetachedFrags(basePath string, cap int) *DetachedFrags {
	baseFileFrags := NewBaseFrags(basePath, Detached)
	baseFileFrags.fragRanges = make([]fragment.FragmentRanges, 0, cap)
	return &DetachedFrags{
		BaseFrags:   *baseFileFrags,
		metaIndexes: make([]*immutable.MetaIndex, 0, cap),
	}
}

func (s *DetachedFrags) IndexCount() int {
	return len(s.metaIndexes)
}

func (s *DetachedFrags) Indexes() interface{} {
	return s.metaIndexes
}

func (s *DetachedFrags) AppendIndexes(metaIndexes ...interface{}) {
	for i := range metaIndexes {
		s.metaIndexes = append(s.metaIndexes, metaIndexes[i].(*immutable.MetaIndex))
	}
}

type AttachedFrags struct {
	BaseFrags
	tsspFiles []immutable.TSSPFile
}

func NewAttachedFrags(basePath string, cap int) *AttachedFrags {
	baseFileFrags := NewBaseFrags(basePath, Attached)
	baseFileFrags.fragRanges = make([]fragment.FragmentRanges, 0, cap)
	return &AttachedFrags{
		BaseFrags: *baseFileFrags,
		tsspFiles: make([]immutable.TSSPFile, 0, cap),
	}
}

func (s *AttachedFrags) IndexCount() int {
	return len(s.tsspFiles)
}

func (s *AttachedFrags) Indexes() interface{} {
	return s.tsspFiles
}

func (s *AttachedFrags) AppendIndexes(tsspFiles ...interface{}) {
	for i := range tsspFiles {
		s.tsspFiles = append(s.tsspFiles, tsspFiles[i].(immutable.TSSPFile))
	}
}

type AttachedIndexInfo struct {
	files []immutable.TSSPFile
	infos []*colstore.PKInfo
}

func NewAttachedIndexInfo(files []immutable.TSSPFile, infos []*colstore.PKInfo) *AttachedIndexInfo {
	return &AttachedIndexInfo{files: files, infos: infos}
}

func (a *AttachedIndexInfo) Files() []immutable.TSSPFile {
	return a.files
}

func (a *AttachedIndexInfo) Infos() []*colstore.PKInfo {
	return a.infos
}

type DetachedIndexInfo struct {
	files []*immutable.MetaIndex
	infos []*colstore.DetachedPKInfo
}

func NewDetachedIndexInfo(files []*immutable.MetaIndex, infos []*colstore.DetachedPKInfo) *DetachedIndexInfo {
	return &DetachedIndexInfo{files: files, infos: infos}
}

func (a *DetachedIndexInfo) Files() []*immutable.MetaIndex {
	return a.files
}

func (a *DetachedIndexInfo) Infos() []*colstore.DetachedPKInfo {
	return a.infos
}

type CSIndexInfo struct {
	AttachedIndexInfo
	version   uint32
	shardPath string
}

func NewCSIndexInfo(shardPath string, info *AttachedIndexInfo, version uint32) *CSIndexInfo {
	return &CSIndexInfo{shardPath: shardPath, AttachedIndexInfo: *info, version: version}
}

func (cs *CSIndexInfo) ShardPath() string {
	return cs.shardPath
}

func (cs *CSIndexInfo) Version() uint32 {
	return cs.version
}
