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

package config

import (
	"errors"
	"log"
	"time"

	"github.com/influxdata/influxdb/toml"
)

const (
	defaultMaxUnorderedFileSize   = 8 * GB
	defaultMaxUnorderedFileNumber = 64
	defaultMaxMergeSelfLevel      = 0
	defaultMinInterval            = 300 * time.Second
	defaultStreamMergeModeLevel   = 2
)

var defaultLevelMergeFileNum = []int{8, 8}

type Merge struct {
	// merge only unordered data
	UnorderedMergeSelf bool `toml:"unordered-merge-self"`

	// The total size of unordered files to be merged each time cannot exceed MaxUnorderedFileSize
	MaxUnorderedFileSize toml.Size `toml:"max-unordered-file-size"`
	// The number of unordered files to be merged each time cannot exceed MaxUnorderedFileNumber
	MaxUnorderedFileNumber int `toml:"max-unordered-file-number"`

	MaxMergeSelfLevel uint16 `toml:"max-merge-self-level"`

	MaxNumOfFileToMergeSelf []int `toml:"max-num-of-file-to-merge-self"`

	MinInterval toml.Duration `toml:"min-interval"`

	StreamMergeModeLevel int `toml:"stream-merge-mode-level"`
}

func NewMergeConfig() *Merge {
	return &Merge{
		UnorderedMergeSelf:      false,
		MaxUnorderedFileSize:    defaultMaxUnorderedFileSize,
		MaxUnorderedFileNumber:  defaultMaxUnorderedFileNumber,
		MinInterval:             toml.Duration(defaultMinInterval),
		MaxMergeSelfLevel:       defaultMaxMergeSelfLevel,
		MaxNumOfFileToMergeSelf: defaultLevelMergeFileNum[:],
		StreamMergeModeLevel:    defaultStreamMergeModeLevel,
	}
}

func (m *Merge) SetMaxNumOfFileToMergeSelf(fileNums []int) error {
	if uint16(len(fileNums)) < m.MaxMergeSelfLevel {
		log.Println(" SetMaxNumOfFileToMergeSelf failed", "merge file levels not valid",
			" set merge file levels:", fileNums, "  max merge file level:", m.MaxMergeSelfLevel)
		return errors.New("invalid merge file levels, merger file levels should not less than max merge level")
	}

	copied := make([]int, len(fileNums))
	copy(copied, fileNums)
	m.MaxNumOfFileToMergeSelf = copied
	log.Println(" SetMaxNumOfFileToMergeSelf success,", " merge file levels:", m.MaxNumOfFileToMergeSelf)
	return nil
}

func (m *Merge) GetMaxNumOfFileToMergeSelf() []int {
	fileNums := make([]int, len(m.MaxNumOfFileToMergeSelf))
	copy(fileNums, m.MaxNumOfFileToMergeSelf)
	return fileNums
}

func (m *Merge) SetMaxMergeSelfLevel(level uint16) error {
	fileNums := uint16(len(m.MaxNumOfFileToMergeSelf))
	if fileNums < level {
		log.Println("SetMaxMergeSelfLevel failed,", " max merge level invalid",
			" merge file levels:", m.MaxNumOfFileToMergeSelf, "set max level:", level)
		return errors.New("invalid max merge level, max merge level should be less than merge file levels")
	}
	m.MaxMergeSelfLevel = level
	log.Println("SetMaxMergeSelfLevel success", " max merge level:", m.MaxMergeSelfLevel)
	return nil
}

func (m *Merge) GetMaxMergeSelfLevel() uint16 {
	totalLevels := uint16(len(m.MaxNumOfFileToMergeSelf))
	if m.MaxMergeSelfLevel > totalLevels {
		log.Println("found invalid max merge self level:", m.MaxMergeSelfLevel, " return current max level:", totalLevels)
		return totalLevels
	}
	return m.MaxMergeSelfLevel
}
