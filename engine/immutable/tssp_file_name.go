// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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
	"fmt"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/obs"
	"go.uber.org/zap"
)

const (
	maxMerge = 0xffff
)

var (
	tsspFileReq = regexp.MustCompile("^[0-9a-f]{8,16}-[0-9a-f]{4}-[0-9a-f]{8}.tssp(.init)?")
)

type TSSPFileName struct {
	seq    uint64
	level  uint16
	extent uint16
	merge  uint16
	order  bool
	lock   *string
}

func NewTSSPFileName(seq uint64, level, merge, extent uint16, order bool, lockPath *string) TSSPFileName {
	return TSSPFileName{
		seq:    seq,
		level:  level,
		merge:  merge % maxMerge,
		extent: extent,
		order:  order,
		lock:   lockPath,
	}
}

func (n *TSSPFileName) String() string {
	return fmt.Sprintf("%08x-%04x-%04x%04x", n.seq, n.level, n.merge, n.extent)
}

func (n *TSSPFileName) SetOrder(v bool) {
	n.order = v
}

func (n *TSSPFileName) SetSeq(seq uint64) {
	n.seq = seq
}

func (n *TSSPFileName) SetMerge(merge uint16) {
	n.merge = merge
}

func (n *TSSPFileName) SetExtend(extend uint16) {
	n.extent = extend
}

func (n *TSSPFileName) SetLevel(l uint16) {
	n.level = l
}

func (n *TSSPFileName) SetLock(l *string) {
	n.lock = l
}

func (n *TSSPFileName) AddSeq(seq uint64) {
	n.seq += seq
}

func (n *TSSPFileName) path(dir string) string {
	if !n.order {
		dir = path.Join(dir, unorderedDir)

		lock := fileops.FileLockOption(*n.lock)
		if err := fileops.MkdirAll(dir, 0750, lock); err != nil {
			log.Error("create dir failed", zap.String("dir", dir), zap.Error(err))
			panic(err)
		}
	}

	return path.Join(dir, fmt.Sprintf("%s%s", n.String(), tsspFileSuffix))
}

func (n *TSSPFileName) TmpPath(dir string) string {
	return n.path(dir) + tmpFileSuffix
}

func (n *TSSPFileName) Path(dir string, tmp bool) string {
	p := n.path(dir)
	if tmp {
		p += tmpFileSuffix
	}
	return p
}

func validFileName(name string) bool {
	return tsspFileReq.MatchString(name)
}

func getRemoteSuffix(name string) string {
	extend := filepath.Ext(name)
	switch extend {
	case tsspFileSuffix:
		return ""
	case obs.ObsFileSuffix:
		return extend
	default:
		// temporarily unsupported suffixes and illegal file names return ""
		return ""
	}
}

func (n *TSSPFileName) ParseFileName(name string) error {
	base := filepath.Base(name)
	if !validFileName(base) {
		return fmt.Errorf("invalid file name:%v", name)
	}

	nl := len(base)
	var nameStr, remoteSuffix string
	if base[nl-tmpSuffixNameLen:] == tmpFileSuffix {
		remoteSuffix = getRemoteSuffix(base[:nl-tmpSuffixNameLen])
		nameStr = base[:nl-(tsspFileSuffixLen+len(remoteSuffix)+tmpSuffixNameLen)]
	} else {
		remoteSuffix = getRemoteSuffix(base)
		nameStr = base[:nl-(tsspFileSuffixLen+len(remoteSuffix))]
	}

	//00008250-0001-00010001.tssp.init
	tmp := strings.Split(nameStr, "-")
	if len(tmp) != 3 && len(tmp[2]) != 8 {
		return fmt.Errorf("invalid file name:%v", name)
	}

	v, err := strconv.ParseUint(tmp[0], 16, 64)
	if err != nil {
		return fmt.Errorf("invalid file name:%v", name)
	}
	n.seq = v

	v, err = strconv.ParseUint(tmp[1], 16, 64)
	if err != nil {
		return fmt.Errorf("invalid file name:%v", name)
	}
	n.level = uint16(v)

	v, err = strconv.ParseUint(tmp[2][:4], 16, 64)
	if err != nil {
		return fmt.Errorf("invalid file name:%v", name)
	}
	n.merge = uint16(v)

	v, err = strconv.ParseUint(tmp[2][4:], 16, 64)
	if err != nil {
		return fmt.Errorf("invalid file name:%v", name)
	}
	n.extent = uint16(v)

	return nil
}

func (n *TSSPFileName) Equal(other *TSSPFileName) bool {
	return n.seq == other.seq && n.level == other.level && n.merge == other.merge && n.extent == other.extent
}

func IsTempleFile(name string) bool {
	if len(name) < tmpSuffixNameLen {
		return false
	}

	return name[len(name)-tmpSuffixNameLen:] == tmpFileSuffix
}
