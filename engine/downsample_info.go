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

package engine

import (
	"fmt"
	"hash/crc32"

	"github.com/openGemini/openGemini/lib/numberenc"
	"github.com/openGemini/openGemini/lib/util"
)

type DownSampleFilesInfo struct {
	taskID   uint64
	level    int
	Names    []string
	OldFiles [][]string
	NewFiles [][]string
}

func (info *DownSampleFilesInfo) reset() {
	info.Names = info.Names[:0]
	info.OldFiles = info.OldFiles[:0]
	info.NewFiles = info.NewFiles[:0]
}

func (info DownSampleFilesInfo) marshal(dst []byte) []byte {
	dst = numberenc.MarshalUint64Append(dst, info.taskID)
	dst = numberenc.MarshalInt64Append(dst, int64(info.level))

	dst = numberenc.MarshalUint16Append(dst, uint16(len(info.Names)))
	for k := range info.Names {
		dst = numberenc.MarshalUint16Append(dst, uint16(len(info.Names[k])))
		dst = append(dst, info.Names[k]...)
	}

	dst = numberenc.MarshalUint16Append(dst, uint16(len(info.OldFiles)))
	for i := range info.OldFiles {
		dst = numberenc.MarshalUint16Append(dst, uint16(len(info.OldFiles[i])))
		for k := range info.OldFiles[i] {
			dst = numberenc.MarshalUint16Append(dst, uint16(len(info.OldFiles[i][k])))
			dst = append(dst, info.OldFiles[i][k]...)
		}
	}

	dst = numberenc.MarshalUint16Append(dst, uint16(len(info.NewFiles)))
	for i := range info.NewFiles {
		dst = numberenc.MarshalUint16Append(dst, uint16(len(info.NewFiles[i])))
		for k := range info.NewFiles[i] {
			dst = numberenc.MarshalUint16Append(dst, uint16(len(info.NewFiles[i][k])))
			dst = append(dst, info.NewFiles[i][k]...)
		}
	}
	valueCrc := crc32.ChecksumIEEE(dst)
	dst = numberenc.MarshalUint32Append(dst, valueCrc)
	return dst
}

func (info *DownSampleFilesInfo) unmarshal(src []byte) ([]byte, error) {
	if len(src) < 20 {
		return src, fmt.Errorf("too small data for name length, %v", len(src))
	}
	info.taskID = numberenc.UnmarshalUint64(src)
	src = src[8:]

	info.level = int(numberenc.UnmarshalInt64(src))
	_, src = info.level, src[8:]

	l := int(numberenc.UnmarshalUint16(src))
	src = src[2:]
	if len(src) < l+4 {
		return src, fmt.Errorf("too small data for name, %v < %v", len(src), l+4)
	}
	if cap(info.Names) < l {
		info.Names = make([]string, l)
	}
	info.Names = info.Names[:l]
	for i := range info.Names {
		if len(src) < 2 {
			return src, fmt.Errorf("too small data for mstName, %v", len(src))
		}
		l, src = int(numberenc.UnmarshalUint16(src)), src[2:]
		if len(src) < l {
			return src, fmt.Errorf("too small data for name, %v < %v", len(src), l)
		}
		info.Names[i], src = util.Bytes2str(src[:l]), src[l:]
	}

	if len(src) < 2 {
		return src, fmt.Errorf("too small data for name, %v < %v", len(src), l+4)
	}
	l, src = int(numberenc.UnmarshalUint16(src)), src[2:]
	if cap(info.OldFiles) < l {
		info.OldFiles = make([][]string, l)
	}
	info.OldFiles = info.OldFiles[:l]
	for i := range info.OldFiles {
		if len(src) < 2 {
			return src, fmt.Errorf("too small data for newFiles, %v", len(src))
		}
		l, src = int(numberenc.UnmarshalUint16(src)), src[2:]
		if len(src) < l {
			return src, fmt.Errorf("too small data for name, %v < %v", len(src), l)
		}
		if cap(info.OldFiles[i]) < l {
			info.OldFiles[i] = make([]string, l)
		}
		info.OldFiles[i] = info.OldFiles[i][:l]
		for k := range info.OldFiles[i] {
			if len(src) < 2 {
				return src, fmt.Errorf("too small data for newFiles, %v", len(src))
			}
			l, src = int(numberenc.UnmarshalUint16(src)), src[2:]
			if len(src) < l {
				return src, fmt.Errorf("too small data for newFiles, %v < %v", len(src), l)
			}
			info.OldFiles[i][k], src = util.Bytes2str(src[:l]), src[l:]
		}
	}

	if len(src) < 2 {
		return src, fmt.Errorf("too small data for name, %v < %v", len(src), l+4)
	}
	l, src = int(numberenc.UnmarshalUint16(src)), src[2:]
	if cap(info.NewFiles) < l {
		info.NewFiles = make([][]string, l)
	}
	info.NewFiles = info.NewFiles[:l]
	for i := range info.NewFiles {
		if len(src) < 2 {
			return src, fmt.Errorf("too small data for newFiles, %v", len(src))
		}
		l, src = int(numberenc.UnmarshalUint16(src)), src[2:]
		if len(src) < l {
			return src, fmt.Errorf("too small data for name, %v < %v", len(src), l)
		}
		if cap(info.NewFiles[i]) < l {
			info.NewFiles[i] = make([]string, l)
		}
		info.NewFiles[i] = info.NewFiles[i][:l]
		for k := range info.NewFiles[i] {
			if len(src) < 2 {
				return src, fmt.Errorf("too small data for newFiles, %v", len(src))
			}
			l, src = int(numberenc.UnmarshalUint16(src)), src[2:]
			if len(src) < l {
				return src, fmt.Errorf("too small data for newFiles, %v < %v", len(src), l)
			}
			info.NewFiles[i][k], src = util.Bytes2str(src[:l]), src[l:]
		}
	}
	return src, nil
}
