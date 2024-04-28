/*
Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.

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

package engine

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"

	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/numberenc"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/util"
)

type ShardMoveFileInfo struct {
	Name       string   // mst names in this shard
	LocalFile  []string // local files involved in this shard move
	RemoteFile []string
}

func (info *ShardMoveFileInfo) marshal(dst []byte) []byte {
	dst = numberenc.MarshalUint16Append(dst, uint16(len(info.Name)))
	dst = append(dst, info.Name...)

	dst = numberenc.MarshalUint16Append(dst, uint16(len(info.LocalFile)))
	for i := range info.LocalFile {
		dst = numberenc.MarshalUint16Append(dst, uint16(len(info.LocalFile[i])))
		dst = append(dst, info.LocalFile[i]...)
	}

	dst = numberenc.MarshalUint16Append(dst, uint16(len(info.RemoteFile)))
	for i := range info.RemoteFile {
		dst = numberenc.MarshalUint16Append(dst, uint16(len(info.RemoteFile[i])))
		dst = append(dst, info.RemoteFile[i]...)
	}
	valueCrc := crc32.ChecksumIEEE(dst)
	dst = numberenc.MarshalUint32Append(dst, valueCrc)
	return dst
}

func (info *ShardMoveFileInfo) unmarshal(src []byte) error {
	if len(src) < 2 {
		return fmt.Errorf("too small data for name length, %v", len(src))
	}

	l := int(numberenc.UnmarshalUint16(src))
	src = src[2:]
	if len(src) < l {
		return fmt.Errorf("too small data for name, %v < %v", len(src), l)
	}
	info.Name, src = util.Bytes2str(src[:l]), src[l:]

	if len(src) < 2 {
		return fmt.Errorf("too small data for localFiles length, %v < %v", len(src), l)
	}
	l, src = int(numberenc.UnmarshalUint16(src)), src[2:]
	if cap(info.LocalFile) < l {
		info.LocalFile = make([]string, l)
	}
	info.LocalFile = info.LocalFile[:l]
	for i := range info.LocalFile {
		if len(src) < 2 {
			return fmt.Errorf("too small data for localFile length, %v", len(src))
		}
		l, src = int(numberenc.UnmarshalUint16(src)), src[2:]

		if len(src) < l {
			return fmt.Errorf("too small data for localFile, %v < %v", len(src), l)
		}
		info.LocalFile[i], src = util.Bytes2str(src[:l]), src[l:]

	}

	if len(src) < 2 {
		return fmt.Errorf("too small data for remoteFiles length, %v < %v", len(src), l+4)
	}
	l, src = int(numberenc.UnmarshalUint16(src)), src[2:]
	if cap(info.RemoteFile) < l {
		info.RemoteFile = make([]string, l)
	}
	info.RemoteFile = info.RemoteFile[:l]
	for i := range info.RemoteFile {
		if len(src) < 2 {
			return fmt.Errorf("too small data for remoteFile length, %v", len(src))
		}
		l, src = int(numberenc.UnmarshalUint16(src)), src[2:]

		if len(src) < l {
			return fmt.Errorf("too small data for remoteFile, %v < %v", len(src), l)
		}
		info.RemoteFile[i], src = util.Bytes2str(src[:l]), src[l:]
	}

	return nil
}

func (info *ShardMoveFileInfo) reset() {
	info.Name = ""
	info.LocalFile = info.LocalFile[:0]
	info.RemoteFile = info.RemoteFile[:0]
}

func readShardMoveLogFile(name string, info *ShardMoveFileInfo) error {
	fi, err := fileops.Stat(name)
	if err != nil {
		return fmt.Errorf("stat shard move log file fail")
	}

	fSize := fi.Size()
	if fSize < CRCLen {
		return fmt.Errorf("too small shard move log file(%v) size %v", name, fSize)
	}

	buf := make([]byte, int(fSize))
	lock := fileops.FileLockOption("")
	pri := fileops.FilePriorityOption(fileops.IO_PRIORITY_NORMAL)
	fd, err := fileops.OpenFile(name, os.O_RDONLY, 0640, lock, pri)
	if err != nil {
		return fmt.Errorf("read shard move log file fail")
	}
	defer util.MustClose(fd)

	n, err := fd.Read(buf)
	if err != nil || n != len(buf) {
		return fmt.Errorf("read shard move log file(%v) fail, file size:%v, read size:%v", name, fSize, n)
	}
	crcValue := buf[fSize-CRCLen:]
	currCrcValue := crc32.ChecksumIEEE(buf[0 : fSize-CRCLen])
	currBytes := make([]byte, CRCLen)
	binary.BigEndian.PutUint32(currBytes, currCrcValue)
	if !bytes.Equal(crcValue, currBytes) {
		return fmt.Errorf("invalid shard move log file(%v) crc", name)
	}

	return info.unmarshal(buf)
}

func shardMoveRecoverReplaceFiles(logInfo *ShardMoveFileInfo, lockPath *string) error {
	var err error
	var exist bool
	var tmpFile string
	for i := range logInfo.RemoteFile {
		// remote file "xxx.init" exist, drop remote file and keep local file
		exist, err = fileops.IsObsFile(logInfo.RemoteFile[i])
		if err != nil {
			return err
		}
		if exist {
			err = fileops.Remove(logInfo.RemoteFile[i])
			if err != nil {
				return err
			}
			continue
		}
		// remote file "xxx" exist, remove local file if it's exist
		tmpFile = logInfo.RemoteFile[i][:len(logInfo.RemoteFile[i])-len(obs.ObsFileTmpSuffix)]
		exist, err = fileops.IsObsFile(tmpFile)
		if err != nil {
			return err
		}
		if exist {
			if _, err = fileops.Stat(logInfo.LocalFile[i]); os.IsNotExist(err) {
				continue
			}
			lock := fileops.FileLockOption(*lockPath)
			if err := fileops.Remove(logInfo.LocalFile[i], lock); err != nil {
				return err
			}
		}
	}
	return nil
}
