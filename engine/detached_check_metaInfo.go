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

package engine

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"

	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/index/sparseindex"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/logstore"
	"github.com/openGemini/openGemini/lib/numberenc"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/util"
	"go.uber.org/zap"
)

type DetachedMetaInfo struct {
	lastMetaIdxSize      uint32
	lastChunkMetaSize    uint32
	lastPkMetaSize       uint32
	lastPkMetaOff        uint32
	lastMetaIdxOff       int64
	lastChunkMetaOff     int64
	lastMetaIdxBlockId   uint64
	lastPkMetaEndBlockId uint64
	obsOpt               *obs.ObsOptions
}

func NewDetachedMetaInfo() *DetachedMetaInfo {
	return &DetachedMetaInfo{}
}

func (d *DetachedMetaInfo) checkAndTruncateDetachedFiles(dir, msName string, bfCols []string) error {
	filePath := d.getDetachedPrefixFilePath(dir, msName)
	//check detached metaIndex file
	err := d.checkAndTruncateMetaIndexFile(filePath)
	if err != nil {
		return err
	}

	//check detached chunkMeta file
	err = d.checkAndTruncateChunkMeta(filePath)
	if err != nil {
		return err
	}

	//check detached data file
	err = d.checkAndTruncateDataFile(filePath)
	if err != nil {
		return err
	}

	//check detached pkMeta index file
	err = d.checkAndTruncatePkMetaIdxFile(filePath)
	if err != nil {
		return err
	}

	//check detached pkIndex file
	err = d.checkAndTruncatePkIdxFile(filePath)
	if err != nil {
		return err
	}

	//check bloom filter files
	return d.checkAndTruncateBfFiles(dir, msName, bfCols)
}

func (d *DetachedMetaInfo) getDetachedPrefixFilePath(dir, msName string) string {
	filePath := filepath.Join(dir, msName)
	prefixDataPathLength := len(obs.GetPrefixDataPath())
	filePath = filePath[prefixDataPathLength:]
	return filePath
}

func (d *DetachedMetaInfo) checkAndTruncateMetaIndexFile(filePath string) error {
	//get detached metaIndex fd
	fd, err := fileops.OpenObsFile(filePath, immutable.MetaIndexFile, d.obsOpt, false)
	if err != nil {
		log.Error("open detached metaIndex file fail", zap.String("name", filePath), zap.Error(err))
		return err
	}
	defer func() {
		_ = fd.Close()
	}()

	fileInfo, err := fd.Stat()
	if err != nil {
		return err
	}

	//detached metaIndex is invalid
	metaItemSize := immutable.MetaIndexItemSize + CRCLen
	if fileInfo.Size() < immutable.MetaIndexHeaderSize+metaItemSize {
		//clear obsMetaIndex and set offset、size as zero to init other obs files
		return d.clearMetaIndex(fd)
	}

	metaItemsSize := fileInfo.Size() - immutable.MetaIndexHeaderSize
	n, m := metaItemsSize/metaItemSize, metaItemsSize%metaItemSize
	//There is some dirty data in metaIndex, so truncate file
	if m > 0 {
		err = fd.Truncate(fileInfo.Size() - m)
		log.Warn("truncate detached metaIndex file", zap.Int64("truncate start size is", fileInfo.Size()-m))
		if err != nil {
			return err
		}
	}

	dst := make([]byte, metaItemSize)
	//the last MetaIndexItem's offset is (n-1)*metaItemSize + MetaIndexHeaderSize)
	_, err = fd.ReadAt(dst, (n-1)*metaItemSize+immutable.MetaIndexHeaderSize)
	if err != nil {
		return err
	}
	//check metaIndex item
	if binary.BigEndian.Uint32(dst[:CRCLen]) != crc32.ChecksumIEEE(dst[CRCLen:]) {
		return fmt.Errorf("get wrong metaIndex")
	}
	dst = dst[CRCLen:]

	//get last metaIndex Item start block id(uint64)、offset(int64)、size(uint32)
	d.lastMetaIdxBlockId, dst = numberenc.UnmarshalUint64(dst), dst[util.Uint64SizeBytes:]
	dst = dst[util.Int64SizeBytes*2:] //min、max time
	d.lastMetaIdxOff, dst = numberenc.UnmarshalInt64(dst), dst[util.Int64SizeBytes:]
	d.lastMetaIdxSize = numberenc.UnmarshalUint32(dst)
	return nil
}

func (d *DetachedMetaInfo) clearMetaIndex(fd fileops.File) error {
	err := fd.Truncate(0)
	log.Warn("truncate detached metaIndex file", zap.Int("truncate start size is", 0))
	if err != nil {
		return err
	}
	d.lastMetaIdxSize = 0
	d.lastMetaIdxOff = 0
	d.lastMetaIdxBlockId = 0
	return nil
}

func (d *DetachedMetaInfo) checkAndTruncateChunkMeta(filePath string) error {
	//get detached chunkMeta fd
	fd, err := fileops.OpenObsFile(filePath, immutable.ChunkMetaFile, d.obsOpt, false)
	if err != nil {
		log.Error("open detached chunkMeta file fail", zap.String("name", filePath), zap.Error(err))
		return err
	}
	defer func() {
		_ = fd.Close()
	}()

	cmFileSize := d.lastMetaIdxOff + int64(d.lastMetaIdxSize)
	if cmFileSize == 0 {
		d.lastChunkMetaSize = 0
		d.lastChunkMetaOff = 0
		return nil
	}

	fileInfo, err := fd.Stat()
	if err != nil {
		return err
	}

	//There is some dirty data in chunkMeta, so truncate file
	if fileInfo.Size() > cmFileSize {
		err = fd.Truncate(cmFileSize)
		log.Warn("truncate detached chunkMeta file", zap.Int64("truncate start size is", cmFileSize))
		if err != nil {
			return err
		}
	}

	//read last chunkMeta Item
	dst := make([]byte, d.lastMetaIdxSize)
	_, err = fd.ReadAt(dst, d.lastMetaIdxOff)
	if err != nil {
		return err
	}
	dst = dst[CRCLen+util.Uint64SizeBytes:] //crc + block id
	d.lastChunkMetaOff, dst = numberenc.UnmarshalInt64(dst), dst[util.Int64SizeBytes:]
	d.lastChunkMetaSize = numberenc.UnmarshalUint32(dst)
	return nil
}

func (d *DetachedMetaInfo) checkAndTruncateDataFile(filePath string) error {
	//get detached data fd
	fd, err := fileops.OpenObsFile(filePath, immutable.DataFile, d.obsOpt, false)
	if err != nil {
		log.Error("open detached data file fail", zap.String("name", filePath), zap.Error(err))
		return err
	}
	defer func() {
		_ = fd.Close()
	}()

	fileInfo, err := fd.Stat()
	if err != nil {
		return err
	}

	dataFileSize := d.lastChunkMetaOff + int64(d.lastChunkMetaSize)
	if fileInfo.Size() > dataFileSize {
		err = fd.Truncate(dataFileSize)
		log.Warn("truncate detached chunkData file", zap.Int64("truncate start size is", dataFileSize))
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *DetachedMetaInfo) checkAndTruncatePkMetaIdxFile(filePath string) error {
	//get detached data fd
	fd, err := fileops.OpenObsFile(filePath, immutable.PrimaryMetaFile, d.obsOpt, false)
	if err != nil {
		log.Error("open detached primary index meta file fail", zap.String("name", filePath), zap.Error(err))
		return err
	}
	defer func() {
		_ = fd.Close()
	}()

	fileInfo, err := fd.Stat()
	if err != nil {
		return err
	}

	//public info's magic number、version、public info len、schema len
	if fileInfo.Size() < immutable.PKMetaInfoLength+int64(util.Uint32SizeBytes) {
		return d.clearPkMetaFile(fd, 0)
	}

	//read pkMeta file's public info len and schema len
	dst := make([]byte, util.Uint32SizeBytes*2)
	_, err = fd.ReadAt(dst, immutable.PkMetaHeaderSize)
	if err != nil {
		return err
	}
	//public info length
	pInfoLen, dst := numberenc.UnmarshalUint32(dst), dst[util.Uint32SizeBytes:]
	schemaLen := numberenc.UnmarshalUint32(dst)
	//start/end block id + offset、size + colsOffset
	singleMetaItemSize := int(schemaLen)*util.Uint32SizeBytes + immutable.PKMetaPrefixSize + CRCLen
	totalMetaItemSize := fileInfo.Size() - int64(pInfoLen)
	n, m := totalMetaItemSize/int64(singleMetaItemSize), totalMetaItemSize%int64(singleMetaItemSize)
	if n == 0 {
		return d.clearPkMetaFile(fd, 0)
	}
	//There is some dirty data in pk meta file, so truncate file
	if m > 0 {
		err = d.clearPkMetaFile(fd, fileInfo.Size()-m)
		if err != nil {
			return err
		}
	}

	//check pkMetaIndex Item
	var startBlockId uint64
	j := n - 1
	for ; j >= 0; j-- {
		dst = make([]byte, singleMetaItemSize)
		_, err = fd.ReadAt(dst, j*int64(singleMetaItemSize)+int64(pInfoLen))
		if err != nil {
			return err
		}
		dst = dst[CRCLen:]
		startBlockId, dst = numberenc.UnmarshalUint64(dst), dst[util.Uint64SizeBytes:]
		if startBlockId == d.lastMetaIdxBlockId {
			d.lastPkMetaEndBlockId, dst = numberenc.UnmarshalUint64(dst), dst[util.Uint64SizeBytes:]
			d.lastPkMetaOff, dst = numberenc.UnmarshalUint32(dst), dst[util.Uint32SizeBytes:]
			d.lastPkMetaSize = numberenc.UnmarshalUint32(dst)
			break
		}
	}

	//There is some dirty data in pk meta file, so truncate file
	if j != n-1 {
		err = fd.Truncate(fileInfo.Size() - j*int64(singleMetaItemSize) + int64(pInfoLen))
		log.Warn("truncate detached pkMetaIndex file", zap.Int64("truncate start size is", fileInfo.Size()-j*int64(singleMetaItemSize)+int64(pInfoLen)))
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *DetachedMetaInfo) clearPkMetaFile(fd fileops.File, size int64) error {
	err := fd.Truncate(size)
	log.Warn("truncate detached pkMetaIndex file", zap.Int64("truncate start size is", size))
	if err != nil {
		return err
	}
	d.lastPkMetaEndBlockId = 0
	d.lastPkMetaSize = 0
	d.lastPkMetaOff = 0
	return nil
}

func (d *DetachedMetaInfo) checkAndTruncatePkIdxFile(filePath string) error {
	//get detached data fd
	fd, err := fileops.OpenObsFile(filePath, immutable.PrimaryKeyFile, d.obsOpt, false)
	if err != nil {
		log.Error("open detached primary index file fail", zap.String("name", filePath), zap.Error(err))
		return err
	}
	defer func() {
		_ = fd.Close()
	}()

	fileInfo, err := fd.Stat()
	if err != nil {
		return err
	}

	dataFileSize := int64(d.lastPkMetaOff + d.lastPkMetaSize)
	if fileInfo.Size() > dataFileSize {
		err = fd.Truncate(dataFileSize)
		log.Warn("truncate detached pkIndex file", zap.Int64("truncate start size is", dataFileSize))
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *DetachedMetaInfo) checkAndTruncateBfFiles(dir, msName string, bfCols []string) error {
	localPath := dir
	prefixDataPathLength := len(obs.GetPrefixDataPath())
	remotePath := localPath[prefixDataPathLength:]

	//get bloom filter cols -> gen bloom filter files
	constant := logstore.GetConstant(logstore.CurrentLogTokenizerVersion)
	for i := range bfCols {
		fdr, fdl, err := d.getBloomFilterFile(remotePath, localPath, msName, bfCols[i])
		if err != nil {
			return err
		}

		remoteInfo, err := fdr.Stat()
		if err != nil {
			return err
		}

		localInfo, err := fdl.Stat()
		if err != nil {
			return err
		}

		rBlockCnt, lBlockCnt := remoteInfo.Size()/constant.VerticalGroupDiskSize*constant.FilterCntPerVerticalGorup, localInfo.Size()/constant.FilterDataDiskSize
		remain := rBlockCnt + lBlockCnt - int64(d.lastPkMetaEndBlockId)
		if remain > 0 {
			if remain >= lBlockCnt {
				err = fdl.Truncate(0)
				log.Warn("truncate detached local bloom filter file", zap.Int64("truncate start size is", 0))
				if err != nil {
					return err
				}
				err = fdr.Truncate((rBlockCnt - remain + lBlockCnt) * constant.FilterDataMemSize)
				log.Warn("truncate detached remote bloom filter file", zap.Int64("truncate start size is", (rBlockCnt-remain+lBlockCnt)*constant.FilterDataMemSize))
				if err != nil {
					return err
				}
			} else {
				err = fdl.Truncate((lBlockCnt - remain) * constant.FilterDataDiskSize)
				log.Warn("truncate detached local bloom filter file", zap.Int64("truncate start size is", (lBlockCnt-remain)*constant.FilterDataDiskSize))
				if err != nil {
					return err
				}
			}
		}
		_ = fdr.Close()
		_ = fdl.Close()
	}
	return nil
}

func (d *DetachedMetaInfo) getBloomFilterFile(remotePath, localPath, msName, columnName string) (fileops.File, fileops.File, error) {
	lock := fileops.FileLockOption("")
	pri := fileops.FilePriorityOption(fileops.IO_PRIORITY_NORMAL)
	localFilePath := sparseindex.GetBloomFilterFilePath(localPath, msName, columnName)
	remoteFilePath := sparseindex.GetBloomFilterFilePath(remotePath, msName, columnName)
	if d.obsOpt != nil {
		path := filepath.Join(d.obsOpt.BasePath, remoteFilePath)
		remoteFilePath = fileops.EncodeObsPath(d.obsOpt.Endpoint, d.obsOpt.BucketName, path, d.obsOpt.Ak, d.obsOpt.Sk)
	}

	fdr, err := fileops.OpenFile(remoteFilePath, os.O_CREATE|os.O_RDWR, 0640, lock, pri)
	if err != nil {
		return nil, nil, err
	}

	lock = ""
	err = fileops.MkdirAll(filepath.Join(localPath, msName), 0750, lock)
	if err != nil {
		panic(err)
	}
	fdl, err := fileops.OpenFile(localFilePath, os.O_CREATE|os.O_RDWR, 0640, lock, pri)
	if err != nil {
		return nil, nil, err
	}
	return fdr, fdl, err
}
