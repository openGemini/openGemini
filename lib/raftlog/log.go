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

/*
Copyright 2020 Dgraph Labs, Inc. and Contributors
This code is originally from: https://github.com/solsson/dgraph/blob/85a3dea0013ca8f33977f06b8e2aea626cf15a37/raftwal/log.go
*/

package raftlog

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/logger"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"
)

/*

maxSize: 32MB

|----------------------slots------------------|--------------logFileOffset--------------...
|--------slot----------|---------slot---------|--------------logFileOffset--------------...
|Term Index Type Offset|Term Index Type Offset|size offset filename|size offset filename...
                                              ↑
                                     logFileOffset(10MB)
slot size: 4*8=32 byte

*/

const (
	// maxNumEntries is maximum number of entries before rotating the file.
	maxNumEntries = 30000 //
	// logFileOffset is offset in the log file where filepath is stored.
	logFileOffset = 1 << 20 // 1MB
	// logFileSize is the initial size of the log file.
	//lint:ignore U1000 keep this
	logFileSize = 16 << 20 // 16MB
	// logFileSize is the max size of the log file.
	maxLogFileSize = 32 << 20 // 32MB
	// entrySize is the size in bytes of a single entry.
	entrySize = 32
	// logSuffix is the suffix for log files.
	logSuffix = ".entry"
)

var (
	emptyEntry = entry(make([]byte, entrySize))
)

type entry []byte

func (e entry) Term() uint64   { return binary.BigEndian.Uint64(e) }
func (e entry) Index() uint64  { return binary.BigEndian.Uint64(e[8:]) }
func (e entry) Type() uint64   { return binary.BigEndian.Uint64(e[16:]) }
func (e entry) Offset() uint64 { return binary.BigEndian.Uint64(e[24:]) }

func marshalEntry(b []byte, term, index, typ, offset uint64) {
	if len(b) != entrySize {
		panic(fmt.Sprintf("marshal entry size should be %d, current %d", entrySize, len(b)))
	}

	binary.BigEndian.PutUint64(b, term)
	binary.BigEndian.PutUint64(b[8:], index)
	binary.BigEndian.PutUint64(b[16:], typ)
	binary.BigEndian.PutUint64(b[24:], offset)
}

// logFile represents a single log file.
type logFile struct {
	fid   int64 // file index id
	entry FileWrapper
}

func logFname(dir string, id int64) string {
	return filepath.Join(dir, fmt.Sprintf("%05d%s", id, logSuffix))
}

// openLogFile opens a logFile in the given directory. The filename is
// constructed based on the value of fid.
func openLogFile(dir string, fid int64) (*logFile, error) {
	logger.GetLogger().Info(fmt.Sprintf("opening log file: %d\n", fid))
	fpath := logFname(dir, fid)
	lf := &logFile{
		fid: fid,
	}
	var err error

	// Open the file in read-write mode and create it if it doesn't exist yet.
	lf.entry, err = OpenFile(fpath, os.O_CREATE|os.O_RDWR, logFileOffset)
	if errors.Is(err, NewFile) {
		logger.GetLogger().Info(fmt.Sprintf("New file: %d\n", fid))
	} else if err != nil {
		return nil, errors.Wrapf(err, "fail to open entry log file: %s", fpath)
	}
	return lf, nil
}

// lastEntry returns the last valid entry in the file.
func (lf *logFile) lastEntry() entry {
	// This would return the first pos, where e.Index() == 0.
	pos := lf.firstEmptySlot()
	if pos > 0 {
		pos--
	}
	return lf.getEntry(pos)
}

// slotGe would return -1 if raftIndex < firstIndex in this file.
// Would return maxNumEntries if raftIndex > lastIndex in this file.
// If raftIndex is found, or the entryFile has empty slots, the offset would be between
// [0, maxNumEntries).
func (lf *logFile) slotGe(raftIndex uint64) int {
	fi := lf.firstIndex()
	// If first index is zero or the first index is less than raftIndex, this
	// raftindex should be in a previous file.
	if fi == 0 || raftIndex < fi {
		return -1
	}

	// Look at the entry at slot diff. If the log has entries for all indices between
	// fi and raftIndex without any gaps, the entry should be there. This is an
	// optimization to avoid having to perform the search below.
	if diff := int(raftIndex - fi); diff < maxNumEntries && diff >= 0 {
		e := lf.getEntry(diff)
		if e.Index() == raftIndex {
			return diff
		}
	}

	// Find the first entry which has in index >= to raftIndex.
	return sort.Search(maxNumEntries, func(i int) bool {
		e := lf.getEntry(i)
		if e.Index() == 0 {
			// We reached too far to the right and found an empty slot.
			return true
		}
		return e.Index() >= raftIndex
	})
}

// delete unmaps and deletes the file.
func (lf *logFile) delete() error {
	logger.GetLogger().Info(fmt.Sprintf("Deleting file: %s", lf.entry.Name()))
	err := lf.entry.Delete()
	if err != nil {
		logger.GetLogger().Info(fmt.Sprintf("while deleting file: %s", lf.entry.Name()), zap.Error(err))
	}
	return err
}

// getLogFiles returns all the log files in the directory sorted by the first
// index in each file.
func getLogFiles(dir string) ([]*logFile, error) {
	fis, err := fileops.ReadDir(dir)
	if err != nil && !os.IsNotExist(err) {
		return nil, errors.Wrapf(err, "fail to read raft entry log dir: %s", dir)
	}
	var entryFiles = make([]os.FileInfo, 0, len(fis))
	for _, fi := range fis {
		if fi.IsDir() {
			continue
		}
		if strings.HasSuffix(fi.Name(), logSuffix) {
			entryFiles = append(entryFiles, fi)
		}
	}

	var files []*logFile
	seen := make(map[int64]struct{})

	for _, fi := range entryFiles {
		fname := fi.Name()
		fname = strings.TrimSuffix(fname, logSuffix)

		fid, err := strconv.ParseInt(fname, 10, 64)
		if err != nil {
			return nil, errors.Wrapf(err, "while parsing: %s", fi.Name())
		}

		if _, ok := seen[fid]; ok {
			return nil, errors.Newf("Entry file with id: %d is repeated. file: %s", fid, fi.Name())
		}
		seen[fid] = struct{}{}

		f, err := openLogFile(dir, fid)
		if err != nil {
			return nil, err
		}
		files = append(files, f)
	}

	// Sort files by the first index they store.
	sort.Slice(files, func(i, j int) bool {
		return files[i].getEntry(0).Index() < files[j].getEntry(0).Index()
	})
	return files, nil
}

// getEntry gets the entry at the slot idx.
func (lf *logFile) getEntry(idx int) entry {
	if lf == nil {
		return emptyEntry
	}
	if idx >= maxNumEntries {
		panic(fmt.Sprintf("idx: %d is greater than maxNumEntries: %d", idx, maxNumEntries))
	}
	offset := idx * entrySize
	return entry(lf.entry.GetEntryData(offset, offset+entrySize))
}

// getRaftEntry gets the entry at the index idx, reads the data from the appropriate
// offset and converts it to a raftpb.Entry object.
func (lf *logFile) getRaftEntry(idx int) raftpb.Entry {
	entry := lf.getEntry(idx)
	re := raftpb.Entry{
		Term:  entry.Term(),
		Index: entry.Index(),
		Type:  raftpb.EntryType(int32(entry.Type())),
	}
	offset := entry.Offset()
	if offset > 0 {
		if offset >= uint64(lf.entry.Size()) {
			panic("valid offset error")
		}
		data := lf.entry.ReadSlice(int64(offset))
		if len(data) > 0 {
			// Copy the data over to allow the file to be deleted later.
			re.Data = append(re.Data, data...)
		}
	}
	return re
}

// firstIndex returns the first index in the file.
func (lf *logFile) firstIndex() uint64 {
	return lf.getEntry(0).Index()
}

// firstEmptySlot returns the index of the first empty slot in the file.
func (lf *logFile) firstEmptySlot() int {
	return sort.Search(maxNumEntries, func(i int) bool {
		e := lf.getEntry(i)
		return e.Index() == 0
	})
}

func (lf *logFile) Close() error {
	return lf.entry.Close()
}
