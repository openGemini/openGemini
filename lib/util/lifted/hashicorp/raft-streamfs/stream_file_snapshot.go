//go:build streamfs
// +build streamfs

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

package raftstreamfs

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"hash"
	"hash/crc64"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/hashicorp/raft"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/util/lifted/hashicorp/go-msgpack/codec"
)

const (
	testPath      = "permTest"
	snapPath      = "snapshots"
	metaFilePath  = "meta.json"
	stateFilePath = "state.bin"
	tmpSuffix     = ".tmp"
)

// fileSnapshotMeta is stored on disk. We also put a CRC
// on disk so that we can verify the snapshot.
type fileSnapshotMeta struct {
	raft.SnapshotMeta
	CRC []byte
}

type snapMetaSlice []*fileSnapshotMeta

// StreamFileSnapshotStore implements the SnapshotStore interface and allows
// snapshots to be made on the stream.
type StreamFileSnapshotStore struct {
	path   string
	retain int
	logger *log.Logger
}

// StreamFileSnapshotSink implements SnapshotSink with a file.
type StreamFileSnapshotSink struct {
	store  *StreamFileSnapshotStore
	logger *log.Logger
	dir    string
	meta   fileSnapshotMeta

	stateFile *fileops.StreamFile
	stateHash hash.Hash64
	buffered  *bufio.Writer

	closed bool
}

// streamBufferedFile is returned when we open a snapshot. This way
// reads are buffered and the file still gets closed.
type streamBufferedFile struct {
	bh *bufio.Reader
	fh *fileops.StreamFile
}

func (b *streamBufferedFile) Read(p []byte) (n int, err error) {
	return b.bh.Read(p)
}

func (b *streamBufferedFile) Close() error {
	return b.fh.Close()
}

// NewStreamFileSnapshotStore creates a new StreamFileSnapshotStore based
// on a base directory. The `retain` parameter controls how many
// snapshots are retained. Must be at least 1.
func NewStreamFileSnapshotStore(base string, retain int, logger *log.Logger) (*StreamFileSnapshotStore, error) {
	if retain < 1 {
		return nil, fmt.Errorf("must retain at least one snapshot")
	}

	// Ensure our path exists
	path := filepath.Join(base, snapPath)
	if err := fileops.MkdirAll(path, 0755); err != nil {
		return nil, fmt.Errorf("snapshot path not accessible: %v", err)
	}

	// Setup the store
	store := &StreamFileSnapshotStore{
		path:   path,
		retain: retain,
		logger: logger,
	}

	// Do a permissions test
	if err := store.testPermissions(); err != nil {
		return nil, fmt.Errorf("permissions test failed: %v", err)
	}
	return store, nil
}

func (f *StreamFileSnapshotStore) testPermissions() error {
	path := filepath.Join(f.path, testPath)
	fh, err := fileops.Create(path, fileops.FilePriorityOption(fileops.IO_PRIORITY_NORMAL))
	if err != nil {
		return err
	}
	fh.Close()
	fileops.Remove(path)
	return nil
}

// Create is used to start a new snapshot
func (f *StreamFileSnapshotStore) Create(version raft.SnapshotVersion, index, term uint64, configuration raft.Configuration,
	configurationIndex uint64, trans raft.Transport) (raft.SnapshotSink, error) {
	// Create a new path
	name := snapshotName(term, index)
	path := filepath.Join(f.path, name+tmpSuffix)

	// Make the directory
	if err := fileops.MkdirAll(path, 0755); err != nil {
		f.logger.Printf("[ERR] snapshot: Failed to make snapshot directory: %v", err)
		return nil, err
	}

	// Create the sink
	sink := &StreamFileSnapshotSink{
		store:  f,
		logger: f.logger,
		dir:    path,
		meta: fileSnapshotMeta{
			SnapshotMeta: raft.SnapshotMeta{
				Version:            version,
				ID:                 name,
				Index:              index,
				Term:               term,
				Peers:              encodePeers(configuration, trans),
				Configuration:      configuration,
				ConfigurationIndex: configurationIndex,
			},
			CRC: nil,
		},
	}

	// Write out the meta data. write ID/Index/Term/Peers when Create func. CRC/Size write when Close func.
	if err := sink.writeMeta(); err != nil {
		f.logger.Printf("[ERR] snapshot: Failed to write metadata: %v", err)
		return nil, err
	}

	// Open the state file
	statePath := filepath.Join(path, stateFilePath)
	fh, err := fileops.Create(statePath, fileops.FilePriorityOption(fileops.IO_PRIORITY_NORMAL))
	if err != nil {
		f.logger.Printf("[ERR] snapshot: Failed to create state file: %v", err)
		return nil, err
	}

	sink.stateFile = fh.(*fileops.StreamFile)

	// Create a CRC64 hash
	sink.stateHash = crc64.New(crc64.MakeTable(crc64.ECMA))

	multi := io.MultiWriter(sink.stateFile, sink.stateHash)
	sink.buffered = bufio.NewWriter(multi)

	return sink, nil
}

// List returns available snapshots in the store.
func (f *StreamFileSnapshotStore) List() ([]*raft.SnapshotMeta, error) {
	// Get the eligible snapshots
	snapshots, err := f.getSnapshots()
	if err != nil {
		f.logger.Printf("[ERR] snapshot: Failed to get snapshots: %v", err)
		return nil, err
	}

	var snapMeta []*raft.SnapshotMeta
	for _, meta := range snapshots {
		snapMeta = append(snapMeta, &meta.SnapshotMeta)
		if len(snapMeta) == f.retain {
			break
		}
	}
	return snapMeta, nil
}

// getSnapshots returns all the known snapshots.
func (f *StreamFileSnapshotStore) getSnapshots() ([]*fileSnapshotMeta, error) {
	// Get the eligible snapshots
	snapshots, err := fileops.ReadDir(f.path)
	if err != nil {
		f.logger.Printf("[ERR] snapshot: Failed to scan snapshot dir: %v", err)
		return nil, err
	}

	// Populate the metadata
	var snapMeta []*fileSnapshotMeta
	for _, snap := range snapshots {
		// Ignore any files
		if !snap.IsDir() {
			continue
		}

		// Ignore any temporary snapshots
		dirName := snap.Name()
		if strings.HasSuffix(dirName, tmpSuffix) {
			f.logger.Printf("[WARN] snapshot: Found temporary snapshot: %v", dirName)
			continue
		}

		// Try to read the meta data
		meta, err := f.readMeta(dirName)
		if err != nil {
			f.logger.Printf("[WARN] snapshot: Failed to read metadata for %v: %v", dirName, err)
			continue
		}

		// Append, but only return up to the retain count
		snapMeta = append(snapMeta, meta)
	}

	// Sort the snapshot, reverse so we get new -> old
	sort.Sort(sort.Reverse(snapMetaSlice(snapMeta)))

	return snapMeta, nil
}

// readMeta is used to read the meta data for a given named backup
func (f *StreamFileSnapshotStore) readMeta(name string) (*fileSnapshotMeta, error) {
	// Open the meta file
	metaPath := filepath.Join(f.path, name, metaFilePath)
	fh, err := fileops.OpenFile(metaPath, os.O_RDONLY, 0666, fileops.FilePriorityOption(fileops.IO_PRIORITY_NORMAL))
	if err != nil {
		return nil, err
	}
	defer fh.Close()

	// Buffer the file IO
	buffered := bufio.NewReader(fh)

	// Read in the JSON
	meta := &fileSnapshotMeta{}
	dec := json.NewDecoder(buffered)
	if err := dec.Decode(meta); err != nil {
		return nil, err
	}
	return meta, nil
}

// Open takes a snapshot ID and returns a ReadCloser for that snapshot.
func (f *StreamFileSnapshotStore) Open(id string) (*raft.SnapshotMeta, io.ReadCloser, error) {
	// Get the metadata
	meta, err := f.readMeta(id)
	if err != nil {
		f.logger.Printf("[ERR] snapshot: Failed to get meta data to open snapshot: %v", err)
		return nil, nil, err
	}

	// Open the state file
	statePath := filepath.Join(f.path, id, stateFilePath)
	fh, err := fileops.OpenFile(statePath, os.O_RDONLY, 0666, fileops.FilePriorityOption(fileops.IO_PRIORITY_NORMAL))
	if err != nil {
		f.logger.Printf("[ERR] snapshot: Failed to open state file: %v", err)
		return nil, nil, err
	}

	// Create a CRC64 hash
	stateHash := crc64.New(crc64.MakeTable(crc64.ECMA))

	stat, statErr := fh.Stat()
	if statErr != nil {
		panic("file state.bin stat() failed.")
	}

	statSize := stat.Size()
	stateData := make([]byte, statSize)
	readCnt, err := fh.Read(stateData)
	if err != nil {
		panic(err)
	}

	if int64(readCnt) != statSize {
		panic("stat.Size() not equal with read count from state.bin.")
	}

	stateHash.Write(stateData)
	// Verify the hash
	computed := stateHash.Sum(nil)
	if bytes.Compare(meta.CRC, computed) != 0 {
		f.logger.Printf("[ERR] snapshot: CRC checksum failed (stored: %v computed: %v)",
			meta.CRC, computed)
		fh.Close()
		return nil, nil, fmt.Errorf("CRC mismatch")
	}

	// Seek to the start
	if _, err := fh.Seek(0, 0); err != nil {
		f.logger.Printf("[ERR] snapshot: State file seek failed: %v", err)
		fh.Close()
		return nil, nil, err
	}

	streamFile := fh.(*fileops.StreamFile)

	// Return a buffered file
	buffered := &streamBufferedFile{
		bh: bufio.NewReader(fh),
		fh: streamFile,
	}

	return &meta.SnapshotMeta, buffered, nil
}

// ReapSnapshots reaps any snapshots beyond the retain count.
func (f *StreamFileSnapshotStore) ReapSnapshots() error {
	snapshots, err := f.getSnapshots()
	if err != nil {
		f.logger.Printf("[ERR] snapshot: Failed to get snapshots: %v", err)
		return err
	}

	for i := f.retain; i < len(snapshots); i++ {
		path := filepath.Join(f.path, snapshots[i].ID)
		if err := fileops.RemoveAll(path); err != nil {
			f.logger.Printf("[ERR] snapshot: Failed to reap snapshot %v: %v", path, err)
			return err
		}
	}
	return nil
}

// ID returns the ID of the snapshot, can be used with Open()
// after the snapshot is finalized.
func (s *StreamFileSnapshotSink) ID() string {
	return s.meta.ID
}

// Write is used to append to the state file. We write to the
// buffered IO object to reduce the amount of context switches.
func (s *StreamFileSnapshotSink) Write(b []byte) (int, error) {
	return s.buffered.Write(b)
}

// Close is used to indicate a successful end.
func (s *StreamFileSnapshotSink) Close() error {
	// Make sure close is idempotent
	if s.closed {
		return nil
	}
	s.closed = true

	// Close the open handles
	if err := s.finalize(); err != nil {
		s.logger.Printf("[ERR] snapshot: Failed to finalize snapshot: %v", err)
		return err
	}

	// Write out the meta data
	if err := s.writeMeta(); err != nil {
		s.logger.Printf("[ERR] snapshot: Failed to write metadata: %v", err)
		return err
	}

	// Move the directory into place
	newPath := strings.TrimSuffix(s.dir, tmpSuffix)
	if err := fileops.RenameFile(s.dir, newPath); err != nil {
		s.logger.Printf("[ERR] snapshot: Failed to move snapshot into place: %v", err)
		return err
	}

	// Reap any old snapshots
	s.store.ReapSnapshots()
	return nil
}

// Cancel is used to indicate an unsuccessful end.
func (s *StreamFileSnapshotSink) Cancel() error {
	// Make sure close is idempotent
	if s.closed {
		return nil
	}
	s.closed = true

	// Close the open handles
	if err := s.finalize(); err != nil {
		s.logger.Printf("[ERR] snapshot: Failed to finalize snapshot: %v", err)
		return err
	}

	// Attempt to remove all artifacts
	return fileops.RemoveAll(s.dir)
}

// finalize is used to close all of our resources.
func (s *StreamFileSnapshotSink) finalize() error {
	// Flush any remaining data.
	if err := s.buffered.Flush(); err != nil {
		return err
	}

	if err := s.stateFile.SyncTrue(); err != nil {
		return err
	}

	// Get the file size
	stat, statErr := s.stateFile.Stat()

	// Close the file
	if err := s.stateFile.Close(); err != nil {
		return err
	}

	// Set the file size, check after we close
	if statErr != nil {
		return statErr
	}
	s.meta.Size = stat.Size()

	// Set the CRC
	s.meta.CRC = s.stateHash.Sum(nil)
	return nil
}

// writeMeta is used to write out the metadata we have.
func (s *StreamFileSnapshotSink) writeMeta() error {
	// Open the meta file
	metaPath := filepath.Join(s.dir, metaFilePath)
	fh, err := fileops.OpenFile(metaPath, os.O_CREATE, 0666, fileops.FilePriorityOption(fileops.IO_PRIORITY_NORMAL))
	if err != nil {
		return err
	}
	defer fh.Close()

	// Buffer the file IO
	buffered := bufio.NewWriter(fh.(*fileops.StreamFile))
	defer buffered.Flush()

	// Write out as JSON
	enc := json.NewEncoder(buffered)
	if err := enc.Encode(&s.meta); err != nil {
		return err
	}
	return nil
}

func snapshotName(term, index uint64) string {
	now := time.Now()
	msec := now.UnixNano() / int64(time.Millisecond)
	return fmt.Sprintf("%d-%d-%d", term, index, msec)
}

// encodePeers is used to serialize a Configuration into the old peers format.
// This is here for backwards compatibility when operating with a mix of old
// servers and should be removed once we deprecate support for protocol version 1.
func encodePeers(configuration raft.Configuration, trans raft.Transport) []byte {
	// Gather up all the voters, other suffrage types are not supported by
	// this data format.
	var encPeers [][]byte
	for _, server := range configuration.Servers {
		if server.Suffrage == raft.Voter {
			encPeers = append(encPeers, trans.EncodePeer(server.ID, server.Address))
		}
	}

	// Encode the entire array.
	buf, err := encodeMsgPack(encPeers)
	if err != nil {
		panic(fmt.Errorf("failed to encode peers: %v", err))
	}

	return buf.Bytes()
}

// Encode writes an encoded object to a new bytes buffer.
func encodeMsgPack(in interface{}) (*bytes.Buffer, error) {
	buf := bytes.NewBuffer(nil)
	hd := codec.MsgpackHandle{}
	enc := codec.NewEncoder(buf, &hd)
	err := enc.Encode(in)
	return buf, err
}

// Implement the sort interface for []*fileSnapshotMeta.
func (s snapMetaSlice) Len() int {
	return len(s)
}

func (s snapMetaSlice) Less(i, j int) bool {
	if s[i].Term != s[j].Term {
		return s[i].Term < s[j].Term
	}
	if s[i].Index != s[j].Index {
		return s[i].Index < s[j].Index
	}
	return s[i].ID < s[j].ID
}

func (s snapMetaSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
