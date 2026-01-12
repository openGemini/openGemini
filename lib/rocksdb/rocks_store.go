//go:build streamfs
// +build streamfs

// Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.
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

package raftrocksdb

// #cgo CFLAGS: -I./../../c-deps/rocksdb
// #cgo LDFLAGS: -L/usr/lib64/ -lrocksdb
// #include "stdlib.h"
// #include "stdio.h"
// #include "string.h"
// #include "c.h"
import "C"

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"unsafe"

	"github.com/hashicorp/go-msgpack/v2/codec"
	"github.com/hashicorp/raft"
	"github.com/openGemini/openGemini/lib/cpu"
	"go.uber.org/zap"
)

var (
	// CF names we perform transactions in
	dbLogs  = string("logs")
	dbConf  = string("conf")
	dbName  = string("raftdb")
	current = string("CURRENT")

	keyCurrentTerm  = []byte("CurrentTerm")
	keyLastVoteTerm = []byte("LastVoteTerm")
	keyLastVoteCand = []byte("LastVoteCand")

	// ErrKeyNotFound An error indicating a given key does not exist
	ErrKeyNotFound = errors.New("not found")
)

type RocksStore struct {
	// conn is the underlying handle to the db
	conn *C.rocksdb_t

	// The path to the Rocks database file
	path string

	options *C.rocksdb_options_t

	cfDbConfHandle *C.rocksdb_column_family_handle_t
	cfDbLogsHandle *C.rocksdb_column_family_handle_t

	Logger *zap.Logger
}

func NewRocksStore(logPath string, maxSize int, maxNum int, path string) (*RocksStore, error) {
	dbPath := C.CString(path)
	defer C.free(unsafe.Pointer(dbPath))
	dbLogPath := C.CString(logPath)
	defer C.free(unsafe.Pointer(dbLogPath))

	var dbOptions *C.rocksdb_options_t
	dbOptions = C.rocksdb_options_create()

	C.rocksdb_options_increase_parallelism(dbOptions, C.int(cpu.GetCpuNum()))
	C.rocksdb_options_set_create_if_missing(dbOptions, C.uchar(1))
	C.rocksdb_options_optimize_level_style_compaction(dbOptions, C.ulong(0))
	C.rocksdb_options_set_create_missing_column_families(dbOptions, C.uchar(1))
	C.rocksdb_options_set_recover_lease_during_open(dbOptions, C.uchar(1))
	C.rocksdb_options_set_db_log_dir(dbOptions, dbLogPath)
	C.rocksdb_options_set_max_log_file_size(dbOptions, C.size_t(maxSize))
	C.rocksdb_options_set_keep_log_file_num(dbOptions, C.size_t(maxNum))

	var streamEnv *C.rocksdb_env_t
	streamEnv = C.rocksdb_create_stream_env()
	if streamEnv == nil {
		return nil, fmt.Errorf("rocksdb create stream env failed, return *rocksdb_env_t is nil")
	}
	C.rocksdb_options_set_env(dbOptions, streamEnv)

	cfNames := make([]*C.char, 3)
	cfNames[0] = C.CString("default")
	cfNames[1] = C.CString(dbConf)
	cfNames[2] = C.CString(dbLogs)

	handles := make([]*C.rocksdb_column_family_handle_t, 3)

	cfOptions := make([]*C.rocksdb_options_t, 3)
	cfOption := C.rocksdb_options_create()
	cfOptions[0] = cfOption
	cfOptions[1] = cfOption
	cfOptions[2] = cfOption
	var err1, err2 *C.char
	db := C.rocksdb_open_column_families(dbOptions, dbPath, 3, (**C.char)(unsafe.Pointer(&cfNames[0])),
		(**C.rocksdb_options_t)(unsafe.Pointer(&cfOptions[0])),
		(**C.rocksdb_column_family_handle_t)(unsafe.Pointer(&handles[0])), &err1)
	if err1 != nil {
		defer C.free(unsafe.Pointer(err1))
		errString := C.GoStringN(err1, C.int(C.strlen(err1)))
		return nil, fmt.Errorf("failed to open cfs(default/dbLogs/dbConf), err: %s", errString)
	}

	C.rocksdb_enable_file_deletions(db, C.uchar(1), &err2)
	if err2 != nil {
		defer C.free(unsafe.Pointer(err2))
		errString := C.GoStringN(err2, C.int(C.strlen(err2)))
		fmt.Printf("rocksdb_enable_file_deletions failed, err: %s.\n", errString)
	}

	return &RocksStore{conn: db, path: path, options: dbOptions, Logger: zap.NewNop(),
		cfDbConfHandle: handles[1], cfDbLogsHandle: handles[2]}, nil
}

func (r *RocksStore) Close() error {
	C.rocksdb_column_family_handle_destroy(r.cfDbConfHandle)
	C.rocksdb_column_family_handle_destroy(r.cfDbLogsHandle)
	C.rocksdb_options_destroy(r.options)
	C.rocksdb_close(r.conn)

	return nil
}

// Set is used to set a key/value set outside of the raft log
func (r *RocksStore) Set(k, v []byte) error {
	key := C.CString(string(k))
	value := C.CString(string(v))

	defer C.free(unsafe.Pointer(key))
	defer C.free(unsafe.Pointer(value))

	var wb *C.rocksdb_writebatch_t
	wb = C.rocksdb_writebatch_create()
	defer C.rocksdb_writebatch_destroy(wb)

	// the value of keyCurrentTerm and keyLastVoteTerm are uint64.
	if bytes.Equal(k, keyCurrentTerm) || bytes.Equal(k, keyLastVoteTerm) {
		// value_len params must specify to 8 explicitly. because value likes [0 0 0 0 0 0 0 3], the C.strlen(value) is 0.
		C.rocksdb_writebatch_put_cf(wb, r.cfDbConfHandle, key, C.strlen(key), value, 8)
	} else {
		C.rocksdb_writebatch_put_cf(wb, r.cfDbConfHandle, key, C.strlen(key), value, C.strlen(value))
	}

	var wOptions *C.rocksdb_writeoptions_t
	wOptions = C.rocksdb_writeoptions_create()
	defer C.rocksdb_writeoptions_destroy(wOptions)

	C.rocksdb_writeoptions_set_sync(wOptions, C.uchar(1))

	var err *C.char
	C.rocksdb_write(r.conn, wOptions, wb, &err)
	if err != nil {
		defer C.free(unsafe.Pointer(err))
		errString := C.GoStringN(err, C.int(C.strlen(err)))
		return fmt.Errorf("rocksdb_write failed, err:%s", errString)
	}

	return nil
}

// SetUint64 is like Set, but handles uint64 values
func (r *RocksStore) SetUint64(key []byte, val uint64) error {
	return r.Set(key, uint64ToByteSlice(val))
}

// Get is used to retrieve a value from the k/v store by key
func (r *RocksStore) Get(key []byte) ([]byte, error) {
	var rOptions *C.rocksdb_readoptions_t
	rOptions = C.rocksdb_readoptions_create()
	defer C.rocksdb_readoptions_destroy(rOptions)

	C.rocksdb_readoptions_set_verify_checksums(rOptions, 1)
	C.rocksdb_readoptions_set_fill_cache(rOptions, 1)

	ptrCharKey := C.CString(string(key))
	defer C.free(unsafe.Pointer(ptrCharKey))

	var err *C.char
	var val *C.char
	var valLen C.size_t
	val = C.rocksdb_get_cf(r.conn, rOptions, r.cfDbConfHandle, ptrCharKey, C.strlen(ptrCharKey), &valLen, &err)
	if err != nil {
		defer C.free(unsafe.Pointer(err))
		errString := C.GoStringN(err, C.int(C.strlen(err)))
		return nil, fmt.Errorf("failed to get value which key=%s, err:%s", string(key), errString)
	}

	if val == nil {
		return nil, ErrKeyNotFound
	}

	if val != nil {
		defer C.free(unsafe.Pointer(val))
	}

	res := C.GoBytes(unsafe.Pointer(val), C.int(valLen))
	return append([]byte{}, res...), nil
}

// GetUint64 is like Get, but handles uint64 values
func (r *RocksStore) GetUint64(key []byte) (uint64, error) {
	val, err := r.Get(key)
	if err != nil {
		return 0, nil
	}

	return byteSliceToUint64(val), nil
}

// FirstIndex returns the first known index from the Raft log.
func (r *RocksStore) FirstIndex() (uint64, error) {
	var rOptions *C.rocksdb_readoptions_t
	rOptions = C.rocksdb_readoptions_create()
	defer C.rocksdb_readoptions_destroy(rOptions)

	C.rocksdb_readoptions_set_verify_checksums(rOptions, 1)
	C.rocksdb_readoptions_set_fill_cache(rOptions, 1)

	var iter *C.rocksdb_iterator_t
	iter = C.rocksdb_create_iterator_cf(r.conn, rOptions, r.cfDbLogsHandle)
	defer C.rocksdb_iter_destroy(iter)

	C.rocksdb_iter_seek_to_first(iter)
	// check iter valid
	if C.rocksdb_iter_valid(iter) == 0 {
		var err *C.char
		C.rocksdb_iter_get_error(iter, &err)
		if err != nil {
			defer C.free(unsafe.Pointer(err))
			errString := C.GoStringN(err, C.int(C.strlen(err)))
			r.Logger.Info("RocksStore rocksdb_iter_seek_to_first failed", zap.String("err", errString))
			return 0, fmt.Errorf("failed to get first index, err:%s", errString)
		}
	}

	var length C.size_t
	str := C.rocksdb_iter_key(iter, &length)

	first := C.GoBytes(unsafe.Pointer(str), 8)
	return byteSliceToUint64(first), nil
}

func (r *RocksStore) LastIndex() (uint64, error) {
	var rOptions *C.rocksdb_readoptions_t
	rOptions = C.rocksdb_readoptions_create()
	defer C.rocksdb_readoptions_destroy(rOptions)

	C.rocksdb_readoptions_set_verify_checksums(rOptions, 1)
	C.rocksdb_readoptions_set_fill_cache(rOptions, 1)

	var iter *C.rocksdb_iterator_t
	iter = C.rocksdb_create_iterator_cf(r.conn, rOptions, r.cfDbLogsHandle)
	defer C.rocksdb_iter_destroy(iter)

	C.rocksdb_iter_seek_to_last(iter)
	// check iter valid
	if C.rocksdb_iter_valid(iter) == 0 {
		var err *C.char
		C.rocksdb_iter_get_error(iter, &err)
		if err != nil {
			defer C.free(unsafe.Pointer(err))
			errString := C.GoStringN(err, C.int(C.strlen(err)))
			r.Logger.Info("RocksStore rocksdb_iter_seek_to_last failed", zap.String("err", errString))
			return 0, fmt.Errorf("failed to get last index, err:%s", errString)
		}
	}

	var length C.size_t
	str := C.rocksdb_iter_key(iter, &length)

	last := C.GoBytes(unsafe.Pointer(str), 8)
	return byteSliceToUint64(last), nil
}

func (r *RocksStore) GetLog(index uint64, log *raft.Log) error {
	var rOptions *C.rocksdb_readoptions_t
	rOptions = C.rocksdb_readoptions_create()
	defer C.rocksdb_readoptions_destroy(rOptions)

	C.rocksdb_readoptions_set_verify_checksums(rOptions, C.uchar(1))
	C.rocksdb_readoptions_set_fill_cache(rOptions, C.uchar(1))

	idx := uint64ToByteSlice(index)
	ptrCharKey := C.CString(string(idx))
	defer C.free(unsafe.Pointer(ptrCharKey))

	var err *C.char
	var val *C.char
	var valLen C.size_t
	val = C.rocksdb_get_cf(r.conn, rOptions, r.cfDbLogsHandle, ptrCharKey, 8, &valLen, &err)
	if err != nil {
		defer C.free(unsafe.Pointer(err))
		errString := C.GoStringN(err, C.int(C.strlen(err)))
		return fmt.Errorf("failed to get value which key=%d, err:%s", index, errString)
	}

	if val == nil {
		return raft.ErrLogNotFound
	}

	if val != nil {
		defer C.free(unsafe.Pointer(val))
	}

	valBytes := C.GoBytes(unsafe.Pointer(val), C.int(valLen))
	return decodeMsg(valBytes, log)
}

func (r *RocksStore) StoreLog(log *raft.Log) error {
	return r.StoreLogs([]*raft.Log{log})
}

func (r *RocksStore) StoreLogs(logs []*raft.Log) error {
	var wb *C.rocksdb_writebatch_t
	wb = C.rocksdb_writebatch_create()
	defer C.rocksdb_writebatch_destroy(wb)

	var wOptions *C.rocksdb_writeoptions_t
	wOptions = C.rocksdb_writeoptions_create()
	defer C.free(unsafe.Pointer(wOptions))

	C.rocksdb_writeoptions_set_sync(wOptions, C.uchar(1))

	for _, log := range logs {
		key := uint64ToByteSlice(log.Index)
		val, err := encodeMsg(log)
		if err != nil {
			return err
		}

		cKey := C.CString(string(key))
		cValue := C.CString(string(val.Bytes()))
		// can't use C.strlen(cValue) to specify value's len, because val.Bytes() may be include 0,
		// C.strlen(cValue) may return length wrong.
		C.rocksdb_writebatch_put_cf(wb, r.cfDbLogsHandle, cKey, 8, cValue, C.ulong(len(val.Bytes())))

		C.free(unsafe.Pointer(cKey))
		C.free(unsafe.Pointer(cValue))
	}

	var err *C.char
	C.rocksdb_write(r.conn, wOptions, wb, &err)
	if err != nil {
		defer C.free(unsafe.Pointer(err))
		errString := C.GoStringN(err, C.int(C.strlen(err)))
		return fmt.Errorf("rocksdb_write failed, err:%s", errString)
	}

	return nil
}

// DeleteRange delete [min, max] logs
func (r *RocksStore) DeleteRange(min, max uint64) error {
	if min > max || max == math.MaxUint64 {
		return fmt.Errorf("min %d or max %d index is invalid", min, max)
	}

	minKey := uint64ToByteSlice(min)
	// max+1 in order to delete index: max
	maxKey := uint64ToByteSlice(max + 1)

	ptrCharMinKey := C.CString(string(minKey))
	ptrCharMaxKey := C.CString(string(maxKey))
	defer C.free(unsafe.Pointer(ptrCharMinKey))
	defer C.free(unsafe.Pointer(ptrCharMaxKey))

	var wb *C.rocksdb_writebatch_t
	wb = C.rocksdb_writebatch_create()
	defer C.rocksdb_writebatch_destroy(wb)

	C.rocksdb_writebatch_delete_range_cf(wb, r.cfDbLogsHandle, ptrCharMinKey, 8, ptrCharMaxKey, 8)

	var wOptions *C.rocksdb_writeoptions_t
	wOptions = C.rocksdb_writeoptions_create()
	defer C.rocksdb_writeoptions_destroy(wOptions)

	C.rocksdb_writeoptions_set_sync(wOptions, C.uchar(1))

	var err *C.char
	C.rocksdb_write(r.conn, wOptions, wb, &err)
	if err != nil {
		defer C.free(unsafe.Pointer(err))
		errString := C.GoStringN(err, C.int(C.strlen(err)))
		return fmt.Errorf("rocksdb_write failed, err:%s", errString)
	}

	return nil
}

func decodeMsg(buf []byte, out interface{}) error {
	r := bytes.NewBuffer(buf)
	hd := codec.MsgpackHandle{}
	dec := codec.NewDecoder(r, &hd)
	return dec.Decode(out)
}

func encodeMsg(in interface{}) (*bytes.Buffer, error) {
	buf := bytes.NewBuffer(nil)
	hd := codec.MsgpackHandle{}
	enc := codec.NewEncoder(buf, &hd)
	err := enc.Encode(in)
	return buf, err
}

func byteSliceToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

func uint64ToByteSlice(u uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, u)
	return buf
}
