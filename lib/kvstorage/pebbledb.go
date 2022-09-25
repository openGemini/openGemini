/*
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.

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

package kvstorage

import (
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/openGemini/openGemini/lib/util"
)

type PebbleDB struct {
	mu     sync.RWMutex
	db     *pebble.DB
	wop    *pebble.WriteOptions
	closed bool
}

type PebbleOptions struct {
	//pebble.Options
	CacheSize                int64 `toml:"cache-size"`
	MemTableSize             int   `toml:"mem-table-size"`
	MemTableNumber           int   `toml:"mem-table-number"`
	L0MaxBytes               int64 `toml:"l0-max-bytes"`
	MaxConcurrentCompactions int   `toml:"max-concurrent-compactions"`
}

const (
	DefaultCacheSize                = 128 << 20
	DefaultMemTableSize             = 64 << 20
	DefaultMemTableNumber           = 6
	DefaultLBaseMaxBytes            = 64 << 20
	DefaultMaxConcurrentCompactions = 2
)

func genPebbleOptions(opts *PebbleOptions) *pebble.Options {
	if opts.CacheSize == 0 {
		opts.CacheSize = DefaultCacheSize
	}

	if opts.L0MaxBytes == 0 {
		opts.L0MaxBytes = DefaultLBaseMaxBytes
	}

	if opts.MemTableNumber == 0 {
		opts.MemTableNumber = DefaultMemTableNumber
	}

	if opts.MaxConcurrentCompactions == 0 {
		opts.MaxConcurrentCompactions = DefaultMaxConcurrentCompactions
	}

	if opts.MemTableSize == 0 {
		opts.MemTableSize = DefaultMemTableSize
	}

	return &pebble.Options{
		Cache:                       pebble.NewCache(opts.CacheSize),
		MemTableSize:                opts.MemTableSize,
		MemTableStopWritesThreshold: opts.MemTableNumber,
		MaxConcurrentCompactions:    opts.MaxConcurrentCompactions,
		LBaseMaxBytes:               opts.L0MaxBytes,
	}
}

func NewPebbleDB(path string, opts *PebbleOptions) *PebbleDB {
	db, err := pebble.Open(path, genPebbleOptions(opts))
	if err != nil {
		panic(err)
	}

	pebbleDB := new(PebbleDB)
	pebbleDB.db = db
	pebbleDB.wop = &pebble.WriteOptions{Sync: true}

	return pebbleDB
}

func (db *PebbleDB) Set(key, value []byte) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil
	}

	return db.db.Set(key, value, db.wop)
}

func (db *PebbleDB) Get(dst, key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil, nil
	}

	value, closer, err := db.db.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, ErrNotFound
		}
		return nil, err
	}
	defer util.MustClose(closer)

	dst = append(dst[:0], value...)
	return dst, err
}

func keyUpperBound(b []byte) []byte {
	end := make([]byte, len(b))
	copy(end, b)
	for i := len(end) - 1; i >= 0; i-- {
		end[i] = end[i] + 1
		if end[i] != 0 {
			return end[:i+1]
		}
	}
	return nil // no upper-bound
}

func (db *PebbleDB) GetByPrefixWithFunc(prefix []byte, f func(k, v []byte) bool) ([]byte, []byte) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil, nil
	}

	prefixIterOptions := func(prefix []byte) *pebble.IterOptions {
		return &pebble.IterOptions{
			LowerBound: prefix,
			UpperBound: keyUpperBound(prefix),
		}
	}

	iter := db.db.NewIter(prefixIterOptions(prefix))
	defer util.MustClose(iter)
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		value := iter.Value()

		if f(key, value) {
			var rkey []byte
			var rvalue []byte
			return append(rkey, key...), append(rvalue, value...)
		}
	}

	return nil, nil
}

// GetByPrefix get key-value
func (db *PebbleDB) GetByPrefix(dst *KVPairs, prefix []byte) (*KVPairs, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil, nil
	}

	prefixIterOptions := func(prefix []byte) *pebble.IterOptions {
		return &pebble.IterOptions{
			LowerBound: prefix,
			UpperBound: keyUpperBound(prefix),
		}
	}

	if dst == nil {
		dst = new(KVPairs)
	}
	iter := db.db.NewIter(prefixIterOptions(prefix))
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		value := iter.Value()

		dst.AddKeyValue(key, value)
	}

	if err := iter.Close(); err != nil {
		return dst, err
	}
	return dst, nil
}

func (db *PebbleDB) Delete(key []byte) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil
	}

	return db.db.Delete(key, db.wop)
}

func (db *PebbleDB) DeleteRange(start, end []byte) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil
	}

	return db.db.DeleteRange(start, end, db.wop)
}

func (db *PebbleDB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return nil
	}

	err := db.db.Close()
	if err != nil {
		return err
	}
	db.closed = true
	return nil
}

func (db *PebbleDB) Closed() bool {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.closed
}

func (db *PebbleDB) Flush() error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil
	}

	return db.db.Flush()
}

func (db *PebbleDB) Apply(b *Batch) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil
	}
	return db.db.Apply((*pebble.Batch)(b), db.wop)
}

type Batch pebble.Batch

func (b *Batch) Set(key, value []byte) error {
	return (*pebble.Batch)(b).Set(key, value, nil)
}

func (b *Batch) Count() uint32 {
	return (*pebble.Batch)(b).Count()
}

func (b *Batch) Close() error {
	return (*pebble.Batch)(b).Close()
}

func (b *Batch) Reset() {
	(*pebble.Batch)(b).Reset()
}

func (db *PebbleDB) NewBatch() *Batch {
	return (*Batch)(db.db.NewBatch())
}

type PebbleDBIterator interface {
	First() bool
	Valid() bool
	Next() bool
	Key() []byte
	Value() []byte
	Close() error
}

type pebbleDBIterator struct {
	LowerBound []byte
	UpperBound []byte
	itr        *pebble.Iterator
}

func (i *pebbleDBIterator) First() bool {
	return i.itr.First()
}

func (i *pebbleDBIterator) Valid() bool {
	return i.itr.Valid()
}

func (i *pebbleDBIterator) Next() bool {
	return i.itr.Next()
}

func (i *pebbleDBIterator) Key() []byte {
	return i.itr.Key()
}

func (i *pebbleDBIterator) Value() []byte {
	return i.itr.Value()
}

func (i *pebbleDBIterator) Close() error {
	return i.itr.Close()
}

func (db *PebbleDB) NewIterator(lowerBound []byte, upperBound []byte) PebbleDBIterator {
	opt := &pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	}

	iter := db.db.NewIter(opt)
	return &pebbleDBIterator{
		LowerBound: lowerBound,
		UpperBound: upperBound,
		itr:        iter,
	}
}
