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
	"errors"
	"sync"
)

type KVStorage interface {
	Get(dst, key []byte) ([]byte, error)
	GetByPrefix(dst *KVPairs, prefix []byte) (*KVPairs, error)
	GetByPrefixWithFunc(prefix []byte, f func(k, v []byte) bool) ([]byte, []byte)
	Set(key, value []byte) error
	Delete(key []byte) error
	DeleteRange(start, end []byte) error
	Flush() error
	Close() error
	Closed() bool
	NewIterator(lowerBound []byte, upperBound []byte) PebbleDBIterator
	NewBatch() *Batch
	Apply(b *Batch) error
}

var (
	ErrNotFound = errors.New("kv: not found")
)

type DBType int

const (
	PEBBLEDB = iota
	ROCKSDB
	LEVELDB
)

type Config struct {
	KVType int            `toml:"kv-type"`
	Path   string         `toml:"path"`
	Pebble *PebbleOptions `toml:"pebble"`
}

func NewConfig() Config {
	return Config{}
}

func NewStorage(config *Config) (KVStorage, error) {
	switch config.KVType {
	case PEBBLEDB:
		return NewPebbleDB(config.Path, config.Pebble), nil
	default:
		return nil, errors.New("not support")
	}
}

type KVPairs struct {
	// B is the underlying byte slice.
	B      []byte
	Keys   [][]byte
	Values [][]byte
}

// AddKeyValue key and value must be occur simultaneously
func (pairs *KVPairs) AddKeyValue(key []byte, value []byte) {
	start := len(pairs.B)
	pairs.B = append(pairs.B, key...)
	pairs.Keys = append(pairs.Keys, pairs.B[start:])

	start = len(pairs.B)
	pairs.B = append(pairs.B, value...)
	pairs.Values = append(pairs.Values, pairs.B[start:])
}

func (pairs *KVPairs) Reset() {
	pairs.B = pairs.B[:0]
	pairs.Keys = pairs.Keys[:0]
	pairs.Values = pairs.Values[:0]
}

var kvPairsPool sync.Pool

func GetKVPairs() *KVPairs {
	v := kvPairsPool.Get()
	if v == nil {
		return &KVPairs{}
	}
	return v.(*KVPairs)
}

func PutKVPairs(pairs *KVPairs) {
	pairs.Reset()
	kvPairsPool.Put(pairs)
}
