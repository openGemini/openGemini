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

package kvstorage_test

import (
	"os"
	"strings"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/influxdata/influxdb/pkg/testing/assert"
	kv "github.com/openGemini/openGemini/lib/kvstorage"
)

var tmp = "/tmp/kvtest"
var kvPath = tmp + "/test_kv"

var (
	db     kv.KVStorage
	kbPool bytesutil.ByteBufferPool
)

func init() {
	InitDB()
}

func InitDB() {
	var err error
	db, err = kv.NewStorage(&kv.Config{
		KVType: kv.PEBBLEDB,
		Path:   kvPath,
		Pebble: &kv.PebbleOptions{},
	})
	if err != nil {
		panic(err)
	}
}

func TestSetAndGet(t *testing.T) {
	defer os.RemoveAll(tmp)

	db.Set([]byte("3"), []byte("aaa"))
	db.Set([]byte("2"), []byte("bbb"))
	db.Set([]byte("1"), []byte("ccc"))

	v, err := db.Get(nil, []byte("2"))
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, string(v), "bbb")
}

func TestDelete(t *testing.T) {
	defer os.RemoveAll(tmp)

	db.Set([]byte("key1"), []byte("value1"))
	v, err := db.Get(nil, []byte("key1"))
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, string(v), "value1")

	if err := db.Delete([]byte("key1")); err != nil {
		t.Fatal(err)
	}

	_, err = db.Get(nil, []byte("key1"))
	assert.Equal(t, err, kv.ErrNotFound)
}

func TestGetByPrefix(t *testing.T) {
	defer os.RemoveAll(tmp)

	db.Set([]byte("1&ccc"), []byte("ccc"))
	db.Set([]byte("1&bbb"), []byte("bbb"))
	db.Set([]byte("1&aaa"), []byte("aaa"))

	db.Set([]byte("2&ccc"), []byte("ccc"))
	db.Set([]byte("2&bbb"), []byte("bbb"))
	db.Set([]byte("2&aaa"), []byte("aaa"))

	var err error
	pairs := kv.GetKVPairs()
	defer kv.PutKVPairs(pairs)
	pairs, err = db.GetByPrefix(pairs, []byte("1&"))
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, len(pairs.Keys), 3)
	assert.Equal(t, string(pairs.Keys[0]) == "1&aaa" && string(pairs.Values[0]) == "aaa", true)
	assert.Equal(t, string(pairs.Keys[1]) == "1&bbb" && string(pairs.Values[1]) == "bbb", true)
	assert.Equal(t, string(pairs.Keys[2]) == "1&ccc" && string(pairs.Values[2]) == "ccc", true)

	pairs.Reset()
	pairs, err = db.GetByPrefix(pairs, []byte("2&"))
	for i := 0; i < len(pairs.Keys); i++ {
		assert.Equal(t, strings.Contains(string(pairs.Keys[0]), "2&"), true)
	}
}

func TestGetByPrefixWithFunc(t *testing.T) {
	defer os.RemoveAll(tmp)

	db.Set([]byte("1&ccc"), []byte("ccc"))
	db.Set([]byte("1&bbb"), []byte("bbb"))
	db.Set([]byte("1&aaa"), []byte("aaa"))

	db.Set([]byte("2&ccc"), []byte("ccc"))
	db.Set([]byte("2&bbb"), []byte("bbb"))
	db.Set([]byte("2&aaa"), []byte("aaa"))

	key, value := db.GetByPrefixWithFunc([]byte("1&"), func(k, v []byte) bool {
		if string(k) == "1&bbb" {
			return true
		}
		return false
	})
	assert.Equal(t, string(key), "1&bbb")
	assert.Equal(t, string(value), "bbb")

	key, value = db.GetByPrefixWithFunc([]byte("2&"), func(k, v []byte) bool {
		if string(k) == "1&bbb" {
			return true
		}
		return false
	})
	assert.Equal(t, key == nil, true)
	assert.Equal(t, value == nil, true)
}

var value = []byte("valuevaluevalueabccbadddefghijksdf2349423seeafgccdsdeeefsallcdgerwsfsfsfswe18398fsgbm,;[]{}")

func BenchmarkPebbleDB_Set(b *testing.B) {
	k := kbPool.Get()
	if db.Closed() {
		InitDB()
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		k.B = append(k.B[:0], []byte("key-")...)
		k.B = encoding.MarshalInt64(k.B, int64(i%1e5))
		b.StartTimer()

		db.Set(k.B, value)
	}

	b.StopTimer()
	db.Close()
	kbPool.Put(k)
	os.RemoveAll(tmp)
}

func BenchmarkPebbleDB_Get(b *testing.B) {
	if db.Closed() {
		InitDB()
	}
	k := kbPool.Get()
	v := kbPool.Get()
	func() {
		for i := 0; i < 1e6; i++ {
			k.B = append(k.B[:0], []byte("key-")...)
			k.B = encoding.MarshalInt64(k.B, int64(i%1e6))
			db.Set(k.B, value)
		}
	}()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		k.B = append(k.B[:0], []byte("key-")...)
		k.B = encoding.MarshalInt64(k.B, int64(i%1e5))
		db.Get(v.B[:0], k.B)
	}
	b.StopTimer()
	db.Close()
	kbPool.Put(k)
	kbPool.Put(v)
	os.RemoveAll(tmp)
}
