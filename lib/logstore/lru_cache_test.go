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

package logstore_test

import (
	"fmt"
	"hash/crc32"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/logstore"
)

func TestRingBuf(t *testing.T) {
	rb := logstore.NewRingBuf[int64](128)
	if rb.Size() != 128 {
		t.Fatalf("NewRingBuf failed, expect size：128, real size：%d", rb.Size())
	}
	rb.Write(0)
	rb.Write(1)
	rb.Write(2)
	rb.Write(3)
	if rb.Len() != 4 {
		t.Fatalf("NewRingBuf failed, expect len：4, real size：%d", rb.Len())
	}
	if rb.ReadOldest() != 0 {
		t.Fatalf("NewRingBuf failed, expect len：0, real size：%d", rb.ReadOldest())
	}
	rb.RemoveOldest()
	if rb.ReadOldest() != 1 {
		t.Fatalf("NewRingBuf failed, expect len：1, real size：%d", rb.ReadOldest())
	}

	for i := 0; i < 130; i++ {
		rb.Write(int64(i))
	}
	if rb.Len() != 127 {
		t.Fatalf("NewRingBuf failed, expect len：127, real size：%d", rb.Len())
	}
	for i := 0; i < 128; i++ {
		rb.RemoveOldest()
	}
	if rb.Len() != 0 {
		t.Fatalf("NewRingBuf failed, expect len：0, real size：%d", rb.Len())
	}
	rb.Write(2)
	rb.Write(3)
	if rb.ReadOldest() != 2 {
		t.Fatalf("NewRingBuf failed, expect len：2, real size：%d", rb.ReadOldest())
	}
	if rb.Len() != 2 {
		t.Fatalf("NewRingBuf failed, expect len：2, real size：%d", rb.Len())
	}
	rb.Reset()
}

func TestGetByteMask(t *testing.T) {
	testArr := [][3]uint32{{0, 1, 0}, {1, 1, 0}, {2, 2, 0x1}, {3, 4, 0x3}, {4, 4, 0x3}, {5, 8, 0x7}, {8, 8, 0x7}, {10, 16, 0xf}, {16, 16, 0xf}}
	for i := 0; i < len(testArr); i++ {
		cnt, mask := logstore.GetBinaryMask(testArr[i][0])
		if cnt != testArr[i][1] {
			t.Fatalf("GetByteMask failed, expect cnt：%d, real cnt：%d", cnt, testArr[i][1])
		}
		if mask != testArr[i][2] {
			t.Fatalf("GetByteMask failed, expect mask: %d, real mask: %d", mask, testArr[i][2])
		}
	}
}

type myString string

func (s *myString) Hash() uint32 {
	return crc32.ChecksumIEEE([]byte(*s))
}

func EvitcMyString(key *myString, value string) {
	fmt.Println("key=", *key, "value=", value)
}

func TestLruCache(t *testing.T) {
	lruCache := logstore.NewLruCache[*myString, string](1000, 4, time.Duration(3*time.Second), EvitcMyString)
	key1 := myString("key1")
	value1 := "value1"
	lruCache.Add(&key1, value1)
	key2 := myString("key2")
	value2 := "value2"
	lruCache.Add(&key2, value2)
	cacheLen := lruCache.Len()
	if cacheLen != 2 {
		t.Fatalf("lruCache len expect: 2, real: %d", cacheLen)
	}

	time.Sleep(2 * time.Second)
	r, ok := lruCache.Get(&key1)
	if !ok {
		t.Fatalf("get the value of key1 failed")
	}
	if r != value1 {
		t.Fatalf("get the value of key1 failed, get: %s, expect: %s", r, value1)
	}

	time.Sleep(2 * time.Second)
	_, ok = lruCache.Get(&key1)
	if !ok {
		t.Fatalf("the value of key1 must not evit")
	}
	_, ok = lruCache.Get(&key2)
	if ok {
		t.Fatalf("the value of key2 must not evit")
	}
}
