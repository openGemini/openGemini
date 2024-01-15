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
package tokenizer

import (
	"bufio"
	"fmt"
	"math/bits"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSimpleTokenizer(t *testing.T) {
	content := "  a b\t\nd  efg"
	tokenizer := NewSimpleTokenizer(CONTENT_SPLIT_TABLE)
	tokenizer.InitInput([]byte(content))

	tokenizer.Next()
	assert.Equal(t, Hash([]byte("a")), tokenizer.CurrentHash())
	tokenizer.Next()
	assert.Equal(t, Hash([]byte("b")), tokenizer.CurrentHash())
	tokenizer.Next()
	assert.Equal(t, Hash([]byte("d")), tokenizer.CurrentHash())
	tokenizer.Next()
	assert.Equal(t, Hash([]byte("efg")), tokenizer.CurrentHash())
	tokenizer.Next()
	assert.False(t, tokenizer.Next())
}

func TestSimpleUtf8Tokenizer(t *testing.T) {
	content := "  a b\t\nd  efg   华为"
	tokenizer := NewSimpleUtf8Tokenizer(CONTENT_SPLIT_TABLE)
	tokenizer.InitInput([]byte(content))

	tokenizer.Next()
	assert.Equal(t, Hash([]byte("a")), tokenizer.CurrentHash())
	tokenizer.Next()
	assert.Equal(t, Hash([]byte("b")), tokenizer.CurrentHash())
	tokenizer.Next()
	assert.Equal(t, Hash([]byte("d")), tokenizer.CurrentHash())
	tokenizer.Next()
	assert.Equal(t, Hash([]byte("efg")), tokenizer.CurrentHash())
	tokenizer.Next()
	assert.Equal(t, Hash([]byte("华")), tokenizer.CurrentHash())
	tokenizer.Next()
	assert.Equal(t, Hash([]byte("为")), tokenizer.CurrentHash())
	assert.False(t, tokenizer.Next())
}

func TestSimpleGramTokenizerWithContent(t *testing.T) {
	content := "  a b\t\nd"
	tokenizer := NewSimpleGramTokenizer(CONTENT_SPLIT_TABLE, 3, MISS_CONTENT_SPLIT_TABLE_INDEX)
	tokenizer.InitInput([]byte(content))

	tokenHash := make(map[uint64]bool)
	for tokenizer.Next() {
		tokenHash[tokenizer.CurrentHash()] = true
	}

	assert.True(t, tokenHash[Hash([]byte("a"))])
	assert.True(t, tokenHash[Hash([]byte("b"))])
	assert.True(t, tokenHash[Hash([]byte("d"))])
	assert.Equal(t, 3, len(tokenHash))
	tokenizer.FreeSimpleGramTokenizer()
}

func TestSimpleGramTokenizerWithTag(t *testing.T) {
	tag := "type:upload"
	tokenizer := NewSimpleGramTokenizer(TAGS_SPLIT_TABLE, VersionLatest, MISS_TAGS_SPLIT_TABLE_INDEX)
	tokenizer.InitInput([]byte(tag))

	tokenHash := make(map[uint64]bool)
	for tokenizer.Next() {
		tokenHash[tokenizer.CurrentHash()] = true
	}

	assert.True(t, tokenHash[Hash([]byte("type:upload"))])
	tokenizer.FreeSimpleGramTokenizer()
}

func TestSimpleGramTokenizerWithTagV2(t *testing.T) {
	tag := "type:upload"
	tokenizer := NewSimpleGramTokenizer(TAGS_SPLIT_TABLE_BEFORE, VersionBefore, MISS_TAGS_SPLIT_TABLE_INDEX_BEFORE)
	tokenizer.InitInput([]byte(tag))

	tokenHash := make(map[uint64]bool)
	for tokenizer.Next() {
		tokenHash[tokenizer.CurrentHash()] = true
	}

	assert.True(t, tokenHash[Hash([]byte("type:upload"))])
}

func TestSimpleGramTokenizerWithSplit(t *testing.T) {
	content := "1.2.3.4 5,6.7-8"
	tokenizer := NewSimpleGramTokenizer(CONTENT_SPLIT_TABLE, 0, MISS_CONTENT_SPLIT_TABLE_INDEX)
	tokenizer.InitInput([]byte(content))

	tokenHash := make(map[uint64]bool)
	for tokenizer.Next() {
		tokenHash[tokenizer.CurrentHash()] = true
	}

	assert.True(t, tokenHash[Hash([]byte("1"))])
	assert.True(t, tokenHash[Hash([]byte("2"))])
	assert.True(t, tokenHash[Hash([]byte("3"))])
	assert.True(t, tokenHash[Hash([]byte("4"))])
	assert.True(t, tokenHash[Hash([]byte("5"))])
	assert.True(t, tokenHash[Hash([]byte("6"))])
	assert.True(t, tokenHash[Hash([]byte("7"))])
	assert.True(t, tokenHash[Hash([]byte("8"))])
	assert.True(t, tokenHash[MultiHash([]byte("2"), []byte("3"))])
	assert.True(t, tokenHash[MultiHash([]byte("3"), []byte("4"))])
}

func TestSimpleGramTokenizerWithSplit2(t *testing.T) {
	content := "test.xx sss 11.10.66.114.81"
	tokenizer := NewSimpleGramTokenizer(CONTENT_SPLIT_TABLE, 2, MISS_CONTENT_SPLIT_TABLE_INDEX)
	tokenizer.InitInput([]byte(content))

	tokenHash := make(map[uint64]bool)
	for tokenizer.Next() {
		if tokenizer.CurrentHash() == 0 {
			continue
		}
		tokenHash[tokenizer.CurrentHash()] = true
	}

	content1 := "10.66.114.81"
	tokenizer1 := NewSimpleGramTokenizerWithIP(CONTENT_SPLIT_TABLE, false)
	tokenizer1.InitInput([]byte(content1))

	tokenHash1 := make(map[uint64]bool)
	for tokenizer1.Next() {
		if tokenizer1.CurrentHash() == 0 {
			continue
		}
		tokenHash1[tokenizer1.CurrentHash()] = true
	}
}

func TestSimpleGramTokenizerLongAscii(t *testing.T) {
	var content []byte
	tokenizer := NewSimpleGramTokenizer(CONTENT_SPLIT_TABLE, 0, MISS_CONTENT_SPLIT_TABLE_INDEX)
	content = append(content, 0xf9)
	tokenizer.InitInput(content)

	tokenHash := make(map[uint64]bool)
	for tokenizer.Next() {
		tokenHash[tokenizer.CurrentHash()] = true
	}
}

func TestSimpleGramTokenizerLongAscii2(t *testing.T) {
	var content []byte
	tokenizer := NewSimpleGramTokenizer(CONTENT_SPLIT_TABLE, 0, MISS_CONTENT_SPLIT_TABLE_INDEX)
	str := "22:51:38"
	content = append(content, str...)
	tokenizer.InitInput(content)

	tokenHash := make(map[uint64]bool)
	for tokenizer.Next() {
		tokenHash[tokenizer.CurrentHash()] = true
	}
}

func TestSimpleGramTokenizerLongAscii3(t *testing.T) {
	var content []byte
	tokenizer := NewSimpleGramTokenizer(CONTENT_SPLIT_TABLE, 0, MISS_CONTENT_SPLIT_TABLE_INDEX)
	str := "2023-08-20 22:51:38.017Z"
	content = append(content, str...)
	tokenizer.InitInput(content)

	tokenHash := make(map[uint64]bool)
	for tokenizer.Next() {
		tokenHash[tokenizer.CurrentHash()] = true
	}

	tokenizer.InitInput(content)
	tokenHash2 := make(map[uint64]bool)
	for tokenizer.Next() {
		tokenHash2[tokenizer.CurrentHash()] = true
	}
	for key := range tokenHash2 {
		if !tokenHash[key] {
			t.Fatal("unexpect")
		}
	}

	tokenizer.InitInput([]byte("22:51:38"))
	tokenHash3 := make(map[uint64]bool)
	for tokenizer.Next() {
		tokenHash3[tokenizer.CurrentHash()] = true
	}
	for key := range tokenHash3 {
		if !tokenHash[key] {
			t.Fatal("unexpect")
		}
	}

}

func TestTokenizerBatchPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
		return
	}
	tokenizer := NewSimpleGramTokenizer(CONTENT_SPLIT_TABLE, 4, MISS_CONTENT_SPLIT_TABLE_INDEX)
	start := time.Now()
	//dataPath := "/opt/access.log"
	dataPath := "/data/test.txt"

	file, err := os.Open(dataPath)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	var cnt int64
	var match int64
	var lines int64
	scanner := bufio.NewScanner(file)
	scanBuf := make([]byte, 1024*100)
	scanner.Buffer(scanBuf, 10*1024*1024)
	scanner.Split(bufio.ScanLines)
	tokenHash := make(map[uint64]bool)
	blockLen := 0
	blockSize := 8 * 1024 * 1024
	blockTokenHash := make(map[uint64]bool)
	blockHash := []int{}
	for scanner.Scan() {
		tokenizer.InitInput(scanner.Bytes())
		blockLen = blockLen + len(scanner.Bytes())
		if blockLen > blockSize {
			blockHash = append(blockHash, len(blockTokenHash))
			blockLen = 0
			blockTokenHash = make(map[uint64]bool)
		}
		for tokenizer.Next() {
			tokenHash[tokenizer.CurrentHash()] = true
			blockTokenHash[tokenizer.CurrentHash()] = true
			if 18420677080871705054 == tokenizer.CurrentHash() {
				match++
			}
			cnt++
		}
		lines++
	}

	sum := 0
	for _, v := range blockHash {
		sum = sum + v
	}
	ava := sum / len(blockHash)

	cost := time.Since(start)
	fmt.Printf("cost=%v cnt %v lines %v hash %v match %v ava %v block num %v\n", cost, cnt, lines, len(tokenHash), match, ava, len(blockHash))
}

func TestTokenizerBatchPerformanceV0(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
		return
	}
	tokenizer := NewSimpleGramTokenizer(CONTENT_SPLIT_TABLE, VersionLatest, MISS_TAGS_SPLIT_TABLE_INDEX)
	start := time.Now()
	//dataPath := "/opt/access.log"
	dataPath := "/data/test/test.txt"

	file, err := os.Open(dataPath)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	var cnt int64
	var match int64
	var lines int64
	scanner := bufio.NewScanner(file)
	scanBuf := make([]byte, 1024*100)
	scanner.Buffer(scanBuf, 10*1024*1024)
	scanner.Split(bufio.ScanLines)
	tokenHash := make(map[uint64]bool)
	blockLen := 0
	blockSize := 1024 * 1024
	blockTokenHash := make(map[uint64]bool)
	blockHash := []int{}
	for scanner.Scan() {
		tokenizer.InitInput(scanner.Bytes())
		blockLen = blockLen + len(scanner.Bytes())
		if blockLen > blockSize {
			blockHash = append(blockHash, len(blockTokenHash))
			blockLen = 0
			blockTokenHash = make(map[uint64]bool)
		}
		for tokenizer.Next() {
			tokenHash[tokenizer.CurrentHash()] = true
			blockTokenHash[tokenizer.CurrentHash()] = true
			if 18420677080871705054 == tokenizer.CurrentHash() {
				match++
			}
			cnt++
		}
		lines++
	}
	sum := 0.0
	for _, v := range blockHash {
		sum = sum + float64(v)
	}
	ava := sum / float64(len(blockHash))

	cost := time.Since(start)
	fmt.Printf("cost=%v cnt %v lines %v hash %v match %v ava %v block num %v\n", cost, cnt, lines, len(tokenHash), match, ava, len(blockHash))
}

func TestTokenizerBatchPerformanceV1(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
		return
	}
	tokenizer := NewSimpleGramTokenizer(CONTENT_SPLIT_TABLE, VersionBefore, MISS_TAGS_SPLIT_TABLE_INDEX_BEFORE)
	start := time.Now()
	//dataPath := "/opt/access.log"
	dataPath := "/data/test/test.txt"

	file, err := os.Open(dataPath)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	var cnt int64
	var match int64
	var lines int64
	scanner := bufio.NewScanner(file)
	scanBuf := make([]byte, 1024*100)
	scanner.Buffer(scanBuf, 10*1024*1024)
	scanner.Split(bufio.ScanLines)
	tokenHash := make(map[uint64]bool)
	blockLen := 0
	blockSize := 1024 * 1024
	blockTokenHash := make(map[uint64]bool)
	blockHash := []int{}
	for scanner.Scan() {
		tokenizer.InitInput(scanner.Bytes())
		blockLen = blockLen + len(scanner.Bytes())
		if blockLen > blockSize {
			blockHash = append(blockHash, len(blockTokenHash))
			blockLen = 0
			blockTokenHash = make(map[uint64]bool)
		}
		for tokenizer.Next() {
			tokenHash[tokenizer.CurrentHash()] = true
			blockTokenHash[tokenizer.CurrentHash()] = true
			if 18420677080871705054 == tokenizer.CurrentHash() {
				match++
			}
			cnt++
		}
		lines++
	}
	sum := 0.0
	for _, v := range blockHash {
		sum = sum + float64(v)
	}
	ava := sum / float64(len(blockHash))

	cost := time.Since(start)
	fmt.Printf("cost=%v cnt %v lines %v hash %v match %v ava %v block num %v\n", cost, cnt, lines, len(tokenHash), match, ava, len(blockHash))
}

func TestSimpleGramTokenizerV1WithSplit(t *testing.T) {
	content := "10.88.199.12123"
	tokenizer := NewSimpleGramTokenizerV1(CONTENT_SPLIT_TABLE, MISS_CONTENT_SPLIT_TABLE_INDEX)
	tokenizer.InitInput([]byte(content))
	for tokenizer.Next() {
		if tokenizer.CurrentHash() != 17740306289234851963 {
			t.Error("get err hash")
		}
	}
}

func TestSimpleGramTokenizerV1WithSplit2(t *testing.T) {
	content := "**10.88.199.12123"
	tokenizer := NewSimpleGramTokenizerV1(CONTENT_SPLIT_TABLE, MISS_CONTENT_SPLIT_TABLE_INDEX)
	tokenizer.InitInput([]byte(content))
	for tokenizer.Next() {
		if tokenizer.CurrentHash() != 17740306289234851963 {
			t.Error("get err hash")
		}
	}
}

func TestSimpleGramTokenizerV1WithSplit3(t *testing.T) {
	content := "10..88.199.12123"
	tokenizer := NewSimpleGramTokenizerV1(CONTENT_SPLIT_TABLE, MISS_CONTENT_SPLIT_TABLE_INDEX)
	tokenizer.InitInput([]byte(content))
	for tokenizer.Next() {
		if tokenizer.CurrentHash() != 17740306289234851963 {
			t.Error("get err hash")
		}
	}
}

func TestSimpleGramTokenizerV1WithSplit4(t *testing.T) {
	content := "....10....88....199..12123"
	tokenizer := NewSimpleGramTokenizerV1(CONTENT_SPLIT_TABLE, MISS_CONTENT_SPLIT_TABLE_INDEX)
	tokenizer.InitInput([]byte(content))
	for tokenizer.Next() {
		if tokenizer.CurrentHash() != 17740306289234851963 {
			t.Error("get err hash")
		}
	}
}

func TestSimpleGramTokenizerV1WithSplit5(t *testing.T) {
	content := "....10....88....199..12123...."
	tokenizer := NewSimpleGramTokenizerV1(CONTENT_SPLIT_TABLE, MISS_CONTENT_SPLIT_TABLE_INDEX)
	tokenizer.InitInput([]byte(content))
	for tokenizer.Next() {
		if tokenizer.CurrentHash() != 17740306289234851963 {
			t.Error("get err hash")
		}
	}
}

func TestSimpleGramTokenizerV1WithSplit6(t *testing.T) {
	content := "....10....88"
	tokenizer := NewSimpleGramTokenizerV1(CONTENT_SPLIT_TABLE, MISS_CONTENT_SPLIT_TABLE_INDEX)
	tokenizer.InitInput([]byte(content))

	tokenizer2 := NewSimpleGramTokenizerWithIP(CONTENT_SPLIT_TABLE, false)
	tokenizer2.InitInput([]byte(content))

	tokenHash2 := make(map[uint64]bool)
	for tokenizer2.Next() {
		tokenHash2[tokenizer2.CurrentHash()] = true
	}

	for tokenizer.Next() {
		if !tokenHash2[tokenizer.CurrentHash()] {
			t.Error("get err hash")
		}
	}
}

func TestSimpleGramTokenizerV1WithSplit7(t *testing.T) {
	content := "h_supplier_orderAdapter_230901.180000.10.68.24.42.246.18257_0"
	tokenizer := NewSimpleGramTokenizerV1(CONTENT_SPLIT_TABLE, MISS_CONTENT_SPLIT_TABLE_INDEX)
	tokenizer.InitInput([]byte(content))

	tokenizer2 := NewSimpleGramTokenizerWithIP(CONTENT_SPLIT_TABLE, false)
	tokenizer2.InitInput([]byte(content))

	tokenHash2 := make(map[uint64]bool)
	for tokenizer2.Next() {
		tokenHash2[tokenizer2.CurrentHash()] = true
	}

	for tokenizer.Next() {
		if !tokenHash2[tokenizer.CurrentHash()] {
			t.Error("get err hash")
		}
	}
}

func TestSimpleGramTokenizerWithSplit8(t *testing.T) {
	content := "...."
	tokenizer := NewSimpleGramTokenizerV1(CONTENT_SPLIT_TABLE, MISS_CONTENT_SPLIT_TABLE_INDEX)
	tokenizer.InitInput([]byte(content))

	tokenizer2 := NewSimpleGramTokenizerWithIP(CONTENT_SPLIT_TABLE, false)
	tokenizer2.InitInput([]byte(content))

	tokenHash2 := make(map[uint64]bool)
	for tokenizer2.Next() {
		tokenHash2[tokenizer2.CurrentHash()] = true
	}

	for tokenizer.Next() {
		if !tokenHash2[tokenizer.CurrentHash()] {
			t.Error("get err hash")
		}
	}
}

func TestSimpleGramTokenizerWithSplit9(t *testing.T) {
	content := "华为1234.华为123.华为12.华为1"
	tokenizer := NewSimpleGramTokenizerV1(CONTENT_SPLIT_TABLE, MISS_CONTENT_SPLIT_TABLE_INDEX)
	tokenizer.InitInput([]byte(content))

	tokenizer2 := NewSimpleGramTokenizerWithIP(CONTENT_SPLIT_TABLE, false)
	tokenizer2.InitInput([]byte(content))

	tokenHash2 := make(map[uint64]bool)
	for tokenizer2.Next() {
		tokenHash2[tokenizer2.CurrentHash()] = true
	}

	for tokenizer.Next() {
		if !tokenHash2[tokenizer.CurrentHash()] {
			t.Error("get err hash")
		}
	}
}

func TestSimpleGramTokenizerWithSplit11(t *testing.T) {
	content := "a._.b._.c"
	tokenizer := NewSimpleGramTokenizerV1(CONTENT_SPLIT_TABLE, MISS_CONTENT_SPLIT_TABLE_INDEX)
	tokenizer.InitInput([]byte(content))

	tokenizer2 := NewSimpleGramTokenizerWithIP(CONTENT_SPLIT_TABLE, false)
	tokenizer2.InitInput([]byte(content))

	tokenHash2 := make(map[uint64]bool)
	for tokenizer2.Next() {
		tokenHash2[tokenizer2.CurrentHash()] = true
	}

	for tokenizer.Next() {
		if !tokenHash2[tokenizer.CurrentHash()] {
			t.Error("get err hash")
		}
	}
}

func TestSimpleGramTokenizerWithSplit12(t *testing.T) {
	content := "1.2\".13\""
	tokenizer := NewSimpleGramTokenizerV1(CONTENT_SPLIT_TABLE, MISS_CONTENT_SPLIT_TABLE_INDEX)
	tokenizer.InitInput([]byte(content))

	tokenizer2 := NewSimpleGramTokenizerWithIP(CONTENT_SPLIT_TABLE, false)
	tokenizer2.InitInput([]byte(content))

	tokenHash2 := make(map[uint64]bool)
	for tokenizer2.Next() {
		tokenHash2[tokenizer2.CurrentHash()] = true
	}

	for tokenizer.Next() {
		if !tokenHash2[tokenizer.CurrentHash()] {
			t.Error("get err hash")
		}
	}
}

func TestSimpleGramTokenizerWithSplit13(t *testing.T) {
	content := "123"
	tokenizer := NewSimpleGramTokenizerV1(CONTENT_SPLIT_TABLE, MISS_CONTENT_SPLIT_TABLE_INDEX)
	tokenizer.InitInput([]byte(content))

	tokenizer2 := NewSimpleGramTokenizerWithIP(CONTENT_SPLIT_TABLE, false)
	tokenizer2.InitInput([]byte(content))

	tokenHash2 := make(map[uint64]bool)
	for tokenizer2.Next() {
		tokenHash2[tokenizer2.CurrentHash()] = true
	}

	for tokenizer.Next() {
		if !tokenHash2[tokenizer.CurrentHash()] {
			t.Error("get err hash")
		}
	}
}

func TestSimpleGramTokenizerWithSplit14(t *testing.T) {
	content := "footer-mobile.png"
	tokenizer := NewSimpleGramTokenizerV1(CONTENT_SPLIT_TABLE, MISS_CONTENT_SPLIT_TABLE_INDEX)
	tokenizer.InitInput([]byte(content))

	tokenizer2 := NewSimpleGramTokenizerWithIP(CONTENT_SPLIT_TABLE, false)
	tokenizer2.InitInput([]byte(content))

	tokenHash2 := make(map[uint64]bool)
	for tokenizer2.Next() {
		tokenHash2[tokenizer2.CurrentHash()] = true
	}

	for tokenizer.Next() {
		if !tokenHash2[tokenizer.CurrentHash()] {
			t.Error("get err hash")
		}
	}
}

func TestTagsSplitter(t *testing.T) {
	if string(TAGS_SPLITTER) != string(byte(6)) {
		t.Fatalf("TAGS_SPLITTER is unexpect %v", TAGS_SPLITTER)
	}
}

func NewSimpleGramTokenizerWithIP(splitTable []byte, ip bool) *SimpleGramTokenizer {
	t := &SimpleGramTokenizer{conjunctionHash: make([]uint64, ConjunctionLength), conjunctionSymbol: make([]bool, ConjunctionLength), ip: ip}
	t.splitTable = splitTable
	return t
}

func MultiHash(multiBytes ...[]byte) uint64 {
	var preHash uint64 = 0
	var hash uint64 = 0
	for _, b := range multiBytes {
		hash = Hash(b)
		if preHash != 0 {
			hash ^= bits.RotateLeft64(hash, 11) ^ (preHash * Prime_64)
			preHash = hash
		}
	}
	return hash
}
