package xorfilter

import (
	"fmt"
	"math/rand/v2"
	"testing"
	"time"
	"unsafe"

	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/assert"
)

var rng = uint64(time.Now().UnixNano())

func TestBasic(t *testing.T) {
	keys := make([]uint64, NUM_KEYS)
	for i := range keys {
		keys[i] = splitmix64(&rng)
	}
	filter, _ := Populate(keys)
	for _, v := range keys {
		assert.Equal(t, true, filter.Contains(v))
	}
	falsesize := 10000000
	matches := 0
	bpv := float64(len(filter.Fingerprints)) * 8.0 / float64(NUM_KEYS)
	fmt.Println("Xor8 filter:")
	fmt.Println("bits per entry ", bpv)
	for i := 0; i < falsesize; i++ {
		v := splitmix64(&rng)
		if filter.Contains(v) {
			matches++
		}
	}
	fpp := float64(matches) * 100.0 / float64(falsesize)
	fmt.Println("false positive rate ", fpp)
	assert.Equal(t, true, fpp < 0.40)
	cut := 1000
	if cut > NUM_KEYS {
		cut = NUM_KEYS
	}
	keys = keys[:cut]
	for trial := 0; trial < 10; trial++ {
		for i := range keys {
			keys[i] = splitmix64(&rng)
		}
		filter, _ = Populate(keys)
		for _, v := range keys {
			assert.Equal(t, true, filter.Contains(v))
		}
	}
}

func TestSmall(t *testing.T) {
	keys := make([]uint64, SMALL_NUM_KEYS)
	for i := range keys {
		keys[i] = splitmix64(&rng)
	}
	filter, _ := Populate(keys)
	for _, v := range keys {
		assert.Equal(t, true, filter.Contains(v))
	}
	falsesize := 10000000
	matches := 0
	for i := 0; i < falsesize; i++ {
		v := splitmix64(&rng)
		if filter.Contains(v) {
			matches++
		}
	}
	fpp := float64(matches) * 100.0 / float64(falsesize)
	assert.Equal(t, true, fpp < 0.40)
	cut := 1000
	if cut > SMALL_NUM_KEYS {
		cut = SMALL_NUM_KEYS
	}
	keys = keys[:cut]
	for trial := 0; trial < 10; trial++ {
		for i := range keys {
			keys[i] = splitmix64(&rng)
		}
		filter, _ = Populate(keys)
		for _, v := range keys {
			assert.Equal(t, true, filter.Contains(v))
		}
	}
}

func BenchmarkPopulate100000(b *testing.B) {
	testsize := 10000
	keys := make([]uint64, testsize)

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		for i := range keys {
			keys[i] = splitmix64(&rng)
		}
		b.StartTimer()
		Populate(keys)
	}
}

func encode(v1, v2 int32) []byte {
	v := make([]byte, 8)
	v = append(v, unsafe.Slice((*byte)(unsafe.Pointer(&v1)), 4)...)
	v = append(v, unsafe.Slice((*byte)(unsafe.Pointer(&v2)), 4)...)
	return v
}

// credit: el10savio
func Test_DuplicateKeys(t *testing.T) {
	keys := []uint64{1, 77, 31, 241, 303, 303}
	_, err := Populate(keys)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
}

func BenchmarkContains100000(b *testing.B) {
	testsize := 10000
	keys := make([]uint64, testsize)
	for i := range keys {
		keys[i] = splitmix64(&rng)
	}
	filter, _ := Populate(keys)

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		filter.Contains(keys[n%len(keys)])
	}
}

const CONSTRUCT_SIZE = 10000000

var bigrandomarray []uint64

func bigrandomarrayInit() {
	if bigrandomarray == nil {
		fmt.Println("bigrandomarray setup with CONSTRUCT_SIZE = ", CONSTRUCT_SIZE)
		bigrandomarray = make([]uint64, CONSTRUCT_SIZE)
		for i := range bigrandomarray {
			bigrandomarray[i] = rand.Uint64()
		}
	}
}

func BenchmarkConstructXor8(b *testing.B) {
	bigrandomarrayInit()
	b.ResetTimer()
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		Populate(bigrandomarray)
	}
}

var xor8big *Xor8

func xor8bigInit() {
	fmt.Println("Xor8 setup")
	keys := make([]uint64, 50000000)
	for i := range keys {
		keys[i] = rand.Uint64()
	}
	xor8big, _ = Populate(keys)
	fmt.Println("Xor8 setup ok")
}

func BenchmarkXor8bigContains50000000(b *testing.B) {
	if xor8big == nil {
		xor8bigInit()
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		xor8big.Contains(rand.Uint64())
	}
}

func TestFSDIssue35_basic(t *testing.T) {
	hashes := make([]uint64, 0)
	for i := 0; i < 2000; i++ {
		v := encode(rand.Int32N(10), rand.Int32N(100000))
		hashes = append(hashes, xxhash.Sum64(v))
	}
	inner, err := Populate(hashes)
	if err != nil {
		panic(err)
	}
	for i, d := range hashes {
		e := inner.Contains(d)
		fmt.Println("checking ", d)
		if !e {
			panic(i)
		}
	}
}

func Test_Issue35_basic(t *testing.T) {
	for test := 0; test < 100; test++ {
		hashes := make([]uint64, 0)
		for i := 0; i < 40000; i++ {
			v := encode(rand.Int32N(10), rand.Int32N(100000))
			hashes = append(hashes, xxhash.Sum64(v))
		}
		inner, err := PopulateBinaryFuse8(hashes)
		if err != nil {
			panic(err)
		}
		for i, d := range hashes {
			e := inner.Contains(d)
			if !e {
				panic(i)
			}

		}
	}
}
