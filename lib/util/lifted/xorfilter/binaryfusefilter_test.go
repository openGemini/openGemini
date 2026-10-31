package xorfilter

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const NUM_KEYS = 1e6
const MID_NUM_KEYS = 11500
const SMALL_NUM_KEYS = 100

type testType uint8

func TestBinaryFuseNBasic(t *testing.T) {
	keys := make([]uint64, NUM_KEYS)
	for i := range keys {
		keys[i] = rand.Uint64()
	}
	filter, _ := NewBinaryFuse[testType](keys)
	for _, v := range keys {
		assert.Equal(t, true, filter.Contains(v))
	}
	falsesize := 10000000
	matches := 0
	bpv := float64(len(filter.Fingerprints)) * 8.0 / float64(NUM_KEYS)
	fmt.Println("Binary Fuse filter:")
	fmt.Println("bits per entry ", bpv)
	for i := 0; i < falsesize; i++ {
		v := rand.Uint64()
		if filter.Contains(v) {
			matches++
		}
	}
	fpp := float64(matches) * 100.0 / float64(falsesize)
	fmt.Println("false positive rate ", fpp)
	cut := 1000
	if cut > NUM_KEYS {
		cut = NUM_KEYS
	}
	keys = keys[:cut]
	for trial := 0; trial < 10; trial++ {
		r := rand.New(rand.NewPCG(uint64(trial), uint64(trial)))
		for i := range keys {
			keys[i] = r.Uint64()
		}
		filter, _ = NewBinaryFuse[testType](keys)
		for _, v := range keys {
			assert.Equal(t, true, filter.Contains(v))
		}

	}
}

func TestBinaryFuseNIssue23(t *testing.T) {
	for trials := 0; trials < 20; trials++ {
		keys := make([]uint64, MID_NUM_KEYS)
		for i := range keys {
			keys[i] = rand.Uint64()
		}
		filter, error := NewBinaryFuse[testType](keys)
		assert.Equal(t, nil, error)
		for _, v := range keys {
			assert.Equal(t, true, filter.Contains(v))
		}
	}
}

func TestBinaryFuseNSmall(t *testing.T) {
	keys := make([]uint64, SMALL_NUM_KEYS)
	for i := range keys {
		keys[i] = rand.Uint64()
	}
	filter, _ := NewBinaryFuse[testType](keys)
	for _, v := range keys {
		assert.Equal(t, true, filter.Contains(v))
	}
	falsesize := 10000000
	matches := 0

	for i := 0; i < falsesize; i++ {
		v := rand.Uint64()
		if filter.Contains(v) {
			matches++
		}
	}
	cut := 1000
	if cut > SMALL_NUM_KEYS {
		cut = SMALL_NUM_KEYS
	}
	keys = keys[:cut]
	for trial := 0; trial < 10; trial++ {
		r := rand.New(rand.NewPCG(uint64(trial), uint64(trial)))
		for i := range keys {
			keys[i] = r.Uint64()
		}
		filter, _ = NewBinaryFuse[testType](keys)
		for _, v := range keys {
			assert.Equal(t, true, filter.Contains(v))
		}

	}
}

func TestBinaryFuseN_ZeroSet(t *testing.T) {
	keys := []uint64{}
	_, err := NewBinaryFuse[testType](keys)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
}

func TestBinaryFuseN_DuplicateKeysBinaryFuseDup(t *testing.T) {
	keys := []uint64{303, 1, 77, 31, 241, 303}
	_, err := NewBinaryFuse[testType](keys)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
}

func TestBinaryFuseN_DuplicateKeysBinaryFuseDup_Issue30(t *testing.T) {
	keys := []uint64{
		14032282262966018013,
		14032282273189634013,
		14434670549455045197,
		14434670549455045197,
		14434715112030278733,
		14434715112030278733,
		1463031668069456414,
		1463031668069456414,
		15078258904550751789,
		15081947205023144749,
		15087793929176324909,
		15087793929514872877,
		15428597303557855302,
		15431797104190473360,
		15454853113467544134,
		1577077805634642122,
		15777410361767557472,
		15907998856512513094,
		15919978655645680696,
		1592170445630803483,
		15933058486048027407,
		15933070362921612719,
		15949859010628284683,
		15950094057516674097,
		15950094057516674097,
		15950492113755294966,
		15999960652771912055,
		16104958339467613609,
		16115083045828466089,
		16115119760717288873,
		16126347135921205846,
		16180939948277777353,
		16205881181578942897,
		16207480993107654476,
		1627916223119626716,
		16303139460042870203,
		16303139460042870203,
		1630429337332308348,
		16309304071237318790,
		16314547479302655419,
		16314547479302655419,
		16369820198817029405,
		16448390727851746333,
		16465049428524180509,
		16465073162513458205,
		16465073285148156957,
		16465073285149870384,
		16465073285149877277,
		16465073285893104669,
		16555387163297522125,
		16592146351271542115,
		16682791020048538670,
		16683514177871458902,
		16699277535828137630,
		16716099852308345174,
		16716099868253794902,
		16856736053711445064,
		16856736054253850696,
		16856736060613333064,
		16877690937235789198,
		16963977918744734769,
		16976350133984177557,
		16976376109946388059,
		17041493382094423395,
		17053822556128759139,
		17067586192959011138,
		17088637646961899303,
		17121323146925062160,
		17130440365429237769,
		17130440365429237769,
		17130440597658279433,
		17130440597658279433,
		17181620514756131957,
		17193256430982721885,
		17193256636319002973,
		17264031033993538756,
		17321155670529409646,
		17514402547088160271,
		17514402547088160271,
		1823133498679825084,
		1823180415377412796,
		18278489907932484471,
		1831024066115736252,
		18341786752172751552,
		18378944050902766168,
		18378944052194427480,
		18403514326223737719,
		18405070344654600695,
		2164472587301781504,
		2164472587301781504,
		2290190445057074187,
		2471837983693302824,
		2471837983693302824,
		3138094539259513280,
		3138094539259513280,
		3138153989894179264,
		3138153989894179264,
		3566850904877432832,
		3566850904877432832,
		3868495676835528327,
		3868495676835528327,
		3981182070595518464,
		3981182070595518464,
		3998521163612422144,
		3998521163612422144,
		3998521164578160640,
		3998521164578160640,
		3998521164581306368,
		3998521164581306368,
		3998521164581329296,
		3998521164581329296,
		4334725363086930304,
		4334725363086930304,
		4337388653622853632,
		4337388653622853632,
		4587006656968527746,
		4587006656968527746,
		4587006831041087252,
		4587006831041087252,
		4825061103098367168,
		4825061103098367168,
	}
	_, err := NewBinaryFuse[testType](keys)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
}

var (
	bogusbool      bool
	binaryfusedbig *BinaryFuse8
	bogusbinary    *BinaryFuse[testType]
)

func BenchmarkBinaryFuseNPopulate1000000(b *testing.B) {
	keys := make([]uint64, NUM_KEYS)
	for i := range keys {
		keys[i] = rand.Uint64()
	}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		bogusbinary, _ = NewBinaryFuse[testType](keys)
	}
}

func BenchmarkConstructBinaryFuseN(b *testing.B) {
	bigrandomarrayInit()
	b.ResetTimer()
	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		bogusbinary, _ = NewBinaryFuse[testType](bigrandomarray)
	}
}

func BenchmarkBinaryFuseNContains1000000(b *testing.B) {
	keys := make([]uint64, NUM_KEYS)
	for i := range keys {
		keys[i] = rand.Uint64()
	}
	filter, _ := NewBinaryFuse[testType](keys)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		bogusbool = filter.Contains(keys[n%len(keys)])
	}
}

func binaryfusedbigInit() {
	fmt.Println("Binary Fuse setup")
	keys := make([]uint64, 50000000)
	for i := range keys {
		keys[i] = rand.Uint64()
	}
	binaryfusedbig, _ = PopulateBinaryFuse8(keys)
	fmt.Println("Binary Fuse setup ok")
}

func BenchmarkBinaryFuseNContains50000000(b *testing.B) {
	if binaryfusedbig == nil {
		binaryfusedbigInit()
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		bogusbool = binaryfusedbig.Contains(rand.Uint64())
	}
}

func TestBinaryFuseN_Issue35(t *testing.T) {
	for test := 0; test < 100; test++ {
		hashes := make([]uint64, 0)
		for i := 0; i < 40000; i++ {
			v := encode(rand.Int32N(10), rand.Int32N(100000))
			hashes = append(hashes, xxhash.Sum64(v))
		}
		inner, err := NewBinaryFuse[testType](hashes)
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

func TestBinaryFuseBuilder(t *testing.T) {
	// Verify that repeated builds with the same builder create the exact same
	// filter as using NewBinaryFuse.
	var bld BinaryFuseBuilder
	for i := 0; i < 100; i++ {
		n := 1 + rand.IntN(1<<rand.IntN(20))
		keys := make([]uint64, n)
		for j := range keys {
			keys[j] = rand.Uint64()
		}
		switch rand.IntN(3) {
		case 0:
			crossCheckFuseBuilder[uint8](t, &bld, keys)
		case 1:
			crossCheckFuseBuilder[uint16](t, &bld, keys)
		case 2:
			crossCheckFuseBuilder[uint32](t, &bld, keys)
		}
	}
}

func crossCheckFuseBuilder[T Unsigned](t *testing.T, bld *BinaryFuseBuilder, keys []uint64) {
	t.Helper()
	filter, err := BuildBinaryFuse[T](bld, slices.Clone(keys))
	require.NoError(t, err)
	expected, err := NewBinaryFuse[T](keys)
	require.NoError(t, err)
	_ = expected
	require.Equal(t, *expected, filter)
}
