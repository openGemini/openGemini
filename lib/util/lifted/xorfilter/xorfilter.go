package xorfilter

import (
	"errors"
	"math"
	"slices"
)

func murmur64(h uint64) uint64 {
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	h ^= h >> 33
	return h
}

// returns random number, modifies the seed
func splitmix64(seed *uint64) uint64 {
	*seed = *seed + 0x9E3779B97F4A7C15
	z := *seed
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	return z ^ (z >> 31)
}

func mixsplit(key, seed uint64) uint64 {
	return murmur64(key + seed)
}

func rotl64(n uint64, c int) uint64 {
	return (n << uint(c&63)) | (n >> uint((-c)&63))
}

func reduce(hash, n uint32) uint32 {
	// http://lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/
	return uint32((uint64(hash) * uint64(n)) >> 32)
}

func fingerprint(hash uint64) uint64 {
	return hash ^ (hash >> 32)
}

// Contains tell you whether the key is likely part of the set
func (filter *Xor8) Contains(key uint64) bool {
	hash := mixsplit(key, filter.Seed)
	f := uint8(fingerprint(hash))
	r0 := uint32(hash)
	r1 := uint32(rotl64(hash, 21))
	r2 := uint32(rotl64(hash, 42))
	h0 := reduce(r0, filter.BlockLength)
	h1 := reduce(r1, filter.BlockLength) + filter.BlockLength
	h2 := reduce(r2, filter.BlockLength) + 2*filter.BlockLength
	return f == (filter.Fingerprints[h0] ^ filter.Fingerprints[h1] ^ filter.Fingerprints[h2])
}

func (filter *Xor8) geth0h1h2(k uint64) hashes {
	hash := mixsplit(k, filter.Seed)
	answer := hashes{}
	answer.h = hash
	r0 := uint32(hash)
	r1 := uint32(rotl64(hash, 21))
	r2 := uint32(rotl64(hash, 42))

	answer.h0 = reduce(r0, filter.BlockLength)
	answer.h1 = reduce(r1, filter.BlockLength)
	answer.h2 = reduce(r2, filter.BlockLength)
	return answer
}

func (filter *Xor8) geth0(hash uint64) uint32 {
	r0 := uint32(hash)
	return reduce(r0, filter.BlockLength)
}

func (filter *Xor8) geth1(hash uint64) uint32 {
	r1 := uint32(rotl64(hash, 21))
	return reduce(r1, filter.BlockLength)
}

func (filter *Xor8) geth2(hash uint64) uint32 {
	r2 := uint32(rotl64(hash, 42))
	return reduce(r2, filter.BlockLength)
}

// scan for values with a count of one
func scanCount(Qi []keyindex, setsi []xorset) ([]keyindex, int) {
	QiSize := 0

	// len(setsi) = filter.BlockLength
	for i := uint32(0); i < uint32(len(setsi)); i++ {
		if setsi[i].count == 1 {
			Qi[QiSize].index = i
			Qi[QiSize].hash = setsi[i].xormask
			QiSize++
		}
	}

	return Qi, QiSize
}

// MaxIterations is the maximum number of iterations allowed before the populate
// function returns an error.
var MaxIterations = 1024

// Populate fills the filter with provided keys. For best results,
// the caller should avoid having too many duplicated keys.
// The function may return an error if the set is empty.
func Populate(keys []uint64) (*Xor8, error) {
	size := len(keys)
	if size == 0 {
		return nil, errors.New("provide a non-empty set")
	}
	capacity := 32 + uint32(math.Ceil(1.23*float64(size)))
	capacity = capacity / 3 * 3 // round it down to a multiple of 3

	filter := &Xor8{}
	var rngcounter uint64 = 1
	filter.Seed = splitmix64(&rngcounter)
	filter.BlockLength = capacity / 3

	// slice capacity defaults to length
	filter.Fingerprints = make([]uint8, capacity)

	stack := make([]keyindex, size)
	Q0 := make([]keyindex, filter.BlockLength)
	Q1 := make([]keyindex, filter.BlockLength)
	Q2 := make([]keyindex, filter.BlockLength)
	sets0 := make([]xorset, filter.BlockLength)
	sets1 := make([]xorset, filter.BlockLength)
	sets2 := make([]xorset, filter.BlockLength)
	iterations := 0

	for {
		iterations += 1
		if iterations > MaxIterations {
			// The probability of this happening is lower than the
			// the cosmic-ray probability (i.e., a cosmic ray corrupts your system).
			return nil, errors.New("too many iterations")
		}

		for i := 0; i < size; i++ {
			key := keys[i]
			hs := filter.geth0h1h2(key)
			sets0[hs.h0].xormask ^= hs.h
			sets0[hs.h0].count++
			sets1[hs.h1].xormask ^= hs.h
			sets1[hs.h1].count++
			sets2[hs.h2].xormask ^= hs.h
			sets2[hs.h2].count++
		}

		// scan for values with a count of one
		Q0, Q0size := scanCount(Q0, sets0)
		Q1, Q1size := scanCount(Q1, sets1)
		Q2, Q2size := scanCount(Q2, sets2)

		stacksize := 0
		for Q0size+Q1size+Q2size > 0 {
			for Q0size > 0 {
				Q0size--
				keyindexvar := Q0[Q0size]
				index := keyindexvar.index
				if sets0[index].count == 0 {
					continue // not actually possible after the initial scan.
				}
				hash := keyindexvar.hash
				h1 := filter.geth1(hash)
				h2 := filter.geth2(hash)
				stack[stacksize] = keyindexvar
				stacksize++
				sets1[h1].xormask ^= hash

				sets1[h1].count--
				if sets1[h1].count == 1 {
					Q1[Q1size].index = h1
					Q1[Q1size].hash = sets1[h1].xormask
					Q1size++
				}
				sets2[h2].xormask ^= hash
				sets2[h2].count--
				if sets2[h2].count == 1 {
					Q2[Q2size].index = h2
					Q2[Q2size].hash = sets2[h2].xormask
					Q2size++
				}
			}
			for Q1size > 0 {
				Q1size--
				keyindexvar := Q1[Q1size]
				index := keyindexvar.index
				if sets1[index].count == 0 {
					continue
				}
				hash := keyindexvar.hash
				h0 := filter.geth0(hash)
				h2 := filter.geth2(hash)
				keyindexvar.index += filter.BlockLength
				stack[stacksize] = keyindexvar
				stacksize++
				sets0[h0].xormask ^= hash
				sets0[h0].count--
				if sets0[h0].count == 1 {
					Q0[Q0size].index = h0
					Q0[Q0size].hash = sets0[h0].xormask
					Q0size++
				}
				sets2[h2].xormask ^= hash
				sets2[h2].count--
				if sets2[h2].count == 1 {
					Q2[Q2size].index = h2
					Q2[Q2size].hash = sets2[h2].xormask
					Q2size++
				}
			}
			for Q2size > 0 {
				Q2size--
				keyindexvar := Q2[Q2size]
				index := keyindexvar.index
				if sets2[index].count == 0 {
					continue
				}
				hash := keyindexvar.hash
				h0 := filter.geth0(hash)
				h1 := filter.geth1(hash)
				keyindexvar.index += 2 * filter.BlockLength

				stack[stacksize] = keyindexvar
				stacksize++
				sets0[h0].xormask ^= hash
				sets0[h0].count--
				if sets0[h0].count == 1 {
					Q0[Q0size].index = h0
					Q0[Q0size].hash = sets0[h0].xormask
					Q0size++
				}
				sets1[h1].xormask ^= hash
				sets1[h1].count--
				if sets1[h1].count == 1 {
					Q1[Q1size].index = h1
					Q1[Q1size].hash = sets1[h1].xormask
					Q1size++
				}

			}
		}

		if stacksize == size {
			// success
			break
		}

		if iterations == 10 {
			keys = pruneDuplicates(keys)
			size = len(keys)
		}

		clear(sets0)
		clear(sets1)
		clear(sets2)

		filter.Seed = splitmix64(&rngcounter)
	}

	stacksize := size
	for stacksize > 0 {
		stacksize--
		ki := stack[stacksize]
		val := uint8(fingerprint(ki.hash))
		if ki.index < filter.BlockLength {
			val ^= filter.Fingerprints[filter.geth1(ki.hash)+filter.BlockLength] ^ filter.Fingerprints[filter.geth2(ki.hash)+2*filter.BlockLength]
		} else if ki.index < 2*filter.BlockLength {
			val ^= filter.Fingerprints[filter.geth0(ki.hash)] ^ filter.Fingerprints[filter.geth2(ki.hash)+2*filter.BlockLength]
		} else {
			val ^= filter.Fingerprints[filter.geth0(ki.hash)] ^ filter.Fingerprints[filter.geth1(ki.hash)+filter.BlockLength]
		}
		filter.Fingerprints[ki.index] = val
	}
	return filter, nil
}

func pruneDuplicates(array []uint64) []uint64 {
	slices.Sort(array)
	pos := 0
	for i := 1; i < len(array); i++ {
		if array[i] != array[pos] {
			array[pos+1] = array[i]
			pos += 1
		}
	}
	return array[:pos+1]
}
