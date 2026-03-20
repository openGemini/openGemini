package xorfilter

import (
	"errors"
	"math"
	"math/bits"
	"unsafe"
)

type Unsigned interface {
	~uint8 | ~uint16 | ~uint32
}

type BinaryFuse[T Unsigned] struct {
	Seed               uint64
	SegmentLength      uint32
	SegmentLengthMask  uint32
	SegmentCount       uint32
	SegmentCountLength uint32

	Fingerprints []T
}

// NewBinaryFuse creates a binary fuse filter with provided keys. For best
// results, the caller should avoid having too many duplicated keys.
//
// The function can mutate the given keys slice to remove duplicates.
//
// The function may return an error if the set is empty.
func NewBinaryFuse[T Unsigned](keys []uint64) (*BinaryFuse[T], error) {
	var b BinaryFuseBuilder
	filter, err := BuildBinaryFuse[T](&b, keys)
	if err != nil {
		return nil, err
	}
	return &filter, nil
}

// BinaryFuseBuilder can be used to reuse memory allocations across multiple
// BinaryFuse builds.
type BinaryFuseBuilder struct {
	alone        reusableBuffer
	t2hash       reusableBuffer
	reverseOrder reusableBuffer
	t2count      reusableBuffer
	reverseH     reusableBuffer
	startPos     reusableBuffer
	fingerprints reusableBuffer
}

// BuildBinaryFuse creates a binary fuse filter with provided keys, reusing
// buffers from the BinaryFuseBuilder if possible. For best results, the caller
// should avoid having too many duplicated keys.
//
// The function can mutate the given keys slice to remove duplicates.
//
// The function may return an error if the set is empty.
func BuildBinaryFuse[T Unsigned](b *BinaryFuseBuilder, keys []uint64) (BinaryFuse[T], error) {
	size := uint32(len(keys))
	var filter BinaryFuse[T]
	filter.initializeParameters(b, size)
	rngcounter := uint64(1)
	filter.Seed = splitmix64(&rngcounter)
	capacity := uint32(len(filter.Fingerprints))

	alone := reuseBuffer[uint32](&b.alone, int(capacity))
	// the lowest 2 bits are the h index (0, 1, or 2)
	// so we only have 6 bits for counting;
	// but that's sufficient
	t2count := reuseBuffer[T](&b.t2count, int(capacity))
	reverseH := reuseBuffer[T](&b.reverseH, int(size))

	t2hash := reuseBuffer[uint64](&b.t2hash, int(capacity))
	reverseOrder := reuseBuffer[uint64](&b.reverseOrder, int(size+1))
	reverseOrder[size] = 1

	// the array h0, h1, h2, h0, h1, h2
	var h012 [6]uint32
	// this could be used to compute the mod3
	// tabmod3 := [5]uint8{0,1,2,0,1}
	iterations := 0
	for {
		iterations += 1
		if iterations > MaxIterations {
			// The probability of this happening is lower than the cosmic-ray
			// probability (i.e., a cosmic ray corrupts your system).
			return BinaryFuse[T]{}, errors.New("too many iterations")
		}

		blockBits := 1
		for (1 << blockBits) < filter.SegmentCount {
			blockBits += 1
		}
		startPos := reuseBuffer[uint](&b.startPos, 1<<blockBits)
		for i := range startPos {
			// important: we do not want i * size to overflow!!!
			startPos[i] = uint((uint64(i) * uint64(size)) >> blockBits)
		}
		for _, key := range keys {
			hash := mixsplit(key, filter.Seed)
			segment_index := hash >> (64 - blockBits)
			for reverseOrder[startPos[segment_index]] != 0 {
				segment_index++
				segment_index &= (1 << blockBits) - 1
			}
			reverseOrder[startPos[segment_index]] = hash
			startPos[segment_index] += 1
		}
		error := 0
		duplicates := uint32(0)

		for i := uint32(0); i < size; i++ {
			hash := reverseOrder[i]
			index1, index2, index3 := filter.getHashFromHash(hash)
			t2count[index1] += 4
			// t2count[index1] ^= 0 // noop
			t2hash[index1] ^= hash
			t2count[index2] += 4
			t2count[index2] ^= 1
			t2hash[index2] ^= hash
			t2count[index3] += 4
			t2count[index3] ^= 2
			t2hash[index3] ^= hash
			// If we have duplicated hash values, then it is likely that
			// the next comparison is true
			if t2hash[index1]&t2hash[index2]&t2hash[index3] == 0 {
				// next we do the actual test
				if ((t2hash[index1] == 0) && (t2count[index1] == 8)) || ((t2hash[index2] == 0) && (t2count[index2] == 8)) || ((t2hash[index3] == 0) && (t2count[index3] == 8)) {
					duplicates += 1
					t2count[index1] -= 4
					t2hash[index1] ^= hash
					t2count[index2] -= 4
					t2count[index2] ^= 1
					t2hash[index2] ^= hash
					t2count[index3] -= 4
					t2count[index3] ^= 2
					t2hash[index3] ^= hash
				}
			}
			if t2count[index1] < 4 {
				error = 1
			}
			if t2count[index2] < 4 {
				error = 1
			}
			if t2count[index3] < 4 {
				error = 1
			}
		}
		if error == 1 {
			for i := uint32(0); i < size; i++ {
				reverseOrder[i] = 0
			}
			for i := uint32(0); i < capacity; i++ {
				t2count[i] = 0
				t2hash[i] = 0
			}
			filter.Seed = splitmix64(&rngcounter)
			continue
		}

		// End of key addition

		Qsize := 0
		// Add sets with one key to the queue.
		for i := uint32(0); i < capacity; i++ {
			alone[Qsize] = i
			if (t2count[i] >> 2) == 1 {
				Qsize++
			}
		}
		stacksize := uint32(0)
		for Qsize > 0 {
			Qsize--
			index := alone[Qsize]
			if (t2count[index] >> 2) == 1 {
				hash := t2hash[index]
				found := t2count[index] & 3
				reverseH[stacksize] = found
				reverseOrder[stacksize] = hash
				stacksize++

				index1, index2, index3 := filter.getHashFromHash(hash)

				h012[1] = index2
				h012[2] = index3
				h012[3] = index1
				h012[4] = h012[1]

				other_index1 := h012[found+1]
				alone[Qsize] = other_index1
				if (t2count[other_index1] >> 2) == 2 {
					Qsize++
				}
				t2count[other_index1] -= 4
				t2count[other_index1] ^= filter.mod3(found + 1) // could use this instead: tabmod3[found+1]
				t2hash[other_index1] ^= hash

				other_index2 := h012[found+2]
				alone[Qsize] = other_index2
				if (t2count[other_index2] >> 2) == 2 {
					Qsize++
				}
				t2count[other_index2] -= 4
				t2count[other_index2] ^= filter.mod3(found + 2) // could use this instead: tabmod3[found+2]
				t2hash[other_index2] ^= hash
			}
		}

		if stacksize+duplicates == size {
			// Success
			size = stacksize
			break
		} else if duplicates > 0 {
			// Duplicates were found, but we did not
			// manage to remove them all. We may simply sort the key to
			// solve the issue. This will run in time O(n log n) and it
			// mutates the input.
			keys = pruneDuplicates(keys)
		}
		for i := uint32(0); i < size; i++ {
			reverseOrder[i] = 0
		}
		for i := uint32(0); i < capacity; i++ {
			t2count[i] = 0
			t2hash[i] = 0
		}
		filter.Seed = splitmix64(&rngcounter)
	}
	if size == 0 {
		return filter, nil
	}

	for i := int(size - 1); i >= 0; i-- {
		// the hash of the key we insert next
		hash := reverseOrder[i]
		xor2 := T(fingerprint(hash))
		index1, index2, index3 := filter.getHashFromHash(hash)
		found := reverseH[i]
		h012[0] = index1
		h012[1] = index2
		h012[2] = index3
		h012[3] = h012[0]
		h012[4] = h012[1]
		filter.Fingerprints[h012[found]] = xor2 ^ filter.Fingerprints[h012[found+1]] ^ filter.Fingerprints[h012[found+2]]
	}

	return filter, nil
}

func (filter *BinaryFuse[T]) initializeParameters(b *BinaryFuseBuilder, size uint32) {
	arity := uint32(3)
	filter.SegmentLength = calculateSegmentLength(arity, size)
	if filter.SegmentLength > 262144 {
		filter.SegmentLength = 262144
	}
	filter.SegmentLengthMask = filter.SegmentLength - 1
	sizeFactor := calculateSizeFactor(arity, size)
	capacity := uint32(0)
	if size > 1 {
		capacity = uint32(math.Round(float64(size) * sizeFactor))
	}
	initSegmentCount := (capacity+filter.SegmentLength-1)/filter.SegmentLength - (arity - 1)
	arrayLength := (initSegmentCount + arity - 1) * filter.SegmentLength
	filter.SegmentCount = (arrayLength + filter.SegmentLength - 1) / filter.SegmentLength
	if filter.SegmentCount <= arity-1 {
		filter.SegmentCount = 1
	} else {
		filter.SegmentCount = filter.SegmentCount - (arity - 1)
	}
	arrayLength = (filter.SegmentCount + arity - 1) * filter.SegmentLength
	filter.SegmentCountLength = filter.SegmentCount * filter.SegmentLength
	filter.Fingerprints = reuseBuffer[T](&b.fingerprints, int(arrayLength))
}

func (filter *BinaryFuse[T]) mod3(x T) T {
	if x > 2 {
		x -= 3
	}

	return x
}

func (filter *BinaryFuse[T]) getHashFromHash(hash uint64) (uint32, uint32, uint32) {
	hi, _ := bits.Mul64(hash, uint64(filter.SegmentCountLength))
	h0 := uint32(hi)
	h1 := h0 + filter.SegmentLength
	h2 := h1 + filter.SegmentLength
	h1 ^= uint32(hash>>18) & filter.SegmentLengthMask
	h2 ^= uint32(hash) & filter.SegmentLengthMask
	return h0, h1, h2
}

// Contains returns `true` if key is part of the set with a false positive probability.
func (filter *BinaryFuse[T]) Contains(key uint64) bool {
	hash := mixsplit(key, filter.Seed)
	f := T(fingerprint(hash))
	h0, h1, h2 := filter.getHashFromHash(hash)
	f ^= filter.Fingerprints[h0] ^ filter.Fingerprints[h1] ^ filter.Fingerprints[h2]
	return f == 0
}

func calculateSegmentLength(arity uint32, size uint32) uint32 {
	// These parameters are very sensitive. Replacing 'floor' by 'round' can
	// substantially affect the construction time.
	if size == 0 {
		return 4
	}
	if arity == 3 {
		return uint32(1) << int(math.Floor(math.Log(float64(size))/math.Log(3.33)+2.25))
	} else if arity == 4 {
		return uint32(1) << int(math.Floor(math.Log(float64(size))/math.Log(2.91)-0.5))
	} else {
		return 65536
	}
}

func calculateSizeFactor(arity uint32, size uint32) float64 {
	if arity == 3 {
		return math.Max(1.125, 0.875+0.25*math.Log(1000000)/math.Log(float64(size)))
	} else if arity == 4 {
		return math.Max(1.075, 0.77+0.305*math.Log(600000)/math.Log(float64(size)))
	} else {
		return 2.0
	}
}

// reusableBuffer allows reuse of a backing buffer to avoid allocations for
// slices of integers.
type reusableBuffer struct {
	buf []uint64
}

type integer interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64
}

// reuseBuffer returns an empty slice of the given size, reusing the last buffer
// if possible.
func reuseBuffer[T integer](b *reusableBuffer, size int) []T {
	const sizeOfUint64 = 8
	// Our backing buffer is a []uint64. Figure out how many uint64s we need
	// to back a []T of the requested size.
	bufSize := int((uintptr(size)*unsafe.Sizeof(T(0)) + sizeOfUint64 - 1) / sizeOfUint64)
	if cap(b.buf) >= bufSize {
		clear(b.buf[:bufSize])
	} else {
		// We need to allocate a new buffer. Increase by at least 25% to amortize
		// allocations; this is what append() does for large enough slices.
		b.buf = make([]uint64, max(bufSize, cap(b.buf)+cap(b.buf)/4))
	}
	return unsafe.Slice((*T)(unsafe.Pointer(unsafe.SliceData(b.buf))), size)
}
