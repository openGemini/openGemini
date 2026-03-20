package xorfilter

type BinaryFuse8 BinaryFuse[uint8]

// PopulateBinaryFuse8 fills the filter with provided keys. For best results,
// the caller should avoid having too many duplicated keys.
// The function may return an error if the set is empty.
func PopulateBinaryFuse8(keys []uint64) (*BinaryFuse8, error) {
	filter, err := NewBinaryFuse[uint8](keys)
	if err != nil {
		return nil, err
	}

	return (*BinaryFuse8)(filter), nil
}

// Contains returns `true` if key is part of the set with a false positive probability of <0.4%.
func (filter *BinaryFuse8) Contains(key uint64) bool {
	return (*BinaryFuse[uint8])(filter).Contains(key)
}
