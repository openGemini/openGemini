/*
Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.

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

package executor

import "math/rand"

type THashValue uint32

type TExtendedHashValue uint64

// only support single hash per sketch, todo
type THashVector THashValue

const MersennePower int = 31
const MersennePrime TExtendedHashValue = (TExtendedHashValue(1) << MersennePower) - 1

// var hashN int = 1

type THasher struct {
	Depth                          int
	WidthLog2                      int
	PolynomialHashBases            []TExtendedHashValue
	MultiplicativeHashCoefficients [][]TExtendedHashValue
}

func NewTHasher(depth int, widthLog2 int, seed uint64) THasher {
	re := THasher{
		Depth:                          depth,
		WidthLog2:                      widthLog2,
		PolynomialHashBases:            make([]TExtendedHashValue, depth),
		MultiplicativeHashCoefficients: make([][]TExtendedHashValue, depth),
	}
	source := rand.NewSource(int64(seed))
	r := rand.New(source)
	for i := 0; i < depth; i++ {
		re.PolynomialHashBases[i] = TExtendedHashValue(r.Int63n(int64(MersennePrime)))
		re.MultiplicativeHashCoefficients[i] = append(re.MultiplicativeHashCoefficients[i], TExtendedHashValue(r.Uint64()))
		re.MultiplicativeHashCoefficients[i] = append(re.MultiplicativeHashCoefficients[i], TExtendedHashValue(r.Uint64()))
	}
	return re
}

// only support single hash per sketch, todo
func (h *THasher) UpdateHashes(key TKey, numBitsBefore int, numBitsAfter int, hashes *THashVector) {
	// polynomial hash
	for i := numBitsBefore; i < numBitsAfter; i++ {
		*hashes = THashVector(ModuleHash(h.PolynomialHashBases[0]*TExtendedHashValue(*hashes) + TExtendedHashValue(B2ui64(key.Get(i)))))
	}
}

func ModuleHash(hash TExtendedHashValue) THashValue {
	hash = (hash >> MersennePower) + (hash & MersennePrime)
	if hash >= MersennePrime {
		return THashValue(hash - MersennePrime)
	}
	return THashValue(hash)
}

// only support single hash per sketch, todo
func (h *THasher) UpdateHashesBytewise(key TKey, numBytesBefore int, numBytesAfter int, hashes *THashVector) {
	for i := numBytesBefore; i < numBytesAfter; i++ {
		*hashes = THashVector(ModuleHash(h.PolynomialHashBases[0]*TExtendedHashValue(*hashes) + TExtendedHashValue(key.GetByte(i))))
	}
}

// only support single hash per sketch, todo
func (h *THasher) TurnHashesIntoIndices(hashes THashVector, indices *[]int) {
	// multiplicative hash function (2.4 from hashing guide)
	for i := 0; i < h.Depth; i++ {
		(*indices)[i] = int((h.MultiplicativeHashCoefficients[i][0]*TExtendedHashValue(hashes) + h.MultiplicativeHashCoefficients[i][1]) >> (64 - h.WidthLog2))
	}
}

// only support single hash per sketch, todo
func (h *THasher) GetInitialHashVector(hashVector *THashVector) {
	*hashVector = 1
}
