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

import (
	"fmt"
	"math"
	"math/rand"
)

type TDdcmParameters struct {
	CountMinSketchWidthLog2  int
	SmallCountMinSketchDepth int
	LargeCountMinSketchDepth int
}

type TDdcm struct {
	Parameters                       TDdcmParameters
	RandomEngine                     *rand.Rand
	TotalCount                       TCounter
	LargeSketch                      TCountMinSketch
	Hashers                          []THasher
	HashersLastLevels                []int
	CycleManager                     TDdcmCycleManagerBase
	SmallSketches                    []TCountMinSketch
	SmallSketchesHasherIds           []int
	SmallSketchesNextSameHasherLevel []int

	// for hash calculation, only surrport single hash per sketch, todo
	PrecalculatedHashes []THashVector
	SmallSketchIndices  []int
	LargeSketchIndices  []int
	SargeSketchHashes   THashVector
	LargeSketchHashes   THashVector
}

func CalcDdcmParameters(errRate, minSearchableFrequency, wrongAnswerProbability float64) (TDdcmParameters, error) {
	var parameters TDdcmParameters
	// width
	if errRate <= 0 {
		return parameters, fmt.Errorf("CalcDdcmParameters errRate > 0")
	}
	parameters.CountMinSketchWidthLog2 = CalcLog2(EulerNumber / errRate)
	// depths
	if minSearchableFrequency <= errRate {
		return parameters, fmt.Errorf("CalcDdcmParameters minSearchableFrequency > errRate")
	}
	parameters.LargeCountMinSketchDepth = int(math.Ceil(math.Log((2.0*(EulerNumber-1.0)/(EulerNumber-2.0)/(minSearchableFrequency-errRate) + EulerNumber/(EulerNumber-2)) / wrongAnswerProbability)))
	parameters.SmallCountMinSketchDepth = 1
	return parameters, nil
}

func NewTDdcm(parameters *TDdcmParameters, seed uint64, cycleManager TDdcmCycleManagerBase) *TDdcm {
	ddcm := &TDdcm{
		Parameters:   *parameters,
		LargeSketch:  NewTCountMinSketch(parameters.LargeCountMinSketchDepth, parameters.CountMinSketchWidthLog2),
		CycleManager: cycleManager.Copy(),
	}
	source := rand.NewSource(int64(seed))
	ddcm.RandomEngine = rand.New(source)
	ddcm.Hashers = append(ddcm.Hashers, NewTHasher(parameters.LargeCountMinSketchDepth, parameters.CountMinSketchWidthLog2, ddcm.RandomEngine.Uint64()))
	ddcm.HashersLastLevels = append(ddcm.HashersLastLevels, -1)
	ddcm.SmallSketchIndices = make([]int, parameters.SmallCountMinSketchDepth)
	ddcm.LargeSketchIndices = make([]int, parameters.LargeCountMinSketchDepth)
	return ddcm
}

func (ddcm *TDdcm) Name() string {
	return "ddcm"
}

func (ddcm *TDdcm) GetTotalCount() TCounter {
	return ddcm.TotalCount
}

func (ddcm *TDdcm) AddKeyCount(key TKey, value TCounter) {
	size := key.GetSize()
	ddcm.ExtendLevels(size)

	for level := size - 1; level >= 0; level-- {
		ddcm.CalcHashesAndGetIndices(key, level, &ddcm.SmallSketchIndices)
		ddcm.SmallSketches[level].Add(ddcm.SmallSketchIndices, value)
	}

	ddcm.CalcLargeSketchIndices(key, &ddcm.LargeSketchIndices)
	ddcm.LargeSketch.Add(ddcm.LargeSketchIndices, value)
	ddcm.TotalCount += value
}

func (ddcm *TDdcm) GetKeyCount(key TKey) TCounter {
	size := key.GetSize()
	if size > len(ddcm.SmallSketches) {
		return 0
	}

	ddcm.CalcLargeSketchIndices(key, &ddcm.LargeSketchIndices)
	return ddcm.LargeSketch.Get(ddcm.LargeSketchIndices)
}

func (ddcm *TDdcm) ExtendLevels(size int) {
	for len(ddcm.SmallSketches) < size {
		level := len(ddcm.SmallSketches)
		ddcm.SmallSketches = append(ddcm.SmallSketches, NewTCountMinSketch(ddcm.Parameters.SmallCountMinSketchDepth, ddcm.Parameters.CountMinSketchWidthLog2))
		ddcm.SmallSketchesNextSameHasherLevel = append(ddcm.SmallSketchesNextSameHasherLevel, level) // no such level, so put `level` here
		hashId := ddcm.CycleManager.AddNewLevel() + 1                                                // + 1 because 0-hasher is reserved for large sketch
		ddcm.SmallSketchesHasherIds = append(ddcm.SmallSketchesHasherIds, hashId)
		for hashId >= len(ddcm.Hashers) {
			ddcm.HashersLastLevels = append(ddcm.HashersLastLevels, -1)
			ddcm.Hashers = append(ddcm.Hashers, NewTHasher(ddcm.Parameters.SmallCountMinSketchDepth, ddcm.Parameters.CountMinSketchWidthLog2, ddcm.RandomEngine.Uint64()))
		}
		prevSameHasherLevel := ddcm.HashersLastLevels[hashId]
		if prevSameHasherLevel != -1 {
			ddcm.SmallSketchesNextSameHasherLevel[prevSameHasherLevel] = level
		}
		ddcm.HashersLastLevels[hashId] = level
		ddcm.PrecalculatedHashes = append(ddcm.PrecalculatedHashes, 0)
	}
}

func (ddcm *TDdcm) CalcLargeSketchIndices(key TKey, indices *[]int) {
	ddcm.Hashers[0].GetInitialHashVector(&ddcm.LargeSketchHashes)
	ddcm.Hashers[0].UpdateHashes(key, 0, key.GetSize(), &ddcm.LargeSketchHashes)
	ddcm.Hashers[0].TurnHashesIntoIndices(ddcm.LargeSketchHashes, indices)
}

func (ddcm *TDdcm) GetFrequentKeys(countLowerBound TCounter) ([]TKey, []TCounter) {
	keys := make([]TKey, 0)
	values := make([]TCounter, 0)
	if countLowerBound == 0 {
		return keys, values
	}
	for size := 0; size <= len(ddcm.SmallSketches); size += 8 {
		ddcm.GetFrequentKeysOfSize(countLowerBound, size, &keys, &values)
	}
	return keys, values
}

func (ddcm *TDdcm) GetFrequentKeysOfSize(countLowerBound TCounter, size int, keys *[]TKey, values *[]TCounter) {
	key := TKey{Data: make([]byte, size/8)}
	currentPrefixSize := 0
	for {
		if currentPrefixSize == size {
			ddcm.Hashers[0].GetInitialHashVector(&ddcm.LargeSketchHashes)
			ddcm.Hashers[0].UpdateHashes(key, 0, int(size), &ddcm.LargeSketchHashes)
			ddcm.Hashers[0].TurnHashesIntoIndices(ddcm.LargeSketchHashes, &ddcm.LargeSketchIndices)
			count := ddcm.LargeSketch.Get(ddcm.LargeSketchIndices)
			if count >= countLowerBound {
				(*keys) = append(*keys, *key.Copy())
				(*values) = append(*values, count)
			}
		} else {
			level := size - currentPrefixSize - 1
			ddcm.CalcHashesAndGetIndices(key, level, &ddcm.SmallSketchIndices)
			count := ddcm.SmallSketches[level].Get(ddcm.SmallSketchIndices)
			if count >= countLowerBound {
				// we found a heavy subtree
				currentPrefixSize++
				if key.Get(currentPrefixSize - 1) {
					key.ChangeBit(currentPrefixSize - 1)
				}
				continue
			}
		}

		// find a 0-edge subtree and switch to the 1-edge subtree
		for currentPrefixSize > 0 && key.Get(currentPrefixSize-1) {
			currentPrefixSize--
		}
		if currentPrefixSize == 0 {
			break
		}
		key.ChangeBit(currentPrefixSize - 1)
	}
}

func (ddcm *TDdcm) CalcHashesAndGetIndices(key TKey, level int, indices *[]int) {
	size := key.GetSize()
	hasherId := ddcm.SmallSketchesHasherIds[level]
	hasher := ddcm.Hashers[hasherId]

	nextSameHasherLevel := ddcm.SmallSketchesNextSameHasherLevel[level]
	var hashFrom int
	if nextSameHasherLevel <= level || nextSameHasherLevel >= size {
		// this hasher did not hash our key
		hasher.GetInitialHashVector(&ddcm.PrecalculatedHashes[level])
		hashFrom = 0
	} else {
		// we already hashed some prefix of our key
		ddcm.PrecalculatedHashes[level] = ddcm.PrecalculatedHashes[nextSameHasherLevel]
		hashFrom = size - 1 - nextSameHasherLevel
	}
	hasher.UpdateHashes(key, hashFrom, size-1-level, &ddcm.PrecalculatedHashes[level])
	hasher.TurnHashesIntoIndices(ddcm.PrecalculatedHashes[level], indices)
}

// sigma, delta, epsilon, Phi
func CreateDdcmWithWdm(failureProbability, wrongAnswerProbability, errRate, minSearchableFrequency float64, seed uint64) *TDdcm {
	parameters, err1 := CalcDdcmParameters(errRate, minSearchableFrequency, wrongAnswerProbability)
	if err1 != nil || (failureProbability <= 0 || failureProbability >= 1) {
		panic("CreateDdcmWithWdm error")
	}
	cyclesNumberExponent := 1.5
	l := 0
	r := int(math.Log(failureProbability*(2-cyclesNumberExponent)/8/(2.0/(minSearchableFrequency-errRate)+1)) /
		math.Log(math.Exp(1.0/EulerNumber)*0.5))
	for r-l > 1 {
		m := (r + l) / 2
		if failureProbability < (2.0/(minSearchableFrequency-errRate)+1)/(1-0.5*cyclesNumberExponent)*float64(m+1)*math.Pow(0.5, float64(m-1)) {
			l = m
		} else {
			r = m
		}
	}
	firstHashCycleLength := r // d
	return NewTDdcm(&parameters, seed, NewTDdcmCycleManagerWdm(cyclesNumberExponent, firstHashCycleLength))
}
