/*
Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.

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
	"math"
	"math/rand"
	"slices"
	"sort"

	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/pool"
)

var NagtPool *pool.UnionPool[TTopnCM05Optimized]

func InitNagtPool(cap int) {
	if cap == 0 {
		NagtPool = pool.NewUnionPool[TTopnCM05Optimized](cpu.GetCpuNum()*2, cpu.GetCpuNum()*2, 0, DefaultCreateTTopnCM05Optimized)
	} else {
		NagtPool = pool.NewUnionPool[TTopnCM05Optimized](cap, cap, 0, DefaultCreateTTopnCM05Optimized)
	}
}

type TTopnCM05OptimizedParameters struct {
	substreamsWidthLog2             int
	substreamsDepth                 int
	substreamFailureProbability     float64
	substreamWrongAnswerProbability float64
	frequencyEstimatorWidthLog2     int
	frequencyEstimatorDepth         int
}

type TMajorityDetector struct {
	TotalCount TCounter
	BitCounts  []TCounter
}

func (detector *TMajorityDetector) Reset() {
	detector.TotalCount = 0
	detector.BitCounts = detector.BitCounts[:0]
}

func (detector *TMajorityDetector) AddKeyCount(key *TKey, count TCounter) {
	size := key.GetSize()
	if cap(detector.BitCounts) < size+1 {
		detector.BitCounts = append(detector.BitCounts, make([]TCounter, (size+1)-len(detector.BitCounts))...)
	}
	detector.BitCounts = detector.BitCounts[:size+1]

	for i := 0; i < size; i++ {
		if key.Get(i) {
			detector.BitCounts[i] += count
		}
	}
	detector.BitCounts[size] += count
	detector.TotalCount += count
}

func (detector *TMajorityDetector) GetTotalCount() TCounter {
	return detector.TotalCount
}

func (detector *TMajorityDetector) GetMajorityKey() (TKey, bool) {
	keyBits := make([]bool, len(detector.BitCounts))
	for i := 0; i < len(detector.BitCounts); i++ {
		if detector.BitCounts[i] == detector.TotalCount-detector.BitCounts[i] {
			return TKey{}, false
		}
		keyBits[i] = (detector.BitCounts[i] > detector.TotalCount-detector.BitCounts[i])
	}
	for len(keyBits) > 0 && !keyBits[len(keyBits)-1] {
		keyBits = keyBits[:len(keyBits)-1]
	}
	if (len(keyBits)+7)%8 != 0 {
		return TKey{}, false
	}

	key := TKey{
		Data: make([]byte, len(keyBits)/8),
	}
	for i := 0; i < len(keyBits)-1; i++ {
		if keyBits[i] {
			key.ChangeBit(i)
		}
	}
	return key, true
}

type TSubstream struct {
	heavyHitterDetector TMajorityDetector
	heavyHitters        []TKey
}

func (sub *TSubstream) Reset() {
	sub.heavyHitterDetector.Reset()
	for i := range sub.heavyHitters {
		sub.heavyHitters[i].Data = sub.heavyHitters[i].Data[:0]
	}
	sub.heavyHitters = sub.heavyHitters[:0]
}

type TSmallCounterSet struct {
	Counters []TCounter
	Min      TCounter
}

func (set *TSmallCounterSet) Replace(before, after TCounter) {
	set.Min = NoCounter
	swapped := false
	for i := 0; i < len(set.Counters); i++ {
		if !swapped && set.Counters[i] == before {
			set.Counters[i] = after
			swapped = true
		}
		if set.Counters[i] < set.Min {
			set.Min = set.Counters[i]
		}
	}
}

type THeavyHitterInfo struct {
	numTimesMajorityElement int
	approximateCounts       TSmallCounterSet
}

type TTopnCM05Optimized struct {
	Parameters               TTopnCM05OptimizedParameters
	RandomEngine             *rand.Rand // not reuse
	HasherIntoSubstreams     THasher    // not reuse
	Substreams               [][]TSubstream
	FrequencyEstimator       TCountMinSketch
	FrequencyEstimatorHasher THasher // not reuse
	TotalCount               TCounter
	HeavyHitters             map[string]*THeavyHitterInfo
	// TODO: make an id for each heavy hitter and use it here
	KeysByIndices [][]map[string]struct{}
}

func (nagt *TTopnCM05Optimized) Reset() {
	nagt.TotalCount = 0
	nagt.RandomEngine = nil
	nagt.HeavyHitters = make(map[string]*THeavyHitterInfo)
	for i := range nagt.KeysByIndices {
		for j := range nagt.KeysByIndices[i] {
			nagt.KeysByIndices[i][j] = make(map[string]struct{})
		}
	}
	for i := range nagt.Substreams {
		for j := range nagt.Substreams[i] {
			nagt.Substreams[i][j].Reset()
		}
	}
	nagt.FrequencyEstimator.Reset()
}

func ConstructParametersFromGuarantees(failureProbability, wrongAnswerProbability, errRate, minSearchableFrequency float64) TTopnCM05OptimizedParameters {
	var parameters TTopnCM05OptimizedParameters
	parameters.substreamsWidthLog2 = CalcLog2(EulerNumber / minSearchableFrequency)
	substreamsWidth := 1 << parameters.substreamsWidthLog2
	parameters.substreamsDepth = int(math.Ceil(math.Log(2.0/wrongAnswerProbability) / math.Log(float64(substreamsWidth)*minSearchableFrequency)))
	parameters.frequencyEstimatorWidthLog2 = CalcLog2(EulerNumber / errRate)
	depthBase := 1
	parameters.frequencyEstimatorDepth = int(math.Ceil(math.Log(2.0*float64(parameters.substreamsDepth<<parameters.substreamsWidthLog2)/wrongAnswerProbability) /
		math.Log(errRate*float64(depthBase<<parameters.frequencyEstimatorWidthLog2))))
	parameters.substreamWrongAnswerProbability = 0
	parameters.substreamFailureProbability = 0
	return parameters
}

func DefaultCreateTTopnCM05Optimized() *TTopnCM05Optimized {
	return CreateTTopnCM05Optimized(DefaultSigma, DefaultDelta, DefaultEps, DefaultPhi, 12)
}

func CreateTTopnCM05Optimized(failureProbability, wrongAnswerProbability, errRate, minSearchableFrequency float64, seed uint64) *TTopnCM05Optimized {
	nagt := &TTopnCM05Optimized{}
	nagt.Parameters = ConstructParametersFromGuarantees(failureProbability, wrongAnswerProbability, errRate, minSearchableFrequency)
	source := rand.NewSource(int64(seed))
	nagt.RandomEngine = rand.New(source)

	nagt.HasherIntoSubstreams = NewTHasher(nagt.Parameters.substreamsDepth, nagt.Parameters.substreamsWidthLog2, nagt.RandomEngine.Uint64())
	nagt.Substreams = make([][]TSubstream, nagt.Parameters.substreamsDepth)
	nagt.FrequencyEstimator = NewTCountMinSketch(nagt.Parameters.frequencyEstimatorDepth, nagt.Parameters.frequencyEstimatorWidthLog2)
	nagt.FrequencyEstimatorHasher = NewTHasher(nagt.Parameters.frequencyEstimatorDepth, nagt.Parameters.frequencyEstimatorWidthLog2, nagt.RandomEngine.Uint64())
	nagt.KeysByIndices = make([][]map[string]struct{}, nagt.Parameters.frequencyEstimatorDepth)
	for i := range nagt.KeysByIndices {
		width := 1 << nagt.Parameters.frequencyEstimatorWidthLog2
		nagt.KeysByIndices[i] = make([]map[string]struct{}, width)
		for j := 0; j < width; j++ {
			nagt.KeysByIndices[i][j] = make(map[string]struct{})
		}
	}

	for i := 0; i < nagt.Parameters.substreamsDepth; i++ {
		subWidth := 1 << nagt.Parameters.substreamsWidthLog2
		nagt.Substreams[i] = make([]TSubstream, subWidth)
	}
	nagt.HeavyHitters = make(map[string]*THeavyHitterInfo)
	return nagt
}

func (nagt *TTopnCM05Optimized) Ready(seed uint64) {
	source := rand.NewSource(int64(seed))
	nagt.RandomEngine = rand.New(source)
	nagt.HasherIntoSubstreams = NewTHasher(nagt.Parameters.substreamsDepth, nagt.Parameters.substreamsWidthLog2, nagt.RandomEngine.Uint64())
	nagt.FrequencyEstimatorHasher = NewTHasher(nagt.Parameters.frequencyEstimatorDepth, nagt.Parameters.frequencyEstimatorWidthLog2, nagt.RandomEngine.Uint64())
}

func (nagt *TTopnCM05Optimized) Name() string {
	return "nagt"
}

func (nagt *TTopnCM05Optimized) GetTotalCount() TCounter {
	return nagt.TotalCount
}

func (nagt *TTopnCM05Optimized) AddKeyCount(key TKey, value TCounter) {
	nagt.TotalCount += value
	newKeyFrequencyEstimatorIndices := make([]int, nagt.Parameters.frequencyEstimatorDepth)
	frequencyEstimatorIndicesReuse := make([]int, nagt.Parameters.frequencyEstimatorDepth)
	{ // update the count approximation sketch
		var frequencyEstimatorHashes THashVector
		nagt.FrequencyEstimatorHasher.GetInitialHashVector(&frequencyEstimatorHashes)
		nagt.FrequencyEstimatorHasher.UpdateHashesBytewise(key, 0, key.GetByteSize(), &frequencyEstimatorHashes)
		nagt.FrequencyEstimatorHasher.TurnHashesIntoIndices(frequencyEstimatorHashes, &newKeyFrequencyEstimatorIndices)
		for i := 0; i < nagt.Parameters.frequencyEstimatorDepth; i++ {
			for oldKey := range nagt.KeysByIndices[i][newKeyFrequencyEstimatorIndices[i]] {
				oldCounter := nagt.FrequencyEstimator.GetCounter(i, newKeyFrequencyEstimatorIndices[i])
				nagt.HeavyHitters[oldKey].approximateCounts.Replace(oldCounter, oldCounter+value)
			}
		}
		nagt.FrequencyEstimator.Add(newKeyFrequencyEstimatorIndices, value)
	}

	// update ddcm in each cell
	var hashesIntoSubstreams THashVector
	nagt.HasherIntoSubstreams.GetInitialHashVector(&hashesIntoSubstreams)
	nagt.HasherIntoSubstreams.UpdateHashesBytewise(key, 0, key.GetByteSize(), &hashesIntoSubstreams)
	indicesIntoSubstreams := make([]int, nagt.Parameters.substreamsDepth)
	nagt.HasherIntoSubstreams.TurnHashesIntoIndices(hashesIntoSubstreams, &indicesIntoSubstreams)
	for i := 0; i < nagt.Parameters.substreamsDepth; i++ {
		substream := &nagt.Substreams[i][indicesIntoSubstreams[i]]
		substream.heavyHitterDetector.AddKeyCount(&key, value)

		newHeavyHitters := make([]TKey, 0)
		subcount := substream.heavyHitterDetector.GetTotalCount()
		if subcount > 0 {
			// subcountFraction := subcount/2 + 1
			heavyHitterFirst, heavyHitterSecond := substream.heavyHitterDetector.GetMajorityKey()
			if heavyHitterSecond {
				newHeavyHitters = append(newHeavyHitters, heavyHitterFirst)
			}
		}
		sort.Slice(newHeavyHitters, func(i, j int) bool {
			return newHeavyHitters[i].Less(newHeavyHitters[j])
		})

		oldHeavyHitters := substream.heavyHitters
		oldI := 0
		newI := 0
		for oldI < len(oldHeavyHitters) || newI < len(newHeavyHitters) {
			bothCountersNotFinished := oldI < len(oldHeavyHitters) && newI < len(newHeavyHitters)
			if oldI == len(oldHeavyHitters) || bothCountersNotFinished && newHeavyHitters[newI].Less(oldHeavyHitters[oldI]) {
				// add new heavy hitter
				heavyHitterKey := newHeavyHitters[newI]
				newI++
				heavyHitterKeyStr := string(heavyHitterKey.Data)
				_, ok := nagt.HeavyHitters[heavyHitterKeyStr]
				if ok {
					nagt.HeavyHitters[heavyHitterKeyStr].numTimesMajorityElement += 1
				} else {
					frequencyEstimatorIndices := frequencyEstimatorIndicesReuse
					if heavyHitterKey.Eq(key) {
						// approximateCount is calculated easier for `key`, which is the key that is probably the new heavy hitter
						frequencyEstimatorIndices = newKeyFrequencyEstimatorIndices
					} else {
						var frequencyEstimatorHashes THashVector
						nagt.FrequencyEstimatorHasher.GetInitialHashVector(&frequencyEstimatorHashes)
						nagt.FrequencyEstimatorHasher.UpdateHashesBytewise(heavyHitterKey, 0, heavyHitterKey.GetByteSize(), &frequencyEstimatorHashes)
						nagt.FrequencyEstimatorHasher.TurnHashesIntoIndices(frequencyEstimatorHashes, &frequencyEstimatorIndices)
					}

					approximateCounts := make([]TCounter, nagt.Parameters.frequencyEstimatorDepth)
					for i := 0; i < nagt.Parameters.frequencyEstimatorDepth; i++ {
						approximateCounts[i] = nagt.FrequencyEstimator.GetCounter(i, frequencyEstimatorIndices[i])
						nagt.KeysByIndices[i][frequencyEstimatorIndices[i]][string(heavyHitterKey.Data)] = struct{}{}
					}
					info := THeavyHitterInfo{1, TSmallCounterSet{
						Min:      slices.Min(approximateCounts),
						Counters: approximateCounts,
					}}
					nagt.HeavyHitters[heavyHitterKeyStr] = &info
				}
			} else if newI == len(newHeavyHitters) || bothCountersNotFinished && oldHeavyHitters[oldI].Less(newHeavyHitters[newI]) {
				// remove old heavy hitter
				heavyHitterKey := oldHeavyHitters[oldI]
				oldI++
				heavyHitterKeyStr := string(heavyHitterKey.Data)
				iter, ok := nagt.HeavyHitters[heavyHitterKeyStr]
				if ok && iter.numTimesMajorityElement == 1 {
					delete(nagt.HeavyHitters, heavyHitterKeyStr)

					frequencyEstimatorIndices := frequencyEstimatorIndicesReuse
					if heavyHitterKey.Eq(key) {
						frequencyEstimatorIndices = newKeyFrequencyEstimatorIndices
					} else {
						var frequencyEstimatorHashes THashVector
						nagt.FrequencyEstimatorHasher.GetInitialHashVector(&frequencyEstimatorHashes)
						nagt.FrequencyEstimatorHasher.UpdateHashesBytewise(heavyHitterKey, 0, heavyHitterKey.GetByteSize(), &frequencyEstimatorHashes)
						nagt.FrequencyEstimatorHasher.TurnHashesIntoIndices(frequencyEstimatorHashes, &frequencyEstimatorIndices)
					}
					for i := 0; i < nagt.Parameters.frequencyEstimatorDepth; i++ {
						delete(nagt.KeysByIndices[i][frequencyEstimatorIndices[i]], heavyHitterKeyStr)
					}
				} else if ok {
					iter.numTimesMajorityElement--
				} else {
					logger.GetLogger().Error("nagt compute err")
				}
			} else {
				// same heavyHitter in old and in new
				newI++
				oldI++
			}
		}
		substream.heavyHitters = newHeavyHitters
	}
}

func (nagt *TTopnCM05Optimized) GetKeyCount(key TKey) TCounter {
	var frequencyEstimatorHashes THashVector
	nagt.FrequencyEstimatorHasher.GetInitialHashVector(&frequencyEstimatorHashes)
	nagt.FrequencyEstimatorHasher.UpdateHashesBytewise(key, 0, key.GetByteSize(), &frequencyEstimatorHashes)
	frequencyEstimatorIndices := make([]int, nagt.Parameters.frequencyEstimatorDepth)
	nagt.FrequencyEstimatorHasher.TurnHashesIntoIndices(frequencyEstimatorHashes, &frequencyEstimatorIndices)
	return nagt.FrequencyEstimator.Get(frequencyEstimatorIndices)
}

func (nagt *TTopnCM05Optimized) GetFrequentKeys(countLowerBound TCounter) ([]TKey, []TCounter) {
	keys := make([]TKey, 0)
	values := make([]TCounter, 0)
	for key, info := range nagt.HeavyHitters {
		count := info.approximateCounts.Min
		if count >= countLowerBound {
			keys = append(keys, TKey{Data: []byte(key)})
			values = append(values, count)
		}
	}
	return keys, values
}

func (nagt *TTopnCM05Optimized) Instance() *TTopnCM05Optimized {
	return nagt
}

func (nagt *TTopnCM05Optimized) MemSize() int {
	return 1
}
