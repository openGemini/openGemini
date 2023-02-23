/*
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.

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
	"sort"
	"sync"

	"github.com/openGemini/openGemini/lib/cpu"
)

const (
	halfWeight = 0.5
)

type Cluster struct {
	Mean   float64
	Weight float64
}

func NewCluster() *Cluster {
	return &Cluster{}
}

func (c *Cluster) Set(m, w float64) {
	c.Mean = m
	c.Weight = w
}

func (c *Cluster) Reset() {
	c.Mean = 0
	c.Weight = 0
}

type ClusterSet []Cluster
type DeleteClusterSet map[float64]float64

func (l ClusterSet) Len() int           { return len(l) }
func (l ClusterSet) Less(i, j int) bool { return l[i].Mean < l[j].Mean }
func (l ClusterSet) Swap(i, j int)      { l[i], l[j] = l[j], l[i] }

var clusterPool = NewClusterPool()

type ClusterPool struct {
	pool  sync.Pool
	cache chan *Cluster
}

func NewClusterPool() *ClusterPool {
	n := cpu.GetCpuNum() * 2
	if n < 4 {
		n = 4
	}
	if n > 256 {
		n = 256
	}
	return &ClusterPool{
		cache: make(chan *Cluster),
	}
}

func (p *ClusterPool) Get() *Cluster {
	select {
	case cluster := <-p.cache:
		return cluster
	default:
		v := p.pool.Get()
		if v != nil {
			cluster, ok := v.(*Cluster)
			if !ok {
				return NewCluster()
			}
			return cluster
		}
		return NewCluster()
	}
}

func (p *ClusterPool) Put(cluster *Cluster) {
	cluster.Reset()
	select {
	case p.cache <- cluster:
	default:
		p.pool.Put(cluster)
	}
}

type OGSketch interface {
	InsertPoints(...float64)
	InsertClusters(...floatTuple)
	DeletePoints(...float64)
	DeleteClusters(...floatTuple)
	Merge(*OGSketchImpl)
	Percentile(float64) float64
	Rank(float64) int64
	Clusters() ClusterSet
	Reset()
	Len() int
	EquiHeightHistogram(int, float64, float64) []float64
	DemarcationHistogram(float64, float64, int, int) []int64
}

type OGSketchImpl struct {
	sketchSize        int
	bufferSize        int
	clusterSetSize    float64
	allWeight         float64
	deleteWeight      float64
	minValue          float64
	maxValue          float64
	demarcationCounts []int64
	accumulativeSum   []float64
	equiHeightBins    []float64
	demarcationBins   []float64
	sketch            ClusterSet
	buffer            ClusterSet
	clusterPool       *ClusterPool
	deleteBuffer      DeleteClusterSet
}

func NewOGSketchImpl(c float64) *OGSketchImpl {
	c = math.Max(c, 1)
	s := &OGSketchImpl{}
	s.clusterSetSize = c
	s.sketchSize = int(2 * math.Ceil(s.clusterSetSize))
	s.bufferSize = int(8 * math.Ceil(s.clusterSetSize))
	s.sketch = make(ClusterSet, 0, s.sketchSize)
	s.buffer = make(ClusterSet, 0, s.bufferSize+1)
	s.accumulativeSum = make([]float64, s.sketchSize)
	s.maxValue = -math.MaxFloat64
	s.minValue = math.MaxFloat64
	s.clusterPool = clusterPool
	s.deleteBuffer = make(DeleteClusterSet)
	return s
}

func (s *OGSketchImpl) updateAccumulativeSum() {
	s.accumulativeSum[0] = s.sketch[0].Weight / 2
	for i := 1; i < len(s.sketch); i++ {
		s.accumulativeSum[i] = s.accumulativeSum[i-1] + (s.sketch[i].Weight+s.sketch[i-1].Weight)/2
	}
}
func (s *OGSketchImpl) InsertPoints(m ...float64) {
	for i := 0; i < len(m); i++ {
		s.insert(m[i], 1)
	}
}

func (s *OGSketchImpl) InsertClusters(cluster ...floatTuple) {
	for i := 0; i < len(cluster); i++ {
		s.insert(cluster[i].values[0], cluster[i].values[1])
	}
}

func (s *OGSketchImpl) Clusters() ClusterSet {
	s.processInsert()
	s.processDelete()
	return s.sketch
}

func (s *OGSketchImpl) DeletePoints(m ...float64) {
	for i := 0; i < len(m); i++ {
		s.delete(m[i], 1)
	}
}

func (s *OGSketchImpl) DeleteClusters(cluster ...floatTuple) {
	for i := 0; i < len(cluster); i++ {
		s.delete(cluster[i].values[0], cluster[i].values[1])
	}
}

func (s *OGSketchImpl) Merge(s1 *OGSketchImpl) {
	s1.processInsert()
	s1.processDelete()
	s.buffer = append(s.buffer, s1.sketch...)
	s.allWeight += s1.allWeight
	s.maxValue = math.Max(s.maxValue, s1.maxValue)
	s.minValue = math.Min(s.minValue, s1.minValue)
	s.processInsert()
	s.processDelete()
}

func (s *OGSketchImpl) Percentile(q float64) float64 {
	s.processInsert()
	s.processDelete()
	if s.accumulativeSum[len(s.sketch)-1]+s.sketch[len(s.sketch)-1].Weight/2 != s.allWeight {
		s.updateAccumulativeSum()
	}
	if len(s.sketch) == 0 || q < 0 || q > 1 {
		return math.NaN()
	}
	rank := q * s.allWeight
	firstHalfWeight := halfWeight * s.sketch[0].Weight
	lastHalfWeight := halfWeight * s.sketch[len(s.sketch)-1].Weight
	if rank < firstHalfWeight {
		return s.minValue + rank/firstHalfWeight*(s.sketch[0].Mean-s.minValue)
	}
	if rank >= s.allWeight-lastHalfWeight {
		return s.maxValue - (s.allWeight-rank)/lastHalfWeight*(s.maxValue-s.sketch[len(s.sketch)-1].Mean)
	}
	idx := sort.Search(len(s.sketch), func(i int) bool {
		return s.accumulativeSum[i] > rank
	})
	return s.sketch[idx-1].Mean +
		2*(rank-s.accumulativeSum[idx-1])/(s.sketch[idx-1].Weight+s.sketch[idx].Weight)*(s.sketch[idx].Mean-s.sketch[idx-1].Mean)
}

func (s *OGSketchImpl) Rank(Q float64) int64 {
	s.processInsert()
	s.processDelete()
	if s.accumulativeSum[len(s.sketch)-1]+s.sketch[len(s.sketch)-1].Weight/2 != s.allWeight {
		s.updateAccumulativeSum()
	}
	if Q >= s.maxValue {
		return int64(s.allWeight)
	}
	if Q <= s.minValue {
		return 0
	}
	firstHalfWeight := halfWeight * s.sketch[0].Weight
	lastHalfWeight := halfWeight * s.sketch[len(s.sketch)-1].Weight
	if Q < s.sketch[0].Mean {
		return int64(firstHalfWeight * (s.sketch[0].Mean - Q) / (s.sketch[0].Mean - s.minValue))
	}
	if Q >= s.sketch[len(s.sketch)-1].Mean {
		return int64(s.allWeight - (s.maxValue-Q)/(s.maxValue-s.sketch[len(s.sketch)-1].Mean)*lastHalfWeight)
	}
	idx := sort.Search(len(s.sketch), func(i int) bool {
		return s.sketch[i].Mean > Q
	})
	return int64(s.accumulativeSum[idx] -
		(s.sketch[idx].Mean-Q)/(s.sketch[idx].Mean-s.sketch[idx-1].Mean)*(s.sketch[idx].Weight+s.sketch[idx-1].Weight)*halfWeight)
}

func (s *OGSketchImpl) Reset() {
	for k := range s.deleteBuffer {
		delete(s.deleteBuffer, k)
	}
	s.equiHeightBins = s.equiHeightBins[:0]
	s.demarcationBins = s.demarcationBins[:0]
	s.demarcationCounts = s.demarcationCounts[:0]
	s.sketch = s.sketch[:0]
	s.buffer = s.buffer[:0]
	s.allWeight = 0
	s.deleteWeight = 0
	s.maxValue = -math.MaxFloat64
	s.minValue = math.MaxFloat64
	for i := 0; i < len(s.accumulativeSum); i++ {
		s.accumulativeSum[i] = 0
	}
}

func (s *OGSketchImpl) Len() int {
	return len(s.sketch) + len(s.buffer)
}

func (s *OGSketchImpl) reverseRuler(k float64) float64 {
	return (math.Sin(math.Min(k, s.clusterSetSize)*math.Pi/s.clusterSetSize-math.Pi/2.0) + 1.0) / 2.0
}

func (s *OGSketchImpl) ruler(q float64) float64 {
	return s.clusterSetSize * (math.Asin(2.0*q-1.0) + math.Pi/2.0) / math.Pi
}

func (s *OGSketchImpl) processInsert() {
	// step1: add clusters to the sketch in order of mean
	if s.buffer.Len() > 0 || s.sketch.Len() > s.sketchSize {
		s.buffer = append(s.buffer, s.sketch...)
		s.sketch = s.sketch[:0]
		sort.Sort(&s.buffer)
		if s.buffer.Len() < s.sketchSize {
			for i := 0; i < len(s.buffer); i++ {
				s.sketch = append(s.sketch, s.buffer[i])
			}
			s.buffer = s.buffer[:0]
			s.minValue = math.Min(s.minValue, s.sketch[0].Mean)
			s.maxValue = math.Max(s.maxValue, s.sketch[s.sketch.Len()-1].Mean)
			return
		}

		// step2: merge clusters based on scale function
		q0 := 0.
		qlimit := s.reverseRuler(s.ruler(q0) + 1.)
		sigmod := s.buffer[0]
		for i := 1; i < len(s.buffer); i++ {
			q := q0 + (sigmod.Weight+s.buffer[i].Weight)/s.allWeight
			if q <= qlimit {
				sigmod.Mean = (sigmod.Mean*sigmod.Weight + s.buffer[i].Mean*s.buffer[i].Weight) / (sigmod.Weight + s.buffer[i].Weight)
				sigmod.Weight += s.buffer[i].Weight
			} else {
				s.sketch = append(s.sketch, sigmod)
				q0 = q0 + sigmod.Weight/s.allWeight
				qlimit = s.reverseRuler(s.ruler(q0) + 1.)
				sigmod = s.buffer[i]
			}
		}
		s.sketch = append(s.sketch, sigmod)
		s.buffer = s.buffer[:0]
		s.minValue = math.Min(s.minValue, s.sketch[0].Mean)
		s.maxValue = math.Max(s.maxValue, s.sketch[s.sketch.Len()-1].Mean)

		// step3: update accumulative sum array
		s.updateAccumulativeSum()
	}
}

func (s *OGSketchImpl) insert(m float64, w float64) {
	if math.IsNaN(m) || w <= 0 || math.IsNaN(w) || math.IsInf(w, 1) {
		return
	}
	s.allWeight += w
	s.buffer = append(s.buffer, Cluster{m, w})
	if len(s.buffer) > s.bufferSize || len(s.sketch) > s.sketchSize {
		s.processInsert()
	}
}

func deleteLeft(sketch ClusterSet, loc int, val *float64) int {
	for loc >= 0 && *val > 0 {
		if sketch[loc].Weight > *val {
			sketch[loc].Weight -= *val
			*val = 0
		} else {
			*val -= sketch[loc].Weight
			sketch[loc].Weight = 0
			loc--
		}
	}
	return loc
}

func deleteRight(sketch ClusterSet, loc int, val *float64) int {
	for loc < sketch.Len() && *val > 0 {
		if sketch[loc].Weight > *val {
			sketch[loc].Weight -= *val
			*val = 0
		} else {
			*val -= sketch[loc].Weight
			sketch[loc].Weight = 0
			loc++
		}
	}
	return loc
}

func (s *OGSketchImpl) deleteToRight(val *float64) {
	loc := deleteRight(s.sketch, 0, val)
	if loc == s.sketch.Len() {
		s.Reset()
		return
	}
	if loc != 0 {
		s.minValue = s.sketch[loc-1].Mean
	}
	s.sketch = append(s.sketch[:0], s.sketch[loc:]...)
}

func (s *OGSketchImpl) deleteToLeft(val *float64) {
	loc := deleteLeft(s.sketch, len(s.sketch)-1, val)
	if loc == -1 {
		s.Reset()
		return
	}
	if loc != len(s.sketch)-1 {
		s.minValue = s.sketch[loc+1].Mean
	}
	s.sketch = s.sketch[:loc+1]
}

func (s *OGSketchImpl) deleteToDouble(key, val float64) {
	locr := sort.Search(len(s.sketch), func(i int) bool {
		return s.sketch[i].Mean >= key
	})
	locl := locr - 1
	meanDistance := s.sketch[locr].Mean - s.sketch[locl].Mean
	wr := val * (key - s.sketch[locl].Mean) / (meanDistance)
	wl := val * (s.sketch[locr].Mean - key) / (meanDistance)
	locl = deleteLeft(s.sketch, locl, &wl)
	locr = deleteRight(s.sketch, locr, &wr)
	if wl > 0 && locl == -1 {
		wr += wl
		wl = 0
		locr = deleteRight(s.sketch, locr, &wr)
	}
	if wr > 0 && locr == s.sketch.Len() {
		wl += wr
		wr = 0
		locl = deleteLeft(s.sketch, locl, &wl)
	}
	if locl == -1 && locr == s.sketch.Len() {
		s.Reset()
		return
	}
	if locl == -1 {
		s.minValue = s.sketch[locr-1].Mean
		s.sketch = append(s.sketch[:0], s.sketch[locr:]...)
	} else if locr == s.sketch.Len() {
		s.maxValue = s.sketch[locl+1].Mean
		s.sketch = s.sketch[:locl+1]
	} else {
		s.sketch = append(s.sketch[:locl+1], s.sketch[locr:]...)
	}
}

func (s *OGSketchImpl) processDelete() {
	if len(s.deleteBuffer) == 0 {
		return
	}
	for key, val := range s.deleteBuffer {
		if key <= s.sketch[0].Mean {
			s.deleteToRight(&val)
		} else if key >= s.sketch[len(s.sketch)-1].Mean {
			s.deleteToLeft(&val)
		} else {
			s.deleteToDouble(key, val)
		}
	}
	s.updateAccumulativeSum()
}

func (s *OGSketchImpl) delete(m float64, w float64) {
	if w <= 0 {
		return
	}
	s.deleteBuffer[m] += w
	s.deleteWeight += w
	if s.deleteWeight >= s.allWeight {
		s.Reset()
		return
	}
	if s.deleteWeight > s.allWeight/2 || len(s.sketch) > s.sketchSize {
		s.processInsert()
		s.processDelete()
		s.allWeight -= s.deleteWeight
		s.deleteWeight = 0
		for k := range s.deleteBuffer {
			delete(s.deleteBuffer, k)
		}
	}
}

func (s *OGSketchImpl) EquiHeightHistogram(binNum int, begin float64, end float64) []float64 {
	pbegin := float64(s.Rank(begin)) / s.allWeight
	intervalNum := float64(s.Rank(end)-s.Rank(begin)) / (s.allWeight * float64(binNum))
	for i := 0; i <= binNum; i++ {
		s.equiHeightBins = append(s.equiHeightBins, s.Percentile(pbegin))
		pbegin += intervalNum
	}
	return s.equiHeightBins
}

func (s *OGSketchImpl) generateExponentialBoundary(begin, factor float64, binsNum int) {
	s.demarcationBins = s.demarcationBins[:0]
	base := factor
	s.demarcationBins = append(s.demarcationBins, begin)
	for i := 0; i < binsNum; i++ {
		s.demarcationBins = append(s.demarcationBins, begin+base)
		begin += base
		base *= factor
	}
}

func (s *OGSketchImpl) generateLieanerBoundary(begin, width float64, binsNum int) {
	s.demarcationBins = s.demarcationBins[:0]
	s.demarcationBins = append(s.demarcationBins, begin)
	for i := 0; i < binsNum; i++ {
		begin += width
		s.demarcationBins = append(s.demarcationBins, begin)
	}
}

func (s *OGSketchImpl) DemarcationHistogram(begin, width float64, binsNum, binsType int) []int64 {
	if binsType == 0 {
		s.generateLieanerBoundary(begin, width, binsNum)
	} else {
		s.generateExponentialBoundary(begin, width, binsNum)
	}
	if len(s.demarcationBins) == 0 {
		s.demarcationCounts = s.demarcationCounts[:0]
		return s.demarcationCounts
	}
	s.demarcationCounts = s.demarcationCounts[:0]
	s.demarcationCounts = append(s.demarcationCounts, s.Rank(s.demarcationBins[0]))
	for i := 1; i < len(s.demarcationBins); i++ {
		s.demarcationCounts = append(s.demarcationCounts, s.Rank(s.demarcationBins[i])-s.Rank(s.demarcationBins[i-1]))
	}
	s.demarcationCounts = append(s.demarcationCounts, int64(s.allWeight)-s.Rank(s.demarcationBins[len(s.demarcationBins)-1]))
	return s.demarcationCounts
}
