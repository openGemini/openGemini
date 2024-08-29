// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor_test

import (
	"math/rand"
	"reflect"
	"sort"
	"testing"

	"github.com/influxdata/influxdb/pkg/testing/assert"
	"github.com/openGemini/openGemini/engine/executor"
)

const (
	dataSize      = 2e3
	dataType      = "normal"
	desiredStdDev = 3
	desiredMean   = 10
	seed          = 42
	binNum        = 10
	begin         = 0
	end           = 100
)

func buildSketch() executor.OGSketch {
	sketch := executor.NewOGSketchImpl(10)
	for i := 0; i < 100; i++ {
		if i%2 != 0 {
			sketch.InsertPoints(float64(i))
		}
	}
	return sketch
}

func buildSketch1() executor.OGSketch {
	sketch := executor.NewOGSketchImpl(100)
	for i := 0; i < 100; i++ {
		if i%2 == 0 {
			sketch.InsertPoints(float64(i))
		}
	}
	return sketch
}

func getDeleteResult(sketch executor.OGSketch) {
	sketch.DeletePoints(float64(98))
	sketch.DeleteClusters(*executor.NewfloatTuple([]float64{92, 10}))
	sketch.DeleteClusters(*executor.NewfloatTuple([]float64{12, 10}))
	for i := 0; i < 98; i++ {
		if i%2 == 0 {
			sketch.DeletePoints(float64(i))
		}
	}
}

func TestOGSketchDelete(t *testing.T) {
	sketch := buildSketch1()
	resultSketch := executor.NewOGSketchImpl(100)
	getDeleteResult(sketch)
	if !reflect.DeepEqual(resultSketch, sketch) {
		t.Error("incorrect delete of OGSketch")
	}
}

func getPercentileResult(sketch executor.OGSketch) []float64 {
	testPercentile := []float64{0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1}
	result := make([]float64, 0)
	for i := range testPercentile {
		result = append(result, sketch.Percentile(testPercentile[i]))
	}
	return result
}

func TestOGSketchPercentile(t *testing.T) {
	sketch := buildSketch()
	ogResult := getPercentileResult(sketch)
	realResult := []float64{1, 10, 20, 30, 40, 50, 60, 70, 80, 90, 99}
	if !reflect.DeepEqual(realResult, ogResult) {
		t.Error("incorrect percentile of OGSketch")
	}
}

func getRankResult(sketch executor.OGSketch) []int64 {
	testRank := []float64{0, 10, 20, 30, 40, 50, 60, 70, 80, 90}
	result := make([]int64, 0)
	for i := range testRank {
		result = append(result, sketch.Rank(testRank[i]))
	}
	return result
}

func TestOGSketchRank(t *testing.T) {
	sketch := buildSketch()
	ogResult := getRankResult(sketch)
	realResult := []int64{0, 5, 10, 15, 20, 25, 30, 35, 40, 45}
	if !reflect.DeepEqual(realResult, ogResult) {
		t.Error("incorrect rank of OGSketch")
	}
}

func TestOGSketchMerge(t *testing.T) {
	sketch := buildSketch()
	sketch1 := buildSketch1()
	sketch.Merge(sketch1.(*executor.OGSketchImpl))
	ogResult := getPercentileResult(sketch)
	realResult := []float64{0, 9.5, 19.5, 29.5, 39.5, 49.5, 59.5, 69.5, 79.5, 89.5, 99}
	if !reflect.DeepEqual(realResult, ogResult) {
		t.Error("OGSketch merge raise error")
	}
}

func TestDemarcationHistogram(t *testing.T) {
	sketch := buildSketch()
	ogLinearHisogramResult := sketch.DemarcationHistogram(30.0, 5, 4, 0)
	realLinearHisogramResult := []int64{15, 2, 3, 2, 3, 25}
	if !reflect.DeepEqual(ogLinearHisogramResult, realLinearHisogramResult) {
		t.Error("incorrect linear histogram of OGSketch")
	}
	ogExpHisogramResult := sketch.DemarcationHistogram(30.0, 2, 4, 1)
	realExpHisogramResult := []int64{15, 1, 2, 4, 8, 20}
	if !reflect.DeepEqual(ogExpHisogramResult, realExpHisogramResult) {
		t.Error("incorrect exponential histogram of OGSketch")
	}
}

func TestOGSketchbuildEquiHeightHistogram(t *testing.T) {
	sketch := buildSketch()
	equiHeightHistogram := sketch.EquiHeightHistogram(10, 1, 99)
	realResult := []float64{1, 10, 20, 30.000000000000004, 40, 50, 60, 70, 80, 89.99999999999999, 99}
	if !reflect.DeepEqual(realResult, equiHeightHistogram) {
		t.Error("OGSketch build equi-height histogram raise error")
	}
}

func TestOGSketchCluster(t *testing.T) {
	sketch := buildSketch()
	assert.Equal(t, len(sketch.Clusters()), 11)
}

func TestOGSketchReset(t *testing.T) {
	sketch := buildSketch()
	sketch.Reset()
	assert.Equal(t, sketch.Len(), 0)
}

func TestOGSketchLen(t *testing.T) {
	sketch := buildSketch()
	assert.Equal(t, sketch.Len(), 50)
}

func builddata(s string, dataSize int) []float64 {
	data := make([]float64, dataSize)
	switch s {
	case "normal":
		for i := range data {
			data[i] = rand.NormFloat64()*desiredStdDev + desiredMean
		}
	case "uniform":
		uniform := rand.New(rand.NewSource(seed))
		for i := range data {
			data[i] = uniform.Float64() * 100
		}
	}
	return data
}

func BenchmarkOGSketchInsertClusters(b *testing.B) {
	data := builddata(dataType, dataSize)
	sort.Float64s(data)
	for j := 0; j < b.N; j++ {
		clusterSize := 1000.
		ogsketch := executor.NewOGSketchImpl(clusterSize * 2)
		ogsketch.InsertPoints(data...)
	}
}

func BenchmarkOGSketchPercentile(b *testing.B) {
	data := builddata(dataType, dataSize)
	sort.Float64s(data)
	clusterSize := 1000.
	ogsketch := executor.NewOGSketchImpl(clusterSize)
	ogsketch.InsertPoints(data...)
	for j := 0; j < b.N; j++ {
		quantile := 0.
		for quantile <= 1 {
			ogsketch.Percentile(quantile)
			quantile += 0.05
		}
	}
}

func BenchmarkOGSketchRank(b *testing.B) {
	data := builddata(dataType, dataSize)
	sort.Float64s(data)
	clusterSize := 1000.
	ogsketch := executor.NewOGSketchImpl(clusterSize)
	ogsketch.InsertPoints(data...)
	for j := 0; j < b.N; j++ {
		rank := 0.
		for rank <= 100 {
			ogsketch.Rank(rank)
			rank += 1
		}
	}
}

func BenchmarkOGSketchMerge(b *testing.B) {
	data := builddata(dataType, dataSize)
	sort.Float64s(data)
	clusterSize := 1000.
	sketch := executor.NewOGSketchImpl(clusterSize)
	sketch.InsertPoints(data...)
	sketch1 := executor.NewOGSketchImpl(clusterSize)
	sketch1.InsertPoints(data...)
	for j := 0; j < b.N; j++ {
		sketch.Merge(sketch1)
	}
}

func BenchmarkDemarcationHistogram(b *testing.B) {
	data := builddata(dataType, dataSize)
	sort.Float64s(data)
	clusterSize := 1000.
	ogsketch := executor.NewOGSketchImpl(clusterSize)
	ogsketch.InsertPoints(data...)
	for j := 0; j < b.N; j++ {
		ogsketch.DemarcationHistogram(10.1, 0.1, 100, 0)
	}
}

func BenchmarkOGSketchBuildEquiHeightHistogram(b *testing.B) {
	data := builddata(dataType, dataSize)
	sort.Float64s(data)
	clusterSize := 1000.
	ogsketch := executor.NewOGSketchImpl(clusterSize)
	ogsketch.InsertPoints(data...)
	for j := 0; j < b.N; j++ {
		ogsketch.EquiHeightHistogram(binNum, begin, end)
	}
}

func BenchmarkOGSketchDelete(b *testing.B) {
	data := builddata(dataType, dataSize)
	sort.Float64s(data)
	clusterSize := 1000.
	ogsketch := executor.NewOGSketchImpl(clusterSize)
	ogsketch.InsertPoints(data...)
	for j := 0; j < b.N; j++ {
		for i := 0; i < 100; i++ {
			if i%2 == 0 {
				ogsketch.DeletePoints(float64(i))
			}
		}
	}
}
