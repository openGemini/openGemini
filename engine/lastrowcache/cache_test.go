// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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

package lastrowcache

import (
	"errors"
	"math/rand"
	"strconv"
	"sync"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/smartystreets/goconvey/convey"
)

func initTestCache() error {
	cfg := config.NewLastRowCacheConfig()
	config.SetLastRowCacheConfig(cfg)
	return InitCache()
}

func TestCacheSetAndGet(t *testing.T) {
	convey.Convey("test cache set and get", t, func() {
		err := initTestCache()
		convey.So(err, convey.ShouldBeNil)

		oldvalue := &sync.Map{}
		oldCacheValue := &CacheValue{timestamp: -99999}
		oldCacheValue.SetFields(oldvalue)

		Set("test", "autogen", []byte("location=Beijing"), oldCacheValue)
		cache.Wait()
		row := &influx.Row{Fields: influx.Fields{influx.Field{
			Key:      "position",
			StrValue: "test",
			Type:     influx.Field_Type_String,
		},
			influx.Field{
				Key:      "temperature",
				NumValue: -10.0,
				Type:     influx.Field_Type_Float,
			},
			influx.Field{
				Key:      "code",
				NumValue: 110,
				Type:     influx.Field_Type_Int,
			},
			influx.Field{
				Key:      "urban",
				NumValue: 1,
				Type:     influx.Field_Type_Boolean,
			},
		}}
		row.IndexKey = []byte("location=Beijing")
		rows := []influx.Row{*row}
		SetRows("test", "autogen", &rows)
		cache.Wait()

		newValue, _ := cache.Get(cacheKey("test", "autogen", []byte("location=Beijing")))
		nv, _ := newValue.(*CacheValue)
		tempIn, _ := nv.fieldKVs.Load().Load("temperature")
		temp, _ := tempIn.(float64)
		convey.So(temp, convey.ShouldEqual, -10)
	})
}

func Benchmark_SetAndRead(t *testing.B) {
	wg := sync.WaitGroup{}
	t.ReportAllocs()
	t.ResetTimer()
	cfg := config.NewLastRowCacheConfig()
	config.SetLastRowCacheConfig(cfg)
	if err := initTestCache(); err != nil {
		t.Fatal(err)
	}

	process := func(index int, group *sync.WaitGroup) {
		cv := &CacheValue{
			timestamp: rand.Int63n(1e6),
		}
		cache.Set(cacheKey("test", "test", []byte(strconv.Itoa(index))), cv, 0)
		cache.Wait()
		cache.Get(cacheKey("test", "test", []byte(strconv.Itoa(index))))
		group.Done()
	}

	concurrency := 8
	for i := 0; i < t.N; i++ {
		for j := 0; j < concurrency; j++ {
			wg.Add(1)
			go process(i, &wg)
		}
		wg.Wait()
	}
}

func Benchmark_Metrics(t *testing.B) {
	wg := sync.WaitGroup{}
	t.ReportAllocs()
	t.ResetTimer()
	cfg := config.NewLastRowCacheConfig()
	cfg.Metrics = true
	config.SetLastRowCacheConfig(cfg)
	if err := initTestCache(); err != nil {
		t.Fatal(err)
	}

	process := func(index int, group *sync.WaitGroup) {
		cv := &CacheValue{
			timestamp: rand.Int63n(1e6),
		}
		seriesK := []byte("location,city=SOIJAGJSAGDAGKADHKAGDKASGDKAKJDGHJAGDJHADGJK")
		cache.Set(cacheKey("test", "test", seriesK), cv, 0)
		cache.Wait()
		cache.Get(cacheKey("test", "test", seriesK))
		cache.Metrics.Ratio()
		cache.Metrics.Hits()
		cache.Metrics.CostEvicted()
		group.Done()
	}

	concurrency := 8
	for i := 0; i < t.N; i++ {
		for j := 0; j < concurrency; j++ {
			wg.Add(1)
			go process(i, &wg)
		}
		wg.Wait()
	}
}

func TestSetRow(t *testing.T) {
	convey.Convey("test set row", t, func() {
		err := initTestCache()
		convey.So(err, convey.ShouldBeNil)

		cache.Set(cacheKey("test", "autogen", []byte("number")), 666, 1)
		row := &influx.Row{Fields: influx.Fields{influx.Field{
			Key:      "position",
			StrValue: "test",
			Type:     influx.Field_Type_String,
		}}}
		convey.Convey("test without key", func() {
			oldValue := &CacheValue{timestamp: -99999}
			oldValue.SetFields(&sync.Map{})
			seriesOld := []byte("test,vin=110")
			Set("test", "autogen", seriesOld, oldValue)
			ans := SetRow("test", "xxx", seriesOld, row)
			convey.So(ans, convey.ShouldBeFalse)
		})

		convey.Convey("test invalid map value", func() {
			ans := SetRow("test", "autogen", []byte("number"), row)
			convey.So(ans, convey.ShouldBeFalse)
		})

		convey.Convey("test older than cache", func() {
			newValue := &CacheValue{timestamp: 99999}
			newValue.SetFields(&sync.Map{})
			seriek := []byte("test,vin=119")
			Set("test", "autogen", seriek, newValue)
			ans := SetRow("test", "autogen", seriek, row)
			convey.So(ans, convey.ShouldBeFalse)
		})

		convey.Convey("test younger than cache", func() {
			newValue := &CacheValue{timestamp: 99999}
			newValue.SetFields(&sync.Map{})
			seriek := []byte("test,vin=001")
			Set("test", "autogen", seriek, newValue)
			row.Timestamp = 199999
			cache.Wait()
			ans := SetRow("test", "autogen", seriek, row)
			convey.So(ans, convey.ShouldBeTrue)
		})
	})
}

func TestCacheGet(t *testing.T) {
	convey.Convey("test cache get failed", t, func() {
		err := initTestCache()
		convey.So(err, convey.ShouldBeNil)

		convey.Convey("cache miss", func() {
			seriesK := []byte("test")
			Set("test", "", seriesK, &CacheValue{})
			_, _, ok := Get("test", "autogen", seriesK, []string{})
			convey.So(ok, convey.ShouldBeFalse)
		})

		convey.Convey("invalid value", func() {
			seriesK := []byte("number")
			cache.Set(cacheKey("test", "autogen", seriesK), 666, 1)
			_, _, ok := Get("test", "autogen", seriesK, []string{})
			convey.So(ok, convey.ShouldBeFalse)
		})

		convey.Convey("nil value", func() {
			seriesK := []byte("valid")
			cacheV := &CacheValue{timestamp: 0}
			cacheV.SetFields(&sync.Map{})
			cache.Set(cacheKey("test", "autogen", seriesK), cacheV, 1)
			cache.Wait()
			_, _, ok := Get("test", "autogen", seriesK, []string{"good"})
			convey.So(ok, convey.ShouldBeFalse)
		})

	})
}

func TestRestartCache(t *testing.T) {
	convey.Convey("test restart cache", t, func() {
		SetLastRowCacheEnabled(true)
		err := initTestCache()
		convey.So(err, convey.ShouldBeNil)
		seriesK := []byte("test")
		Set("test", "autogen", seriesK, &CacheValue{})
		cache.Wait()

		err = RestartCache()
		convey.So(err, convey.ShouldBeNil)
		_, ok := cache.Get(cacheKey("test", "autogen", seriesK))
		convey.So(ok, convey.ShouldBeFalse)

		p1 := gomonkey.ApplyFunc(DisableEnableCache, func(_ bool) error {
			return errors.New("GG")
		})
		defer p1.Reset()

		err = RestartCache()
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func TestClearCache(t *testing.T) {
	convey.Convey("test restart cache", t, func() {
		SetLastRowCacheEnabled(true)
		err := initTestCache()
		convey.So(err, convey.ShouldBeNil)
		seriesK := []byte("test")
		Set("test", "autogen", seriesK, &CacheValue{})
		cache.Wait()

		ClearCache()
		_, ok := cache.Get(cacheKey("test", "autogen", seriesK))
		convey.So(ok, convey.ShouldBeFalse)
	})
}

func TestGetWithPreviousCache(t *testing.T) {
	convey.Convey("test set cache with same series and previous cache", t, func() {
		SetLastRowCacheEnabled(true)
		err := initTestCache()
		convey.So(err, convey.ShouldBeNil)

		seriesK := []byte("test")
		oldM := sync.Map{}
		oldM.Store("x", 1)
		cacheV := &CacheValue{timestamp: 1}
		cacheV.SetFields(&oldM)
		Set("test", "autogen", seriesK, cacheV)
		cache.Wait()

		newN := sync.Map{}
		oldM.Store("y", 2)
		cacheV = &CacheValue{timestamp: 1}
		cacheV.SetFields(&newN)
		Set("test", "autogen", seriesK, cacheV)
		cache.Wait()

		convey.Convey("test get all fields ok", func() {
			_, _, ok := Get("test", "autogen", seriesK, []string{"x", "y"})
			convey.So(ok, convey.ShouldBeTrue)
		})

		convey.Convey("test get missing some fields", func() {
			_, _, ok := Get("test", "autogen", seriesK, []string{"x", "y", "z"})
			convey.So(ok, convey.ShouldBeFalse)
		})

	})
}
