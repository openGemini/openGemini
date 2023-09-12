/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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
package fileops

import (
	"math"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/config"
)

func TestBackGroundReadLimit(t *testing.T) {
	backLimit := 64 * config.MB
	SetBackgroundReadLimiter(backLimit)

	start := time.Now()
	allocCnt := 4
	step := backLimit / 3
	for i := 0; i < allocCnt; i++ {
		_ = BackGroundReaderWait(step)
	}

	actualDuration := time.Since(start).Seconds()
	expectDuration := float64(step*allocCnt) / float64(backLimit)
	delta := math.Abs(expectDuration - actualDuration)
	if delta > float64(step)/float64(backLimit) {
		t.Fatal("limiter not as expected, actual duration", actualDuration, "expect duration", expectDuration)
	}

}
