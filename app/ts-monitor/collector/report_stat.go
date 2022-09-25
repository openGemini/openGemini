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

package collector

import "sync"

const (
	// Maximum number of retries in case of reporting failure
	maxRetry = 2
)

type ReportStat struct {
	// key: filename, value: number of failures
	failed map[string]int
	mu     sync.Mutex
}

func NewReportStat() *ReportStat {
	return &ReportStat{
		failed: make(map[string]int),
	}
}

func (s *ReportStat) TryAgain(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.failed[key]; ok {
		s.failed[key]++
		return s.failed[key] <= maxRetry
	}

	s.failed[key] = 1
	return true
}

func (s *ReportStat) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.failed, key)
}
