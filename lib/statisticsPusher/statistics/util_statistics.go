// Copyright Huawei Cloud Computing Technologies Co., Ltd.
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

package statistics

import (
	"fmt"
	"strconv"
	"sync"
	"time"
)

const (
	maxTimeDiff = 60 * time.Second
)

func addTagFieldToBuffer(statisticsName string, tagMap map[string]string, fieldMap map[string]interface{}, buffer []byte) []byte {
	/* append measurement name*/
	buffer = append(buffer, statisticsName...)
	buffer = append(buffer, ',')

	/* append tags */
	index := 0
	for k, v := range tagMap {
		buffer = append(buffer, k...)
		buffer = append(buffer, '=')
		buffer = append(buffer, formatString(v, false)...)
		if index != len(tagMap)-1 {
			buffer = append(buffer, ',')
		}
		index++
	}
	buffer = append(buffer, ' ')

	/* append values */
	index = 0
	for k, v := range fieldMap {
		buffer = append(buffer, k...)
		buffer = append(buffer, '=')
		switch item := v.(type) {
		case int64, float64:
			buffer = append(buffer, fmt.Sprintf("%v", item)...)
		case string:
			buffer = append(buffer, formatString(item, true)...)
		case bool:
			buffer = append(buffer, fmt.Sprintf("%t", item)...)
		default:
			panic(fmt.Sprintf("don't support type %T!\n", v))
		}
		if index != len(fieldMap)-1 {
			buffer = append(buffer, ',')
		}
		index++
	}
	return buffer
}

func AddPointToBuffer(statisticsName string, tagMap map[string]string, fieldMap map[string]interface{}, buffer []byte) []byte {
	/* append measurement name*/
	buffer = addTagFieldToBuffer(statisticsName, tagMap, fieldMap, buffer)

	buffer = append(buffer, ' ')
	buffer = append(buffer, NewTimestamp().Bytes()...)
	buffer = append(buffer, '\n')
	return buffer
}

func AddTimeToBuffer(statisticsName string, tagMap map[string]string, fieldMap map[string]interface{}, t time.Time, buffer []byte) []byte {
	buffer = addTagFieldToBuffer(statisticsName, tagMap, fieldMap, buffer)

	buffer = append(buffer, ' ')
	buffer = append(buffer, []byte(strconv.FormatInt(t.UnixNano(), 10))...)
	buffer = append(buffer, '\n')
	return buffer
}

func formatString(s string, quote bool) string {
	buf := make([]byte, 0, len(s)+2)
	if quote {
		buf = append(buf, '"')
	}

	for i := 0; i < len(s); i++ {
		if s[i] == ' ' || s[i] == 0 || s[i] == '\n' || s[i] == '\r' || s[i] == '\t' ||
			s[i] == '"' || s[i] == '=' || s[i] == ',' {
			continue
		}
		buf = append(buf, s[i])
	}

	if quote {
		buf = append(buf, '"')
	}
	return string(buf)
}

func AllocTagMap(tagMap, globalTag map[string]string) {
	for k, v := range globalTag {
		tagMap[k] = v
	}
}

var timestampInstance = &Timestamp{}

type Timestamp struct {
	val      int64
	interval int64
	buf      []byte

	mu sync.RWMutex
}

func NewTimestamp() *Timestamp {
	return timestampInstance
}

func (ts *Timestamp) Bytes() []byte {
	ts.mu.RLock()
	defer ts.mu.RUnlock()

	return ts.buf
}

func (ts *Timestamp) Init(interval time.Duration) {
	ts.interval = interval.Nanoseconds()
	ts.Reset()
}

func (ts *Timestamp) Reset() {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	val := time.Now().UnixNano()
	val -= val % ts.interval

	if ts.val == 0 {
		ts.update(val)
		return
	}

	diff := ts.val - val
	if diff < 0 {
		diff *= -1
	}

	if diff >= maxTimeDiff.Nanoseconds() {
		ts.update(val)
	}
}

func (ts *Timestamp) Incr() {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	ts.update(ts.val + ts.interval)
}

func (ts *Timestamp) update(v int64) {
	ts.val = v
	ts.buf = []byte(strconv.FormatInt(v, 10))
}

type Metric struct {
	tags map[string]string
}

func (m *Metric) Init(tags map[string]string) {
	m.tags = tags
}
