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
	"sync"

	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics/opsStat"
)

type SpdyStatistics struct {
	queue chan *SpdyJob
	done  chan struct{}

	// map[remote_addr] map[SpdyLink<<8 | SpdyItem] int64
	data map[string]map[uint16]int64

	mu  sync.Mutex
	buf []byte
}

const (
	jobQueueSize = 4096
)

const (
	Sql2Meta SpdyLink = iota
	Sql2Store
	Meta2Meta
	Meta2Store
	Store2Meta
	LinkEnd
)

const (
	ConnTotal SpdyItem = iota
	FailedConnTotal
	ClosedConnTotal
	SuccessCreateSessionTotal
	FailedCreateSessionTotal
	ClosedSessionTotal
	ItemEnd
)

var links = [LinkEnd]string{
	Sql2Meta:   "sql2meta",
	Sql2Store:  "sql2store",
	Meta2Meta:  "meta2meta",
	Meta2Store: "meta2store",
	Store2Meta: "store2meta",
}

var items = [ItemEnd]string{
	ConnTotal:                 "connTotal",
	FailedConnTotal:           "failedConnTotal",
	ClosedConnTotal:           "closedConnTotal",
	SuccessCreateSessionTotal: "successCreateSessionTotal",
	FailedCreateSessionTotal:  "failedCreateSessionTotal",
	ClosedSessionTotal:        "closedSessionTotal",
}

type SpdyItem uint8
type SpdyLink uint8

type SpdyJob struct {
	link  SpdyLink
	item  SpdyItem
	addr  string
	value int64
}

func NewSpdyJob(link SpdyLink) *SpdyJob {
	return &SpdyJob{
		link:  link,
		item:  0,
		value: 1,
	}
}

func (job *SpdyJob) Key() uint16 {
	return uint16(job.link)<<8 | uint16(job.item)
}

func (job *SpdyJob) Clone() *SpdyJob {
	return &SpdyJob{
		link:  job.link,
		item:  job.item,
		addr:  job.addr,
		value: job.value,
	}
}

func (job *SpdyJob) SetItem(i SpdyItem) *SpdyJob {
	job.item = i
	return job
}

func (job *SpdyJob) SetValue(i int64) *SpdyJob {
	job.value = i
	return job
}

func (job *SpdyJob) SetAddr(addr string) *SpdyJob {
	job.addr = addr
	return job
}

var spdyStat *SpdyStatistics
var spdyTagMap map[string]string
var spdyStatisticsName = "spdy"

func NewSpdyStatistics() *SpdyStatistics {
	return spdyStat
}

func init() {
	spdyTagMap = make(map[string]string)
	spdyStat = &SpdyStatistics{
		done:  make(chan struct{}),
		queue: make(chan *SpdyJob, jobQueueSize),
		data:  make(map[string]map[uint16]int64),
	}
	go spdyStat.run()
}

func InitSpdyStatistics(tags map[string]string) {
	for k, v := range tags {
		spdyTagMap[k] = v
	}
}

func CollectSpdyStatistics(buffer []byte) ([]byte, error) {
	buffer = append(buffer, NewSpdyStatistics().CollectBuf()...)
	return buffer, nil
}

func (s *SpdyStatistics) Add(job *SpdyJob) {
	if job == nil || job.item >= ItemEnd {
		return
	}
	go func() {
		s.queue <- job
	}()
}

func (s *SpdyStatistics) Close() {
	close(s.done)
}

func (s *SpdyStatistics) CollectBuf() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.buf = s.buf[:0]
	for addr, values := range s.data {
		spdyTagMap["remote_addr"] = addr

		valueMap := make(map[string]interface{}, len(values))
		for k, v := range values {
			spdyTagMap["link"] = links[int(k>>8)]
			i := SpdyItem(k & 0xff)

			if v == 0 || i >= ItemEnd {
				continue
			}

			vk := items[i]
			valueMap[vk] = v
		}
		s.buf = AddPointToBuffer(spdyStatisticsName, spdyTagMap, valueMap, s.buf)
	}

	return s.buf
}

func (s *SpdyStatistics) run() {
	for {
		select {
		case job, ok := <-s.queue:
			if !ok {
				return
			}

			s.processJob(job)
		case <-s.done:
			return
		}
	}
}

func (s *SpdyStatistics) processJob(job *SpdyJob) {
	s.mu.Lock()
	defer s.mu.Unlock()

	k := job.Key()
	if _, ok := s.data[job.addr]; !ok {
		s.data[job.addr] = make(map[uint16]int64, ItemEnd)
	}

	s.data[job.addr][k] += job.value
}

func CollectOpsSpdyStatistics() []opsStat.OpsStatistic {
	spdyStat.mu.Lock()
	defer spdyStat.mu.Unlock()

	statistic := opsStat.NewStatistic("spdy")
	for addr, values := range spdyStat.data {
		spdyTagMap["remote_addr"] = addr

		for k, v := range values {
			spdyTagMap["link"] = links[int(k>>8)]
			i := SpdyItem(k & 0xff)

			if v == 0 || i >= ItemEnd {
				continue
			}

			vk := items[i]
			statistic.Values[vk] = v
		}
	}

	// Add any supplied tags.
	for k, v := range spdyTagMap {
		statistic.Tags[k] = v
	}
	return []opsStat.OpsStatistic{
		statistic,
	}
}
