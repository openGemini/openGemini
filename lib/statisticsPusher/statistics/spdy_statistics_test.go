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

package statistics

import (
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func newSpdyStatistics() *SpdyStatistics {
	spdyStat = &SpdyStatistics{
		done:  make(chan struct{}),
		queue: make(chan *SpdyJob, jobQueueSize),
		data:  make(map[string]map[uint16]int64),
	}
	go spdyStat.run()
	return spdyStat
}

func TestSpdyStatistics(t *testing.T) {
	ts := NewTimestamp()
	ts.Init(time.Second)
	ss := newSpdyStatistics()
	defer ss.Close()

	ss.Add(NewSpdyJob(Sql2Meta).SetAddr("127.0.0.1:8092").SetItem(ConnTotal))
	ss.Add(NewSpdyJob(Meta2Meta).SetAddr("127.0.0.1:8091").SetItem(ClosedConnTotal))

	job := NewSpdyJob(Sql2Store).SetAddr("127.0.0.1:8401").SetItem(ConnTotal)
	job2 := job.Clone()
	job2.SetItem(FailedConnTotal)
	job2.SetValue(20)
	ss.Add(job2)

	time.Sleep(time.Second)

	got := sortBuf(string(ss.CollectBuf()), "\n", true)
	exp := fmt.Sprintf(`spdy,link=sql2meta,remote_addr=127.0.0.1:8092 connTotal=1 %s
spdy,link=meta2meta,remote_addr=127.0.0.1:8091 closedConnTotal=1 %s
spdy,link=sql2store,remote_addr=127.0.0.1:8401 failedConnTotal=20 %s`,
		string(ts.Bytes()), string(ts.Bytes()), string(ts.Bytes()))

	exp = sortBuf(exp, "\n", true)

	if got != exp {
		t.Fatalf("spdy statistics failed. \nexp: \n%s \ngot: \n%s \n", exp, got)
	}
}

func sortBuf(buf string, sep string, deep bool) string {
	buf = strings.TrimSpace(buf)

	arr := strings.Split(buf, sep)
	if deep {
		for i := 0; i < len(arr); i++ {
			tmp := strings.Split(arr[i], " ")
			tmp[0] = sortBuf(tmp[0], ",", false)
			arr[i] = strings.Join(tmp, " ")
		}
	}

	sort.Strings(arr)
	return strings.Join(arr, sep)
}

func TestBenchmarkSpdyStatistics(t *testing.T) {
	ss := newSpdyStatistics()
	defer ss.Close()

	job := NewSpdyJob(Sql2Meta).SetAddr("127.0.0.1:8092").SetItem(ConnTotal)
	begin := time.Now()

	total := 2000000
	for i := 0; i < total; i++ {
		go ss.Add(job)
	}

	for {
		if len(ss.queue) == 0 {
			break
		}
	}

	use := time.Since(begin)
	fmt.Println(use.Nanoseconds()/int64(total), "op/ns")
}

func TestOpsSpdyStatistics(t *testing.T) {
	ts := NewTimestamp()
	ts.Init(time.Second)
	ss := newSpdyStatistics()
	defer ss.Close()

	ss.Add(NewSpdyJob(Sql2Meta).SetAddr("127.0.0.1:8092").SetItem(ConnTotal))
	ss.Add(NewSpdyJob(Meta2Meta).SetAddr("127.0.0.1:8091").SetItem(ClosedConnTotal))

	job := NewSpdyJob(Sql2Store).SetAddr("127.0.0.1:8401").SetItem(ConnTotal)
	job2 := job.Clone()
	job2.SetItem(FailedConnTotal)
	job2.SetValue(20)
	ss.Add(job2)

	time.Sleep(time.Second)

	stats := CollectOpsSpdyStatistics()
	assert.Equal(t, 1, len(stats))
	assert.Equal(t, "spdy", stats[0].Name)
}
