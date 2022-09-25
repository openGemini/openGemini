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

package meta

import (
	"fmt"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/openGemini/openGemini/lib/metaclient"
)

func TestCheckLeaderChanged(t *testing.T) {
	s := &Store{notifyCh: make(chan bool, 1), stepDown: make(chan struct{}), closing: make(chan struct{})}
	s.cm = NewClusterManager(s)
	s.raft = &raftWrapper{}
	c := raft.MakeCluster(1, t, nil)
	time.Sleep(time.Second)
	s.raft.raft = c.Leader()
	s.Node = &metaclient.Node{ID: 1}
	s.wg.Add(1)
	go s.checkLeaderChanged()
	s.notifyCh <- false
	time.Sleep(time.Second)
	select {
	case <-s.stepDown:

	default:
		t.Fatal(fmt.Errorf("leader should step down"))
	}
	close(s.closing)
	s.wg.Wait()
}
