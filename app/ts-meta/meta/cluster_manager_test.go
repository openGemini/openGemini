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

package meta_test

import (
	"testing"
	"time"

	"github.com/openGemini/openGemini/app/ts-meta/meta"
	"github.com/openGemini/openGemini/open_src/github.com/hashicorp/serf/serf"
	"github.com/stretchr/testify/assert"
)

func TestClusterManagerHandleJoinEvent(t *testing.T) {
	dir := t.TempDir()
	ms, err := meta.InitStore(dir, "127.0.0.1")
	defer func() {
		ms.Close()
	}()
	if err != nil {
		t.Fatal(err)
	}

	cmd := meta.GenerateCreateDataNodeCmd("127.0.0.1:8400", "127.0.0.1:8401")
	if err = ms.GetStore().ApplyCmd(cmd); err != nil {
		t.Fatal(err)
	}
	cm := ms.GetStore().GetClusterManager()
	e := *generateMemberEvent(serf.EventMemberJoin, "2", 1, serf.StatusAlive)
	eventCh := cm.GetEventCh()
	eventCh <- e
	time.Sleep(1 * time.Second)
	cm.WaitEventDone()
	dn := ms.GetStore().GetData().DataNode(2)
	assert.Equal(t, serf.StatusAlive, dn.Status)
	assert.Equal(t, uint64(1), dn.LTime)
}

func TestClusterManagerHandleFailedEvent(t *testing.T) {
	dir := t.TempDir()
	ms, err := meta.InitStore(dir, "127.0.0.1")
	defer func() {
		ms.Close()
	}()
	if err != nil {
		t.Fatal(err)
	}

	cmd := meta.GenerateCreateDataNodeCmd("127.0.0.1:8400", "127.0.0.1:8401")
	if err = ms.GetStore().ApplyCmd(cmd); err != nil {
		t.Fatal(err)
	}
	cm := ms.GetStore().GetClusterManager()
	e := *generateMemberEvent(serf.EventMemberFailed, "2", 1, serf.StatusFailed)
	eventCh := cm.GetEventCh()
	eventCh <- e
	time.Sleep(1 * time.Second)
	cm.WaitEventDone()
	dn := ms.GetStore().GetData().DataNode(2)
	assert.Equal(t, serf.StatusFailed, dn.Status)
	assert.Equal(t, uint64(1), dn.LTime)
}

func TestClusterManagerDoNotHandleOldEvent(t *testing.T) {
	dir := t.TempDir()
	ms, err := meta.InitStore(dir, "127.0.0.1")
	defer func() {
		ms.Close()
	}()
	if err != nil {
		t.Fatal(err)
	}

	cmd := meta.GenerateCreateDataNodeCmd("127.0.0.1:8400", "127.0.0.1:8401")
	if err = ms.GetStore().ApplyCmd(cmd); err != nil {
		t.Fatal(err)
	}
	cm := ms.GetStore().GetClusterManager()
	e := *generateMemberEvent(serf.EventMemberJoin, "2", 3, serf.StatusAlive)
	eventCh := cm.GetEventCh()
	eventCh <- e
	time.Sleep(1 * time.Second)
	cm.WaitEventDone()
	dn := ms.GetStore().GetData().DataNode(2)
	assert.Equal(t, serf.StatusAlive, dn.Status)
	assert.Equal(t, uint64(3), dn.LTime)

	e = *generateMemberEvent(serf.EventMemberFailed, "2", 2, serf.StatusFailed)
	eventCh <- e
	time.Sleep(1 * time.Second)
	cm.WaitEventDone()
	dn = ms.GetStore().GetData().DataNode(2)
	assert.Equal(t, serf.StatusAlive, dn.Status)
	assert.Equal(t, uint64(3), dn.LTime)
}

func TestEventCanBeHandledAfterLeaderChange(t *testing.T) {
	dir := t.TempDir()
	ms, err := meta.InitStore(dir, "127.0.0.1")
	defer func() {
		ms.Close()
	}()
	if err != nil {
		t.Fatal(err)
	}

	cmd := meta.GenerateCreateDataNodeCmd("127.0.0.1:8400", "127.0.0.1:8401")
	if err = ms.GetStore().ApplyCmd(cmd); err != nil {
		t.Fatal(err)
	}
	cm := ms.GetStore().GetClusterManager()
	e := *generateMemberEvent(serf.EventMemberJoin, "2", 3, serf.StatusAlive)
	eventCh := cm.GetEventCh()
	eventCh <- e
	cm.Stop()
	cm.Start()
	time.Sleep(time.Second)
	cm.WaitEventDone()
	dn := ms.GetStore().GetData().DataNode(2)
	assert.Equal(t, serf.StatusAlive, dn.Status)
	assert.Equal(t, uint64(3), dn.LTime)
}

func generateMemberEvent(t serf.EventType, name string, ltime uint64, status serf.MemberStatus) *serf.MemberEvent {
	return &serf.MemberEvent{
		Type:      t,
		EventTime: serf.LamportTime(ltime),
		Members: []serf.Member{
			serf.Member{Name: name,
				Tags:   map[string]string{"role": "store"},
				Status: status},
		},
	}
}

func TestClusterManagerReSendEvent(t *testing.T) {
	dir := t.TempDir()
	ms, err := meta.InitStore(dir, "127.0.0.1")
	defer func() {
		ms.Close()
	}()
	if err != nil {
		t.Fatal(err)
	}

	cmd := meta.GenerateCreateDataNodeCmd("127.0.0.1:8400", "127.0.0.1:8401")
	if err = ms.GetStore().ApplyCmd(cmd); err != nil {
		t.Fatal(err)
	}
	cm := ms.GetStore().GetClusterManager()
	e := *generateMemberEvent(serf.EventMemberJoin, "2", 3, serf.StatusAlive)
	eventCh := cm.GetEventCh()
	eventCh <- e
	time.Sleep(time.Second)
	cm.WaitEventDone()
	cm.Stop()
	e = *generateMemberEvent(serf.EventMemberFailed, "2", 4, serf.StatusFailed)
	eventCh <- e
	time.Sleep(time.Second)
	cm.WaitEventDone()
	cm.Start()
	time.Sleep(time.Second)
	cm.WaitEventDone()
	dn := ms.GetStore().GetData().DataNode(2)
	assert.Equal(t, serf.StatusFailed, dn.Status)
	assert.Equal(t, uint64(4), dn.LTime)
}

func TestClusterManagerResendLastEventWhenLeaderChange(t *testing.T) {
	dir := t.TempDir()
	ms, err := meta.InitStore(dir, "127.0.0.1")
	defer func() {
		ms.Close()
	}()
	if err != nil {
		t.Fatal(err)
	}

	cmd := meta.GenerateCreateDataNodeCmd("127.0.0.1:8400", "127.0.0.1:8401")
	if err = ms.GetStore().ApplyCmd(cmd); err != nil {
		t.Fatal(err)
	}
	followerCm := meta.NewClusterManager(ms.GetStore())
	followerCh := followerCm.GetEventCh()
	e := *generateMemberEvent(serf.EventMemberJoin, "2", 3, serf.StatusAlive)
	followerCh <- e
	failedE := *generateMemberEvent(serf.EventMemberFailed, "2", 3, serf.StatusFailed)
	followerCh <- failedE
	time.Sleep(time.Second)
	assert.Equal(t, 0, len(followerCh), "follower eventCh will be clear by checkEvents")
	followerCm.Close()
}
