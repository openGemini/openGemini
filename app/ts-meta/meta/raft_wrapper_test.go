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
	"testing"

	"github.com/hashicorp/raft"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/stretchr/testify/assert"
)

func TestRaftWrapperApply(t *testing.T) {
	var r *raftWrapper
	err := r.apply([]byte("raft is not open"))
	assert.True(t, errno.Equal(err, errno.RaftIsNotOpen), "apply should fail with error raft is not open")
	r = &raftWrapper{}
	c := raft.MakeCluster(3, t, nil)
	follows := c.Followers()
	assert.Equal(t, 2, len(follows))
	r.raft = follows[0]
	err = r.apply([]byte("not leader"))
	assert.True(t, errno.Equal(err, errno.MetaIsNotLeader), "apply should fail with error not leader")
}

func TestBootFirst(t *testing.T) {
	c := &config.Meta{
		RPCBindAddress: "127.0.0.1:8400",
		JoinPeers:      nil,
		Domain:         "localhost",
	}

	assert.False(t, bootFirst(c))

	c.JoinPeers = []string{c.Domain + ":8400"}
	assert.True(t, bootFirst(c))
}
