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

package machine_test

import (
	"encoding/binary"
	"testing"

	"github.com/openGemini/openGemini/lib/machine"
	"github.com/stretchr/testify/assert"
)

func TestMachineID(t *testing.T) {
	randID := machine.GetMachineID()

	machine.InitMachineID("127.0.0.1")
	assert.Equal(t, randID, machine.GetMachineID())

	machine.InitMachineID("127.0.0:8844")
	assert.Equal(t, randID, machine.GetMachineID())

	machine.InitMachineID("0.0.0.0:8844")
	assert.Equal(t, randID, machine.GetMachineID())

	buf := [8]byte{}

	// test ipv4
	machine.InitMachineID("127.0.0.1:8433")
	id := machine.GetMachineID()
	binary.BigEndian.PutUint64(buf[:], id)
	assert.Equal(t, id&0xffff, uint64(8433), "invalid port")
	assert.Equal(t, buf[2:6], []byte{127, 0, 0, 1}, "invalid ip")

	// test double ipv4
	machine.InitMachineID("127.0.0.1:8433,127.0.0.2:8635")
	id = machine.GetMachineID()
	binary.BigEndian.PutUint64(buf[:], id)
	assert.Equal(t, id&0xffff, uint64(8433), "invalid port")
	assert.Equal(t, buf[2:6], []byte{127, 0, 0, 1}, "invalid ip")

	// test ipv6
	machine.InitMachineID("[ff88::ff88:3eff:fe5b:3611]:8844")
	id = machine.GetMachineID()
	binary.BigEndian.PutUint64(buf[:], id)
	assert.Equal(t, id&0xffff, uint64(8844), "invalid port")
	assert.Equal(t, buf[2:6], []byte{0xfe, 0x5b, 0x36, 0x11}, "invalid ip")
}
