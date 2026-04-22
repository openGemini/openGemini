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

package machine

import (
	"encoding/binary"
	"net"
	"strconv"
	"strings"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/rand"
	"go.uber.org/zap"
)

// machineID: 2 byte random, 4 byte ip, 2 byte port
var machineID uint64

func init() {
	machineID = uint64(rand.Int63())
}

func GetMachineID() uint64 {
	return machineID
}

func InitMachineID(addr string) {
	addrs := strings.Split(addr, ",")
	if len(addrs) > 0 {
		addr = addrs[0]
	}
	addr = strings.TrimSpace(addr)
	ip, port, err := parseAddr(addr)
	if err != nil {
		logger.NewLogger(errno.ModuleNetwork).Warn("failed to parse address", zap.Error(err))
		return
	}

	r := uint64(rand.Int63() & 0xffff)
	buf := []byte{
		uint8(r >> 8), uint8(r & 0xff),
		ip[0], ip[1], ip[2], ip[3],
		uint8(port >> 8), uint8(port & 0xff),
	}

	machineID = binary.BigEndian.Uint64(buf[:])
}

func parseAddr(addr string) (net.IP, uint64, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, 0, err
	}

	ip := net.ParseIP(host)
	if len(ip) != net.IPv6len {
		return nil, 0, errno.NewError(errno.InvalidAddress, addr)
	}

	ip = ip[net.IPv6len-4:]
	if binary.BigEndian.Uint32(ip) == 0 {
		return nil, 0, errno.NewError(errno.BadListen, addr)
	}

	p, _ := strconv.ParseUint(port, 10, 64)
	return ip, p, nil
}
