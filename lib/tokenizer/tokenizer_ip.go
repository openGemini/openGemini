// Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.
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

package tokenizer

import (
	"bytes"
	"encoding/binary"
	"math/bits"
	"net"
)

var IPv4Masks []net.IPMask // store ipv4 mask by 8bit step
var IPv6Masks []net.IPMask // store ipv6 mask by 8bit step

func init() {
	IPv4Masks = make([]net.IPMask, net.IPv4len)
	var ipv4Mask net.IPMask = make([]byte, net.IPv4len)
	// generate [32, 24, 16, 8] ipv4 mask
	for i := 0; i < net.IPv4len; i++ {
		ipv4Mask[i] = 0xff
		IPv4Masks[net.IPv4len-i-1] = make([]byte, net.IPv4len)
		copy(IPv4Masks[net.IPv4len-i-1], ipv4Mask)
	}

	IPv6Masks = make([]net.IPMask, net.IPv6len)
	var ipv6Mask net.IPMask = make([]byte, net.IPv6len)
	// generate [128 120 112 104 96 88 80 72 64 56 48 40 32 24 16 8] ipv6 mask
	for i := 0; i < net.IPv6len; i++ {
		ipv6Mask[i] = 0xff
		IPv6Masks[net.IPv6len-i-1] = make([]byte, net.IPv6len)
		copy(IPv6Masks[net.IPv6len-i-1], ipv6Mask)
	}
}

// select mask by ip version
func selectMasksByIp(inputIP *net.IP) []net.IPMask {
	if inputIP == nil {
		return nil
	}
	if inputIP.To4() != nil {
		return IPv4Masks
	} else if inputIP.To16() != nil {
		return IPv6Masks
	} else {
		// unknown ip version
		return nil
	}
}

type IpTokenizer struct {
	inputIP      net.IP
	curMaskIndex int
	hashValue    uint64
}

func (it *IpTokenizer) InitInput(bytes []byte) {
	it.inputIP = net.ParseIP(string(bytes))
	it.curMaskIndex = 0
}

func NewIpTokenizer() *IpTokenizer {
	t := &IpTokenizer{}
	return t
}

func (it *IpTokenizer) GetMatchedMaskIndex(mask net.IPMask) int {
	// floor mask to nearest 8bit
	// e.g. 255.255.128.0 -> 255.255.0.0
	for i := 0; i < len(mask); i++ {
		if mask[i] > 0 && mask[i] < 255 {
			mask[i] = 0
		}
	}

	switch len(mask) {
	case net.IPv4len:
		for i, ipMask := range IPv4Masks {
			if bytes.Equal(ipMask, mask) {
				return i
			}
		}
	case net.IPv6len:
		for i, ipMask := range IPv6Masks {
			if bytes.Equal(ipMask, mask) {
				return i
			}
		}
	default:
	}

	// no mask matched
	return -1
}

// HashWithMaskIndex Generate corresponding hash values for different range Bloom filters.
func (it *IpTokenizer) HashWithMaskIndex(inputIP *net.IP, curMaskIndex int) {
	it.hashValue = 0
	// If there is no matching mask, Bloom filter is not used.
	if curMaskIndex < 0 {
		return
	}

	masks := selectMasksByIp(inputIP)

	if masks == nil || curMaskIndex >= len(masks) {
		return
	}

	maskedIpBytes := inputIP.Mask(masks[curMaskIndex])
	for _, ipByte := range maskedIpBytes {
		// The curMaskIndex is used to generate an offset for the bloom hash value,
		// distinguishing different ranges of Bloom filters
		it.hashValue ^= bits.RotateLeft64(it.hashValue+uint64(curMaskIndex), 11) ^ (uint64(ipByte) * Prime_64)
	}
}

// Next convert ip to 8bit range
// IPv4: eg. ip 1.2.3.4 will generate [1.2.3.4/32, 1.2.3.0/24, 1.2.0.0/16, 1.0.0.0/8]
// IPv6: eg. ip 99fa:25a4:b87c:3d56:99b2:46d0:3c9e:17b1 will generate [99fa:25a4:b87c:3d56:99b2:46d0:3c9e:17b1, ..., 9900::0/8]
// add to BloomFilter
func (it *IpTokenizer) Next() bool {

	it.hashValue = 0

	masks := selectMasksByIp(&it.inputIP)

	if masks == nil || it.curMaskIndex >= len(masks) {
		return false
	}

	if it.curMaskIndex < len(masks) {
		it.HashWithMaskIndex(&it.inputIP, it.curMaskIndex)
		it.curMaskIndex++
		return true
	}

	return false
}

func (it *IpTokenizer) ProcessTokenizerBatch(input, output []byte, offsets, lens []int32) int {
	for i := range offsets {
		it.InitInput(input[offsets[i] : offsets[i]+lens[i]])
		for it.Next() {
			hash := it.CurrentHash()
			target := uint32(hash >> 46)
			var offsetLow int = int((hash >> 28) & 0x1ff)
			var offsetHigh int = int((hash >> 37) & 0x1ff)
			v := table[offsetLow] | (table[offsetHigh] << 32)
			s := binary.LittleEndian.Uint64(output[target : target+8])
			if (v & s) == v {
				continue
			}
			binary.LittleEndian.PutUint64(output[target:target+8], v|s)
		}
	}
	return 0
}

func (it *IpTokenizer) CurrentHash() uint64 {
	return it.hashValue
}

func (it *IpTokenizer) FreeSimpleGramTokenizer() {}
