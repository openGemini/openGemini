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

package metaclient

import (
	"io"
	"os"
	"path"
	"path/filepath"
	"strconv"

	"github.com/openGemini/openGemini/lib/util"
)

const (
	ClockFileName = "clock"
)

var LogicClock uint64

type Node struct {
	path   string
	ID     uint64
	Clock  uint64
	ConnId uint64
}

func NewNode(path string) *Node {
	return &Node{
		path: path,
	}
}

func (n *Node) LoadLogicalClock() error {
	clockPath := filepath.Join(n.path, ClockFileName)
	var file *os.File
	// for compatibility: if clock is already exist read it else do not create it and use ltime as logicalclock
	file, err := os.OpenFile(path.Clean(clockPath), os.O_RDONLY, 0600)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	defer util.MustClose(file)

	var content []byte
	content, err = io.ReadAll(file)
	if err != nil {
		return err
	}
	if len(content) == 0 {
		content = []byte("1")
	}

	var clock int
	clock, err = strconv.Atoi(string(content))
	if err != nil {
		return err
	}

	LogicClock = uint64(clock)
	if err = os.WriteFile(clockPath, []byte(strconv.Itoa(clock+1)), 0600); err != nil {
		return err
	}
	return nil
}
