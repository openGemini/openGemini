//go:build streamfs
// +build streamfs

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

package engine

import (
	"fmt"
	"os"
	"path"
	"reflect"
	"strconv"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/fileops"
)

type streamFencer struct {
	lockFd   fileops.File
	dataPath string
	walPath  string
	db       string
	pt       uint32
}

func (sf *streamFencer) Fence() error {
	// lock
	if _, err := fileops.Stat(sf.lockPath()); err == nil {
		if err := fileops.RecoverLease(sf.lockPath()); err != nil {
			return fmt.Errorf("recover lease for %s failed: %v", sf.lockPath(), err)
		}
	} else if os.IsNotExist(err) {
		// File not exist, no need to fence.
		return sf.CreateLockFile()
	} else {
		return err
	}

	var err error
	err = sf.CreateLockFile()
	if err != nil {
		return err
	}

	// Seal wal.
	if err = sf.seal(); err != nil {
		return err
	}
	return nil
}

func (sf *streamFencer) lockDir() string {
	return path.Join(sf.dataPath, config.DataDirectory, sf.db, strconv.Itoa(int(sf.pt)))
}

func (sf *streamFencer) lockPath() string {
	return path.Join(sf.lockDir(), "LOCK")
}

func (sf *streamFencer) CreateLockFile() (err error) {
	if err = fileops.MkdirAll(sf.lockDir(), 0750); err != nil {
		return err
	}

	pri := fileops.FilePriorityOption(fileops.IO_PRIORITY_NORMAL)
	sf.lockFd, err = fileops.CreateV1(sf.lockPath(), pri)
	return err
}

func (sf *streamFencer) seal() error {
	var sealAllWalFiles func(string) error
	sealAllWalFiles = func(pathName string) error {
		fis, err := fileops.ReadDir(pathName)
		if os.IsNotExist(err) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("sealAllWalFiles %s failed: %v", pathName, err)
		}
		for _, fi := range fis {
			if fi.IsDir() {
				if err = sealAllWalFiles(path.Join(pathName, fi.Name())); err != nil {
					return err
				}
			} else {
				if err = fileops.SealFile(path.Join(pathName, fi.Name()), ""); err != nil {
					return err
				}
			}
		}
		return nil
	}

	if err := sealAllWalFiles(path.Join(sf.walPath, config.WalDirectory, sf.db, fmt.Sprintf("%d", sf.pt))); err != nil {
		return err
	}

	return nil
}

func (sf *streamFencer) ReleaseFence() error {
	if sf.lockFd != nil && !reflect.ValueOf(sf.lockFd).IsNil() {
		return sf.lockFd.Close()
	}
	return nil
}

func newFencer(dataPath, walPath, db string, pt uint32) fencer {
	return &streamFencer{
		dataPath: dataPath,
		walPath:  walPath,
		db:       db,
		pt:       pt,
	}
}
