// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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

package fileops

import (
	"os"

	"github.com/openGemini/openGemini/lib/util"
)

type Reader struct {
	fp   File
	size int64
}

func (r *Reader) Open(file string, lock string) error {
	fp, err := OpenFile(file, os.O_RDONLY, 0600, FileLockOption(lock))
	if err != nil {
		return err
	}
	size, err := fp.Size()
	if err != nil {
		util.MustClose(fp)
		return err
	}

	r.fp = fp
	r.size = size
	return nil
}

func (r *Reader) File() File {
	return r.fp
}

func (r *Reader) Size() int64 {
	return r.size
}

func (r *Reader) Close() error {
	return r.fp.Close()
}
