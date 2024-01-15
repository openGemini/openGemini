/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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

package immutable

import (
	"sort"

	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/request"
	"github.com/openGemini/openGemini/lib/util"
)

const (
	PKMetaInfoLength int64 = 12
	PkMetaHeaderSize       = int64(util.Uint32SizeBytes * 2)
)

type DetachedPKMetaInfoReader struct {
	r fileops.BasicFileReader
}

func NewDetachedPKMetaInfoReader(path string, opts *obs.ObsOptions) (*DetachedPKMetaInfoReader, error) {
	fd, err := obs.OpenObsFile(path, PrimaryMetaFile, opts)
	if err != nil {
		return nil, err
	}
	dr := fileops.NewFileReader(fd, nil)
	return &DetachedPKMetaInfoReader{r: dr}, nil
}

func (reader *DetachedPKMetaInfoReader) Read() (*colstore.DetachedPKMetaInfo, error) {
	pkMetaInfo, err := reader.readPKMetaInfoLength([]int64{0}, []int64{PKMetaInfoLength})
	if err != nil {
		return nil, err
	}
	pkMetaInfo, err = reader.readPKMetaInfoItem([]int64{0}, []int64{pkMetaInfo[0].Offset})
	if err != nil {
		return nil, err
	}
	return pkMetaInfo[0], nil
}

func (reader *DetachedPKMetaInfoReader) readPKMetaInfoLength(offset, length []int64) ([]*colstore.DetachedPKMetaInfo, error) {
	c := make(chan *request.StreamReader, 1)
	reader.r.StreamReadBatch(offset, length, c, PKMetaLimitNum)
	pkItems := make([]*colstore.DetachedPKMetaInfo, len(offset))
	i := 0
	for r := range c {
		if r.Err != nil {
			return nil, r.Err
		}
		pkItems[i] = &colstore.DetachedPKMetaInfo{}
		_, err := pkItems[i].UnmarshalPublicSize(r.Content)
		if err != nil {
			return nil, err
		}
		i += 1
	}
	return pkItems, nil
}

func (reader *DetachedPKMetaInfoReader) readPKMetaInfoItem(offset, length []int64) ([]*colstore.DetachedPKMetaInfo, error) {
	c := make(chan *request.StreamReader, 1)
	reader.r.StreamReadBatch(offset, length, c, PKMetaLimitNum)
	pkItems := make([]*colstore.DetachedPKMetaInfo, len(offset))
	i := 0
	for r := range c {
		if r.Err != nil {
			return nil, r.Err
		}
		pkItems[i] = &colstore.DetachedPKMetaInfo{Offset: r.Offset}
		_, err := pkItems[i].Unmarshal(r.Content)
		if err != nil {
			return nil, err
		}
		i += 1
	}
	sort.Slice(pkItems, func(i, j int) bool {
		return pkItems[i].Offset < pkItems[j].Offset
	})
	return pkItems, nil
}
