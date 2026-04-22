/*
Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.

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
	"errors"
	"io/fs"
	"testing"
	"time"

	"github.com/openGemini/openGemini/engine/clearevent"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/stretchr/testify/require"
)

type MockSupplier struct{}

func (supplier *MockSupplier) GetNoClearShard() (uint64, error) {
	return uint64(1), nil
}

type MockSupplierError struct{}

func (supplier *MockSupplierError) GetNoClearShard() (uint64, error) {
	return uint64(1), errors.New("mock error")
}

func TestDoLoad(t *testing.T) {
	lock := ""
	ctx := &fileLoadContext{}
	m := &MmsTables{
		lock:      &lock,
		closed:    make(chan struct{}),
		sequencer: NewSequencer(),
		logger:    logger.NewLogger(errno.ModuleUnknown),
	}
	tiers := uint64(util.Cleared)
	m.tier = &tiers
	loader := newFileLoader(m, ctx)

	doLoad(&MockSupplier{}, []fs.FileInfo{
		&FakeInfo{
			NameFn: func() string {
				return "/tmp/name"
			},
			SizeFn: func() int64 {
				return 4096
			},
		},
	}, loader, m, m.logger)
	_, err := ctx.getError()
	require.Error(t, err, "ERROR_FILE_NOT_FOUND")

	m.path = "/tmp/openGemini/data/data/prom1/0/autogen/1_1744588800000000000_1745193600000000000_1"

	doLoad(&MockSupplier{}, []fs.FileInfo{
		&FakeInfo{
			NameFn: func() string {
				return "/tmp/name"
			},
			SizeFn: func() int64 {
				return 4096
			},
		},
	}, loader, m, m.logger)
	_, err = ctx.getError()
	require.Error(t, err, "ERROR_FILE_NOT_FOUND")

	doLoad(&MockSupplierError{}, []fs.FileInfo{
		&FakeInfo{
			NameFn: func() string {
				return "/tmp/name"
			},
			SizeFn: func() int64 {
				return 4096
			},
		},
	}, loader, m, m.logger)
	_, err = ctx.getError()
	require.Error(t, err, "ERROR_FILE_NOT_FOUND")

	doLoad(nil, []fs.FileInfo{
		&FakeInfo{
			NameFn: func() string {
				return "/tmp/name"
			},
			SizeFn: func() int64 {
				return 4096
			},
		},
	}, loader, m, m.logger)
	_, err = ctx.getError()
	require.Error(t, err, "ERROR_FILE_NOT_FOUND")

	m.path = "/tmp/openGemini/data/data/prom1/0/autogen/1_test_1745193600000000000_1"
	doLoad(nil, []fs.FileInfo{
		&FakeInfo{
			NameFn: func() string {
				return "/tmp/name"
			},
			SizeFn: func() int64 {
				return 4096
			},
		},
	}, loader, m, m.logger)
	_, err = ctx.getError()
	require.Error(t, err, "ERROR_FILE_NOT_FOUND")
}

func TestReplaceByNoClearShardId(t *testing.T) {
	lock := ""
	m := &MmsTables{
		lock:      &lock,
		closed:    make(chan struct{}),
		sequencer: NewSequencer(),
		logger:    logger.NewLogger(errno.ModuleUnknown),
	}
	tiers := uint64(util.Cleared)
	m.tier = &tiers

	_, _, err := m.ReplaceByNoClearShardId(nil)
	require.Error(t, err)
}

func TestDoDelete(t *testing.T) {
	lock := ""
	m := &MmsTables{
		lock:      &lock,
		closed:    make(chan struct{}),
		sequencer: NewSequencer(),
		logger:    logger.NewLogger(errno.ModuleUnknown),
	}
	m.Order = map[string]*TSSPFiles{
		"mst": &TSSPFiles{
			files: []TSSPFile{
				&MocTsspFile{},
			},
		},
	}

	m.OutOfOrder = map[string]*TSSPFiles{
		"mst": &TSSPFiles{
			files: []TSSPFile{
				&MocTsspFile{},
			},
		},
	}

	orders := map[string][]TSSPFile{
		"mst": []TSSPFile{
			&MocTsspFile{},
		},
		"mst1": []TSSPFile{
			&MocTsspFile{},
		},
	}

	unOrders := map[string][]TSSPFile{
		"mst": []TSSPFile{
			&MocTsspFile{},
		},
		"mst1": []TSSPFile{
			&MocTsspFile{},
		},
	}
	err := m.doDelete(orders, unOrders)
	require.NoError(t, err)
}

func TestDoReplaceAndLoadRemote(t *testing.T) {
	lock := ""
	ctx := &fileLoadContext{}
	m := &MmsTables{
		lock:      &lock,
		closed:    make(chan struct{}),
		sequencer: NewSequencer(),
		logger:    logger.NewLogger(errno.ModuleUnknown),
		path:      "1_11111111_1111111111_1",
	}
	tiers := uint64(util.Cleared)
	m.tier = &tiers
	loader := newFileLoader(m, ctx)
	m.Order = map[string]*TSSPFiles{
		"mst": &TSSPFiles{
			files: []TSSPFile{
				&MocTsspFile{},
			},
		},
	}

	m.OutOfOrder = map[string]*TSSPFiles{
		"mst": &TSSPFiles{
			files: []TSSPFile{
				&MocTsspFile{},
			},
		},
	}
	supplier := &clearevent.SupplierWithId{
		NoClearShard: uint64(1),
	}

	oldOrders := make(map[string][]TSSPFile)
	oldUnOrders := make(map[string][]TSSPFile)
	m.doReplaceAndLoadRemote(supplier, []fs.FileInfo{
		&FakeInfo{
			NameFn: func() string {
				return "mst/"
			},
		},
	}, oldOrders, oldUnOrders, loader)
	_, err := ctx.getError()
	require.NoError(t, err)

	m.doReplaceAndLoadRemote(&MockSupplierError{}, []fs.FileInfo{
		&FakeInfo{
			NameFn: func() string {
				return "mst/"
			},
		},
	}, oldOrders, oldUnOrders, loader)
	_, err = ctx.getError()
	require.NoError(t, err)

	m.path = ""
	m.doReplaceAndLoadRemote(supplier, []fs.FileInfo{
		&FakeInfo{
			NameFn: func() string {
				return "mst/"
			},
		},
	}, oldOrders, oldUnOrders, loader)
	_, err = ctx.getError()
	require.NoError(t, err)
}

func TestReplaceAndClear(t *testing.T) {
	lock := ""
	m := &MmsTables{
		lock:      &lock,
		closed:    make(chan struct{}),
		sequencer: NewSequencer(),
		logger:    logger.NewLogger(errno.ModuleUnknown),
		path:      "1_11111111_1111111111_1",
		Conf:      &Config{maxRowsPerSegment: 1},
	}
	tiers := uint64(util.Cleared)
	m.tier = &tiers
	m.Order = map[string]*TSSPFiles{
		"mst": &TSSPFiles{
			files: []TSSPFile{
				&MocTsspFile{},
			},
		},
	}

	m.OutOfOrder = map[string]*TSSPFiles{
		"mst": &TSSPFiles{
			files: []TSSPFile{
				&MocTsspFile{},
			},
		},
	}
	supplier := &clearevent.SupplierWithId{
		NoClearShard: uint64(1),
	}
	_, _, err := m.replaceAndClear(supplier, []fs.FileInfo{
		&FakeInfo{
			NameFn: func() string {
				return "mst/"
			},
		},
	}, time.Now())
	require.NoError(t, err)
}
