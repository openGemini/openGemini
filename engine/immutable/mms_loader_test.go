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

package immutable

import (
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/openGemini/openGemini/engine/index/sparseindex"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/stretchr/testify/require"
)

func TestMmsLoader(t *testing.T) {
	lock := ""
	ctx := &fileLoadContext{}
	loader := newFileLoader(&MmsTables{
		lock:      &lock,
		closed:    make(chan struct{}),
		sequencer: NewSequencer(),
	}, ctx)

	dir := t.TempDir()
	loader.Load(path.Join(dir, "not_exists"), "mst", true)
	loader.Wait()
	_, err := ctx.getError()
	require.NotEmpty(t, err)

	require.NoError(t, os.MkdirAll(path.Join(dir, "mst", unorderedDir, unorderedDir), 0700))
	require.NoError(t, os.WriteFile(path.Join(dir, "mst", "00000001-0000-00000000.tssp.init"), []byte{1}, 0600))

	ctx = &fileLoadContext{}
	loader = newFileLoader(&MmsTables{
		lock:   &lock,
		closed: make(chan struct{}),
	}, ctx)
	loader.Load(path.Join(dir, "mst"), "mst", true)
	loader.Wait()
	_, err = ctx.getError()
	require.NoError(t, err)
}

func TestMmsLoadIndexFiles(t *testing.T) {
	lock := ""
	ctx := &fileLoadContext{}
	loader := newFileLoader(&MmsTables{
		lock:      &lock,
		closed:    make(chan struct{}),
		sequencer: NewSequencer(),
	}, ctx)

	dir := t.TempDir()
	err := os.MkdirAll(path.Join(dir, "mst"), 0700)
	err = os.WriteFile(path.Join(dir, "mst", "segment.idx"), []byte{1}, 0600)
	if err != nil {
		t.Fatal(err)
	}
	loader.Load(path.Join(dir, "mst"), "mst", true)
	loader.Wait()
	_, err = ctx.getError()
	require.NoError(t, err)
}

func TestIsDetachedIdxFile(t *testing.T) {
	fileName := sparseindex.GetFullTextDetachFilePath("", "")
	ans := isDetachedIdxFile(fileName)
	if !ans {
		t.Fatal("fileName is detached idx file")
	}
}

func TestRemoteDirIsOrder(t *testing.T) {
	type test struct {
		path     string
		expected bool
	}

	tests := []test{
		{path: "/home/user/out-of-order/file1", expected: false},
		{path: "/home/user/file1", expected: true},
		{path: "/home/user", expected: true},
		{path: "/home/file1", expected: true},
		{path: "/file1", expected: true},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			if got := remoteDirIsOrder(tt.path); got != tt.expected {
				t.Errorf("isRemoteUnorderedDir(%q) = %v, want %v", tt.path, got, tt.expected)
			}
		})
	}
}

func TestLoadRemote(t *testing.T) {
	dir := t.TempDir()
	defer fileops.RemoveAll(dir)

	lock := ""
	ctx := &fileLoadContext{}
	loader := newFileLoader(&MmsTables{
		lock:      &lock,
		closed:    make(chan struct{}),
		sequencer: NewSequencer(),
	}, ctx)

	obsOpt := &obs.ObsOptions{
		Enabled: true,
	}
	loader.LoadRemote(dir, "cpu", obsOpt)
}

func TestLoadDirs(t *testing.T) {
	dir := t.TempDir()
	defer fileops.RemoveAll(dir)

	lock := ""
	ctx := &fileLoadContext{}
	loader := newFileLoader(&MmsTables{
		lock:      &lock,
		closed:    make(chan struct{}),
		sequencer: NewSequencer(),
	}, ctx)

	err := os.MkdirAll(path.Join(dir, "mst"), 0700)
	err = os.WriteFile(path.Join(dir, "mst", "load_dirs.obs"), []byte{1}, 0600)
	if err != nil {
		t.Fatal(err)
	}
	nameDirs, _ := fileops.ReadDir(path.Join(dir, "mst"))
	loader.loadDirs(nameDirs, "mst", "")

	err = os.MkdirAll(path.Join(dir, "cpu"), 0700)
	err = os.WriteFile(path.Join(dir, "cpu", "load_dirs.xxx"), []byte{1}, 0600)
	if err != nil {
		t.Fatal(err)
	}
	nameDirs, _ = fileops.ReadDir(path.Join(dir, "cpu"))
	loader.loadDirs(nameDirs, "cpu", "")
}

func TestMmsLoader_verifySeq(t *testing.T) {
	ctx := &fileLoadContext{}
	dir := t.TempDir()

	buildTSSPFile(dir, 1, "mst0")
	buildTSSPFile(dir, 10, "mst1")
	buildTSSPFile(dir, 3, "mst2")

	loader := newFileLoader(buildMmsTables(dir), ctx)
	defer loader.mst.Close()

	var load = func() {
		for _, mst := range []string{"mst0", "mst1", "mst2"} {
			loader.Load(filepath.Join(dir, mst), mst, true)
			loader.Wait()
			loader.mst.fileSeq = ctx.getMaxSeq()
		}
	}

	load()
	_, err := ctx.getError()
	require.NoError(t, err)

	buildTSSPFile(dir, 4, "mst2")
	load()
	_, err = ctx.getError()
	require.NoError(t, err)

	files, ok := loader.mst.getTSSPFiles("mst2", true)
	require.True(t, ok)
	require.Equal(t, 2, files.Len())
}

func TestMmsLoader_addTSSPFile_WithErr(t *testing.T) {
	ctx := &fileLoadContext{}
	dir := t.TempDir()

	loader := newFileLoader(buildMmsTables(dir), ctx)
	defer loader.mst.Close()

	loader.addTSSPFile(filepath.Join(dir, "mst0", "xxx.tssp"), "mst0", true, &MocTsspFile{err: errFileClosed})
	loader.Wait()
	_, err := ctx.getError()
	require.Error(t, err)
}

func TestMmsLoader_addTSSPFile_WithoutErr(t *testing.T) {
	ctx := &fileLoadContext{}
	dir := t.TempDir()

	loader := newFileLoader(buildMmsTables(dir), ctx)
	defer loader.mst.Close()

	loader.addTSSPFile(filepath.Join(dir, "mst0", "xxx.tssp"), "mst0", true, &MocTsspFile{})
	loader.Wait()
	_, err := ctx.getError()
	require.NoError(t, err)
}

func buildMmsTables(dir string) *MmsTables {
	conf := NewTsStoreConfig()
	tier := uint64(util.Hot)
	lockPath := ""
	store := NewTableStore(dir, &lockPath, &tier, true, conf)
	store.SetImmTableType(config.TSSTORE)
	return store
}

func buildTSSPFile(dir string, seq uint64, mst string) {
	lockPath := ""
	conf := NewTsStoreConfig()
	tm := time.Now()
	var idMinMax MinMax
	var startValue = 1.1
	idCount := 20

	write := func(ids []uint64, data map[uint64]*record.Record, msb *MsBuilder) {
		for _, id := range ids {
			rec := data[id]
			err := msb.WriteData(id, rec)
			if err != nil {
				panic(err)
			}
		}
	}

	store := buildMmsTables(dir)
	ids, data := genTestData(idMinMax.min, idCount, 1, &startValue, &tm)
	fileName := NewTSSPFileName(seq, 0, 0, 0, true, &lockPath)
	msb := NewMsBuilder(store.path, mst, &lockPath, conf, 10, fileName, store.Tier(), nil, 2, config.TSSTORE, nil, 0)
	write(ids, data, msb)
	store.AddTable(msb, true, false)
	store.Close()
}

func readDirForReload(ctx *fileLoadContext, dir, mst string, isOrder bool) {
	nameDirs, err := fileops.ReadDir(dir)
	if err != nil {
		return
	}
	for i := range nameDirs {
		item := nameDirs[i]
		ctx.appendReloadFile(mst, &tsspInfo{file: filepath.Join(dir, item.Name()), order: isOrder})
	}
}

func TestMmsReLoadFiles(t *testing.T) {
	ctx := NewFileLoadContext()
	dir := t.TempDir()
	defer fileops.RemoveAll(dir)

	buildTSSPFile(dir, 0, "mst0")
	buildTSSPFile(dir, 1, "mst0")
	buildTSSPFile(dir, 2, "mst0")
	buildTSSPFile(dir, 0, "mst1")
	buildTSSPFile(dir, 1, "mst1")
	buildTSSPFile(dir, 2, "mst1")

	for _, mst := range []string{"mst0", "mst1"} {
		readDirForReload(ctx, filepath.Join(dir, mst), mst, true)
	}

	loader := newFileLoader(buildMmsTables(dir), ctx)
	defer loader.mst.Close()

	loader.ReloadFiles(5)
	_, err := ctx.getError()
	require.NoError(t, err)

	if len(ctx.reloadFiles) != 0 {
		t.Fatalf("there are still %d files that have not been loaded", len(ctx.reloadFiles))
	}
}

func readDirForReloadSpecifiedFiles(tsspFiles *TSSPFiles, dir, mst string, isOrder bool) {
	nameDirs, err := fileops.ReadDir(dir)
	if err != nil {
		return
	}
	for i := range nameDirs {
		item := nameDirs[i]
		tsspFiles.AppendReloadFiles(&tsspInfo{file: filepath.Join(dir, item.Name()), order: isOrder})
	}
}

func TestMmsReloadSpecifiedFiles(t *testing.T) {
	ctx := NewFileLoadContext()
	dir := t.TempDir()
	defer fileops.RemoveAll(dir)

	mst := "mst0"
	buildTSSPFile(dir, 0, mst)
	buildTSSPFile(dir, 1, mst)
	buildTSSPFile(dir, 2, mst)

	tsspFiles := NewTSSPFiles()
	readDirForReloadSpecifiedFiles(tsspFiles, filepath.Join(dir, mst), mst, true)

	ReloadSpecifiedFiles(buildMmsTables(dir), mst, tsspFiles)
	_, err := ctx.getError()
	require.NoError(t, err)
	if len(ctx.reloadFiles) != 0 {
		t.Fatalf("there are still %d files that have not been loaded", len(ctx.reloadFiles))
	}
}
