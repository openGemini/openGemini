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
	"testing"

	"github.com/openGemini/openGemini/engine/index/sparseindex"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/obs"
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
