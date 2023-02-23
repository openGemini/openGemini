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
