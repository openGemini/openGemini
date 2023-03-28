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

package sherlock

import (
	"bytes"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_createAndGetFileInfo(t *testing.T) {
	tmpDir := t.TempDir()
	_ = os.RemoveAll(tmpDir)
	fi, filename, err := createAndGetFileInfo(tmpDir, CPU)
	require.NoError(t, err)
	defer fi.Close()
	require.Equal(t, true, strings.Contains(filename, tmpDir))
	file := path.Base(filename)
	require.Equal(t, true, strings.Contains(file, "cpu"))
	require.Equal(t, true, strings.HasSuffix(file, "pb.gz"))
}

func Test_writeFile(t *testing.T) {
	tmpDir := t.TempDir()

	var buf bytes.Buffer
	buf.WriteString("hello world")
	filename, err := writeFile(buf, Memory, &dumpOptions{
		dumpPath: tmpDir,
	})
	require.NoError(t, err)
	file := path.Base(filename)
	require.Equal(t, true, strings.Contains(file, "mem"))
	require.Equal(t, true, strings.HasSuffix(file, "pb.gz"))
}
