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

package pusher

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/stretchr/testify/assert"
)

func TestFileConfig(t *testing.T) {
	file := "/data/logs/stat.log"
	conf := &FileConfig{App: "sql", Path: file}
	conf.Parser()

	path := filepath.Clean(conf.GetSavePath())

	assert.Regexp(t, regexp.MustCompile("^[/\\\\]data[/\\\\]logs[/\\\\]sql-stat-\\d+.log$"), path)
	assert.Equal(t, "stat", conf.GetName())
	assert.Equal(t, filepath.Dir(file), conf.GetDir())
}

func TestFilePusher(t *testing.T) {
	tmpdir := t.TempDir()
	conf := &FileConfig{Path: filepath.Join(tmpdir, "stat_metric.data")}
	conf.Parser()
	path := conf.GetSavePath()
	_ = os.RemoveAll(path)
	p := NewFile(conf, true, logger.NewLogger(errno.ModuleUnknown))
	defer p.Stop()

	lines := []string{
		`mst,tid=t001,cid=c001 value=1.1  

`,
		`mst,tid=t002,cid=c002 value=1.2`,
		`  mst,tid=t003,cid=c003 value=1.3`,
	}

	for _, line := range lines {
		if !assert.NoError(t, p.Push([]byte(line))) {
			return
		}
	}

	reader := NewSnappyReader()
	if !assert.NoError(t, reader.OpenFile(path)) {
		return
	}
	reader.EnableCompress()
	defer reader.Close()

	for _, exp := range lines {
		exp = strings.TrimSpace(exp)
		line, err := reader.ReadBlock()
		if err == io.EOF {
			return
		}
		if !assert.NoError(t, err) {
			return
		}

		assert.Equal(t, exp, string(line))
	}
}

func TestRemoveOldFiles(t *testing.T) {
	tmpdir := t.TempDir()
	conf := &FileConfig{Path: filepath.Join(tmpdir, "stat_metric.data")}
	conf.Parser()
	p := NewFile(conf, true, logger.NewLogger(errno.ModuleUnknown))
	p.Stop()

	var files []string
	var err error
	_, err = makeFile(tmpdir, nil, 1, 5)
	if !assert.NoError(t, err) {
		return
	}

	time.Sleep(time.Second * 3 / 2)
	files, err = makeFile(tmpdir, files, 10, 5)
	if !assert.NoError(t, err) {
		return
	}
	p.removeExpireFiles(conf.GlobPattern(), time.Second)

	matches, err := filepath.Glob(conf.GlobPattern())
	if !assert.NoError(t, err) {
		return
	}
	sort.Strings(files)
	sort.Strings(matches)

	if !reflect.DeepEqual(files, matches) {
		t.Fatalf("failed to remove old files; \n exp: %+v \n got: %+v\n", files, matches)
	}
}

func makeFile(dir string, files []string, start, num int) ([]string, error) {
	for i := 0; i < num; i++ {
		path := filepath.Join(dir, fmt.Sprintf("stat_metric-2022-03-%02d.data", start+i))
		_ = os.Remove(path)
		files = append(files, filepath.Clean(path))
		fd, err := os.OpenFile(path, os.O_CREATE, 0644)
		if err != nil {
			return nil, err
		}
		_ = fd.Close()
	}
	return files, nil
}
