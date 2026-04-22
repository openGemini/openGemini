// Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.
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

package index_test

import (
	"testing"

	"github.com/openGemini/openGemini/engine/index"
	"github.com/openGemini/openGemini/lib/fileops"
	indextype "github.com/openGemini/openGemini/lib/index"
	"github.com/openGemini/openGemini/lib/tokenizer"
)

func TestMinMaxIndexWriter(t *testing.T) {
	testCompDir := t.TempDir()
	_ = fileops.RemoveAll(testCompDir)

	msName := "cpu"
	minMaxWriter := index.NewIndexWriter(testCompDir, msName, "", "", indextype.MinMax, tokenizer.CONTENT_SPLITTER)
	err := minMaxWriter.Open()
	if err != nil {
		t.Fatal(err)
	}

	err = minMaxWriter.CreateAttachIndex(nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	_, _ = minMaxWriter.CreateDetachIndex(nil, nil, nil, nil)

	err = minMaxWriter.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestSetIndexWriter(t *testing.T) {
	testCompDir := t.TempDir()
	_ = fileops.RemoveAll(testCompDir)

	msName := "cpu"
	setWriter := index.NewIndexWriter(testCompDir, msName, "", "", indextype.Set, tokenizer.CONTENT_SPLITTER)
	err := setWriter.Open()
	if err != nil {
		t.Fatal(err)
	}

	err = setWriter.CreateAttachIndex(nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	_, _ = setWriter.CreateDetachIndex(nil, nil, nil, nil)

	err = setWriter.Close()
	if err != nil {
		t.Fatal(err)
	}
}
