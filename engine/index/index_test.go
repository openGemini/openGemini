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
	"os"
	"path"
	"testing"

	"github.com/openGemini/openGemini/engine/index"
	"github.com/openGemini/openGemini/lib/fileops"
	indextype "github.com/openGemini/openGemini/lib/index"
	"github.com/openGemini/openGemini/lib/tokenizer"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/stretchr/testify/require"
)

func TestMinMaxIndexWriter(t *testing.T) {
	testCompDir := t.TempDir()
	_ = fileops.RemoveAll(testCompDir)

	msName := "cpu"

	ir := &influxql.IndexRelation{
		IndexNames:   []string{indextype.MinMaxIndex},
		Oids:         []uint32{uint32(indextype.MinMax)},
		IndexList:    []*influxql.IndexList{&influxql.IndexList{IList: []string{"field1"}}},
		IndexOptions: []*influxql.IndexOptions{},
	}

	err := os.MkdirAll(path.Join(testCompDir, msName), 0700)
	require.NoError(t, err)

	minMaxWriter := index.NewIndexWriter(testCompDir, msName, "", "", *ir, 0, tokenizer.CONTENT_SPLITTER)
	minMaxWriter.Open()

	err = minMaxWriter.CreateAttachIndex(nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	_, _ = minMaxWriter.CreateDetachIndex(nil, nil, nil, nil)

	err = minMaxWriter.Flush()
	require.NoError(t, err)

	err = minMaxWriter.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestSetIndexWriter(t *testing.T) {
	testCompDir := t.TempDir()
	_ = fileops.RemoveAll(testCompDir)

	msName := "cpu"

	ir := &influxql.IndexRelation{
		IndexNames:   []string{indextype.SetIndex},
		Oids:         []uint32{uint32(indextype.Set)},
		IndexList:    []*influxql.IndexList{&influxql.IndexList{IList: []string{"field1"}}},
		IndexOptions: []*influxql.IndexOptions{},
	}

	setWriter := index.NewIndexWriter(testCompDir, msName, "", "", *ir, 0, tokenizer.CONTENT_SPLITTER)
	setWriter.Open()

	err := setWriter.CreateAttachIndex(nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	_, _ = setWriter.CreateDetachIndex(nil, nil, nil, nil)

	err = setWriter.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestBloomFilterUniversalIndexWriter(t *testing.T) {
	testCompDir := t.TempDir()
	_ = fileops.RemoveAll(testCompDir)

	msName := "cpu"

	ir := &influxql.IndexRelation{
		IndexNames:   []string{indextype.BloomFilterUniversalIndex},
		Oids:         []uint32{uint32(indextype.BloomFilterUniversal)},
		IndexList:    []*influxql.IndexList{&influxql.IndexList{IList: []string{"field1"}}},
		IndexOptions: []*influxql.IndexOptions{},
	}

	bloomFilterUniversalWriter := index.NewIndexWriter(testCompDir, msName, "", "", *ir, 0, tokenizer.CONTENT_SPLITTER)
	if bloomFilterUniversalWriter == nil {
		t.Fatal("Expected BloomFilterUniversal writer, got nil")
	}

	bloomFilterUniversalWriter.Open()

	err := bloomFilterUniversalWriter.CreateAttachIndex(nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	_, _ = bloomFilterUniversalWriter.CreateDetachIndex(nil, nil, nil, nil)

	err = bloomFilterUniversalWriter.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestBloomFilterUniversalIndexWriterWithParams(t *testing.T) {
	testCompDir := t.TempDir()
	_ = fileops.RemoveAll(testCompDir)

	msName := "cpu"

	ir := &influxql.IndexRelation{
		IndexNames:   []string{indextype.BloomFilterUniversalIndex},
		Oids:         []uint32{uint32(indextype.BloomFilterUniversal)},
		IndexList:    []*influxql.IndexList{&influxql.IndexList{IList: []string{"field1"}}},
		IndexParam:   []*influxql.IndexParam{&influxql.IndexParam{IList: []influxql.Expr{&influxql.NumberLiteral{Val: 0.0005}}}},
		IndexOptions: []*influxql.IndexOptions{},
	}

	bloomFilterUniversalWriter := index.NewIndexWriter(testCompDir, msName, "", "", *ir, 0, tokenizer.CONTENT_SPLITTER)
	if bloomFilterUniversalWriter == nil {
		t.Fatal("Expected BloomFilterUniversal writer, got nil")
	}

	bloomFilterUniversalWriter.Open()

	err := bloomFilterUniversalWriter.CreateAttachIndex(nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	_, _ = bloomFilterUniversalWriter.CreateDetachIndex(nil, nil, nil, nil)

	err = bloomFilterUniversalWriter.Close()
	if err != nil {
		t.Fatal(err)
	}
}
