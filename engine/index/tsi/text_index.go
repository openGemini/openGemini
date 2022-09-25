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

package tsi

import (
	"fmt"

	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
)

type TextIndex struct {
}

func NewTextIndex(opts *Options) (*TextIndex, error) {
	textIndex := &TextIndex{}

	if err := textIndex.Open(); err != nil {
		return nil, err
	}

	return textIndex, nil
}

func (idx *TextIndex) Open() error {
	fmt.Println("TextIndex Open")
	// TODO
	return nil
}

func (idx *TextIndex) Close() error {
	fmt.Println("TextIndex Close")
	// TODO
	return nil
}

func (idx *TextIndex) CreateIndexIfNotExists(primaryIndex PrimaryIndex, row *influx.Row, version uint16) (uint64, error) {
	//fmt.Println("TextIndex CreateIndexIfNotExists")
	// TODO
	return 0, nil
}

func (idx *TextIndex) Search(primaryIndex PrimaryIndex, span *tracing.Span, name []byte, opt *query.ProcessorOptions) ([][]byte, error) {
	sids, _ := primaryIndex.GetPrimaryKeys(name, opt)
	fmt.Println("TextIndex Search", len(sids))
	// TODO
	return nil, nil
}

func (idx *TextIndex) Delete(primaryIndex PrimaryIndex, name []byte, condition influxql.Expr, tr TimeRange) error {
	sids, _ := primaryIndex.GetDeletePrimaryKeys(name, condition, tr)
	fmt.Println("TextIndex Delete", len(sids))
	// TODO
	return nil
}

func TextIndexHandler(opt *Options, primaryIndex PrimaryIndex) *IndexAmRoutine {
	index, _ := NewTextIndex(opt)
	return &IndexAmRoutine{
		amKeyType:    Text,
		amOpen:       TextOpen,
		amBuild:      TextBuild,
		amInsert:     TextInsert,
		amDelete:     TextDelete,
		amScan:       TextScan,
		amClose:      TextClose,
		index:        index,
		primaryIndex: primaryIndex,
	}
}

func TextBuild(relation *IndexRelation) error {
	return nil
}

func TextOpen(index interface{}) error {
	textIndex := index.(*TextIndex)
	return textIndex.Open()
}

func TextInsert(index interface{}, primaryIndex PrimaryIndex, name []byte, row interface{}, version uint16) (uint64, error) {
	textIndex := index.(*TextIndex)
	insertRow := row.(*influx.Row)
	return textIndex.CreateIndexIfNotExists(primaryIndex, insertRow, version)
}

// upper function call should analyze result
func TextScan(index interface{}, primaryIndex PrimaryIndex, span *tracing.Span, name []byte, opt *query.ProcessorOptions) (interface{}, error) {
	textIndex := index.(*TextIndex)
	return textIndex.Search(primaryIndex, span, name, opt)
}

func TextDelete(index interface{}, primaryIndex PrimaryIndex, name []byte, condition influxql.Expr, tr TimeRange) error {
	textIndex := index.(*TextIndex)
	return textIndex.Delete(primaryIndex, name, condition, tr)
}

func TextClose(index interface{}) error {
	textIndex := index.(*TextIndex)
	return textIndex.Close()
}
