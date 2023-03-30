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

package clv

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
)

func TestAnalyzer_Analyze(t *testing.T) {
	type fields struct {
		readTree    *TrieDicNode
		writeTree   *TrieWriteTree
		path        string
		measurement string
		field       string
		version     uint32
		needSample  bool
	}
	type args struct {
		log string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    map[uint16]VToken
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &Analyzer{
				readTree:    tt.fields.readTree,
				writeTree:   tt.fields.writeTree,
				path:        tt.fields.path,
				measurement: tt.fields.measurement,
				field:       tt.fields.field,
				version:     tt.fields.version,
				needSample:  tt.fields.needSample,
			}
			got, err := a.Analyze(tt.args.log)
			if (err != nil) != tt.wantErr {
				t.Errorf("Analyze() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Analyze() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAnalyzer_AssignId(t *testing.T) {
	type fields struct {
		readTree    *TrieDicNode
		writeTree   *TrieWriteTree
		path        string
		measurement string
		field       string
		version     uint32
		needSample  bool
	}
	type args struct {
		id   uint32
		node *TrieDicNode
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   uint32
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &Analyzer{
				readTree:    tt.fields.readTree,
				writeTree:   tt.fields.writeTree,
				path:        tt.fields.path,
				measurement: tt.fields.measurement,
				field:       tt.fields.field,
				version:     tt.fields.version,
				needSample:  tt.fields.needSample,
			}
			if got := a.AssignId(tt.args.id, tt.args.node); got != tt.want {
				t.Errorf("AssignId() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAnalyzer_InsertToReadDic(t *testing.T) {
	type fields struct {
		readTree    *TrieDicNode
		writeTree   *TrieWriteTree
		path        string
		measurement string
		field       string
		version     uint32
		needSample  bool
	}
	type args struct {
		tokens []string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &Analyzer{
				readTree:    tt.fields.readTree,
				writeTree:   tt.fields.writeTree,
				path:        tt.fields.path,
				measurement: tt.fields.measurement,
				field:       tt.fields.field,
				version:     tt.fields.version,
				needSample:  tt.fields.needSample,
			}
			a.insertToReadDic(tt.args.tokens)
		})
	}
}

func TestAnalyzer_InsertToWriteTree(t *testing.T) {
	type fields struct {
		readTree    *TrieDicNode
		writeTree   *TrieWriteTree
		path        string
		measurement string
		field       string
		version     uint32
		needSample  bool
	}
	type args struct {
		tokens []string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &Analyzer{
				readTree:    tt.fields.readTree,
				writeTree:   tt.fields.writeTree,
				path:        tt.fields.path,
				measurement: tt.fields.measurement,
				field:       tt.fields.field,
				version:     tt.fields.version,
				needSample:  tt.fields.needSample,
			}
			a.InsertToWriteTree(tt.args.tokens)
		})
	}
}

func TestAnalyzer_Version(t *testing.T) {
	type fields struct {
		readTree    *TrieDicNode
		writeTree   *TrieWriteTree
		path        string
		measurement string
		field       string
		version     uint32
		needSample  bool
	}
	tests := []struct {
		name   string
		fields fields
		want   uint32
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &Analyzer{
				readTree:    tt.fields.readTree,
				writeTree:   tt.fields.writeTree,
				path:        tt.fields.path,
				measurement: tt.fields.measurement,
				field:       tt.fields.field,
				version:     tt.fields.version,
				needSample:  tt.fields.needSample,
			}
			if got := a.Version(); got != tt.want {
				t.Errorf("Version() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAnalyzer_findLongestTokens(t *testing.T) {
	type fields struct {
		readTree    *TrieDicNode
		writeTree   *TrieWriteTree
		path        string
		measurement string
		field       string
		version     uint32
		needSample  bool
	}
	type args struct {
		tokens []string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   VToken
		want1  int
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &Analyzer{
				readTree:    tt.fields.readTree,
				writeTree:   tt.fields.writeTree,
				path:        tt.fields.path,
				measurement: tt.fields.measurement,
				field:       tt.fields.field,
				version:     tt.fields.version,
				needSample:  tt.fields.needSample,
			}
			got, got1 := a.findLongestTokens(tt.args.tokens)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("findLongestTokens() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("findLongestTokens() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestAnalyzer_insert2ReadTree(t *testing.T) {
	type fields struct {
		readTree    *TrieDicNode
		writeTree   *TrieWriteTree
		path        string
		measurement string
		field       string
		version     uint32
		needSample  bool
	}
	type args struct {
		tokens string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &Analyzer{
				readTree:    tt.fields.readTree,
				writeTree:   tt.fields.writeTree,
				path:        tt.fields.path,
				measurement: tt.fields.measurement,
				field:       tt.fields.field,
				version:     tt.fields.version,
				needSample:  tt.fields.needSample,
			}
			a.insertToReadTree(tt.args.tokens)
		})
	}
}

func getWriteTree() *TrieWriteTree {
	return NewTrieWriteTree()

	//tree.Insert()
}

func TestAnalyzer_save2MergeSet(t *testing.T) {
	type fields struct {
		readTree    *TrieDicNode
		writeTree   *TrieWriteTree
		path        string
		measurement string
		field       string
		version     uint32
		needSample  bool
	}
	type args struct {
		root *TrieDicNode
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &Analyzer{
				readTree:    tt.fields.readTree,
				writeTree:   tt.fields.writeTree,
				path:        tt.fields.path,
				measurement: tt.fields.measurement,
				field:       tt.fields.field,
				version:     tt.fields.version,
				needSample:  tt.fields.needSample,
			}
			if _, err := a.saveToMergeSet(tt.args.root); (err != nil) != tt.wantErr {
				t.Errorf("saveToMergeSet() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestNewTrieDicNode(t *testing.T) {
	tests := []struct {
		name string
		want *TrieDicNode
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewTrieDicNode(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewTrieDicNode() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTokenizer(t *testing.T) {
	type args struct {
		log string
	}
	tests := []struct {
		name string
		args args
		want []string
	}{
		// TODO: Add test cases.
		{
			name: "test english str",
			args: args{log: "GET /french/splash_inet.html HTTP"},
			want: []string{"get", "french", "splash_inet.html", "http"},
		},
		{
			name: "test empty str",
			args: args{log: ""},
			want: []string{},
		},
		{
			name: "test symbol str",
			args: args{log: "a:B:c, &*Da, c**#"},
			want: []string{"a:b:c", "da", "c"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Tokenizer(tt.args.log); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Tokenizer() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTrieDicNode_FindNodeById(t *testing.T) {
	type fields struct {
		child []*TrieDicNode
		token string
		id    uint32
	}
	type args struct {
		id uint32
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *TrieDicNode
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tree := &TrieDicNode{
				child: tt.fields.child,
				token: tt.fields.token,
				id:    tt.fields.id,
			}
			if got := tree.FindNodeById(tt.args.id); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("FindNodeById() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTrieDicNode_FindNodeByToken(t *testing.T) {
	type fields struct {
		child []*TrieDicNode
		token string
		id    uint32
	}
	type args struct {
		token string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *TrieDicNode
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tree := &TrieDicNode{
				child: tt.fields.child,
				token: tt.fields.token,
				id:    tt.fields.id,
			}
			if got := tree.FindNodeByToken(tt.args.token); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("FindNodeByToken() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTrieWriteTree_Insert(t *testing.T) {
	type args struct {
		tokens []string
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
		{
			name: "test_logs",
			args: args{tokens: []string{
				"GET /images/hm_bg.jpg HTTP/1.0",
				"GET /images/hm_arw.gif HTTP/1.0",
				"GET /images/nav_bg_top.gif HTTP/1.0",
				"GET /images/hm_bg.jpg HTTP/1.0",
				"GET /images/bord_d.gif HTTP/1.0",
				"GET /images/arw_lk.gif HTTP/1.0",
				"GET /french/index.html HTTP/1.0",
				"GET /images/teams_hm_bg.jpg HTTP/1.0"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tree := NewTrieWriteTree()
			for _, token := range tt.args.tokens {
				tokens := Tokenizer(token)
				tree.Insert(tokens)
			}

			if int(tree.sampleNum) != len(tt.args.tokens) {
				t.Errorf("insert failed = %v, want %v", tree.sampleNum, len(tt.args.tokens))
			}
		})

	}
}

func TestTrieWriteTree_Prune(t *testing.T) {
	type fields struct {
		root      TrieWriteNode
		lock      sync.RWMutex
		sampleNum uint32
	}
	type args struct {
		th int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tree := &TrieWriteTree{
				root:      tt.fields.root,
				lock:      tt.fields.lock,
				sampleNum: tt.fields.sampleNum,
			}
			tree.Prune(tt.args.th)
		})
	}
}

func TestTrieWriteTree_Transfer(t *testing.T) {
	type fields struct {
		root      TrieWriteNode
		lock      sync.RWMutex
		sampleNum uint32
	}
	tests := []struct {
		name   string
		fields fields
		want   *TrieDicNode
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tree := &TrieWriteTree{
				root:      tt.fields.root,
				lock:      tt.fields.lock,
				sampleNum: tt.fields.sampleNum,
			}
			if got := tree.Transfer(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Transfer() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTrieWriteTree_insertNode(t *testing.T) {
	type fields struct {
		root      TrieWriteNode
		lock      sync.RWMutex
		sampleNum uint32
	}
	type args struct {
		tokens []string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tree := &TrieWriteTree{
				root:      tt.fields.root,
				lock:      tt.fields.lock,
				sampleNum: tt.fields.sampleNum,
			}
			tree.insertNode(tt.args.tokens)
		})
	}
}

func TestTrieWriteTree_pruneNode(t *testing.T) {
	type fields struct {
		root      TrieWriteNode
		lock      sync.RWMutex
		sampleNum uint32
	}
	type args struct {
		node *TrieWriteNode
		th   int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tree := &TrieWriteTree{
				root:      tt.fields.root,
				lock:      tt.fields.lock,
				sampleNum: tt.fields.sampleNum,
			}
			tree.pruneNode(tt.args.node, tt.args.th)
		})
	}
}

func TestTrieWriteTree_transferNode(t *testing.T) {
	type fields struct {
		root      TrieWriteNode
		lock      sync.RWMutex
		sampleNum uint32
	}
	type args struct {
		token string
		node  *TrieWriteNode
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *TrieDicNode
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tree := &TrieWriteTree{
				root:      tt.fields.root,
				lock:      tt.fields.lock,
				sampleNum: tt.fields.sampleNum,
			}
			if got := tree.transferNode(tt.args.token, tt.args.node); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("transferNode() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getAnalyzer(t *testing.T) {
	type args struct {
		path    string
		name    string
		field   string
		version uint32
	}
	tests := []struct {
		name    string
		args    args
		want    *Analyzer
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getAnalyzer(tt.args.path, tt.args.name, tt.args.field, tt.args.version)
			if (err != nil) != tt.wantErr {
				t.Errorf("getAnalyzer() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getAnalyzer() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getAnalyzerVersion(t *testing.T) {
	type args struct {
		path  string
		name  string
		field string
	}
	tests := []struct {
		name    string
		args    args
		want    uint32
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _, err := getAnalyzerVersion(tt.args.path, tt.args.name, tt.args.field)
			if (err != nil) != tt.wantErr {
				t.Errorf("getAnalyzerVersion() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("getAnalyzerVersion() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_loadFromMergeSet(t *testing.T) {
	type args struct {
		path    string
		name    string
		field   string
		version uint32
	}
	tests := []struct {
		name    string
		args    args
		want    *Analyzer
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := loadAnalyzer(tt.args.path, tt.args.name, tt.args.field, tt.args.version)
			if (err != nil) != tt.wantErr {
				t.Errorf("loadFromMergeSet() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("loadFromMergeSet() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_readFromDicTree(t *testing.T) {
	type args struct {
		node  *TrieDicNode
		token []byte
		items *[][]byte
	}

	var items [][]byte
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
		{name: "readdic", args: args{
			node: &TrieDicNode{
				token: "root",
				id:    0,
				child: []*TrieDicNode{
					&TrieDicNode{
						token: "a",
						id:    1,
						child: []*TrieDicNode{
							&TrieDicNode{
								token: "b",
								id:    2,
								child: []*TrieDicNode{
									&TrieDicNode{
										token: "k",
										id:    4,
									},
								},
							},
							&TrieDicNode{
								token: "c",
								id:    3,
								child: []*TrieDicNode{
									&TrieDicNode{
										token: "m",
										id:    5,
									},
								},
							},
						},
					},
					&TrieDicNode{
						token: "d",
						id:    6,
						child: []*TrieDicNode{
							&TrieDicNode{
								token: "e",
								id:    7,
							},
						},
					},
				},
			},
			token: nil,
			items: &items,
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			readFromDicTree(tt.args.node, tt.args.token, tt.args.items)
		})
		for _, item := range *tt.args.items {
			fmt.Println(item)
		}
	}
}
