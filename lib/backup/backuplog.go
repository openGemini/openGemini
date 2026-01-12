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

package backup

type BackupLogInfo struct {
	FullBackupTime int64                 `json:"fullBackupTime"`
	IncBackupTime  int64                 `json:"incBackupTime"`
	FileListMap    map[string][][]string `json:"orderFileListMap"`
}

type IncBackupLogInfo struct {
	AddFileListMap map[string][][]string `json:"addOrderFileListMap"`
	DelFileListMap map[string][][]string `json:"delOrderFileListMap"`
}

type BackupResult struct {
	Result    string              `json:"result"`
	Databases map[string]struct{} `json:"databases"`
	Time      int64               `json:"time"`
	DataDir   string              `json:"dataDir"`
	MetaDir   string              `json:"metaDir"`
	WalDir    string              `json:"walDir"`
}

type NodeInfoMap struct {
	SrcNode uint64                           `json:"srcNode"`
	DstNode uint64                           `json:"dstNode"`
	DbMap   map[string]map[string]*RpInfoMap `json:"dbMap"`
	PtMap   map[string]map[string]string     `json:"ptMap"`
}

type RpInfoMap struct {
	ShardMap map[string]string `json:"shardMap"`
	IndexMap map[string]string `json:"indexMap"`
}
