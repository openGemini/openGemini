/*
Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.

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
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/record"
)

type TSSP2ParquetPlan struct {
	Schema   map[string]uint8
	FilePath string

	enable bool
}

func NewTSSP2ParquetPlan(level uint16) *TSSP2ParquetPlan {
	pl := config.GetStoreConfig().TSSPToParquetLevel
	return &TSSP2ParquetPlan{
		Schema: make(map[string]uint8),
		enable: pl > 0 && pl == level,
	}
}

func (p *TSSP2ParquetPlan) OnWriteRecord(rec *record.Record) {
	for i := range rec.Schema {
		p.Schema[rec.Schema[i].Name] = uint8(rec.Schema[i].Type)
	}
}

func (p *TSSP2ParquetPlan) OnWriteChunkMeta(cm *ChunkMeta) {
	for i := range cm.colMeta {
		p.Schema[cm.colMeta[i].Name()] = cm.colMeta[i].Type()
	}
}

func (p *TSSP2ParquetPlan) EnableSplitFile() bool {
	return !p.enable
}

func (p *TSSP2ParquetPlan) OnNewFile(f TSSPFile) {
	p.FilePath = f.Path()
}

func (p *TSSP2ParquetPlan) Enable() bool {
	return p.enable
}
