// Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.
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

package analyzer

import (
	"fmt"
	"log"
	"os"

	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

func init() {
	immutable.SetCompactLimit(800*1024*1024, 800*1024*1024)
	immutable.SetSnapshotLimit(800*1024*1024, 800*1024*1024)
	fileops.SetBackgroundReadLimiter(800 * 1024 * 1024)
}

type Analyzer struct {
	result *AnalyzeResult
	itr    ColumnIterator
}

func NewAnalyzer() *Analyzer {
	return &Analyzer{
		result: NewAnalyzeResult(),
	}
}

func (a *Analyzer) Analyze(file string) {
	log.Println("begin analyze: ", file)
	stat, statErr := os.Stat(file)
	if statErr != nil {
		log.Println(statErr)
		return
	}
	a.result.fileSize = int(stat.Size())

	lockPath := ""
	f, err := immutable.OpenTSSPFile(file, &lockPath, true, false)
	if err != nil {
		panic(err)
	}
	defer util.MustClose(f)

	a.AnalyzeTSSPFile(f)
	a.Print(f)
}

func (a *Analyzer) AnalyzeTSSPFile(file immutable.TSSPFile) {
	fi := immutable.NewFileIterator(file, logger.NewLogger(errno.ModuleStorageEngine))

	itr := immutable.NewColumnIterator(fi)
	a.itr.result = a.result
	a.itr.fi = fi
	if err := itr.Run(&a.itr); err != nil {
		panic(err)
	}
}

func (a *Analyzer) Print(file immutable.TSSPFile) {
	ret := a.result

	trailer := file.FileStat()

	var originSize, compressSize = 0, 0
	for _, size := range ret.originSize {
		originSize += size
	}
	for _, size := range ret.compressSize {
		compressSize += size
	}

	if originSize == 0 {
		log.Println("!!! originDataSize is zero")
		return
	}

	printLine("FileSize", formatDataSize(ret.fileSize))
	printLine("SeriesTotal", ret.seriesTotal)
	printLine("SegmentTotal", ret.segmentTotal)
	printLine("RowTotal", ret.rowTotal)
	printLine("ColumnTotal", ret.columnTotal)
	printSizeRatio("MetaIndexSize", int(trailer.MetaIndexSize()), ret.fileSize)
	printSizeRatio("ChunkMetaSize", int(trailer.IndexSize()), ret.fileSize)
	printSizeRatio("ChunkDataSize", int(trailer.DataSize()), ret.fileSize)
	printSizeRatio("BitmapSize", ret.bitmapSize, ret.fileSize)
	printSizeRatio("PreAggSize", ret.preAggSize, ret.fileSize)
	printLine("StringOffsetSize", ret.offsetSize)
	printRatio("OneRowChunk", ret.oneRowChunk, ret.seriesTotal)
	printRatio("AllNilSegment", ret.allNilSegment, ret.segmentTotal)
	printRatio("NoNilSegment", ret.noNilSegment, ret.segmentTotal)

	fmt.Println("")
	fmt.Println("------------- Raw Data Distribution ---------------")
	printLine("Total", originSize)
	for typ, size := range ret.originSize {
		printSizeRatio("  ["+influx.FieldTypeName[typ]+"]", size, originSize)
	}

	fmt.Println("")
	fmt.Println("------------- Percentage of each type of data in a file ---------------")
	printSizeRatio("Total", compressSize, ret.fileSize)
	for typ, size := range ret.compressSize {
		printSizeRatio("  ["+influx.FieldTypeName[typ]+"]", size, ret.fileSize)
	}

	fmt.Println("")
	fmt.Println("------------- Compression rate of each type of data ---------------")
	printSizeRatio("Total", compressSize, originSize)

	for typ, size := range ret.compressSize {
		printSizeRatio("  ["+influx.FieldTypeName[typ]+"]", size, ret.originSize[typ])
	}

	fmt.Println("")
	fmt.Println("==== Compression Algorithm Distribution ====")
	printCompressAlgo("Time", ret.timeCompressAlgo)
	printCompressAlgo("Integer", ret.intCompressAlgo)
	printCompressAlgo("Float", ret.floatCompressAlgo)

	fmt.Println("")
}
