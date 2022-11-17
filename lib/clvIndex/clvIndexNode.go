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
package clvIndex

import (
	"sync"
	"time"

	"github.com/openGemini/openGemini/lib/utils"
	"github.com/openGemini/openGemini/lib/vGram/gramIndex"
	"github.com/openGemini/openGemini/lib/vToken/tokenIndex"
)

/*
	SHARDBUFFER is the number of data of a SHARD, LogIndex is a counter, and BuffLogStrings is used to store all the data of a SHARD, which is used to build indexes in batches.
*/
const SHARDBUFFER = 500000
const UPDATE_INTERVAL time.Duration = 30

type semaphore int

const (
	update semaphore = iota
	close
)

type CLVIndexNode struct {
	dicType CLVDicType
	dic     *CLVDictionary

	indexType       CLVIndexType
	VgramIndexRoot  *gramIndex.IndexTree
	LogTreeRoot     *gramIndex.LogTree
	VtokenIndexRoot *tokenIndex.IndexTree

	dataSignal chan semaphore
	dataLock   sync.Mutex
	dataLen    int
	dataBuf    []utils.LogSeries
}

func NewCLVIndexNode(indexType CLVIndexType, dic *CLVDictionary) *CLVIndexNode {
	clvIndex := &CLVIndexNode{
		dicType:         CLVC,
		dic:             dic,
		indexType:       indexType,
		VgramIndexRoot:  gramIndex.NewIndexTree(QMINGRAM, QMAXGRAM),
		LogTreeRoot:     gramIndex.NewLogTree(QMAXGRAM),
		VtokenIndexRoot: tokenIndex.NewIndexTree(QMINTOKEN, QMAXTOKEN),
		dataSignal:      make(chan semaphore),
		dataBuf:         make([]utils.LogSeries, 0, SHARDBUFFER),
	}
	clvIndex.Open()
	return clvIndex
}

var LogIndex = 0

func (clvIndex *CLVIndexNode) Open() {
	go clvIndex.updateClvIndexRoutine()
}

func (clvIndex *CLVIndexNode) Close() {
	clvIndex.dataSignal <- close
}

func (clvIndex *CLVIndexNode) updateClvIndexRoutine() {
	timer := time.NewTimer(UPDATE_INTERVAL * time.Second)
	defer timer.Stop()
	for {
		select {
		case <-timer.C: //update the index tree periodically
			clvIndex.updateClvIndex() //VGRAM  todo
		case signal, ok := <-clvIndex.dataSignal:
			if !ok {
				return
			}
			if signal == close {
				return
			}
			clvIndex.updateClvIndex() //VGRAM  todo
		}
	}
}

func (clvIndex *CLVIndexNode) updateClvIndex() {
	var logbuf []utils.LogSeries

	clvIndex.dataLock.Lock()
	if clvIndex.dataLen == 0 {
		clvIndex.dataLock.Unlock()
		return
	}
	logbuf = clvIndex.dataBuf
	clvIndex.dataBuf = make([]utils.LogSeries, 0, SHARDBUFFER)
	clvIndex.dataLen = 0
	clvIndex.dataLock.Unlock()

	if clvIndex.indexType == VGRAM {
		clvIndex.CreateCLVVGramIndexIfNotExists(logbuf)
	}
	if clvIndex.indexType == VTOKEN {
		clvIndex.CreateCLVVTokenIndexIfNotExists(logbuf)
	}
	LogIndex = 0
}

func (clvIndex *CLVIndexNode) CreateCLVIndexIfNotExists(log string, tsid uint64, timeStamp int64) {
	if LogIndex < SHARDBUFFER {
		clvIndex.dataBuf = append(clvIndex.dataBuf, utils.LogSeries{Log: log, Tsid: tsid, TimeStamp: timeStamp})
		LogIndex += 1
	}
	if LogIndex >= SHARDBUFFER {
		clvIndex.dataSignal <- update
	}
}

//addIndex

func (clvIndexNode *CLVIndexNode) CreateCLVVGramIndexIfNotExists(buffLogStrings []utils.LogSeries) {
	if clvIndexNode.dicType == CLVC {
		clvIndexNode.VgramIndexRoot, _, clvIndexNode.LogTreeRoot = gramIndex.AddIndex(buffLogStrings, QMINGRAM, QMAXGRAM, LOGTREEMAX, clvIndexNode.dic.VgramDicRoot.Root(), clvIndexNode.LogTreeRoot, clvIndexNode.VgramIndexRoot)
	}
	if clvIndexNode.dicType == CLVL {
		clvIndexNode.VgramIndexRoot, _, clvIndexNode.LogTreeRoot = gramIndex.AddIndex(buffLogStrings, QMINGRAM, clvIndexNode.dic.VgramDicRoot.Qmax(), LOGTREEMAX, clvIndexNode.dic.VgramDicRoot.Root(), clvIndexNode.LogTreeRoot, clvIndexNode.VgramIndexRoot)
	}
}

func (clvIndexNode *CLVIndexNode) CreateCLVVTokenIndexIfNotExists(buffLogStrings []utils.LogSeries) {
	if clvIndexNode.dicType == CLVC {
		clvIndexNode.VtokenIndexRoot, _ = tokenIndex.AddIndex(buffLogStrings, QMINTOKEN, QMAXTOKEN, clvIndexNode.dic.VtokenDicRoot.Root(), clvIndexNode.VtokenIndexRoot)
	}
	if clvIndexNode.dicType == CLVL {
		clvIndexNode.VtokenIndexRoot, _ = tokenIndex.AddIndex(buffLogStrings, QMINTOKEN, clvIndexNode.dic.VtokenDicRoot.Qmax(), clvIndexNode.dic.VtokenDicRoot.Root(), clvIndexNode.VtokenIndexRoot)
	}
}
