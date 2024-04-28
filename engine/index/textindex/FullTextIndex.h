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
#ifndef FULL_TEXT_INDEX_H
#define FULL_TEXT_INDEX_H

#include <stdlib.h>
#include <stdint.h>
#include <iostream>
#include "invert.h"
#include "mempool.h"

#define SPLIT_TABLE_SIZE 256
class SimpleGramTokenizer {
public:
    ~SimpleGramTokenizer()
    {
    }

    explicit SimpleGramTokenizer()
    {
    }

public:
    void Init(const char *splitStr, uint32_t len, bool hasChin);
    bool NextBatch(ColVal *colVal, uint32_t startRow, VToken *vtoken, uint32_t batchLen);

private:
    int8_t splitTable[SPLIT_TABLE_SIZE];
    bool hasNonASCII;
};

class FullTextIndex {
public:
    virtual ~FullTextIndex()
    {
    }
    explicit FullTextIndex()
    {
    }

private:
    void InsertPostingList(MemPool *pool, Invert *root, VToken *vtoken, uint32_t batchLen);
    void PutToSortInvert(MemPool *pool, Invert *root);
    uint32_t GetHashTableBkts(uint32_t rowsCnt);
    void UpdateHashSizeCurve(uint32_t rowsCnt, uint32_t tokensCnt);

public:
    int32_t Init(const char *splitStr, uint32_t len, bool hasChin);
    MemPool *AddDocument(char *val, uint32_t valLen, uint32_t *offset, uint32_t offLen, uint32_t startRow, uint32_t endRow);

private:
    SimpleGramTokenizer tokenizer;
    uint32_t nodeCount;
};

#endif
