//go:build linux && amd64
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

#ifndef MEMPOOL_HEADER_H
#define MEMPOOL_HEADER_H

#include <mutex>
#include <stack>
#include <cstring>
#include "invert.h"

#define RES_LENS_LEN 9
#define MAX_HASHCURVE_SIZE 100
#define HASH_FACTOR_SHIFT 13 // 2^13 = 8192

struct PoolStatistic {
    uint32_t eleCnt;
    uint32_t eleUsedCnt;
    uint32_t containerCnt;
    uint32_t containerUsedCnt;
    uint32_t pairGroupCnt;
    uint32_t pairGroupUsedCnt;
    uint32_t runContainerCnt;
    uint32_t bitsetContainerCnt;
    uint32_t arrayContainerCnt;
    uint32_t hashBktSize;

    void Reset() {
        eleCnt = 0;
        eleUsedCnt = 0;
        containerCnt = 0;
        containerUsedCnt = 0;
        pairGroupCnt = 0;
        pairGroupUsedCnt = 0;
        runContainerCnt = 0;
        bitsetContainerCnt = 0;
        arrayContainerCnt = 0;
        hashBktSize = 0;
    }

    void ResetContainerCnt() {
        runContainerCnt = 0;
        bitsetContainerCnt = 0;
        arrayContainerCnt = 0;
    }

    void SetHashBktSize(uint32_t iHashBktSize) {
        hashBktSize = iHashBktSize;
    }

    void PrintObjCnt() {
        LOG_INFO("object statistics, this=[%p], element=[%u,%u], container=[%u,%u], pair=[%u,%u], containerType=[%u,%u,%u]",
            this, eleCnt, eleUsedCnt, containerCnt, containerUsedCnt, pairGroupCnt, pairGroupUsedCnt,
            runContainerCnt, bitsetContainerCnt, arrayContainerCnt);
    }

    void PrintUsedMem() {
        uint64_t totalBytes = uint64_t(eleCnt) * sizeof(InvertElement) + uint64_t(containerCnt) * sizeof(RunContainer) +
            uint64_t(pairGroupCnt) * sizeof(RunPairGroup) + HASHTABLE_BKTS_SIZE*sizeof(ListNode);
        LOG_INFO("memory consumption statistics, this=[%p], ObjSize=[%ld, %ld, %ld], TotalMem=[%lu]",
            this, sizeof(InvertElement), sizeof(RunContainer), sizeof(RunPairGroup), totalBytes);
    }
};

struct HashFactor {
    uint32_t rowCnt;
    uint32_t tokenCnt;
};

class MemPool {
public:
    ~MemPool()
    {
    }
    explicit MemPool()
    {
        invert = nullptr;
        vtokenInit = false;
        stat.Reset();
        ListInit(&elementPool);
        ListInit(&containerPool);
        ListInit(&pairGroupPool);
        std::memset(hashFactors, 0, MAX_HASHCURVE_SIZE * sizeof(HashFactor));
        keysOffs.Reset();
        dataOffs.Reset();
        invertGroup.Reset();
    }

public:
    /* InvertElement pool */
    InvertElement *GetInvertElement();
    void PutInvertElement(InvertElement *e);
    /* RunContainer pool */
    RunContainer *GetRunContainer();
    void PutRunContainer(RunContainer *c);
    /* RunPairGroup pool */
    RunPairGroup *GetRunPairGroup();
    void PutRunPairGroup(RunPairGroup *p);

    /* Invert root pool */
    Invert *GetInvert();
    /* vtoken pool */
    VToken *GetVToken();
    /* sort element pool */
    InvertGroup *GetInvertGroup();
    /* hash section */
    ListNode *GetHashSections();

public:
    bool AddPostingData(char *key, uint32_t keyLen, uint32_t rowId);
    bool Next(char *keys, uint32_t keysLen, char *data, uint32_t dataLen, uint32_t res[RES_LENS_LEN]);
    uint32_t SerializedContainer(char *data, RunContainer *c);
    void SerializedCookieHeader(char *data, uint16_t containerCnt, uint32_t cardinalityCnt);

private:
    HashFactor hashFactors[MAX_HASHCURVE_SIZE];

public:
    /* statistic */
    PoolStatistic stat;
    /* memory pool */
    ListNode elementPool; // InvertElement pool
    ListNode containerPool; // RunContainer pool
    ListNode pairGroupPool; // RunPairGroup pool
    ListNode hashSection[HASHTABLE_SECTION_SIZE]; // HashSections

    Invert *invert; // the root of invert node
    VToken vtoken[TOKENIZE_STEP];  // vtoken memory pool
    bool vtokenInit;

    /* for output data */
    InvertGroup invertGroup;

    uint32_t invertIter;
    OffsBuf keysOffs;
    OffsBuf dataOffs;
};

class MemPools {
public:
    ~MemPools()
    {
    }
    explicit MemPools()
    {
    }
public:
    MemPool *GetMemPool();
    void PutMemPool(MemPool *pool);

public:
    std::stack<MemPool *> poolStack;
    std::mutex lock;
};

MemPool *GetMemPool();
void PutMemPool(MemPool *pool);

#endif
