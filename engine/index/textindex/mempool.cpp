//go:build linux
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

#include "mempool.h"

bool InvertElementHashEqual(const struct ListNode *nodeA, const struct ListNode *nodeB)
{
    InvertElement *eleA = (InvertElement *)NODE_ENTRTY(nodeA, InvertElement, node);
    InvertElement *eleB = (InvertElement *)NODE_ENTRTY(nodeB, InvertElement, node);
    if (eleA->token.len != eleB->token.len ||
        std::memcmp(eleA->token.data, eleB->token.data, eleA->token.len) != 0) {
        return false;
    }
    return true;
}

RunContainer *GetRunContainerFromPool(void *memPool)
{
    MemPool *pool = (MemPool *)memPool;
    return pool->GetRunContainer();
}

void PutRunContainerToPool(void *memPool, RunContainer *c)
{
    MemPool *pool = (MemPool *)memPool;
    pool->PutRunContainer(c);
}

RunPairGroup *GetRunPairGroupFromPool(void *memPool)
{
    MemPool *pool = (MemPool *)memPool;
    return pool->GetRunPairGroup();
}

Invert *NewInvert(uint32_t bktCapSize)
{
    Invert *invert = (Invert *)malloc(sizeof(Invert));
    if (invert == nullptr) {
        return nullptr;
    }
    if (invert->hashTable.Init(bktCapSize, InvertElementHashEqual) == -1) {
        free(invert);
        return nullptr;
    }
    return invert;
}

/* InvertElement */
InvertElement *MemPool::GetInvertElement()
{
    InvertElement *e = nullptr;
    if (ListEmpty(&elementPool)) {
        e = (InvertElement *)malloc(sizeof(InvertElement));
        if (e == nullptr) {
            return nullptr;
        }
        stat.eleCnt++;
    } else {
        ListNode *onode = elementPool.next;
        ListRemove(onode);
        e = NODE_ENTRTY(onode, InvertElement, node);
    }
    stat.eleUsedCnt++;
    ListInit(&e->node);
    ListInit(&e->containers);
    e->curContainer = nullptr;
    e->getRunContainer = GetRunContainerFromPool;
    e->putRunContainer = PutRunContainerToPool;
    return e;
}

void MemPool::PutInvertElement(InvertElement *e)
{
    stat.eleUsedCnt--;
    ListInsertToTail(&elementPool, &e->node);
}

/* RunContainer */
RunContainer *MemPool::GetRunContainer()
{
    RunContainer *c = nullptr;
    if (ListEmpty(&containerPool)) {
        c = (RunContainer *)malloc(sizeof(RunContainer));
        if (c == nullptr) {
            return nullptr;
        } 
        stat.containerCnt++;
    } else {
         ListNode *onode = containerPool.next;
        ListRemove(onode);
        c = NODE_ENTRTY(onode, RunContainer, node);
    }
    stat.containerUsedCnt++;
    c->key = 0;
    c->cardinality = 0;
    c->nRuns = 0;
    c->getRunPariGroup = GetRunPairGroupFromPool;
    ListInit(&(c->pairs));
    return c;
}

void MemPool::PutRunContainer(RunContainer *c)
{
    stat.containerUsedCnt--;
    ListInsertToTail(&containerPool, &c->node);
}

/* RunPairGroup */
RunPairGroup *MemPool::GetRunPairGroup()
{
    RunPairGroup *p = nullptr;
    if (ListEmpty(&pairGroupPool)) {
        p = (RunPairGroup *)malloc(sizeof(RunPairGroup));
        if (p == nullptr) {
            return nullptr;
        }
        stat.pairGroupCnt++;
    } else {
        ListNode *onode = pairGroupPool.next;
        ListRemove(onode);
        p = NODE_ENTRTY(onode, RunPairGroup, node);
    }
    stat.pairGroupUsedCnt++;
    ListInit(&p->node);
    return p;
}

void MemPool::PutRunPairGroup(RunPairGroup *p)
{
    stat.pairGroupUsedCnt--;
    ListInsertToTail(&pairGroupPool, &p->node);
}

uint32_t MemPool::GetHashTableBktCnt(uint32_t rowCnt)
{
    uint32_t factorIdx = rowCnt>>HASH_FACTOR_SHIFT;
    if (factorIdx >= MAX_HASHCURVE_SIZE) {
        factorIdx = MAX_HASHCURVE_SIZE - 1;
    }
    while (factorIdx < MAX_HASHCURVE_SIZE) {
        if (hashFactors[factorIdx].rowCnt != 0) {
            break;
        }
        factorIdx++;
    }
    uint32_t hashTableBkts = 0;
    if (factorIdx < MAX_HASHCURVE_SIZE) {
        hashTableBkts = uint32_t(uint64_t(hashFactors[factorIdx].tokenCnt) * uint64_t(rowCnt) / hashFactors[factorIdx].rowCnt);
    } else {
        hashTableBkts = rowCnt << 1;
    }
    hashTableBkts = ((hashTableBkts>>16) + 1) << 16;
    if (hashTableBkts < MIN_HASHTABLE_BKTS) {
        return MIN_HASHTABLE_BKTS;
    }
    if (hashTableBkts > MAX_HASHTABLE_BKTS) {
        return MAX_HASHTABLE_BKTS;
    }
    return hashTableBkts;
}

void MemPool::UpdateHashFactor(uint32_t rowCnt, uint32_t tokenCnt)
{
    uint32_t factorIdx = rowCnt>>HASH_FACTOR_SHIFT;
    if (factorIdx >= MAX_HASHCURVE_SIZE) {
        factorIdx = MAX_HASHCURVE_SIZE - 1;
    }
    if (hashFactors[factorIdx].tokenCnt < tokenCnt) {
        hashFactors[factorIdx].rowCnt = rowCnt;
        hashFactors[factorIdx].tokenCnt = tokenCnt;
    }
}

/* invert */
Invert *MemPool::GetInvert(uint32_t rowCnt)
{
    uint32_t bktsCnt = GetHashTableBktCnt(rowCnt);
    if (invert == nullptr) {
        invert = NewInvert(MAX_HASHTABLE_BKTS);
    }
    invert->ReInitBkt(bktsCnt);
    return invert;
}

/* vtoken */
VToken *MemPool::GetVToken()
{
    if (!vtokenInit) {
        for (int i = 0; i < TOKENIZE_STEP; i++) {
            vtoken[i].Init();
        }
    } else {
        for (int i = 0; i < TOKENIZE_STEP; i++) {
            vtoken[i].Reset();
        }
    }
    vtokenInit = true;
    return vtoken;
}

/* sortInvert */
InvertGroup *MemPool::GetInvertGroup()
{
    return &invertGroup;
}

#define SET_RES(res, FirstTokenStart, FirstTokenEnd, LastTokenSatrt, LastTokenEnd, \
    KeysSize, KeysToalSize, DataSize, DataTotalSize, ItemsCnt) do { \
    res[0] = FirstTokenStart;   \
    res[1] = FirstTokenEnd;     \
    res[2] = LastTokenSatrt;    \
    res[3] = LastTokenEnd;      \
    res[4] = KeysSize;          \
    res[5] = KeysToalSize;      \
    res[6] = DataSize;          \
    res[7] = DataTotalSize;     \
    res[8] = ItemsCnt;          \
} while(0)

void MemPool::SerializedContainer(char *data, RunContainer *c) {
    data[0] = c->seriType;
    data[1] = char(c->cardinality>>8);
    data[2] = char(c->cardinality);
    data[3] = char(c->key>>8);
    data[4] = char(c->key);
    int nRunsInGroup = 0, nRuns = 0, idx = 5;
    ListNode *tmpNode = nullptr, *node = nullptr;
    if (c->seriType = RUN_CONTAINER_TYPE) {
        data[5] = char(c->nRuns>>8);
        data[6] = char(c->nRuns);
        idx = 7;
        LIST_FOR_EACH_SAFE(node, tmpNode, &(c->pairs)) {
            RunPairGroup *pg = (RunPairGroup *)NODE_ENTRTY(node, RunPairGroup, node);
            ListRemove(node);
            nRunsInGroup = 0;
            for (int i = 0; i < PAIR_COUNT_PER_GROUP; i++) {
                if (nRuns >= c->nRuns) {
                    break;
                }
                // serialize
                RunPair *curPair = &(pg->pairs[nRunsInGroup]);
                data[idx] = char(curPair->value>>8);
                data[idx+1] = char(curPair->value);
                data[idx+2] = char(curPair->length>>8);
                data[idx+3] = char(curPair->length);
                idx += 4;
                nRunsInGroup++;
                nRuns++;
            }
            PutRunPairGroup(pg);
        }
        stat.runContainerCnt++;
    } else if (c->seriType = BITSET_CONTAINER_TYPE) {
        LIST_FOR_EACH_SAFE(node, tmpNode, &(c->pairs)) {
            RunPairGroup *pg = (RunPairGroup *)NODE_ENTRTY(node, RunPairGroup, node);
            ListRemove(node);
            nRunsInGroup = 0;
            for (int i = 0; i < PAIR_COUNT_PER_GROUP; i++) {
                if (nRuns >= c->nRuns) {
                    break;
                }
                // serialize
                RunPair *curPair = &(pg->pairs[nRunsInGroup]);
                for (int j = 0; j <= curPair->length; j++) {
                    uint16_t val = curPair->value + j;
                    int setIdx = idx + (val>>3);
                    int setOff = val&0x7;
                    data[setIdx] |= (1<<setOff);
                }
                nRunsInGroup++;
                nRuns++;
            }
            PutRunPairGroup(pg);
        }
        stat.bitsetContainerCnt++;
    } else if (c->seriType = ARRAY_CONTAINER_TYPE) {
        LIST_FOR_EACH_SAFE(node, tmpNode, &(c->pairs)) {
            RunPairGroup *pg = (RunPairGroup *)NODE_ENTRTY(node, RunPairGroup, node);
            ListRemove(node);
            nRunsInGroup = 0;
            for (int i = 0; i < PAIR_COUNT_PER_GROUP; i++) {
                if (nRuns >= c->nRuns) {
                    break;
                }
                // serialize
                RunPair *curPair = &(pg->pairs[nRunsInGroup]);
                for (int j = 0; j <= curPair->length; j++) {
                    uint16_t val = curPair->value + j;
                    data[idx] = char(val>>8);
                    data[idx+1] = char(val);
                    idx += 2;
                }
                nRunsInGroup++;
                nRuns++;
            }
            PutRunPairGroup(pg);
        }
        stat.arrayContainerCnt++;
    }
}

/* output the data */
bool MemPool::Next(char *keys, uint32_t keysLen, char *data, uint32_t dataLen, uint32_t res[RES_LENS_LEN])
{
    uint32_t keysOffset = 0;
    uint32_t dataOffset = 0;
    uint32_t firstTokenEnd = 0;
    uint32_t lastTokenStart = 0;
    uint32_t startIter = invertIter;

    if (keysOffs.Prepare(4 * invertGroup.len) != SUCCESS) {
        return true;
    }
    if (dataOffs.Prepare(4 * invertGroup.len) != SUCCESS) {
        return true;
    }
    struct ListNode *lnode = nullptr, *tmp = nullptr;
    while (invertIter < invertGroup.len) {
        InvertElement *ele = invertGroup.group[invertIter];
        ele->ResetContainersBufToFirst();
        if (ele->curContainer == nullptr) {
            invertIter++;
            continue;
        }
        // at least there is still memory space to copy a post container
        uint32_t seriSize = GetSeriSizeAndSetSeriType(ele->curContainer) + COOKIE_HEADER_SIZE;
        if (keysOffset + ele->token.len + keysOffs.len + 4 > keysLen || 
            dataOffset + seriSize + dataOffs.len + 4 > dataLen) {
            break;
        }

        // move keys;
        if (firstTokenEnd == 0) {
            firstTokenEnd = ele->token.len;
        }
        lastTokenStart = keysOffset;
        memcpy(&keys[keysOffset], ele->token.data, ele->token.len);
        keysOffset += ele->token.len;
        keysOffs.Append(keysOffset);

        // move posting lists;
        struct ListNode *list = &ele->containers;
        char *cookieHeader = &data[dataOffset];
        bool next = true;
        uint32_t containerCnt = 0;
        uint32_t cardinalityCnt = 0;
        LIST_FOR_EACH_SAFE(lnode, tmp, list) {
            RunContainer *container = (RunContainer *)NODE_ENTRTY(lnode, RunContainer, node);
            uint32_t seriSize = GetSeriSizeAndSetSeriType(container);
            if (dataOffset + seriSize + dataOffs.len + 4 + COOKIE_HEADER_SIZE  > dataLen) {
                next = false;
                break;
            }
            SerializedContainer(data, container);
            containerCnt++;
            dataOffset += seriSize;
            ListRemove(lnode);
            PutRunContainer(container);
        }
        dataOffs.Append(dataOffset);
        // Next Element
        if (next) {
            PutInvertElement(ele);
            invertIter++;
        }
    }

    // copy the key offs and data offs
    memcpy(&keys[keysOffset], keysOffs.buf, keysOffs.len);
    memcpy(&data[dataOffset], dataOffs.buf, dataOffs.len);
    SET_RES(res, 0, firstTokenEnd, lastTokenStart, keysOffset, keysOffset, keysOffset + keysOffs.len,
                dataOffset, dataOffset + dataOffs.len, invertIter - startIter);
    if (invertIter >= invertGroup.len) {
        stat.PrintUsedMem();
        stat.ResetContainerCnt();
        return false;
    }
    return true;
}

MemPool *MemPools::GetMemPool()
{
    MemPool *pool = nullptr;
    lock.lock();
    if (poolStack.empty()) {
        pool = new MemPool;
    } else {
        pool = poolStack.top();
        poolStack.pop();
    }
    lock.unlock();
    return pool;
}

void MemPools::PutMemPool(MemPool *pool)
{
    if (pool == nullptr) {
        return;
    }
    lock.lock();
    poolStack.push(pool);
    lock.unlock();
}

static MemPools g_memPools;

MemPool *GetMemPool()
{
    return g_memPools.GetMemPool();
}

void PutMemPool(MemPool *pool)
{
    g_memPools.PutMemPool(pool);
}
