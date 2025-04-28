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

#include "mempool.h"

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
    if (invert->hashTable.Init(bktCapSize, nullptr) == -1) {
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
    ListInit(&e->hashSection);
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

/* invert */
Invert *MemPool::GetInvert()
{
    if (invert == nullptr) {
        invert = NewInvert(HASHTABLE_BKTS_SIZE);
        if (invert == nullptr) {
            return nullptr;
        }
    }
    invert->ReInitBkt(HASHTABLE_BKTS_SIZE);
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

/* hash section */
ListNode *MemPool::GetHashSections()
{
    for (int i = 0; i < HASHTABLE_SECTION_SIZE; i++) {
        ListInit(&hashSection[i]);
    }
    return hashSection;
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

uint32_t MemPool::SerializedContainer(char *data, RunContainer *c) {
    data[0] = c->seriType;
    data[1] = char(c->key>>8);
    data[2] = char(c->key);
    data[3] = char(c->cardinality>>8);
    data[4] = char(c->cardinality);
    uint32_t nRunsInGroup = 0, nRuns = 0, idx = 5;
    ListNode *tmpNode = nullptr, *node = nullptr;
    if (c->seriType == RUN_CONTAINER_TYPE) {
        data[5] = char(c->nRuns>>8);
        data[6] = char(c->nRuns);
        idx = 7;
        LIST_FOR_EACH_SAFE(node, tmpNode, &(c->pairs)) {
            RunPairGroup *pg = (RunPairGroup *)NODE_ENTRTY(node, RunPairGroup, node);
            ListRemove(node);
            nRunsInGroup = 0;
            for (int i = 0; i < MAX_PAIRS_PER_GROUP; i++) {
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
    } else if (c->seriType == BITSET_CONTAINER_TYPE) {
        std::memset(&data[idx], 0, BITSET_CONTAINER_SIZE);
        LIST_FOR_EACH_SAFE(node, tmpNode, &(c->pairs)) {
            RunPairGroup *pg = (RunPairGroup *)NODE_ENTRTY(node, RunPairGroup, node);
            ListRemove(node);
            nRunsInGroup = 0;
            for (int i = 0; i < MAX_PAIRS_PER_GROUP; i++) {
                if (nRuns >= c->nRuns) {
                    break;
                }
                // serialize to U64 BigEndian
                RunPair *curPair = &(pg->pairs[nRunsInGroup]);
                for (int j = 0; j <= curPair->length; j++) {
                    uint16_t val = curPair->value + j;
                    uint16_t byteIdx = val>>3;
                    uint16_t setIdx = idx + (byteIdx&0xFFF8) + 7 - (byteIdx&0x7); // BigEndian
                    int setOff = val&0x7;
                    data[setIdx] |= (1<<setOff);
                }
                nRunsInGroup++;
                nRuns++;
            }
            PutRunPairGroup(pg);
        }
        stat.bitsetContainerCnt++;
        return idx + BITSET_CONTAINER_SIZE;
    } else if (c->seriType == ARRAY_CONTAINER_TYPE) {
        LIST_FOR_EACH_SAFE(node, tmpNode, &(c->pairs)) {
            RunPairGroup *pg = (RunPairGroup *)NODE_ENTRTY(node, RunPairGroup, node);
            ListRemove(node);
            nRunsInGroup = 0;
            for (int i = 0; i < MAX_PAIRS_PER_GROUP; i++) {
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
    return idx;
}

void MemPool::SerializedCookieHeader(char *data, uint16_t containerCnt, uint32_t cardinalityCnt)
{
    data[0] = char(containerCnt>>8);
    data[1] = char(containerCnt);
    data[2] = char(cardinalityCnt>>24);
    data[3] = char(cardinalityCnt>>16);
    data[4] = char(cardinalityCnt>>8);
    data[5] = char(cardinalityCnt);
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
        // reserve storage space for the cookie header for the inverted table
        uint16_t containerCnt = 0;
        uint32_t cardinalityCnt = 0;
        char *cookieHeader = &data[dataOffset];
        dataOffset += COOKIE_HEADER_SIZE;
        // move posting lists;
        bool next = true;
        struct ListNode *list = &ele->containers;
        LIST_FOR_EACH_SAFE(lnode, tmp, list) {
            RunContainer *container = (RunContainer *)NODE_ENTRTY(lnode, RunContainer, node);
            uint32_t seriSize = GetSeriSizeAndSetSeriType(container);
            if (dataOffset + seriSize + dataOffs.len + 4 > dataLen) {
                next = false;
                break;
            }
            containerCnt++;
            cardinalityCnt += uint32_t(container->cardinality) + 1;
            uint32_t realSize = SerializedContainer(&data[dataOffset], container);
            if (seriSize != realSize) {
                LOG_ERROR("fetal error occurred in container serialization, type=%d, seriSize=%d, realSeriSize=%d",
                    container->seriType, seriSize, realSize);
            }
            dataOffset += seriSize;
            ListRemove(lnode);
            PutRunContainer(container);
        }
        SerializedCookieHeader(cookieHeader, containerCnt, cardinalityCnt);
        dataOffs.Append(dataOffset);
        // Next Element
        if (next) {
            invertGroup.group[invertIter] = nullptr;
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
        stat.ResetContainerCnt();
        return false;
    }
    return true;
}

// for test
bool MemPool::AddPostingData(char *key, uint32_t keyLen, uint32_t rowId)
{
    for (int i = 0; i < invertGroup.len; i++) {
        InvertElement *ele = invertGroup.group[i];
        if (ele->token.len != keyLen ||
            std::memcmp(ele->token.data, key, keyLen) != 0){
            continue;
        }
        ele->AppendInvertState(this, rowId);
        return true;
    }
    InvertElement *ele = GetInvertElement();
    if (ele == nullptr) {
        return false;
    }
    ele->token.data = key;
    ele->token.len = keyLen;
    ele->AppendInvertState(this, rowId);
    invertGroup.group[invertGroup.len] = ele;
    invertGroup.len++;
    return true;
}

MemPool *MemPools::GetMemPool()
{
    MemPool *pool = nullptr;
    lock.lock();
    if (poolStack.empty()) {
        pool = new MemPool;
        LOG_INFO("allocate memory pool:%p", pool);
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
