//go:build linux && amd64
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

#ifndef INVERT_HEADER_H
#define INVERT_HEADER_H

#include <stdint.h>
#include <stdbool.h>
#include <stdlib.h>
#include <cstring>
#include <cstdlib>
#include <ctime>
#include <iostream>

/* error status */
#define SUCCESS 0
#define ERROR_INVALID_POINTER               1
#define ERROR_ALLOC_MEM_FAIL                2
#define ERR_START_BIT                       16
#define CGO_ERROR(status)                   ((status)<<ERR_START_BIT|__LINE__)
#define CGO_SUCCESS(status)                 ((status)==SUCCESS)
#define CGO_FAILURE(status)                 ((status)!=SUCCESS)
#define MIN_HASHTABLE_BKTS                  32768  // 2^15
#define MAX_HASHTABLE_BKTS                  1048576 // 2^20

#define LOG_INFO(format, ...) do {                      \
    char tBuf[64] = {0};                                \
    time_t now = time(0);                               \
    tm *local = localtime(&now);                        \
    strftime(tBuf, 64, "%Y-%m-%d %H:%M:%S", local);     \
    fprintf(stderr, "%s [%s:%d][%s][INFO] " format "\n", tBuf, __FILE__, __LINE__, __func__, ##__VA_ARGS__); \
} while(0)

#define LOG_ERROR(format, ...) do {                     \
    char tBuf[64] = {0};                                \
    time_t now = time(0);                               \
    tm *local = localtime(&now);                        \
    strftime(tBuf, 64, "%Y-%m-%d %H:%M:%S", local);     \
    fprintf(stderr, "%s [%s:%d][%s][ERROR] " format "\n", tBuf, __FILE__, __LINE__, __func__, ##__VA_ARGS__); \
} while(0)

/* Round by multiples of 2 */
static inline uint32_t RoundByMulOf2(uint32_t val)
{
    uint32_t bitCnt = 1;
    while (val > 0) {
        bitCnt++;
        val = val >> 1;
    }
    return (1 << bitCnt);
}

/* list */
struct ListNode {
    struct ListNode *prev, *next;
};

#define OFFSETOF(TYPE, MEMBER)	((size_t)&((TYPE *)0)->MEMBER)
#define NODE_ENTRTY(node, type, member)  ((type*)((char *)(node) - OFFSETOF(type, member)))

static inline void ListInit(struct ListNode *node) {
    node->prev = node;
    node->next = node;
}

static inline bool ListEmpty(struct ListNode *node) {
    return node->next == node;
}

/* insert the 'node' to the front of 'pos' */
static inline void ListInsertToTail(struct ListNode *pos, struct ListNode *node) {
    node->prev = pos->prev;
    pos->prev = node;
    node->next = pos;
    node->prev->next = node;
}

/* insert the 'node' to the rear of 'pos' */
static inline void ListInsertToHead(struct ListNode *pos, struct ListNode *node) {
    node->next = pos->next;
    pos->next = node;
    node->prev = pos;
    node->next->prev = node;
}

static inline void ListRemove(struct ListNode *node) {
    node->prev->next = node->next;
    node->next->prev = node->prev;
}

#define LIST_FOR_EACH(node, list) \
    for (node = (list)->next; \
         node != (list); \
         node = (node)->next)

#define LIST_FOR_EACH_SAFE(node, tmp, list) \
    for (node = (list)->next, tmp = (node)->next; \
         node != (list); \
         node = tmp, tmp = (node)->next)

/* offs buf */
struct OffsBuf {
    char *buf;
    int32_t len;
    int32_t cap;

    void Reset() {
        buf = nullptr;
        len = 0;
        cap = 0;
    }

    int32_t Prepare(int32_t size)
    {
        len = 0;
        if (buf != nullptr && size <= cap) {
            return SUCCESS;
        }
        if (buf != nullptr) {
            LOG_INFO("OffsBuf free, len = %d, cap=%d", len, cap);
            free(buf);
        }
        int32_t newSize = (int32_t)RoundByMulOf2(uint32_t(size));
        buf = (char *)malloc(newSize * sizeof(char));
        if (buf == nullptr) {
            LOG_ERROR("OffsBuf malloc failed, len = %d, cap=%d, newCap=%d", len, cap, newSize);
            return CGO_ERROR(ERROR_ALLOC_MEM_FAIL);
        }
        cap = newSize;
        LOG_INFO("OffsBuf malloc success, len = %d, cap=%d", len, cap);
        return SUCCESS;
    }

    void Append(uint32_t val)
    {
        if (len + 4 > cap) {
            LOG_ERROR("OffsBuf size is not enough len=%d, cap=%d", len, cap);
            return;
        }
        buf[len] = char(val>>24);
        buf[len+1] = char(val>>16);
        buf[len+2] = char(val>>8);
        buf[len+3] = char(val);
        len += 4;
    }

    int32_t Size()
    {
        return len;
    }
};

/* ColVal */
struct ColVal {
    char        *val;       // data bufs
    uint32_t    valLen;     // length of data bufs
    uint32_t    *offset;    // row data offset. [off[i], off[i+1]) is i-th row. the last row data is [off[offLen - 1], valLen)
    uint32_t    offLen;     // length of offset

};

static inline void InitColVal(ColVal *colVal, char *val, uint32_t valLen, uint32_t *offset, uint32_t offLen) {
    colVal->val = val;
    colVal->valLen = valLen;
    colVal->offset = offset;
    colVal->offLen = offLen;
}

#define BITSET_CONTAINER_TYPE 1
#define ARRAY_CONTAINER_TYPE 2
#define RUN_CONTAINER_TYPE 3
#define COOKIE_HEADER_SIZE 6 // sizeof(ContainerCount) + sizeof(CardinalityCount)

#define PAIR_COUNT_SHIFT_NUM 8
#define PAIR_COUNT_PER_GROUP (1<<PAIR_COUNT_SHIFT_NUM)
#define PAIR_NUMBER_MASK (PAIR_COUNT_PER_GROUP - 1)

/*
 * value:  start position of the run
 * length: length of the run is `length + 1`
 * e.g.{3, 0} = [3], {3, 4} = [3, 4, 5, 6, 7],
 */
struct RunPair {
    uint16_t value;
    uint16_t length;
};

struct RunPairGroup {
    ListNode node;
    RunPair pairs[PAIR_COUNT_PER_GROUP];
};

typedef RunPairGroup *(*GetRunPairGroup)(void *memPool);

struct RunContainer {
    ListNode node;
    ListNode pairs; // RunPairGroup list
    RunPairGroup *curPairs;
    GetRunPairGroup getRunPariGroup; // func
    uint16_t key;
    uint16_t cardinality;
    uint16_t nRuns;
    uint8_t  seriType;
};

static inline int32_t GetArrayContainerSerializedSize(int32_t card) {
    return 5 + card * 2; // sizeof(containerHeader) + Cardinality * sizeof(uint16_t)
}

static inline int32_t GetBitsetContainerSerializedSize(void) {
    return 5 + 8192; // sizeof(containerHeader)  + sizeof(bitset);
}

static inline int32_t GetRunContainerSerializedSize(int32_t nRuns) {
    return 5 + 2 + nRuns * 4;  // sizeof(containerHeader)  + sizeof(Nruns) + Nruns*sizeof(RunPair)
}

static inline int32_t GetSeriSizeAndSetSeriType(RunContainer *c) {
    int32_t seriSize = GetRunContainerSerializedSize(c->nRuns);
    int32_t bitSetSize = GetBitsetContainerSerializedSize();
    int32_t arraySize = GetArrayContainerSerializedSize(c->cardinality);

    c->seriType = RUN_CONTAINER_TYPE;
    if (seriSize > bitSetSize) {
        seriSize = bitSetSize;
        c->seriType = BITSET_CONTAINER_TYPE;
    }
    if (seriSize > arraySize) {
        seriSize = arraySize;
        c->seriType = ARRAY_CONTAINER_TYPE;
    }
    return seriSize;
}

// RowData
struct RowData {
    char *data; // data alloced by the caller, and cannot be held for a long time
    int32_t len; // length of data 
    int32_t off; // current reading offset of the data, private variable.
    int32_t number; // the term number in the current data. eg:"get a pencil", the number of  "pencil" is 2.
};

// Token
static inline size_t BkdrHash(const char *input, int32_t len) 
{
    if (input == nullptr) {
        return 0;
    }
    size_t hashValue = 0UL;
    for (int32_t i = 0; i < len; i ++) {
        hashValue = (hashValue << 7) + (hashValue << 1) + hashValue + input[i]; // seed = 131
    }
    return hashValue;
}

struct Token {
    char *data;
    int32_t len;
    size_t hashValue;

    void Reset()
    {
        data = nullptr;
        len = 0;
        hashValue = 0;
    }

    void Reinit(char *idata, int32_t ilen)
    {
        data = idata;
        len = ilen;
        hashValue = BkdrHash(data, len);
    }

    void Hash()
    {
        hashValue = BkdrHash(data, len);
    }

    size_t GetHashValue()
    {
        if (hashValue == 0) {
            hashValue = BkdrHash(data, len);
        }
        return hashValue;
    }

    bool operator==(const Token& t) const
    {
        if (len != t.len || 
            hashValue != t.hashValue || 
            std::memcmp(data, t.data, len) != 0) {
            return false;
        }
        return true;
    }

    bool operator<(const Token& t) const
    {
        int32_t i = 0;
        while (i < len && i < t.len) {
            if (data[i] == t.data[i]) {
                i++;
                continue;
            }
            if (data[i] < t.data[i]) {
                return true;
            } else {
                return false;
            }
        }
        if (len < t.len) {
            return true;
        } else {
            return false;
        }
    }

    void operator=(const Token& t)
    {
        data = t.data;
        len = t.len;
        hashValue = t.hashValue;
    }
};

struct hash_fn
{
    size_t operator() (const Token& node) const 
    {
        if (node.hashValue != 0) {
            return node.hashValue;
        }
        return BkdrHash(node.data, node.len);
    }
};

// VToken
#define TOKENIZE_STEP 256
#define VTOKEN_INIT_SIZE 256
#define VTOKEN_GROW_SIZE 64
struct VToken {
    Token *tokens;
    uint32_t cap; // the capacity of the tokens array, default is DEFAULT_VTOKEN_LENS
    uint32_t len; // the length of the tokens array, actual usage length
    uint32_t id;  // the TokenID or RowID of v-token(in the dictionary)
	uint16_t pos; // the first token's position of variable-tokens in the document

    int32_t Init() {
        Reset();
        cap = 0;
        tokens = (Token *)malloc(VTOKEN_INIT_SIZE * sizeof(Token));
        if (tokens == nullptr) {
            LOG_ERROR("failed to allocate memory for vtoken");
            return CGO_ERROR(ERROR_ALLOC_MEM_FAIL);
        }
        cap = VTOKEN_INIT_SIZE;
        return SUCCESS;
    }

    int32_t Expand()
    {
        // span the meory:
        Token *tmpToken =  (Token *)malloc((cap + VTOKEN_GROW_SIZE) * sizeof(Token));
        if (tmpToken == nullptr) {
            LOG_ERROR("failed to allocate memory for vtoken");
            return CGO_ERROR(ERROR_ALLOC_MEM_FAIL);
        }
        LOG_INFO("VToken Expand-%p, len=%u, cap=%u, newCap=%u", this, len, cap, cap+VTOKEN_GROW_SIZE);
        if (tokens != nullptr) {
            (void)std::memcpy(tmpToken, tokens, len * sizeof(Token));
            free(tokens);
        }
        tokens = tmpToken;
        cap = cap + VTOKEN_GROW_SIZE;
        return SUCCESS;
    }

    int32_t Append(Token *token)
    {
        if (len >= cap) {
            // span the memory
            int32_t status = Expand();
            if (CGO_FAILURE(status)) {
                return status;
            }
        }
        tokens[len].data = token->data;
        tokens[len].len = token->len;
        tokens[len].hashValue = token->hashValue;
        len++;
        return SUCCESS;
    }

    void Reset(){
        len = 0;
        id = 0;
        pos = 0;
    }
};

typedef RunContainer *(*GetRunContainer)(void *memPool);
typedef void (*PutRunContainer)(void *memPool, RunContainer *c);
#define CONTAINER_VLAUE_MASK 0xFFFF

struct InvertElement {
    ListNode node;
    Token token;

    // posting list info
    ListNode containers;
    RunContainer *curContainer; // currently used Container
    GetRunContainer getRunContainer;
    PutRunContainer putRunContainer;

    InvertElement()
    {
        ListInit(&node);
        ListInit(&containers);
        curContainer = nullptr;
    }

    bool operator==(const InvertElement& t) const
    {
        return (token == t.token);
    }

    bool operator<(const InvertElement& t) const
    {
        return (token < t.token);
    }

    void AppendInvertState(void *pool, uint32_t rowId)
    {
        uint16_t key = rowId >> 16;
        uint16_t val = rowId & CONTAINER_VLAUE_MASK;
        if (curContainer == nullptr || curContainer->key != key) {
            curContainer = getRunContainer(pool);
            if (curContainer == nullptr) {
                return;
            }
            RunPairGroup *pg = curContainer->getRunPariGroup(pool);
            if (pg == nullptr) {
                putRunContainer(pool, curContainer);
                curContainer = nullptr;
                LOG_ERROR("New RunPairGroup failed");
                return;
            }
            curContainer->key = key;
            curContainer->cardinality = 1;
            curContainer->nRuns = 0;
            curContainer->curPairs = pg;
            pg->pairs[0].value = val;
            pg->pairs[0].length = 0;
            ListInsertToTail(&curContainer->pairs, &(pg->node));
            ListInsertToTail(&containers, &(curContainer->node));
            return;
        }
        int pairNum = curContainer->nRuns & PAIR_NUMBER_MASK;
        RunPair *curPair = &(curContainer->curPairs->pairs[pairNum]);
        if(val <= curPair->value + curPair->length) {
            return;
        }

        if (val == curPair->value + curPair->length + 1) {
            curPair->length++;
        } else {
            pairNum++;
            if (pairNum == PAIR_COUNT_PER_GROUP) {
                RunPairGroup *pg = curContainer->getRunPariGroup(pool);
                if (pg == nullptr) {
                    LOG_ERROR("New RunPairGroup failed When new runs");
                    return;
                }
                curContainer->curPairs = pg;
                ListInsertToTail(&curContainer->pairs, &(pg->node));
                pairNum = 0;
            }
            RunPair *curPair = &(curContainer->curPairs->pairs[pairNum]);
            curPair->value = val;
            curPair->length = 0;
            curContainer->nRuns++;
        }
        curContainer->cardinality++;
    }

    void ResetContainersBufToFirst() {
        curContainer == nullptr;
        if (!ListEmpty(&containers)) {
            curContainer = (RunContainer *)NODE_ENTRTY(containers.next, RunContainer, node);
        }
    }
};

struct InvertGroup {
    InvertElement **group;
    uint32_t len;
    uint32_t cap;

    void Reset() {
        group = nullptr;
        len = 0;
        cap = 0;
    }

    int32_t Prepare(uint32_t size)
    {
        len = 0;
        if (group != nullptr) {
            if (size <= cap) {
                return SUCCESS;
            } else {
                LOG_INFO("InvertGroup Prepare[free] this=%p, len=%u, cap=%u", this, len, cap);
                free(group);
            }
        } else {
            if (size < MAX_HASHTABLE_BKTS) {
                size = MAX_HASHTABLE_BKTS;
            }
        }
        group = (InvertElement **)malloc(size * sizeof(InvertElement *));
        if (group == nullptr) {
            LOG_ERROR("InvertGroup Prepare[malloc] failed, len=%u", size);
            return CGO_ERROR(ERROR_ALLOC_MEM_FAIL);
        }
        cap = size;
        LOG_INFO("InvertGroup Prepare[malloc] success, this=%p, len=%u, cap=%u", this, len, cap);
        return SUCCESS;
    }

    void Insert(InvertElement *ele)
    {
        if (len < cap) {
            group[len] = ele;
            len++;
        }
    }
};

static inline int CompareEle(const void *a, const void *b)
{
    InvertElement **ai = (InvertElement **)a;
    InvertElement **bi = (InvertElement **)b;
    if ((*ai)->token < (*bi)->token) {
        return -1;
    }
    return 1;
}

/* hashTable */
typedef bool (*HashEqualFunc)(const struct ListNode *nodea, const struct ListNode *nodeb);

struct HashTable {
    uint32_t bktSize;
    uint32_t bktCapSize;
    ListNode *bkts;
    HashEqualFunc equal;

    /* return 0: success, return -1: failed */
    int Init(uint32_t iBktCap, HashEqualFunc iequal)
    {
        bktCapSize = iBktCap;
        equal = iequal;
        bkts = (ListNode *)malloc(bktCapSize * sizeof(ListNode));
        if (bkts == nullptr) {
            return -1;
        }
        LOG_INFO("HashTable malloc, this=%p, bktCapSize=%u", this, bktCapSize);
        for (int i = 0; i < bktCapSize; i++) {
            ListInit(&bkts[i]);
        }
        bktSize = bktCapSize;
        return 0;
    }

    int ReInitBkt(uint32_t ibktSize) {
        if (ibktSize > bktCapSize) {
            ibktSize = bktCapSize;
        }
        bktSize = ibktSize;
        for (int i = 0; i < bktSize; i++) {
            ListInit(&bkts[i]);
        }
        return 0;
    }

    void InsertByToken(struct ListNode *node, Token *token)
    {
        size_t code = token->hashValue % bktSize;
        ListInsertToTail(&bkts[code], node);
    }

    ListNode *FindByToken(Token *token)
    {
        size_t code = token->hashValue % bktSize;
        ListNode *list = &bkts[code];
        ListNode *tmpNode = nullptr;
        LIST_FOR_EACH(tmpNode, list) {
            InvertElement *tempEle = (InvertElement *)NODE_ENTRTY(tmpNode, InvertElement, node);
            if (tempEle->token.len == token->len && std::memcmp(tempEle->token.data, token->data, tempEle->token.len) == 0) {
                return tmpNode;
            }
        }
        return nullptr;
    }
};

struct Invert {
    struct HashTable hashTable;
    int32_t ReInitBkt(uint32_t bktSize)
    {
        return hashTable.ReInitBkt(bktSize);
    }
};

#endif
