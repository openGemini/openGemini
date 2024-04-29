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

#include <algorithm>
#include "FullTextIndex.h"

void SimpleGramTokenizer::Init(const char *splitStr, uint32_t len, bool hasChin) {
    hasNonASCII = hasChin;
    for (int i = 0; i < SPLIT_TABLE_SIZE; i++) {
        if (i < 0x80) {
            splitTable[i] = 1; // 1-byte utf-8
        } else if (i < 0xe0) {
            splitTable[i] = 2; // 2-byte utf-8
        } else if (i < 0xf0) {
            splitTable[i] = 3; // 3-byte utf-8
        } else if (i < 0xf8) {
            splitTable[i] = 4; // 4-byte utf-8
        } else if (i < 0xfc) {
            splitTable[i] = 5; // 5-byte utf-8
        } else {
            splitTable[i] = 6; // 6-byte utf-8
        }
    }
    for (int i = 0; i < len; i++) {
        splitTable[splitStr[i]] = 0;
    }
}

bool SimpleGramTokenizer::NextBatch(ColVal *colVal, uint32_t startRow, VToken *vtoken, uint32_t batchLen)
{
    // slip for tokenizer
    int i = 0;
    uint32_t endRow = startRow + batchLen;
    while (startRow < endRow) {
        uint32_t startloc = colVal->offset[startRow];
        uint32_t endloc = colVal->valLen; // last row
        if (startRow < colVal->offLen - 1) {
            endloc = colVal->offset[startRow + 1];
        }

        uint32_t tokenStart = startloc;
        Token token = {};
        while (startloc < endloc) {
            uint8_t c = (uint8_t)colVal->val[startloc];
            if (splitTable[c] == 0) { // split char;
                if (tokenStart == startloc) {
                    tokenStart = ++startloc;
                    continue;
                }
                token.Reinit(&colVal->val[tokenStart], startloc - tokenStart);
                vtoken[i].Append(&token);
                startloc++;
                tokenStart = startloc;
                continue;
            } else if (splitTable[c] > 1) {  // es. chinese character
                token.Reinit(&colVal->val[tokenStart], splitTable[c]);
                vtoken[i].Append(&token);
                startloc += splitTable[c];
                tokenStart = startloc;
                continue;
            }
            startloc++;
        }
        if (startloc > tokenStart) {
            token.Reinit(&colVal->val[tokenStart], startloc - tokenStart);
            vtoken[i].Append(&token);
        }
        vtoken[i].id = startRow;
        i++;
        startRow++;
    }
    return true;
}

int32_t FullTextIndex::Init(const char *splitStr, uint32_t len, bool hasChin)
{
    tokenizer.Init(splitStr, len, hasChin);
    return SUCCESS;
}

void FullTextIndex::PutToSortInvert(MemPool *pool, Invert *root)
{
    InvertGroup *invertGroup = pool->GetInvertGroup();
    invertGroup->Prepare(nodeCount);
    pool->invertIter = 0;

    HashTable *hashTable = &root->hashTable;
    for (int i = 0; i < hashTable->bktSize; i++) {
        if (ListEmpty(&hashTable->bkts[i])) {
            continue;
        }
        ListNode *tmpNode = nullptr, *node = nullptr;
        LIST_FOR_EACH_SAFE(node, tmpNode, &hashTable->bkts[i]) {
            InvertElement *ele = (InvertElement *)NODE_ENTRTY(node, InvertElement, node);
            ListRemove(node);
            invertGroup->Insert(ele);
        }
    }
    qsort(invertGroup->group, invertGroup->len, sizeof(InvertElement *), CompareEle);
}

void FullTextIndex::InsertPostingList(MemPool *pool, Invert *root, VToken *vtoken, uint32_t batchLen)
{
    HashTable *hashTable = &(root->hashTable);
 
    InvertElement *ele = nullptr;
    for (int32_t i = 0; i < batchLen; i++) {
        for (int32_t j = 0; j < vtoken[i].len; j++) {
            ListNode *node = hashTable->FindByToken(&(vtoken[i].tokens[j]));
            if (node == nullptr) {
                ele = pool->GetInvertElement();
                ele->token = vtoken[i].tokens[j];
                hashTable->InsertByToken(&ele->node, &(vtoken[i].tokens[j]));
                nodeCount++;
            } else {
                ele = NODE_ENTRTY(node, InvertElement, node);
            }
            ele->AppendInvertState(pool, vtoken[i].id);
        }
        vtoken[i].Reset();
    }
}

// the scope of the text-index to be constructed is [startRow, endRow), which includes startRow but does not include endRow.
MemPool *FullTextIndex::AddDocument(char *val, uint32_t valLen, uint32_t *offset, uint32_t offLen, uint32_t startRow, uint32_t endRow)
{
    MemPool *pool = GetMemPool();
    if (pool == nullptr) {
        return nullptr;
    }
    VToken *vtoken = pool->GetVToken();
    Invert *root = pool->GetInvert(offLen);
    ColVal colVal;
    InitColVal(&colVal, val, valLen, offset, offLen);

    nodeCount = 0;
    while (startRow < endRow) {
        uint32_t batchLen = TOKENIZE_STEP;
        if (startRow + TOKENIZE_STEP > endRow) {
            batchLen = endRow - startRow;
        }
        if (tokenizer.NextBatch(&colVal, startRow, vtoken, batchLen)) {
            InsertPostingList(pool, root, vtoken, batchLen);
        }
        startRow += batchLen;
    }
    PutToSortInvert(pool, root);
    pool->UpdateHashFactor(offLen, nodeCount);
    return pool;
}
