//go:build linux && amd64
// Copyright Huawei Cloud Computing Technologies Co., Ltd.
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

#include "FullTextIndex.h"

extern "C" {
    #include "textbuilder_c.h"
}

TextIndexBuilder NewTextIndexBuilder(const char *splitStr, uint32_t len, bool hasChin)
{
    FullTextIndex *fullTextIndex = (FullTextIndex *)malloc(sizeof(FullTextIndex));
    if (fullTextIndex == NULL) {
        LOG_ERROR("new FullTextIndex failed");
        return nullptr;
    }
    int32_t status = fullTextIndex->Init(splitStr, len, hasChin);
    if (status != SUCCESS) {
        free(fullTextIndex);
        LOG_ERROR("FullTextIndex init failed");
        return nullptr;
    }
    return fullTextIndex;
}

void FreeTextIndexBuilder(TextIndexBuilder builder) 
{
    FullTextIndex *textIndex = reinterpret_cast<FullTextIndex *>(builder);
    if (textIndex == nullptr) {
        LOG_ERROR("input builder is invalid");
        return;
    }
    free(textIndex);
}

MemElement AddDocument(TextIndexBuilder builder, char *val, uint32_t valLen, uint32_t *offset, uint32_t offLen, uint32_t startRow, uint32_t endRow)
{
    FullTextIndex *textIndex = reinterpret_cast<FullTextIndex *>(builder);
    if (textIndex == nullptr) {
        LOG_ERROR("input text index is invalid");
        return nullptr;
    }
    return (MemElement)textIndex->AddDocument(val, valLen, offset, offLen, startRow, endRow);
}

// res[0]: first token start-offset
// res[1]: first token end-offset
// res[2]: last token start-offset
// res[3]: last token end-offset
// res[4]: keys size
// res[5]: keys total size = keys size + keys offs size
// res[6]: data size
// res[7]: data total size = data size + data offs size
// res[8]: items count
bool NextData(MemElement memElement, char *keys, uint32_t keysLen, char *data, uint32_t dataLen, uint32_t *res)
{
    MemPool *memPool = reinterpret_cast<MemPool *>(memElement);
    if (memPool == nullptr) {
        LOG_ERROR("input memory element is invalid");
        return false;
    }
    return memPool->Next(keys, keysLen, data, dataLen, res);
}

void PutMemElement(MemElement memElement)
{
    MemPool *memPool = reinterpret_cast<MemPool *>(memElement);
    if (memPool == nullptr) {
        LOG_ERROR("input memory element is invalid");
        return;
    }
    PutMemPool(memPool);
}

// get memory element for test
MemElement GetMemElement(uint32_t groupSize)
{
    MemPool *pool = GetMemPool();
    InvertGroup *invertGroup = pool->GetInvertGroup();
    int32_t ret = invertGroup->Prepare(groupSize);
    if (ret != SUCCESS) {
        PutMemElement(pool);
        return nullptr;
    }
    pool->invertIter = 0;
    return (MemElement)pool;
}

// add posting data for test
bool AddPostingToMem(MemElement memElement, char *key, uint32_t keyLen, uint32_t rowId)
{
    MemPool *memPool = reinterpret_cast<MemPool *>(memElement);
    if (memPool == nullptr) {
        LOG_ERROR("input memory element is invalid");
        return false;
    }
    return memPool->AddPostingData(key, keyLen, rowId);
}