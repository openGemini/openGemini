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

#include <stdint.h>
#include <stdbool.h>
#include <string.h>

/* full text index builder */
typedef void *TextIndexBuilder;
typedef void *MemElement;

TextIndexBuilder NewTextIndexBuilder(const char *splitStr, uint32_t len, bool hasChin);
void FreeTextIndexBuilder(TextIndexBuilder builder);
MemElement AddDocument(TextIndexBuilder builder, char *val, uint32_t valLen, uint32_t *offset, uint32_t offLen, uint32_t startRow, uint32_t endRow);
bool NextData(MemElement memElement, char *keys, uint32_t keysLen, char *data, uint32_t dataLen, uint32_t *res);
void PutMemElement(MemElement memElement);

// for test
MemElement GetMemElement(uint32_t groupSize);
bool AddPostingToMem(MemElement memElement, char *key, uint32_t keyLen, uint32_t rowId);
