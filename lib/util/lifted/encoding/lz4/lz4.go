//go:build !linux || !amd64 || !cgo

package lz4

import "github.com/pierrec/lz4/v4"

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

func CompressBlockBound(size int) int {
	return lz4.CompressBlockBound(size)
}

func CompressBlock(source, dest []byte) (int, error) {
	return lz4.CompressBlock(source, dest, nil)
}

func DecompressSafe(source, dest []byte) (int, error) {
	return lz4.UncompressBlock(source, dest)
}
