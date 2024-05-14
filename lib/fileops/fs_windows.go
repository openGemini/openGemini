//go:build !streamfs
// +build !streamfs

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

package fileops

func Mmap(fd int, offset int64, length int) (data []byte, err error) {
	return
}

func MmapRW(fd int, offset int64, length int) (data []byte, err error) {
	return
}

func MUnmap(data []byte) error {
	return nil
}

func Fadvise(fd int, offset int64, length int64, advice int) (err error) {
	return
}

func Fdatasync(file File) (err error) {
	return
}
