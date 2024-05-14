//go:build !streamfs && (linux || freebsd)
// +build !streamfs
// +build linux freebsd

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

import (
	"fmt"
	"syscall"
)

const defaultMmapSize = 4 * 1024 // 4K

func Mmap(fd int, offset int64, length int) (data []byte, err error) {
	if length <= 0 {
		length = defaultMmapSize
	}

	data, err = syscall.Mmap(fd, offset, length, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return
	}

	if err = Fadvise(fd, offset, int64(length), syscall.MADV_RANDOM); err != nil {
		return nil, fmt.Errorf("madvise: %+v", err)
	}

	return
}

func MmapRW(fd int, offset int64, length int) (data []byte, err error) {
	if length <= 0 {
		length = defaultMmapSize
	}

	data, err = syscall.Mmap(fd, offset, length, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return
	}

	if err = Fadvise(fd, offset, int64(length), syscall.MADV_RANDOM); err != nil {
		return nil, fmt.Errorf("madvise: %+v", err)
	}

	return
}

func MUnmap(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	return syscall.Munmap(data)
}

func Fadvise(fd int, offset int64, length int64, advice int) (err error) {
	_, _, e1 := syscall.Syscall6(syscall.SYS_FADVISE64, uintptr(fd), uintptr(offset), uintptr(length), uintptr(advice), 0, 0)
	if e1 != 0 {
		err = e1
	}
	return
}

func Fdatasync(file File) (err error) {
	return syscall.Fdatasync(int(file.Fd()))
}

func RecoverLease(lock string) error {
	return nil
}
