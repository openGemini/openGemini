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

package sherlock

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// rotateProfilesFiles rotates profile files, ensure that the number of files does not exceed the threshold, and delete expired files
func rotateProfilesFiles(directory string, dumpType configureType, maxNum int, maxAge time.Duration) error {
	files, err := listProfileFiles(directory, dumpType)
	if err != nil {
		return err
	}
	// processing expired files
	err = deleteOldProfileFiles(files, directory, maxAge)
	if err != nil {
		return err
	}

	// processing redundant files
	err = deleteRedundantProfileFiles(files, directory, maxNum)
	if err != nil {
		return err
	}

	return nil
}

// listProfileFiles gets a list of profile files in the directory
func listProfileFiles(directory string, dumpType configureType) ([]os.FileInfo, error) {
	var filename string
	switch dumpType {
	case CPU:
		filename = filepath.Join(directory, allCpuProfiles)
	case Memory:
		filename = filepath.Join(directory, allMemProfiles)
	case Goroutine:
		filename = filepath.Join(directory, allGrtProfiles)
	default:
		return nil, fmt.Errorf("do not support type: %s", dumpType.string())
	}

	files, err := filepath.Glob(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to list profile files: %v", err)
	}

	var fileInfos []os.FileInfo
	for _, file := range files {
		fileInfo, err := os.Stat(file)
		if err != nil {
			continue
		}
		fileInfos = append(fileInfos, fileInfo)
	}

	return fileInfos, nil
}

// deleteOldProfileFiles deletes Profile files that are older than the specified time
func deleteOldProfileFiles(files []os.FileInfo, directory string, maxAge time.Duration) error {
	currentTime := time.Now()
	for _, file := range files {
		if currentTime.Sub(file.ModTime()) > maxAge {
			// file expired, delete it
			filePath := filepath.Join(directory, file.Name())
			_ = os.Remove(filePath) //nolint
		}
	}
	return nil
}

// deleteRedundantProfileFiles deletes too many files
func deleteRedundantProfileFiles(files []os.FileInfo, directory string, maxNum int) error {
	// if the number of files exceeds the threshold, delete the oldest file
	if len(files) > maxNum {
		sort.Sort(byModTime(files))
		filesToDelete := files[:len(files)-maxNum]

		for _, file := range filesToDelete {
			filePath := filepath.Join(directory, file.Name())
			_ = os.Remove(filePath) //nolint
		}
	}
	return nil
}

// byModTime Used to sort files by modification time
type byModTime []os.FileInfo

func (s byModTime) Len() int {
	return len(s)
}

func (s byModTime) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s byModTime) Less(i, j int) bool {
	return s[i].ModTime().Before(s[j].ModTime())
}
