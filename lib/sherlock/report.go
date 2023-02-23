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

package sherlock

import (
	"bytes"
	"fmt"
	"os"
	"path"
	"time"
)

func createAndGetFileInfo(filePath string, dumpType configureType) (*os.File, string, error) {
	filepath := formatFilename(filePath, dumpType)
	f, err := os.OpenFile(filepath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0640)
	if err != nil && os.IsNotExist(err) {
		if err = os.MkdirAll(filePath, 0750); err != nil {
			return nil, filepath, err
		}
		f, err = os.OpenFile(filepath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0640)
		if err != nil {
			return nil, filepath, err
		}
	}
	return f, filepath, err
}

func formatFilename(filePath string, dumpType configureType) string {
	suffix := time.Now().Format("20060102150405.000") + ".pb.gz"
	return path.Join(filePath, check2name[dumpType]+"."+suffix)
}

func writeFile(data bytes.Buffer, dumpType configureType, dumpOpts *dumpOptions) (string, error) {
	buf := data.Bytes()

	file, filename, err := createAndGetFileInfo(dumpOpts.dumpPath, dumpType)
	if err != nil {
		return filename, fmt.Errorf("pprof %v open file failed : %w", type2name[dumpType], err)
	}
	defer file.Close() //nolint

	if _, err = file.Write(buf); err != nil {
		return filename, fmt.Errorf("pprof %v write to file failed : %w", type2name[dumpType], err)
	}
	return filename, nil
}
