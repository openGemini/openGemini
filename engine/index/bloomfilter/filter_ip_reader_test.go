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

package bloomfilter

import (
	"os"
	"testing"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/fileops"
	indextype "github.com/openGemini/openGemini/lib/index"
	"github.com/openGemini/openGemini/lib/logstore"
	"github.com/openGemini/openGemini/lib/tokenizer"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/stretchr/testify/assert"
)

func TestReadLineFilterIp(t *testing.T) {
	version := uint32(4)
	tmpDir := t.TempDir()
	config.SetSFSConfig(tmpDir)
	defer os.RemoveAll(tmpDir)

	fileName := "00000001-0001-00000000.bloomfilter_ip.bf"
	filterLogName := tmpDir + "/" + fileName
	filterLogFd, err := fileops.OpenFile(filterLogName, os.O_CREATE|os.O_RDWR, 0640)
	if err != nil {
		t.Errorf("open file failed: %s", err)
	}

	output := make([]byte, logstore.GetConstant(version).FilterDataDiskSize)

	tokenizerIp := tokenizer.NewIpTokenizer()
	tokenizerIp.ProcessTokenizerBatch([]byte("1.2.3.4"), output, []int32{0}, []int32{int32(len([]byte("1.2.3.4")))})
	filterLogFd.Write(output)
	filterLogFd.Sync()

	expr := &influxql.BinaryExpr{
		Op:  influxql.EQ,
		LHS: &influxql.VarRef{Val: "ip"},
		RHS: &influxql.StringLiteral{Val: "1.2.3.4"},
	}
	splitMap := make(map[string][]byte)

	filterReader, err := CreateFilterReader(indextype.BloomFilterIp, tmpDir, nil, expr, 4, splitMap, fileName)
	if err != nil {
		t.Errorf("NewMultiFiledLineFilterReader failed: %s", err)
	}
	filterReader.StartSpan(nil)
	isExist, _ := filterReader.IsExist(int64(0), nil)
	assert.True(t, isExist)
}

func TestReadLineFilterIpSubnet(t *testing.T) {
	version := uint32(4)
	tmpDir := t.TempDir()
	config.SetSFSConfig(tmpDir)
	defer os.RemoveAll(tmpDir)

	fileName := "00000001-0001-00000000.bloomfilter_ip.bf"
	filterLogName := tmpDir + "/" + fileName
	filterLogFd, err := fileops.OpenFile(filterLogName, os.O_CREATE|os.O_RDWR, 0640)
	if err != nil {
		t.Errorf("open file failed: %s", err)
	}

	output := make([]byte, logstore.GetConstant(version).FilterDataDiskSize)

	tokenizerIp := tokenizer.NewIpTokenizer()
	tokenizerIp.ProcessTokenizerBatch([]byte("1.2.3.4"), output, []int32{0}, []int32{int32(len([]byte("1.2.3.4")))})
	filterLogFd.Write(output)
	filterLogFd.Sync()

	expr := &influxql.BinaryExpr{
		Op:  influxql.IPINRANGE,
		LHS: &influxql.VarRef{Val: "ip"},
		RHS: &influxql.StringLiteral{Val: "1.2.3.0/24"},
	}
	splitMap := make(map[string][]byte)

	filterReader, err := CreateFilterReader(indextype.BloomFilterIp, tmpDir, nil, expr, 4, splitMap, fileName)
	if err != nil {
		t.Errorf("NewMultiFiledLineFilterReader failed: %s", err)
	}
	filterReader.StartSpan(nil)
	isExist, _ := filterReader.IsExist(int64(0), nil)
	assert.True(t, isExist)
}
