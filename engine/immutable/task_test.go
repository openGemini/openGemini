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

package immutable_test

import (
	"fmt"
	"testing"

	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/stretchr/testify/require"
)

func TestFullCompactOneFile(t *testing.T) {
	var begin int64 = 1e12
	defer beforeTest(t, 10)()
	schemas := getDefaultSchemas()

	immutable.SetMergeFlag4TsStore(1)
	mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
	defer mh.store.Close()
	rg := newRecordGenerator(begin, defaultInterval, false)

	conf := config.GetStoreConfig()
	conf.TSSPToParquetLevel = 2
	conf.Compact.CompactRecovery = true
	defer func() {
		conf.TSSPToParquetLevel = 0
		conf.Compact.CompactRecovery = false
	}()

	for i := 0; i < 5; i++ {
		rg.setBegin(begin).incrBegin(i + 5)
		mh.addRecord(100, rg.generate(schemas, 1))
		require.NoError(t, mh.saveToOrder())

		require.NoError(t, mh.store.FullCompact(1))
		mh.store.Wait()
		fmt.Println(len(mh.store.Order["mst"].Files()))
	}

	require.NoError(t, mh.store.FullCompact(1))
	mh.store.Wait()
	require.Equal(t, 1, len(mh.store.Order["mst"].Files()))
}
