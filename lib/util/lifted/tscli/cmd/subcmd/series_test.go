// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package subcmd

import (
	"errors"
	"testing"

	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/openGemini/opengemini-client-go/opengemini"
	"github.com/stretchr/testify/require"
)

func TestSearchSeriesKeys(t *testing.T) {
	getSeries := func(sid uint64, buf []byte, condition influxql.Expr, callback func(*influx.SeriesKey)) error {
		require.Equal(t, uint64(42), sid)
		require.Nil(t, condition)
		// A tag array yields multiple keys; the API may reuse callback storage.
		key := &influx.SeriesKey{
			Measurement: []byte("cpu_0000"),
			TagSet:      []influx.TagKV{{Key: []byte("host"), Value: []byte("a")}},
		}
		callback(key)
		key.TagSet[0].Value[0] = 'b'
		callback(key)
		key.Measurement[0] = 'X'
		return nil
	}
	keys, err := searchSeriesKeys(getSeries, 42)
	require.NoError(t, err)
	require.Len(t, keys, 2)
	for i, want := range []string{"cpu,host=a", "cpu,host=b"} {
		got, err := newTxtParser().parse2SeriesKeyWithoutVersion(keys[i], nil, false, &opengemini.Point{})
		require.NoError(t, err)
		require.Equal(t, want, string(got))
	}
}

func TestSearchSeriesKeysErrors(t *testing.T) {
	lookupErr := errors.New("lookup failed")
	getSeries := func(uint64, []byte, influxql.Expr, func(*influx.SeriesKey)) error {
		return lookupErr
	}
	keys, err := searchSeriesKeys(getSeries, 42)
	require.ErrorIs(t, err, lookupErr)
	require.Nil(t, keys)

	getSeries = func(uint64, []byte, influxql.Expr, func(*influx.SeriesKey)) error {
		return nil
	}
	keys, err = searchSeriesKeys(getSeries, 42)
	require.EqualError(t, err, "no series keys for series ID 42")
	require.Nil(t, keys)
}
