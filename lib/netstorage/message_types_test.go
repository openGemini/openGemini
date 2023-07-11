// Code generated by tmpl; DO NOT EDIT.
// https://github.com/benbjohnson/tmpl
//
// Source: message_types_test.go.tmpl

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

package netstorage_test

import (
	"reflect"
	"testing"

	store "github.com/openGemini/openGemini/lib/netstorage"
	"github.com/stretchr/testify/assert"
)

func TestMessageTypes(t *testing.T) {
	data := map[uint8][2]interface{}{
		store.UnknownMessage:                         {nil, nil},
		store.SeriesKeysRequestMessage:               {&store.SeriesKeysRequest{}, &store.SeriesKeysResponse{}},
		store.SeriesExactCardinalityRequestMessage:   {&store.SeriesExactCardinalityRequest{}, &store.SeriesExactCardinalityResponse{}},
		store.SeriesCardinalityRequestMessage:        {&store.SeriesCardinalityRequest{}, &store.SeriesCardinalityResponse{}},
		store.ShowTagValuesRequestMessage:            {&store.ShowTagValuesRequest{}, &store.ShowTagValuesResponse{}},
		store.ShowTagValuesCardinalityRequestMessage: {&store.ShowTagValuesCardinalityRequest{}, &store.ShowTagValuesCardinalityResponse{}},
		store.GetShardSplitPointsRequestMessage:      {&store.GetShardSplitPointsRequest{}, &store.GetShardSplitPointsResponse{}},
		store.DeleteRequestMessage:                   {&store.DeleteRequest{}, &store.DeleteResponse{}},
		store.ShowQueriesRequestMessage:              {&store.ShowQueriesRequest{}, &store.ShowQueriesResponse{}},
	}

	for typ, items := range data {
		req := store.NewMessage(typ)
		respTyp := store.GetResponseMessageType(typ)
		resp := store.NewMessage(respTyp)

		assert.Equal(t, reflect.TypeOf(req), reflect.TypeOf(items[0]),
			"incorrect request message, type: %d, epx: %s, got: %s",
			typ, reflect.TypeOf(req), reflect.TypeOf(items[0]))

		assert.Equal(t, reflect.TypeOf(resp), reflect.TypeOf(items[1]),
			"incorrect response message, type: %d, epx: %s, got: %s",
			respTyp, reflect.TypeOf(resp), reflect.TypeOf(items[1]))
	}
}
