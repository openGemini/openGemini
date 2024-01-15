package tsi

/*
Copyright 2019-2022 VictoriaMetrics, Inc.
This code is originally from: https://github.com/VictoriaMetrics/VictoriaMetrics/blob/v1.67.0/lib/storage/index_db_test.go

2022.01.23 It has been modified and used for test merge rows in merge table
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
*/

import (
	"bytes"
	"reflect"
	"sort"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/openGemini/openGemini/engine/index/mergeindex"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/mergeset"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/assert"
)

func TestMergeIndexRows(t *testing.T) {
	f := func(items []string, expectedItems []string) {
		t.Helper()
		var data []byte
		var itemsB []mergeset.Item
		for _, item := range items {
			data = append(data, item...)
			itemsB = append(itemsB, mergeset.Item{
				Start: uint32(len(data) - len(item)),
				End:   uint32(len(data)),
			})
		}
		resultData, resultItemsB := mergeIndexRows(data, itemsB)
		if len(resultItemsB) != len(expectedItems) {
			t.Fatalf("unexpected len(resultItemsB); got %d; want %d", len(resultItemsB), len(expectedItems))
		}
		if !checkItemsSorted(resultData, resultItemsB) {
			t.Fatalf("result items aren't sorted; items:\n%q", resultItemsB)
		}
		buf := resultData
		for i, it := range resultItemsB {
			item := it.Bytes(resultData)
			if !bytes.HasPrefix(buf, item) {
				t.Fatalf("unexpected prefix for resultData #%d;\ngot\n%X\nwant\n%X", i, buf, item)
			}
			buf = buf[len(item):]
		}
		if len(buf) != 0 {
			t.Fatalf("unexpected tail left in resultData: %X", buf)
		}
		var resultItems []string
		for _, it := range resultItemsB {
			resultItems = append(resultItems, string(it.Bytes(resultData)))
		}
		if !reflect.DeepEqual(expectedItems, resultItems) {
			t.Fatalf("unexpected items;\ngot\n%X\nwant\n%X", resultItems, expectedItems)
		}
	}
	xy := func(nsPrefix byte, key, value string, tsids []uint64) string {
		name := "mn"
		dst := mergeindex.MarshalCommonPrefix(nil, nsPrefix)
		compositeKey := kbPool.Get()
		compositeKey.B = marshalCompositeTagKey(compositeKey.B[:0], []byte(name), []byte(key))
		dst = marshalTagValue(dst, compositeKey.B)
		dst = marshalTagValue(dst, []byte(value))
		kbPool.Put(compositeKey)

		for _, tsid := range tsids {
			dst = encoding.MarshalUint64(dst, tsid)
		}
		return string(dst)
	}
	x := func(key, value string, tsids []uint64) string {
		return xy(nsPrefixTagToTSIDs, key, value, tsids)
	}

	f(nil, nil)
	f([]string{}, nil)
	f([]string{"foo"}, []string{"foo"})
	f([]string{"a", "b", "c", "def"}, []string{"a", "b", "c", "def"})
	f([]string{"\x00", "\x00b", "\x00c", "\x00def"}, []string{"\x00", "\x00b", "\x00c", "\x00def"})
	f([]string{
		x("", "", []uint64{1}),
		x("", "", []uint64{2}),
		x("", "", []uint64{3}),
		x("", "", []uint64{4}),
	}, []string{
		x("", "", []uint64{1}),
		x("", "", []uint64{2, 3}),
		x("", "", []uint64{4}),
	})
	f([]string{
		x("", "", []uint64{1}),
		x("", "", []uint64{2}),
		x("", "", []uint64{3}),
	}, []string{
		x("", "", []uint64{1}),
		x("", "", []uint64{2}),
		x("", "", []uint64{3}),
	})
	f([]string{
		x("", "", []uint64{1}),
		x("", "", []uint64{2}),
		x("", "", []uint64{3}),
		x("", "", []uint64{4}),
		"xyz",
	}, []string{
		x("", "", []uint64{1}),
		x("", "", []uint64{2, 3, 4}),
		"xyz",
	})
	f([]string{
		"\x00asdf",
		x("", "", []uint64{1}),
		x("", "", []uint64{2}),
		x("", "", []uint64{3}),
		x("", "", []uint64{4}),
	}, []string{
		"\x00asdf",
		x("", "", []uint64{1, 2, 3}),
		x("", "", []uint64{4}),
	})
	f([]string{
		"\x00asdf",
	}, []string{
		"\x00asdf",
	})
	f([]string{
		"\x00asdf",
		x("", "", []uint64{1}),
		x("", "", []uint64{2}),
		x("", "", []uint64{3}),
		x("", "", []uint64{4}),
		"xyz",
	}, []string{
		"\x00asdf",
		x("", "", []uint64{1, 2, 3, 4}),
		"xyz",
	})
	f([]string{
		"\x00asdf",
		x("", "", []uint64{1}),
		x("", "", []uint64{2}),
		"xyz",
	}, []string{
		"\x00asdf",
		x("", "", []uint64{1, 2}),
		"xyz",
	})
	f([]string{
		"\x00asdf",
		x("", "", []uint64{1}),
		x("", "", []uint64{2, 3, 4}),
		x("", "", []uint64{5, 6}),
		"foo",
	}, []string{
		"\x00asdf",
		x("", "", []uint64{1, 2, 3, 4, 5, 6}),
		"foo",
	})
	f([]string{
		"\x00asdf",
		x("", "", []uint64{1}),
		x("", "a", []uint64{2, 3, 4}),
		x("", "a", []uint64{5, 6}),
		x("", "b", []uint64{3, 5}),
		"foo",
	}, []string{
		"\x00asdf",
		x("", "", []uint64{1}),
		x("", "a", []uint64{2, 3, 4, 5, 6}),
		x("", "b", []uint64{3, 5}),
		"foo",
	})
	f([]string{
		"\x00asdf",
		x("", "", []uint64{1}),
		x("x", "a", []uint64{2, 3, 4}),
		x("y", "", []uint64{2, 3, 4, 5}),
		x("y", "x", []uint64{3, 5}),
		"foo",
	}, []string{
		"\x00asdf",
		x("", "", []uint64{1}),
		x("x", "a", []uint64{2, 3, 4}),
		x("y", "", []uint64{2, 3, 4, 5}),
		x("y", "x", []uint64{3, 5}),
		"foo",
	})

	// Construct big source chunks
	var tsids1 []uint64
	var tsids2 []uint64

	tsids1 = tsids1[:0]
	tsids2 = tsids1[:0]
	for i := 0; i < mergeindex.MaxTSIDsPerRow-1; i++ {
		tsids1 = append(tsids1, uint64(i))
		tsids2 = append(tsids2, uint64(i)+mergeindex.MaxTSIDsPerRow-1)
	}
	f([]string{
		"\x00aa",
		x("foo", "bar", tsids1),
		x("foo", "bar", tsids2),
		"x",
	}, []string{
		"\x00aa",
		x("foo", "bar", append(tsids1, tsids2...)),
		"x",
	})

	tsids1 = tsids1[:0]
	tsids2 = tsids2[:0]
	for i := 0; i < mergeindex.MaxTSIDsPerRow; i++ {
		tsids1 = append(tsids1, uint64(i))
		tsids2 = append(tsids2, uint64(i)+mergeindex.MaxTSIDsPerRow)
	}
	f([]string{
		"\x00aa",
		x("foo", "bar", tsids1),
		x("foo", "bar", tsids2),
		"x",
	}, []string{
		"\x00aa",
		x("foo", "bar", tsids1),
		x("foo", "bar", tsids2),
		"x",
	})

	tsids1 = tsids1[:0]
	tsids2 = tsids2[:0]
	for i := 0; i < 3*mergeindex.MaxTSIDsPerRow; i++ {
		tsids1 = append(tsids1, uint64(i))
		tsids2 = append(tsids2, uint64(i)+3*mergeindex.MaxTSIDsPerRow)
	}
	f([]string{
		"\x00aa",
		x("foo", "bar", tsids1),
		x("foo", "bar", tsids2),
		"x",
	}, []string{
		"\x00aa",
		x("foo", "bar", tsids1),
		x("foo", "bar", tsids2),
		"x",
	})
	f([]string{
		"\x00aa",
		x("foo", "bar", tsids1),
		x("foo", "bar", tsids2),
		x("foo", "bar", []uint64{997, 998, 999}),
		"x",
	}, []string{
		"\x00aa",
		x("foo", "bar", tsids1),
		x("foo", "bar", tsids2),
		x("foo", "bar", []uint64{997, 998, 999}),
		"x",
	})
}

func TestMergeItemsForLabelStore(t *testing.T) {
	f := func(items []string, expectedItems []string) {
		t.Helper()
		var data []byte
		var itemsB []mergeset.Item
		for _, item := range items {
			data = append(data, item...)
			itemsB = append(itemsB, mergeset.Item{
				Start: uint32(len(data) - len(item)),
				End:   uint32(len(data)),
			})
		}
		resultData, resultItemsB := mergeIndexRowsForColumnStore(data, itemsB, true)
		if len(resultItemsB) != len(expectedItems) {
			t.Fatalf("unexpected len(resultItemsB); got %d; want %d", len(resultItemsB), len(expectedItems))
		}
		if !checkItemsSorted(resultData, resultItemsB) {
			t.Fatalf("result items aren't sorted; items:\n%q", resultItemsB)
		}
		buf := resultData
		for i, it := range resultItemsB {
			item := it.Bytes(resultData)
			if !bytes.HasPrefix(buf, item) {
				t.Fatalf("unexpected prefix for resultData #%d;\ngot\n%X\nwant\n%X", i, buf, item)
			}
			buf = buf[len(item):]
		}
		if len(buf) != 0 {
			t.Fatalf("unexpected tail left in resultData: %X", buf)
		}
		var resultItems []string
		for _, it := range resultItemsB {
			resultItems = append(resultItems, string(it.Bytes(resultData)))
		}
		if !reflect.DeepEqual(expectedItems, resultItems) {
			t.Fatalf("unexpected items;\ngot\n%X\nwant\n%X", resultItems, expectedItems)
		}
	}

	xy := func(nsPrefix byte, key string, values []string) string {
		name := "mn"
		dst := mergeindex.MarshalCommonPrefix(nil, nsPrefix)
		compositeKey := kbPool.Get()
		compositeKey.B = marshalCompositeTagKey(compositeKey.B[:0], []byte(name), []byte(key))
		dst = marshalTagValue(dst, compositeKey.B)
		for _, value := range values {
			dst = marshalTagValue(dst, []byte(value))
		}
		kbPool.Put(compositeKey)

		return string(dst)
	}
	x := func(key string, values []string) string {
		return xy(nsPrefixTagKeysToTagValues, key, values)
	}

	f(nil, nil)
	f([]string{}, nil)
	f([]string{"foo"}, []string{"foo"})
	f([]string{
		"\x00aa",
		"dsa",
	}, []string{
		"\x00aa",
		"dsa",
	})
	f([]string{
		"\x00aa",
		x("a", []string{"a", "b"}),
		"dsa",
	}, []string{
		"\x00aa",
		x("a", []string{"a", "b"}),
		"dsa",
	})
	f([]string{
		"\x00aa",
		x("a", []string{"a"}),
		x("a", []string{"b"}),
		x("a", []string{"c"}),
		x("a", []string{"d"}),
		"dsa",
	}, []string{
		"\x00aa",
		x("a", []string{"a", "b", "c", "d"}),
		"dsa",
	})
	f([]string{
		"\x00aa",
		x("a", []string{"a"}),
		x("a", []string{"a"}),
		x("a", []string{"a"}),
		x("a", []string{"b"}),
		x("a", []string{"c"}),
		x("a", []string{"c"}),
		x("a", []string{"c"}),
		x("a", []string{"d"}),
		x("a", []string{"d"}),
		"dsa",
	}, []string{
		"\x00aa",
		x("a", []string{"a", "b", "c", "d"}),
		"dsa",
	})
	f([]string{
		"\x00aa",
		x("a", []string{"a", "b"}),
		x("a", []string{"c", "d"}),
		"dsa",
	}, []string{
		"\x00aa",
		x("a", []string{"a", "b", "c", "d"}),
		"dsa",
	})
	f([]string{
		"\x00aa",
		x("a", []string{"a", "b"}),
		x("a", []string{"c", "d"}),
		"dsa",
	}, []string{
		"\x00aa",
		x("a", []string{"a", "b", "c", "d"}),
		"dsa",
	})
	f([]string{
		"\x00aa",
		x("a", []string{"c", "d"}),
		x("a", []string{"a", "b"}),
		x("b", []string{"c", "d"}),
		x("b", []string{"c", "d"}),
		x("b", []string{"a", "b"}),
		"dsa",
	}, []string{
		"\x00aa",
		x("a", []string{"a", "b", "c", "d"}),
		x("b", []string{"a", "b", "c", "d"}),
		"dsa",
	})
}

func checkItemsSorted(data []byte, items []mergeset.Item) bool {
	if len(items) == 0 {
		return true
	}
	prevItem := items[0].String(data)
	for _, it := range items[1:] {
		currItem := it.String(data)
		if prevItem > currItem {
			return false
		}
		prevItem = currItem
	}
	return true
}
func TestTagKeyReflection(t *testing.T) {
	r := NewTagKeyReflection([]string{"B", "A"}, []string{"A", "B"})
	assert.Equal(t, []int{1, 0}, r.order)
	assert.Equal(t, 2, len(r.buf))
}

func TestMakeGroupTagsKey(t *testing.T) {
	sortResult := []string{}
	r := genDimensionPosition([]string{"sex", "address"})
	tags := influx.PointTags{}
	tags = append(tags, influx.Tag{Key: "address", Value: "shanghai"})
	tags = append(tags, influx.Tag{Key: "age_region", Value: "teenager"})
	tags = append(tags, influx.Tag{Key: "country", Value: "china"})
	tags = append(tags, influx.Tag{Key: "sex", Value: "male"})
	res1 := MakeGroupTagsKey([]string{"address", "sex"}, tags, []byte{}, r)
	assert.Equal(t, "sex\x00male\x00address\x00shanghai", string(res1))
	sortResult = append(sortResult, string(res1))

	tags2 := influx.PointTags{}
	tags2 = append(tags2, influx.Tag{Key: "address", Value: "shanghai"})
	tags2 = append(tags2, influx.Tag{Key: "age_region", Value: "teenager"})
	tags2 = append(tags2, influx.Tag{Key: "country", Value: "china"})
	tags2 = append(tags2, influx.Tag{Key: "sex", Value: "female"})
	res2 := MakeGroupTagsKey([]string{"address", "sex"}, tags2, []byte{}, r)
	assert.Equal(t, "sex\x00female\x00address\x00shanghai", string(res2))
	sortResult = append(sortResult, string(res2))

	tags3 := influx.PointTags{}
	tags3 = append(tags3, influx.Tag{Key: "address", Value: "beijing"})
	tags3 = append(tags3, influx.Tag{Key: "age_region", Value: "teenager"})
	tags3 = append(tags3, influx.Tag{Key: "country", Value: "china"})
	tags3 = append(tags3, influx.Tag{Key: "sex", Value: "female"})
	res3 := MakeGroupTagsKey([]string{"address", "sex"}, tags3, []byte{}, r)
	assert.Equal(t, "sex\x00female\x00address\x00beijing", string(res3))
	sortResult = append(sortResult, string(res3))

	sort.Strings(sortResult)
	assert.Equal(t, []string{"sex\x00female\x00address\x00beijing", "sex\x00female\x00address\x00shanghai", "sex\x00male\x00address\x00shanghai"}, sortResult)
}
