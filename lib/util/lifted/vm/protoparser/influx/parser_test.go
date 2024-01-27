package influx

/*
Copyright 2019-2021 VictoriaMetrics, Inc.
This code is originally from: https://github.com/VictoriaMetrics/VictoriaMetrics/tree/v1.67.0/lib/protoparser/influx/parser_test.go and has been modified.

2022.01.23 Add parser test for field of strings.
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
*/

import (
	"strings"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnmarshalRows(t *testing.T) {
	enableTagArray := false
	f := func(dst []Row, s string, tagsPool []Tag, fieldsPool []Field, nExpected int) ([]Row, []Tag, []Field) {
		t.Helper()
		dst, tagsPool, fieldsPool, _ = unmarshalRows(dst, s, tagsPool, fieldsPool, enableTagArray)
		n := len(dst)
		if n != nExpected {
			t.Fatalf("unexpected len for unmarshalRows %q; got %d; want %d", s, n, nExpected)
		}
		return dst, tagsPool, fieldsPool
	}

	var rows []Row
	var tagsPool []Tag
	var fieldsPool []Field

	req := "cpu,hostname=host_0,region=eu-west-1,datacenter=eu-west-1c,rack=87,os=Ubuntu16.04LTS,arch=x64,team=NYC,service=18,service_version=1,service_environment=production usage_user=58i,usage_system=2i,usage_idle=24i,usage_nice=61i,usage_iowait=22i,usage_irq=63i,usage_softirq=6i,usage_steal=44i,usage_guest=80i,usage_guest_nice=38f 1622851200000000000\ncpu,hostname=host_1,region=ap-southeast-1,datacenter=ap-southeast-1b,rack=97,os=Ubuntu15.10,arch=x86,team=LON,service=12,service_version=0,service_environment=production usage_user=47i,usage_system=93i,usage_idle=16i,usage_nice=23i,usage_iowait=29i,usage_irq=48i,usage_softirq=5i,usage_steal=63i,usage_guest=17i,usage_guest_nice=52i 1622851200000000000\ncpu,hostname=host_2,region=eu-central-1,datacenter=eu-central-1a,rack=26,os=Ubuntu16.04LTS,arch=x64,team=SF,service=14,service_version=0,service_environment=staging usage_user=93i,usage_system=39i,usage_idle=16i,usage_nice=69i,usage_iowait=17i,usage_irq=62i,usage_softirq=80i,usage_steal=20i,usage_guest=2i,usage_guest_nice=26i 1622851200000000000\ncpu,hostname=host_3,region=us-east-1,datacenter=us-east-1a,rack=7,os=Ubuntu16.04LTS,arch=x64,team=NYC,service=19,service_version=0,service_environment=test usage_user=55i,usage_system=20i,usage_idle=15i,usage_nice=61i,usage_iowait=30i,usage_irq=19i,usage_softirq=20i,usage_steal=5i,usage_guest=16i,usage_guest_nice=65i 1622851200000000000\n"
	rows, tagsPool, fieldsPool = f(rows[:0], req, tagsPool[:0], fieldsPool[:0], 4)

	req = "cpu,hostname=host_0,region=eu-west-1,datacenter=eu-west-1c,rack=87,os=Ubuntu16.04LTS,arch=x64,team=NYC,service=18,service_version=1,service_environment=production usage_user=58i,usage_system=2i,usage_idle=24i,usage_nice=61i,usage_iowait=22i,usage_irq=63i,usage_softirq=6i,usage_steal=44i,usage_guest=80i,usage_guest_nice=38i 1622851200000000000\n"
	rows, tagsPool, fieldsPool = f(rows[:0], req, tagsPool[:0], fieldsPool[:0], 1)

	req = "cpu,hostname=host_0,region=eu-west-1,datacenter=eu-west-1c,rack=87,os=Ubuntu16.04LTS,arch=x64,team=NYC,service=18,service_version=1,service_environment=production hostName=\"service1\",usage_user=58i,usage_system=2i,usage_idle=true 1622851200000000000\n"
	rows, tagsPool, fieldsPool = f(rows[:0], req, tagsPool[:0], fieldsPool[:0], 1)

	req = "cpu,hostname=host_1 hostName=\"service1\",usage_user=58i,usage_system=2f,usage_idle=true 1622851200000000000\n"
	rows, tagsPool, fieldsPool = f(rows[:0], req, tagsPool[:0], fieldsPool[:0], 1)

	req = "cpu,hostname=host_0,region=eu-west-1,datacenter=eu-west-1c,rack=87,os=Ubuntu16.04LTS,arch=x64,team=NYC,service=18,service_version=1,service_environment=production hostName=\"service1,usage_user=58i,usage_system=2,usage_idle=true 1622851200000000000\n"
	rows, tagsPool, fieldsPool = f(rows[:0], req, tagsPool[:0], fieldsPool[:0], 0)

	req = "cpu,hostname=host_1 hostName=\"service1\",usage_user=58i,usage_system=2f,usage_idle=true \"2000-01-01T00:00:00Z\"\n"
	rows, tagsPool, fieldsPool = f(rows[:0], req, tagsPool[:0], fieldsPool[:0], 0)
}

func TestUnmarshalRows_error(t *testing.T) {
	enableTagArray := false
	f := func(dst []Row, s string, tagsPool []Tag, fieldsPool []Field, expectedErr string) {
		t.Helper()
		_, _, _, err := unmarshalRows(dst, s, tagsPool, fieldsPool, enableTagArray)
		if err != nil {
			require.EqualError(t, err, expectedErr)
		} else {
			require.Equal(t, "", expectedErr)
		}
	}

	var rows []Row
	var tagsPool []Tag
	var fieldsPool []Field

	req := "cpu,hostname=,region=eu-west-1 usage_user=58i,usage_system=2i 1622851200000000000\ncpu,hostname,region usage_user=47i,usage_system=93i 1622851200000000000\n"
	f(rows[:0], req, tagsPool[:0], fieldsPool[:0], `missing tag value for "hostname"`)
	req = "cpu,hostname=,region=eu-west-1 usage_user=58i,usage_system=2i 1622851200000000000\ncpu,hostname,region usage_user=47i,usage_system=93i 1622851200000000000\ncpu,hostname=host_2,region=eu-central-1  usage_user=93i,usage_system=39i 1622851200000000000\n"
	f(rows[:0], req, tagsPool[:0], fieldsPool[:0], `missing tag value for "hostname"`)

	req = `cpu,host=server01 value="disk mem" 1610467200000000000`
	f(rows[:0], req, tagsPool[:0], fieldsPool[:0], "")
	req = `cpu,host=server01 value="disk\ mem" 1610467300000000000`
	f(rows[:0], req, tagsPool[:0], fieldsPool[:0], "")
	req = `cpu,host=server01 value="disk\\ mem" 1610467400000000000`
	f(rows[:0], req, tagsPool[:0], fieldsPool[:0], "")
	req = `cpu,host=server01 value="disk\\\ mem" 1610467500000000000`
	f(rows[:0], req, tagsPool[:0], fieldsPool[:0], "")
	req = `cpu,host=server01 value="disk\\\\ mem" 1610467600000000000`
	f(rows[:0], req, tagsPool[:0], fieldsPool[:0], "")
	req = `cpu,host=server01 value="disk\ mem\ host " 1610467700000000000`
	f(rows[:0], req, tagsPool[:0], fieldsPool[:0], "")
	req = `cpu,host=server01 value="disk\ mem\\ host " 1610467800000000000`
	f(rows[:0], req, tagsPool[:0], fieldsPool[:0], "")
	req = `cpu,host=server01 value="disk\ mem\\\ host " 1610467900000000000`
	f(rows[:0], req, tagsPool[:0], fieldsPool[:0], "")
	req = `cpu,host=server01 value="disk\ mem\\\\ host " 1610468000000000000`
	f(rows[:0], req, tagsPool[:0], fieldsPool[:0], "")
	req = `cpu,host=server01 value="disk\\ mem\ host " 1610468100000000000`
	f(rows[:0], req, tagsPool[:0], fieldsPool[:0], "")
	req = `cpu,host=server01 value="disk\\ mem\\ host " 1610468200000000000`
	f(rows[:0], req, tagsPool[:0], fieldsPool[:0], "")
	req = `cpu,host=server01 value="disk\\\ mem\ host " 1610468300000000000`
	f(rows[:0], req, tagsPool[:0], fieldsPool[:0], "")
	req = `cpu,host=server01 value="disk\\\\ mem\ host " 1610468400000000000`
	f(rows[:0], req, tagsPool[:0], fieldsPool[:0], "")
	req = `cpu,host=server01 value="disk\" mem\\\"" 1610468500000000000`
	f(rows[:0], req, tagsPool[:0], fieldsPool[:0], "")
	req = `cpu,host=server01 value="disk\" mem\\\" host\\" 1610468600000000000`
	f(rows[:0], req, tagsPool[:0], fieldsPool[:0], "")

}

func TestNextUnquotedChar(t *testing.T) {
	f := func(s string, ch byte, noUnescape bool, nExpected int) {
		t.Helper()
		n := nextUnquotedChar(s, ch, noUnescape, true)
		if n != nExpected {
			t.Fatalf("unexpected n for nextUnqotedChar(%q, '%c', %v); got %d; want %d", s, ch, noUnescape, n, nExpected)
		}
	}

	f(``, ' ', false, -1)
	f(``, ' ', true, -1)
	f(`""`, ' ', false, -1)
	f(`""`, ' ', true, -1)
	f(`"foo bar\" " baz`, ' ', false, 12)
	f(`"foo bar\" " baz`, ' ', true, 10)
}

func TestIndexOptionsMarshalUnmarshal(t *testing.T) {
	funcMarshal := func(dst []byte, rows []Row, indexOptionPool []IndexOption) {
		t.Helper()
		for _, row := range rows {
			dst, _ = row.marshalIndexOptions(dst)

			newRow := Row{Name: "cpu_0001"}
			_, indexOptionPool, _ = newRow.unmarshalIndexOptions(dst, indexOptionPool)
			if indexOptionPool[0].IndexList[0] != 0 {
				t.Fatalf("unexpected len for IndexOptionsMarshalUnmarshal got %d; want %s", indexOptionPool[0].IndexList[0], "value")
			}
		}
	}

	rows := []Row{{
		Name: "cpu_0001",
		Tags: PointTags{
			Tag{
				Key:   "hostname",
				Value: "host_0",
			},
		},
		Fields: Fields{
			Field{
				Key:      "value",
				NumValue: 1.0,
				Type:     3,
			},
		},
		Timestamp: 1622851200000000000,
		IndexOptions: []IndexOption{{
			IndexList: []uint16{0},
			Oid:       1,
		},
		},
	},
	}
	var dst []byte
	var indexOpt []IndexOption
	funcMarshal(dst, rows, indexOpt)
}

func TestGetNameWithVersion(t *testing.T) {
	assert.Equal(t, "mst_0001", GetNameWithVersion("mst", 1))
	assert.Equal(t, "mst_000a", GetNameWithVersion("mst", 10))
	assert.Equal(t, "mst_000f", GetNameWithVersion("mst", 15))

	assert.Equal(t, "mst_0010", GetNameWithVersion("mst", 16))
	assert.Equal(t, "mst_0011", GetNameWithVersion("mst", 17))
	assert.Equal(t, "mst_0111", GetNameWithVersion("mst", 1<<8+1<<4+1))
	assert.Equal(t, "mst_1111", GetNameWithVersion("mst", 1<<12+1<<8+1<<4+1))

	assert.Equal(t, "mst_0100", GetNameWithVersion("mst", 1<<8))
	assert.Equal(t, "mst_1000", GetNameWithVersion("mst", 1<<12))
}

func BenchmarkGetNameWithVersion(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		GetNameWithVersion("some_string", 65533)
	}
}

func TestUnmarshalShardKeyByTagOp(t *testing.T) {
	r := Row{
		Name:          "mst",
		Tags:          PointTags{Tag{Key: "tag1", Value: "v1"}, Tag{Key: "tag2", Value: "v2"}, Tag{Key: "tag3", Value: "v3"}},
		Fields:        Fields{Field{Key: "fk1", NumValue: 1}},
		ColumnToIndex: map[string]int{"tag1": 0, "tag2": 1, "tag3": 2},
	}
	err := r.UnmarshalShardKeyByTagOp([]string{"tag1", "tag2", "tag3", "tag4"})
	assert.EqualError(t, err, "point should have all shard key")

	r = Row{
		Name:          "mst",
		Tags:          PointTags{Tag{Key: "tag1", Value: "v1"}, Tag{Key: "tag2", Value: "v2"}, Tag{Key: "tag3", Value: "v3"}},
		Fields:        Fields{Field{Key: "fk1", NumValue: 1}},
		ColumnToIndex: map[string]int{"tag1": 0, "tag2": 2, "tag3": 1},
	}
	err = r.UnmarshalShardKeyByTagOp([]string{"tag1", "tag2", "tag3"})
	assert.EqualError(t, err, "point should have all shard key")

}

func TestUnmarshalRows_With_TagArray(t *testing.T) {
	enableTagArray := true
	f := func(dst []Row, s string, tagsPool []Tag, fieldsPool []Field, nExpected int) ([]Row, []Tag, []Field) {
		t.Helper()
		dst, tagsPool, fieldsPool, _ = unmarshalRows(dst, s, tagsPool, fieldsPool, enableTagArray)
		n := len(dst)
		if n != nExpected {
			t.Fatalf("unexpected len for unmarshalRows %q; got %d; want %d", s, n, nExpected)
		}
		return dst, tagsPool, fieldsPool
	}

	var rows []Row
	var tagsPool []Tag
	var fieldsPool []Field

	req := "cpu,hostname=host_0,region=eu-west-1,datacenter=eu-west-1c,rack=87,os=Ubuntu16.04LTS,arch=[x64,x86],team=NYC,service=18,service_version=1,service_environment=production usage_user=58i,usage_system=2i,usage_idle=24i,usage_nice=61i,usage_iowait=22i,usage_irq=63i,usage_softirq=6i,usage_steal=44i,usage_guest=80i,usage_guest_nice=38f 1622851200000000000\ncpu,hostname=host_1,region=ap-southeast-1,datacenter=ap-southeast-1b,rack=97,os=Ubuntu15.10,arch=x86,team=LON,service=12,service_version=0,service_environment=production usage_user=47i,usage_system=93i,usage_idle=16i,usage_nice=23i,usage_iowait=29i,usage_irq=48i,usage_softirq=5i,usage_steal=63i,usage_guest=17i,usage_guest_nice=52i 1622851200000000000\ncpu,hostname=host_2,region=eu-central-1,datacenter=eu-central-1a,rack=26,os=Ubuntu16.04LTS,arch=x64,team=SF,service=14,service_version=0,service_environment=staging usage_user=93i,usage_system=39i,usage_idle=16i,usage_nice=69i,usage_iowait=17i,usage_irq=62i,usage_softirq=80i,usage_steal=20i,usage_guest=2i,usage_guest_nice=26i 1622851200000000000\ncpu,hostname=host_3,region=us-east-1,datacenter=us-east-1a,rack=7,os=Ubuntu16.04LTS,arch=x64,team=NYC,service=19,service_version=0,service_environment=test usage_user=55i,usage_system=20i,usage_idle=15i,usage_nice=61i,usage_iowait=30i,usage_irq=19i,usage_softirq=20i,usage_steal=5i,usage_guest=16i,usage_guest_nice=65i 1622851200000000000\n"
	rows, tagsPool, fieldsPool = f(rows[:0], req, tagsPool[:0], fieldsPool[:0], 4)

	req = "cpu,hostname=host_0,region=eu-west-1,datacenter=eu-west-1c,rack=87,os=Ubuntu16.04LTS,arch=[x64,x86],team=NYC,service=18,service_version=1,service_environment=production usage_user=58i,usage_system=2i,usage_idle=24i,usage_nice=61i,usage_iowait=22i,usage_irq=63i,usage_softirq=6i,usage_steal=44i,usage_guest=80i,usage_guest_nice=38i 1622851200000000000\n"
	rows, tagsPool, fieldsPool = f(rows[:0], req, tagsPool[:0], fieldsPool[:0], 1)

	req = "cpu,hostname=host_0,region=eu-west-1,datacenter=eu-west-1c,rack=87,os=Ubuntu16.04LTS,arch=[x64,x86],team=NYC,service=18,service_version=1,service_environment=production hostName=\"service1\",usage_user=58i,usage_system=2i,usage_idle=true 1622851200000000000\n"
	rows, tagsPool, fieldsPool = f(rows[:0], req, tagsPool[:0], fieldsPool[:0], 1)

	req = "cpu,hostname=host_1 hostName=\"service1\",usage_user=58i,usage_system=2f,usage_idle=true 1622851200000000000\n"
	rows, tagsPool, fieldsPool = f(rows[:0], req, tagsPool[:0], fieldsPool[:0], 1)

	req = "cpu,hostname=host_0,region=eu-west-1,datacenter=eu-west-1c,rack=87,os=Ubuntu16.04LTS,arch=[x64,x86],team=NYC,service=18,service_version=1,service_environment=production hostName=\"service1,usage_user=58i,usage_system=2,usage_idle=true 1622851200000000000\n"
	rows, tagsPool, fieldsPool = f(rows[:0], req, tagsPool[:0], fieldsPool[:0], 0)

	req = "cpu,hostname=host_1 hostName=\"service1\",usage_user=58i,usage_system=2f,usage_idle=true \"2000-01-01T00:00:00Z\"\n"
	rows, tagsPool, fieldsPool = f(rows[:0], req, tagsPool[:0], fieldsPool[:0], 0)
}

func TestUnmarshalRows_With_TagArray_Error(t *testing.T) {
	enableTagArray := true
	var err error
	f := func(dst []Row, s string, tagsPool []Tag, fieldsPool []Field, nExpected int) ([]Row, []Tag, []Field, error) {
		t.Helper()
		dst, tagsPool, fieldsPool, err = unmarshalRows(dst, s, tagsPool, fieldsPool, enableTagArray)
		return dst, tagsPool, fieldsPool, err
	}

	var rows []Row
	var tagsPool []Tag
	var fieldsPool []Field

	req := "cpu,hostname=host_0,arch=]x64,x86[,team=NYC hostName=\"service1\",usage_user=58i,usage_system=2,usage_idle=true 1622851200000000000\n"
	rows, tagsPool, fieldsPool, err = f(rows[:0], req, tagsPool[:0], fieldsPool[:0], 0)
	if !strings.Contains(err.Error(), "missing tag value for") {
		t.Fatalf("unexpected result for unmarshalRows %q", req)
	}

	req = "cpu,hostname=host_0,arch=[x64,x86[,team=NYC hostName=\"service1\",usage_user=58i,usage_system=2,usage_idle=true 1622851200000000000\n"
	rows, tagsPool, fieldsPool, err = f(rows[:0], req, tagsPool[:0], fieldsPool[:0], 0)
	if !strings.Contains(err.Error(), "missing tag value for") {
		t.Fatalf("unexpected result for unmarshalRows %q", req)
	}

	req = "cpu,hostname=host_0,arch=[x64,[x86]],hostName=\"service1\",usage_user=58i,usage_system=2,usage_idle=true 1622851200000000000\n"
	rows, tagsPool, fieldsPool, err = f(rows[:0], req, tagsPool[:0], fieldsPool[:0], 0)
	if !strings.Contains(err.Error(), "missing tag value for") {
		t.Fatalf("unexpected result for unmarshalRows %q", req)
	}

	req = "cpu,hostname=host_0,arch=a[x64,x86],hostName=\"service1\",usage_user=58i,usage_system=2,usage_idle=true 1622851200000000000\n"
	rows, tagsPool, fieldsPool, err = f(rows[:0], req, tagsPool[:0], fieldsPool[:0], 0)
	if !strings.Contains(err.Error(), "parse fail, point without fields is unsupported") {
		t.Fatalf("unexpected result for unmarshalRows %q", req)
	}

	req = "cpu,hostname=host_0,arch=[x64,x86]b,hostName=\"service1\",usage_user=58i,usage_system=2,usage_idle=true 1622851200000000000\n"
	rows, tagsPool, fieldsPool, err = f(rows[:0], req, tagsPool[:0], fieldsPool[:0], 0)
	if !strings.Contains(err.Error(), "missing tag value for") {
		t.Fatalf("unexpected result for unmarshalRows %q", req)
	}

	req = "cpu,hostname=host_0,arch=[x64,x86,],team=NYC hostName=\"service1\",usage_user=58i,usage_system=2,usage_idle=true 1622851200000000000\n"
	rows, tagsPool, fieldsPool, err = f(rows[:0], req, tagsPool[:0], fieldsPool[:0], 1)
	n := len(rows)
	if n != 1 {
		t.Fatalf("unexpected len for unmarshalRows %q; got %d; want %d", req, n, 1)
	}

	req = "cpu,hostname=host_0,arch=[x64,x86,],team=[NYC,YC] hostName=\"service1\",usage_user=58i,usage_system=2,usage_idle=true 1622851200000000000\n"
	rows, tagsPool, fieldsPool, err = f(rows[:0], req, tagsPool[:0], fieldsPool[:0], 1)
	n = len(rows)
	if n != 1 {
		t.Fatalf("unexpected len for unmarshalRows %q; got %d; want %d", req, n, 1)
	}

	req = "cpu,hostname=host_0,arch=[x64,,],team=[NYC,YC] hostName=\"service1\",usage_user=58i,usage_system=2,usage_idle=true 1622851200000000000\n"
	rows, tagsPool, fieldsPool, err = f(rows[:0], req, tagsPool[:0], fieldsPool[:0], 1)
	n = len(rows)
	if n != 1 {
		t.Fatalf("unexpected len for unmarshalRows %q; got %d; want %d", req, n, 1)
	}

	req = "cpu,hostname=host_0,arch=[x64],team=[NYC,YC] hostName=\"service1\",usage_user=58i,usage_system=2,usage_idle=true 1622851200000000000\n"
	rows, tagsPool, fieldsPool, err = f(rows[:0], req, tagsPool[:0], fieldsPool[:0], 1)
	n = len(rows)
	if n != 1 {
		t.Fatalf("unexpected len for unmarshalRows %q; got %d; want %d", req, n, 1)
	}

	req = "cpu,hostname=host_0,arch=[x64\\,x86],team=[NYC,YC] hostName=\"service1\",usage_user=58i,usage_system=2,usage_idle=true 1622851200000000000\n"
	rows, tagsPool, fieldsPool, err = f(rows[:0], req, tagsPool[:0], fieldsPool[:0], 1)
	n = len(rows)
	if n != 1 {
		t.Fatalf("unexpected len for unmarshalRows %q; got %d; want %d", req, n, 1)
	}
}

func TestSqlUnmarshalTags_Enable_TagArray(t *testing.T) {
	enableTagArray := true
	tagStr := "tag1=tv1,tag=tv2,tag3=tv3,tag4=tv4,tag5=tv5,tag6=tv6,tag7=tv7,tag8=tv8,tag9=tv9,tag10=tv10"
	var tags []Tag
	tags, _ = unmarshalTags(tags, tagStr, true, enableTagArray)
	assert.Equal(t, len(tags), 10)
}

func TestSqlUnmarshalTags_Disable_TagArray(t *testing.T) {
	enableTagArray := false
	tagStr := "tag1=tv1,tag=tv2,tag3=tv3,tag4=tv4,tag5=tv5,tag6=tv6,tag7=tv7,tag8=tv8,tag9=tv9,tag10=tv10"
	var tags []Tag
	tags, _ = unmarshalTags(tags, tagStr, true, enableTagArray)
	assert.Equal(t, len(tags), 10)
}

func BenchmarkSqlUnmarshalTags_Enable_TagArray(b *testing.B) {
	enableTagArray := true
	tagStr := "tag1=tv1,tag=tv2,tag3=tv3,tag4=tv4,tag5=tv5,tag6=tv6,tag7=tv7,tag8=tv8,tag9=tv9,tag10=tv10"
	var tags []Tag
	for i := 0; i < b.N; i++ {
		_, _ = unmarshalTags(tags, tagStr, true, enableTagArray)
	}
}

func BenchmarkSqlUnmarshalTags_Disable_TagArray(b *testing.B) {
	enableTagArray := false
	tagStr := "tag1=tv1,tag=tv2,tag3=tv3,tag4=tv4,tag5=tv5,tag6=tv6,tag7=tv7,tag8=tv8,tag9=tv9,tag10=tv10"
	var tags []Tag
	for i := 0; i < b.N; i++ {
		_, _ = unmarshalTags(tags, tagStr, true, enableTagArray)
	}
}

func genMarshalTags(dst []byte, tags PointTags) []byte {
	dst = encoding.MarshalUint32(dst, uint32(len(tags)))
	for i := range tags {
		kl := len(tags[i].Key)
		dst = encoding.MarshalUint16(dst, uint16(kl)) //append(dst, uint8(kl))
		dst = append(dst, tags[i].Key...)
		vl := len(tags[i].Value)
		dst = encoding.MarshalUint16(dst, uint16(vl)) //append(dst, uint8(vl))
		dst = append(dst, tags[i].Value...)
	}
	return dst
}

func TestStoreUnmarshalTags_Enable_TagArray(t *testing.T) {
	tags := PointTags{Tag{Key: "tag1", Value: "v1"}, Tag{Key: "tag2", Value: "v2"}, Tag{Key: "tag3", Value: "v3"}}
	var tagStr []byte
	tagStr = genMarshalTags(tagStr, tags)

	row := &Row{}
	tagpool := []Tag{}
	row.unmarshalTags(tagStr, tagpool)
	assert.Equal(t, len(row.Tags), 3)
}

func TestStoreUnmarshalTags_Disable_TagArray(t *testing.T) {
	tags := PointTags{Tag{Key: "tag1", Value: "v1"}, Tag{Key: "tag2", Value: "v2"}, Tag{Key: "tag3", Value: "v3"}}
	var tagStr []byte
	tagStr = genMarshalTags(tagStr, tags)

	row := &Row{}
	tagpool := []Tag{}
	row.unmarshalTags(tagStr, tagpool)
	assert.Equal(t, len(row.Tags), 3)
}

func BenchmarkStoreUnmarshalTags_Enable_TagArray(b *testing.B) {
	tags := PointTags{Tag{Key: "tag1", Value: "v1"}, Tag{Key: "tag2", Value: "v2"}, Tag{Key: "tag3", Value: "v3"}}
	var tagStr []byte
	tagStr = genMarshalTags(tagStr, tags)

	row := &Row{}
	tagpool := []Tag{}
	for i := 0; i < b.N; i++ {
		row.unmarshalTags(tagStr, tagpool)
	}
}

func BenchmarkStoreUnmarshalTags_Disable_TagArray(b *testing.B) {
	tags := PointTags{Tag{Key: "tag1", Value: "v1"}, Tag{Key: "tag2", Value: "v2"}, Tag{Key: "tag3", Value: "v3"}}
	var tagStr []byte
	tagStr = genMarshalTags(tagStr, tags)

	row := &Row{}
	tagpool := []Tag{}
	for i := 0; i < b.N; i++ {
		row.unmarshalTags(tagStr, tagpool)
	}
}
