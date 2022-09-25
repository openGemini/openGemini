package influx

/*
Copyright 2019-2021 VictoriaMetrics, Inc.
This code is originally from: https://github.com/VictoriaMetrics/VictoriaMetrics/tree/v1.67.0/lib/protoparser/influx/parser_test.go and has been modified.

2022.01.23 Add parser test for field of strings.
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
*/

import (
	"testing"
)

func TestUnmarshalRows(t *testing.T) {
	f := func(dst []Row, s string, tagsPool []Tag, fieldsPool []Field, nExpected int) ([]Row, []Tag, []Field) {
		t.Helper()
		dst, tagsPool, fieldsPool, _ = unmarshalRows(dst, s, tagsPool, fieldsPool)
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

			newRow := Row{Name: "cpu"}
			_, indexOptionPool, _ = newRow.unmarshalIndexOptions(dst, indexOptionPool)
			if indexOptionPool[0].IndexList[0] != 0 {
				t.Fatalf("unexpected len for IndexOptionsMarshalUnmarshal got %d; want %s", indexOptionPool[0].IndexList[0], "value")
			}
		}
	}

	rows := []Row{{
		Name: "cpu",
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
