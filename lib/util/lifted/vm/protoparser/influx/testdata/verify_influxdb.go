//go:build ignore

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"time"

	v1 "github.com/influxdata/influxdb/models"
	v2 "github.com/influxdata/influxdb/v2/models"
)

type testCase struct {
	Name, Input, Measurement         string
	Fields                           map[string]interface{}
	Influxdb1Fields, Influxdb2Fields map[string]interface{}
	Tags                             map[string]string
	Timestamp                        int64
	Error                            bool
}

func main() {
	data, err := os.ReadFile(os.Args[1])
	if err != nil {
		panic(err)
	}
	var cases []testCase
	if err := json.Unmarshal(data, &cases); err != nil {
		panic(err)
	}
	failures := 0
	check := func(version string, tc testCase, n int, err error, fields map[string]interface{}, timestamp int64, measurement string, tags map[string]string) {
		if tc.Error {
			if err == nil || n != 0 {
				fmt.Printf("FAIL %s %s: points=%d err=%v\n", version, tc.Name, n, err)
				failures++
			} else {
				fmt.Printf("PASS %s %s: rejected\n", version, tc.Name)
			}
			return
		}
		for k, v := range fields {
			if i, ok := v.(int64); ok {
				fields[k] = float64(i)
			}
		}
		expectedFields := tc.Fields
		if version == "v1.0.0" && tc.Influxdb1Fields != nil {
			expectedFields = tc.Influxdb1Fields
		}
		if version == "v2.0.0" && tc.Influxdb2Fields != nil {
			expectedFields = tc.Influxdb2Fields
		}
		expectedTime := tc.Timestamp
		if expectedTime == 0 {
			expectedTime = 99
		}
		expectedName := tc.Measurement
		if expectedName == "" {
			expectedName = "cpu"
		}
		if err != nil || n != 2 || !reflect.DeepEqual(fields, expectedFields) || timestamp != expectedTime || measurement != expectedName || len(tags) != len(tc.Tags) || (len(tc.Tags) > 0 && !reflect.DeepEqual(tags, tc.Tags)) {
			fmt.Printf("FAIL %s %s: points=%d fields=%#v timestamp=%d name=%q tags=%v err=%v\n", version, tc.Name, n, fields, timestamp, measurement, tags, err)
			failures++
		} else {
			fmt.Printf("PASS %s %s: fields=%#v\n", version, tc.Name, fields)
		}
	}
	for _, tc := range cases {
		input := tc.Input + "\n"
		if !tc.Error {
			input += "sentinel value=42i 42\n"
		}
		p1, e1 := v1.ParsePointsWithPrecision([]byte(input), time.Unix(0, 99), "n")
		var f1 map[string]interface{}
		var ts1 int64
		var name1 string
		tags1 := map[string]string{}
		if len(p1) > 0 {
			f1 = p1[0].Fields()
			ts1 = p1[0].UnixNano()
			name1 = p1[0].Name()
			for k, v := range p1[0].Tags() {
				tags1[k] = v
			}
		}
		check("v1.0.0", tc, len(p1), e1, f1, ts1, name1, tags1)
		p2, e2 := v2.ParsePointsWithPrecision([]byte(input), time.Unix(0, 99), "n")
		var f2 map[string]interface{}
		var ts2 int64
		var name2 string
		tags2 := map[string]string{}
		if len(p2) > 0 {
			var err error
			f2, err = p2[0].Fields()
			if err != nil {
				panic(err)
			}
			ts2 = p2[0].UnixNano()
			name2 = string(p2[0].Name())
			for _, t := range p2[0].Tags() {
				tags2[string(t.Key)] = string(t.Value)
			}
		}
		check("v2.0.0", tc, len(p2), e2, f2, ts2, name2, tags2)
	}
	if failures > 0 {
		os.Exit(1)
	}
}
