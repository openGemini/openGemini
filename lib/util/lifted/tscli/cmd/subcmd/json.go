// Copyright 2025 openGemini Authors
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

package subcmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/openGemini/openGemini-cli/common"
	"github.com/openGemini/opengemini-client-go/opengemini"
)

const (
	PromTimeIndex  = 0
	PromValueIndex = 1
)

// JsonPResult prom json format
type JsonPResult struct {
	Metric map[string]string `json:"metric"`
	Values [][2]any          `json:"values,omitempty"`
	Value  [2]any            `json:"value,omitempty"`
}

func (fsm *ImportFileFSM) processJsonP(dec *json.Decoder) (FSMCall, error) {
	var res JsonPResult
	switch fsm.state {
	case importStateDDL:
		return func(ctx context.Context, command *ImportCommand) error {
			// prom json exp: {"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"localhost:9090","job":"prometheus"},"value":[1709258327.955,"2"]}]}}
			t, err := dec.Token()
			if err != nil {
				slog.Error("parse prom json failed", "reason", err)
				return err
			}
			if t == "result" {
				_, err := dec.Token() // skip [
				if err != nil {
					slog.Error("parse prom json failed", "reason", err)
					return err
				}
				fsm.state = importStateDML
				command.cfg.ColumnWrite = false // only support row protocol
				// setup fsm config
				cmdStr := fmt.Sprintf("CREATE DATABASE %s", command.cfg.Database)
				_, err = command.httpClient.Query(ctx, &opengemini.Query{
					Command: cmdStr, // CREATE DATABASE xxx
				})
				if err != nil {
					slog.Error("execute ddl failed", "reason", err, "command", cmdStr)
					return err
				}
				slog.Info("execute ddl success", "command", cmdStr)
				fsm.database = command.cfg.Database
				fsm.retentionPolicy = command.cfg.RetentionPolicy
				fsm.measurement = command.cfg.Measurement
				fsm.tagMap = make(map[string]FieldPos)
				fsm.fieldMap = make(map[string]FieldPos)
				if len(command.cfg.Fields) == 0 {
					command.cfg.Fields = []string{"value"}
				}
				for _, tag := range command.cfg.Tags { // tags
					fsm.tagMap[tag] = FieldPos{}
				}
				for _, field := range command.cfg.Fields { // fields
					fsm.fieldMap[field] = FieldPos{}
					break // field only one column
				}
			}
			return nil
		}, nil
	case importStateDML:
		return func(ctx context.Context, command *ImportCommand) error {
			if err := dec.Decode(&res); err != nil {
				slog.Error("parse prom json failed", "reason", err)
				return err
			}
			if command.fsm.database == "" {
				return errors.New("database is required")
			}
			if command.fsm.retentionPolicy == "" {
				command.fsm.retentionPolicy = common.DefaultRetentionPolicy // "autogen"
			}
			if command.cfg.Measurement == "" {
				return errors.New("measurement is required")
			}

			// parse to datas "m,location=coyote_creek,randtag=2 index=15 1566123456"
			var dataSlice []string
			sb := strings.Builder{}
			sb.WriteString(command.fsm.measurement)
			if len(command.fsm.tagMap) > 0 {
				for tag := range command.fsm.tagMap {
					sb.WriteString(",")
					sb.WriteString(tag)
					sb.WriteString("=")
					sb.WriteString(res.Metric[tag])
				}
			} else {
				for tag, v := range res.Metric {
					sb.WriteString(",")
					sb.WriteString(tag)
					sb.WriteString("=")
					sb.WriteString(v)
				}
			}
			line := sb.String()

			if len(res.Values) > 0 {
				for _, v := range res.Values {
					// v[0] is float64, parse to string
					dataSlice = append(dataSlice, fmt.Sprintf("%s %s=%s %s", line, command.cfg.Fields[0], v[PromValueIndex], strconv.FormatFloat(v[PromTimeIndex].(float64), 'f', -1, 64)))
				}
			} else {
				dataSlice = append(dataSlice, fmt.Sprintf("%s %s=%s %s", line, command.cfg.Fields[0], res.Value[PromValueIndex], strconv.FormatFloat(res.Value[PromTimeIndex].(float64), 'f', -1, 64)))
			}

			command.fsm.batchLPBuffer = append(command.fsm.batchLPBuffer, dataSlice...)
			if len(command.fsm.batchLPBuffer) < command.cfg.BatchSize {
				return nil
			}
			return command.excuteByLPBuffer(ctx)

		}, nil
	}

	return FSMCallEmpty, nil
}

// JsonIResult influx json format
type JsonIResult struct {
	Measurement string            `json:"name"`
	Tags        map[string]string `json:"tags,omitempty"`
	Fields      []string          `json:"columns"`
	Values      [][]any           `json:"values"`
}

func (fsm *ImportFileFSM) processJsonI(dec *json.Decoder) (FSMCall, error) {
	var res JsonIResult
	switch fsm.state {
	case importStateDDL:
		return func(ctx context.Context, command *ImportCommand) error {
			t, err := dec.Token()
			if err != nil {
				slog.Error("parse influx json failed", "reason", err)
				return err
			}

			if t == "series" {
				_, err := dec.Token() // skip [
				if err != nil {
					slog.Error("parse influx json failed", "reason", err)
					return err
				}

				fsm.state = importStateDML
				command.cfg.ColumnWrite = false // only support row protocol
				// update db, rp
				fsm.database = command.cfg.Database
				fsm.retentionPolicy = command.cfg.RetentionPolicy

				// create db
				cmdStr := fmt.Sprintf("CREATE DATABASE %s", command.cfg.Database)
				if _, err = command.httpClient.Query(ctx, &opengemini.Query{
					Command: cmdStr, // CREATE DATABASE xxx
				}); err != nil {
					slog.Error("execute ddl failed", "reason", err, "command", cmdStr)
					return err
				}
				slog.Info("execute ddl success", "command", cmdStr)
			}
			return nil
		}, nil
	case importStateDML:
		return func(ctx context.Context, command *ImportCommand) error {
			if err := dec.Decode(&res); err != nil {
				slog.Error("parse influx json failed", "reason", err)
				return err
			}
			if command.fsm.database == "" {
				return errors.New("database is required")
			}
			if command.fsm.retentionPolicy == "" {
				command.fsm.retentionPolicy = common.DefaultRetentionPolicy // "autogen"
			}

			// parse to datas "m,location=coyote_creek,randtag=2 index=15 1566123456"
			var dataSlice []string

			// parse struct
			line := res.Measurement
			if len(res.Tags) != 0 {
				for tag, val := range res.Tags {
					line += fmt.Sprintf(",%s=%s", tag, val)
				}
			}
			// reuse fsm config
			command.fsm.clearFieldConfig()
			for i, field := range res.Fields {
				if field == "time" {
					command.fsm.timeField = FieldPos{field, i}
				} else {
					command.fsm.fieldMap[field] = FieldPos{field, i}
				}
			}

			// parse the value of fields
			for _, value := range res.Values {
				var fields string
				var timestamp string
				var tidx int = -1 // time column not exist
				if command.fsm.timeField.Name != "" {
					tidx = command.fsm.timeField.Pos
				}
				sb := strings.Builder{}
				for _, field := range command.fsm.fieldMap {
					if field.Pos < len(value) {
						fk, fv := field.Name, value[field.Pos] // fields value (string, float64, int64, bool)  ->  string
						sb.WriteString(fk)
						sb.WriteString("=")
						sb.WriteString(command.parse2String(fv, TypeField))
						sb.WriteString(",")
					}
				}
				fields = sb.String()
				if len(fields) > 0 {
					fields = fields[:len(fields)-1]
				}

				if tidx != -1 && tidx < len(value) {
					tsp := value[tidx] // 1234567890 or "2010-07-01T18:48:00Z" -> "1234567890"
					timestamp = command.parse2String(tsp, TypeTimestamp)
				}

				if tidx == -1 || timestamp == "" {
					dataSlice = append(dataSlice, fmt.Sprintf("%s %s", line, fields))
				} else {
					dataSlice = append(dataSlice, fmt.Sprintf("%s %s %s", line, fields, timestamp))
				}

			}

			command.fsm.batchLPBuffer = append(command.fsm.batchLPBuffer, dataSlice...)
			if len(command.fsm.batchLPBuffer) < command.cfg.BatchSize {
				return nil
			}
			return command.excuteByLPBuffer(ctx)
		}, nil
	}

	return FSMCallEmpty, nil
}

func (fsm *ImportFileFSM) clearFieldConfig() {
	fsm.tagMap = make(map[string]FieldPos)
	fsm.fieldMap = make(map[string]FieldPos)
	fsm.timeField = FieldPos{}
}

const (
	TypeTimestamp = iota
	TypeField
)

func (c *ImportCommand) parse2String(s any, t int) string {
	if t == TypeField { // field
		switch s := s.(type) { // fields value (string, float64, int64, bool)  ->  string
		case float64:
			return strconv.FormatFloat(s, 'f', -1, 64)
		case int64:
			return strconv.FormatInt(s, 10)
		case int32:
			return strconv.FormatInt(int64(s), 10)
		case int:
			return strconv.FormatInt(int64(s), 10)
		case bool:
			if s {
				return "true"
			}
			return "false"
		case string:
			return fmt.Sprintf("\"%s\"", s)
		}
		return "\"\""
	} else if t == TypeTimestamp { // 1234567890 or "2010-07-01T18:48:00Z" -> "1234567890"
		switch s := s.(type) {
		case float64:
			return strconv.FormatFloat(s, 'f', -1, 64)
		case int64:
			return strconv.FormatInt(s, 10)
		case int32:
			return strconv.FormatInt(int64(s), 10)
		case int:
			return strconv.FormatInt(int64(s), 10)
		case string: // must parse "2010-07-01T18:48:00Z" ->  "1234567890"
			if tt, err := time.Parse(time.RFC3339, s); err != nil {
				return ""
			} else {
				tsp := tt.UnixNano() / c.cfg.TimeMultiplier
				return strconv.FormatFloat(float64(tsp), 'f', -1, 64)
			}
		}
	}
	return ""
}
