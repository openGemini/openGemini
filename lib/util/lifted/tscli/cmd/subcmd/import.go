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
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/openGemini/openGemini-cli/common"
	"github.com/openGemini/openGemini-cli/core"
	"github.com/openGemini/opengemini-client-go/opengemini"
	"github.com/openGemini/opengemini-client-go/proto"
	"github.com/valyala/fastjson/fastfloat"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

const (
	importFormatLineProtocol = "line_protocol"
	importFormatCSV          = "csv"
	importFormatJSONInflux   = "jsoni"
	importFormatJSONProm     = "jsonp"

	importTokenDDL             = "# DDL"
	importTokenDML             = "# DML"
	importTokenDatabase        = "# CONTEXT-DATABASE:"
	importTokenRetentionPolicy = "# CONTEXT-RETENTION-POLICY:"
	importTokenMeasurement     = "# CONTEXT-MEASUREMENT:"
	importTokenTags            = "# CONTEXT-TAGS:"
	importTokenFields          = "# CONTEXT-FIELDS:"
	importTokenTimeField       = "# CONTEXT-TIME:"
	timeFilterToken            = "# openGemini EXPORT:"
)

var (
	builderEntities = make(map[string]opengemini.WriteRequestBuilder)
)

func NewColumnWriterClient(cfg *ImportConfig) (proto.WriteServiceClient, error) {
	var dialOptions = []grpc.DialOption{
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                10 * time.Second,
			Timeout:             3 * time.Second,
			PermitWithoutStream: true,
		}),
		// https://github.com/grpc/grpc/blob/master/doc/connection-backoff.md
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  time.Second,
				Multiplier: 1.6,
				Jitter:     0.2,
				MaxDelay:   time.Second * 30,
			},
			MinConnectTimeout: time.Second * 20,
		}),
		grpc.WithInitialWindowSize(1 << 24),                                    // 16MB
		grpc.WithInitialConnWindowSize(1 << 24),                                // 16MB
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64 * 1024 * 1024)), // 64MB
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(64 * 1024 * 1024)), // 64MB
	}
	if cfg.EnableTls {
		var tlsManager, err = core.NewCertificateManager(cfg.CACert, cfg.Cert, cfg.CertKey)
		if err != nil {
			return nil, err
		}
		cred := credentials.NewTLS(tlsManager.CreateTls(cfg.InsecureTls, cfg.InsecureHostname))
		dialOptions = append(dialOptions, grpc.WithTransportCredentials(cred))
	} else {
		dialOptions = append(dialOptions, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	conn, err := grpc.NewClient(cfg.Host+":"+strconv.Itoa(cfg.ColumnWritePort), dialOptions...)
	if err != nil {
		return nil, err
	}
	return proto.NewWriteServiceClient(conn), nil
}

type ImportConfig struct {
	*core.CommandLineConfig
	Path            string
	Format          string
	ColumnWrite     bool
	ColumnWritePort int
	BatchSize       int
	Tags            []string
	Fields          []string
	TimeField       string
}

type ImportCommand struct {
	cfg         *ImportConfig
	httpClient  core.HttpClient
	writeClient proto.WriteServiceClient
	fsm         *ImportFileFSM
}

func (c *ImportCommand) Run(config *ImportConfig) error {
	if config.Format == "" {
		config.Format = importFormatLineProtocol
	}
	if config.BatchSize <= 0 {
		config.BatchSize = common.DefaultBatchSize
	}

	httpClient, err := core.NewHttpClient(config.CommandLineConfig)
	if err != nil {
		slog.Error("create http client failed", "reason", err)
		return err
	}
	c.httpClient = httpClient
	if config.ColumnWritePort == 0 {
		config.ColumnWritePort = common.DefaultColumnWritePort
	}
	c.writeClient, err = NewColumnWriterClient(config)
	if err != nil {
		slog.Error("create column writer client failed", "reason", err)
		return err
	}

	if err = config.configTimeMultiplier(); err != nil {
		slog.Error("create column writer client failed", "reason", err)
		return err
	}

	c.cfg = config
	c.fsm = new(ImportFileFSM)
	return c.process()
}

func (c *ImportCommand) process() error {
	file, err := os.Open(c.cfg.Path)
	if err != nil {
		slog.Error("open file failed", "file", c.cfg.Path, "reason", err)
		return err
	}
	defer file.Close()
	var ctx = context.Background()
	switch c.cfg.Format {
	case importFormatLineProtocol:
		scanner := bufio.NewReader(file)
		for {
			line, err := scanner.ReadBytes('\n')
			if err != nil {
				if err == io.EOF {
					if len(line) > 0 {
						fsmCall, err := c.fsm.processLineProtocol(string(line))
						if err != nil {
							slog.Error("process line protocol failed", "reason", err)
							break
						}
						err = fsmCall(ctx, c)
						if err != nil {
							slog.Error("call line protocol fsm function failed", "reason", err)
						}
					}
					break
				}
				slog.Error("read line failed", "reason", err)
				continue
			}
			fsmCall, err := c.fsm.processLineProtocol(string(line))
			if err != nil {
				slog.Error("process line protocol failed", "reason", err)
				continue
			}
			err = fsmCall(ctx, c)
			if err != nil {
				slog.Error("call line protocol fsm function failed", "reason", err)
				continue
			}
		}
		if err := c.fsm.clearBuffer()(ctx, c); err != nil {
			slog.Error("clear buffer failed", "reason", err)
		}
		slog.Info("process finished", "path", c.cfg.Path)
		return nil
	case importFormatCSV:
		slog.Info("tips: csv file import only support by column write protocol")
		csvReader := csv.NewReader(file)
		csvReader.Comment = '#'
		for {
			row, err := csvReader.Read()
			if err != nil {
				if err == io.EOF {
					break
				}
				slog.Error("read csv line failed", "reason", err)
				continue
			}
			fsmCall, err := c.fsm.processCSV(row)
			if err != nil {
				slog.Error("process csv line failed", "reason", err)
				continue
			}
			err = fsmCall(ctx, c)
			if err != nil {
				slog.Error("call csv line fsm function failed", "reason", err)
				continue
			}
		}
		if err := c.fsm.clearBuffer()(ctx, c); err != nil {
			slog.Error("clear buffer failed", "reason", err)
		}
		slog.Info("process finished", "path", c.cfg.Path)
		return nil
	// support jsonProm
	case importFormatJSONProm:
		slog.Info("tips: prom json file import only support by row write protocol")
		dec := json.NewDecoder(file)
		for dec.More() {
			fsmCall, err := c.fsm.processJsonP(dec)
			if err != nil {
				slog.Error("process prom json line failed", "reason", err)
				continue
			}
			err = fsmCall(ctx, c)
			if err != nil {
				slog.Error("call prom json fsm function failed", "reason", err)
				continue
			}
		}
		if err := c.fsm.clearBuffer()(ctx, c); err != nil {
			slog.Error("clear buffer failed", "reason", err)
		}
		slog.Info("process finished", "path", c.cfg.Path)
		return nil
	// support jsonInflux
	case importFormatJSONInflux:
		slog.Info("tips: influx json file import only support by row write protocol")
		dec := json.NewDecoder(file)
		for dec.More() {
			fsmCall, err := c.fsm.processJsonI(dec)
			if err != nil {
				slog.Error("process influx json line failed", "reason", err)
				continue
			}
			err = fsmCall(ctx, c)
			if err != nil {
				slog.Error("call influx json fsm function failed", "reason", err)
				continue
			}
		}
		if err := c.fsm.clearBuffer()(ctx, c); err != nil {
			slog.Error("clear buffer failed", "reason", err)
		}
		slog.Info("process finished", "path", c.cfg.Path)
		return nil
	default:
		return fmt.Errorf("unknown --format %s, only support line_protocol, csv", c.cfg.Format)
	}
}

type ImportState int

const (
	importStateDDL = iota
	importStateDML
)

type ImportFileFSM struct {
	state            ImportState
	database         string
	retentionPolicy  string
	measurement      string
	tagMap           map[string]FieldPos
	fieldMap         map[string]FieldPos
	timeField        FieldPos
	batchLPBuffer    []string
	batchPointBuffer []*opengemini.Point
}

type FieldPos struct {
	Name string
	Pos  int
}

type FSMCall func(ctx context.Context, command *ImportCommand) error

var FSMCallEmpty = func(ctx context.Context, command *ImportCommand) error { return nil }

// clear the last batch data
func (fsm *ImportFileFSM) clearBuffer() FSMCall {
	return func(ctx context.Context, command *ImportCommand) error {
		var errs error
		for len(fsm.batchLPBuffer) != 0 {
			err := command.excuteByLPBuffer(ctx)
			errs = errors.Join(errs, err)
		}

		if len(fsm.batchPointBuffer) != 0 { // only support column write
			err := command.executeByPointBuffer(ctx)
			errs = errors.Join(errs, err)
		}
		return errs
	}
}

func (fsm *ImportFileFSM) processLineProtocol(data string) (FSMCall, error) {
	if strings.HasPrefix(data, importTokenDDL) || strings.HasPrefix(data, timeFilterToken) {
		fsm.state = importStateDDL
		return FSMCallEmpty, nil
	}
	if strings.HasPrefix(data, importTokenDML) {
		fsm.state = importStateDML
		fsm.retentionPolicy = "autogen"
		return FSMCallEmpty, nil
	}
	switch fsm.state {
	case importStateDDL:
		data = strings.TrimSpace(data)
		if data == "" {
			return FSMCallEmpty, nil
		}
		return func(ctx context.Context, command *ImportCommand) error {
			_, err := command.httpClient.Query(ctx, &opengemini.Query{
				Command: data, // CREATE DATABASE NOAA_water_database
			})
			if err != nil {
				slog.Error("execute ddl failed", "reason", err, "command", data)
				return err
			}
			slog.Info("execute ddl success", "command", data)
			return nil
		}, nil
	case importStateDML:
		if strings.HasPrefix(data, importTokenDatabase) {
			fsm.database = strings.TrimSpace(strings.Split(data, ":")[1])
			return FSMCallEmpty, nil
		}
		if strings.HasPrefix(data, importTokenRetentionPolicy) {
			fsm.retentionPolicy = strings.TrimSpace(strings.Split(data, ":")[1])
			return FSMCallEmpty, nil
		}
		if strings.HasPrefix(data, "#") { // skip line with prefix #
			return FSMCallEmpty, nil
		}
		// skip blank lines
		if strings.TrimSpace(data) == "" {
			return FSMCallEmpty, nil
		}
		data = strings.TrimSpace(data)
		return func(ctx context.Context, command *ImportCommand) error {
			if command.fsm.database == "" {
				return errors.New("database is required, make sure `# CONTEXT-DATABASE:` token is exist")
			}

			command.fsm.batchLPBuffer = append(command.fsm.batchLPBuffer, data)
			if len(command.fsm.batchLPBuffer) < command.cfg.BatchSize {
				return nil
			}
			return command.excuteByLPBuffer(ctx)
		}, nil
	}
	return FSMCallEmpty, nil
}

func (fsm *ImportFileFSM) processCSV(data []string) (FSMCall, error) {
	if len(data) == 0 {
		return FSMCallEmpty, nil
	}

	switch fsm.state {
	case importStateDDL: // line 1 is the csv header
		fsm.state = importStateDML
		return func(ctx context.Context, command *ImportCommand) error {
			cmdStr := fmt.Sprintf("CREATE DATABASE %s", command.cfg.Database)
			_, err := command.httpClient.Query(ctx, &opengemini.Query{
				Command: cmdStr, // CREATE DATABASE NOAA_water_database
			})
			if err != nil {
				slog.Error("execute ddl failed", "reason", err, "command", cmdStr)
				return err
			}
			slog.Info("execute ddl success", "command", cmdStr)

			command.cfg.ColumnWrite = true // only support
			fsm.database = command.cfg.Database
			fsm.retentionPolicy = command.cfg.RetentionPolicy
			fsm.measurement = command.cfg.Measurement
			fsm.tagMap = make(map[string]FieldPos)
			fsm.fieldMap = make(map[string]FieldPos)
			for _, tag := range command.cfg.Tags { // tags
				fsm.tagMap[tag] = FieldPos{}
			}

			for _, field := range command.cfg.Fields { // fields
				fsm.fieldMap[field] = FieldPos{}
			}
			if len(data) > 0 {
				data[0] = strings.TrimPrefix(data[0], "\ufeff") // jump BOM
			}

			for idx, datum := range data { // column name
				_, ok := fsm.tagMap[datum]
				if ok {
					fsm.tagMap[datum] = FieldPos{datum, idx}
					continue
				}
				_, ok = fsm.fieldMap[datum]
				if ok {
					fsm.fieldMap[datum] = FieldPos{datum, idx}
					continue
				}
				if command.cfg.TimeField == datum {
					fsm.timeField = FieldPos{datum, idx}
					continue
				}

				if len(command.cfg.Fields) == 0 { // If --field is not specified, the remaining column are fields.
					fsm.fieldMap[datum] = FieldPos{datum, idx}
				} else {
					slog.Info("ignore column name", "column", datum)
				}
			}
			for _, field := range command.cfg.Fields {
				if fsm.fieldMap[field].Name == "" {
					return fmt.Errorf("field name (%s) not in csv header", field)
				}
				if _, exsit := fsm.tagMap[field]; exsit {
					return errors.New(field + " is in both tags and fields")
				}
			}

			for _, tag := range command.cfg.Tags {
				if fsm.tagMap[tag].Name == "" {
					return fmt.Errorf("tag name (%s) not in csv header", tag)
				}
				if _, exist := fsm.fieldMap[tag]; exist {
					return errors.New(tag + " is in both tags and fields")
				}
			}

			if fsm.timeField.Name == "" {
				return errors.New("time name not in csv header " + command.cfg.TimeField)
			}
			slog.Info("parse header success")
			return nil
		}, nil
	case importStateDML: // data line
		return func(ctx context.Context, command *ImportCommand) error {
			if command.fsm.database == "" {
				return errors.New("database is required")
			}
			if command.fsm.retentionPolicy == "" {
				command.fsm.retentionPolicy = common.DefaultRetentionPolicy // "autogen"
			}
			if command.cfg.Measurement == "" {
				return errors.New("measurement is required")
			}
			if len(fsm.fieldMap) == 0 {
				return errors.New("field is required")
			}

			var point = &opengemini.Point{
				Measurement: command.cfg.Measurement,
				Timestamp:   command.parseTimestamp2Int64(data[fsm.timeField.Pos]),
				Tags:        make(map[string]string),
				Fields:      make(map[string]interface{}),
			}
			for _, tag := range fsm.tagMap {
				point.Tags[tag.Name] = data[tag.Pos]
			}
			for _, field := range fsm.fieldMap {
				point.Fields[field.Name] = data[field.Pos]
			}

			command.fsm.batchPointBuffer = append(command.fsm.batchPointBuffer, point)

			if len(command.fsm.batchPointBuffer) < command.cfg.BatchSize { // continue collect data
				return nil
			}
			// consumption data, if data is full
			return command.executeByPointBuffer(ctx)
		}, nil
	}
	return FSMCallEmpty, nil
}

func (c *ImportCommand) excuteByLPBuffer(ctx context.Context) error {
	var err error
	defer func() {
		if len(c.fsm.batchLPBuffer) <= c.cfg.BatchSize {
			c.fsm.batchLPBuffer = c.fsm.batchLPBuffer[:0]
		} else { // more than one batch
			c.fsm.batchLPBuffer = c.fsm.batchLPBuffer[c.cfg.BatchSize:]
		}
	}()
	var lines = strings.Join(c.fsm.batchLPBuffer[:min(c.cfg.BatchSize, len(c.fsm.batchLPBuffer))], "\n")
	if c.cfg.ColumnWrite {
		var builderName = c.fsm.database + "." + c.fsm.retentionPolicy
		builder, ok := builderEntities[builderName]
		if !ok {
			builder, err = opengemini.NewWriteRequestBuilder(c.fsm.database, c.fsm.retentionPolicy)
			if err != nil {
				return err
			}
			builderEntities[builderName] = builder
		}
		parser := core.NewLineProtocolParser(lines)
		points, err := parser.Parse(c.cfg.TimeMultiplier)
		if err != nil {
			return err
		}
		var recordBuilder = make(map[string]opengemini.RecordBuilder)
		var recordLines []opengemini.RecordLine
		for _, point := range points {
			newLine, err := buildRecordLine(recordBuilder, point)
			if err != nil {
				return err
			}
			recordLines = append(recordLines, newLine.Build(point.Timestamp))
		}
		request, err := builder.Authenticate(c.cfg.Username, c.cfg.Password).AddRecord(recordLines...).Build()
		if err != nil {
			return err
		}
		response, err := c.writeClient.Write(ctx, request)
		if err != nil {
			return err
		}
		switch response.Code {
		case 0:
			return nil
		case 1:
			return fmt.Errorf("write failed, code: %d, partial write failure", response.GetCode())
		case 2:
			return fmt.Errorf("write failed, code: %d, write failure", response.GetCode())
		default:
			return fmt.Errorf("unexpected response code: %d", response.Code)
		}
	} else {
		err = c.httpClient.Write(ctx, c.fsm.database, c.fsm.retentionPolicy, lines, c.cfg.Precision)
	}
	return err
}

func (c *ImportCommand) executeByPointBuffer(ctx context.Context) error {
	var err error
	defer func() {
		c.fsm.batchPointBuffer = c.fsm.batchPointBuffer[:0]
	}()
	var builderName = c.fsm.database + "." + c.fsm.retentionPolicy
	builder, ok := builderEntities[builderName]
	if !ok {
		var buildRequestErr error
		builder, buildRequestErr = opengemini.NewWriteRequestBuilder(c.fsm.database, c.fsm.retentionPolicy)
		if buildRequestErr != nil {
			err = errors.Join(err, buildRequestErr)
			return err
		}
		builderEntities[builderName] = builder
	}
	var recordBuilder = make(map[string]opengemini.RecordBuilder)
	var recordLines []opengemini.RecordLine
	for _, point := range c.fsm.batchPointBuffer {
		newLine, err := buildRecordLine(recordBuilder, point)
		if err != nil {
			return err
		}
		recordLines = append(recordLines, newLine.Build(point.Timestamp))
	}
	var buildErr error
	request, buildErr := builder.Authenticate(c.cfg.Username, c.cfg.Password).AddRecord(recordLines...).Build()
	if buildErr != nil {
		err = errors.Join(err, buildErr)
		return err
	}
	response, writeErr := c.writeClient.Write(ctx, request)
	if writeErr != nil {
		err = errors.Join(err, writeErr)
		return err
	}
	switch response.Code {
	case 0:
		return err
	case 1:
		return fmt.Errorf("%w\nwrite failed, code: %d, partial write failure", err, response.GetCode())
	case 2:
		return fmt.Errorf("%w\nwrite failed, code: %d, write failure", err, response.GetCode())
	default:
		return fmt.Errorf("%w\nunexpected response code: %d", err, response.Code)
	}
}

func buildRecordLine(recordBuilder map[string]opengemini.RecordBuilder, point *opengemini.Point) (opengemini.RecordBuilder, error) {
	rb, ok := recordBuilder[point.Measurement]
	if !ok {
		var recordBuilderErr error
		rb, recordBuilderErr = opengemini.NewRecordBuilder(point.Measurement)
		if recordBuilderErr != nil {
			return nil, recordBuilderErr
		}
		recordBuilder[point.Measurement] = rb
	}
	newLine := rb.NewLine()
	for key, value := range point.Tags {
		newLine.AddTag(key, value)
	}
	for key, value := range point.Fields {
		newLine.AddField(key, value)
	}
	return newLine, nil
}

func (c *ImportCommand) parseTimestamp2Int64(s string) int64 {
	tsp := int64(fastfloat.ParseBestEffort(s))
	return tsp * c.cfg.TimeMultiplier
}

func (icfg *ImportConfig) configTimeMultiplier() error {
	switch icfg.Precision {
	case "ns":
		icfg.TimeMultiplier = 1 // 1
	case "us":
		icfg.TimeMultiplier = 1e3 // 1e3
	case "ms":
		icfg.TimeMultiplier = 1e6 // 1e6
	case "s":
		icfg.TimeMultiplier = 1e9 // 1e9
	case "": // ns
		icfg.TimeMultiplier = 1 // 1
	default:
		return errors.New("incorrect timestamp precision, only support (s, ms, us, ns)")
	}
	return nil
}
