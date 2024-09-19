// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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

package geminicli

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/influxdb/client"
)

const (
	batchSize = 5000
)

// Importer is the importer used for importing data
type Importer struct {
	client           HttpClient
	clientCreator    HttpClientCreator
	database         string
	retentionPolicy  string
	precision        string
	writeConsistency string

	batch                 []string
	totalInserts          int
	failedInserts         int
	totalCommands         int
	throttlePointsWritten int
	startTime             time.Time
	throttle              *time.Ticker

	stderrLogger *log.Logger
	stdoutLogger *log.Logger
}

// NewImporter will return an initialized Importer struct
func NewImporter() *Importer {

	return &Importer{
		batch:         make([]string, 0, batchSize),
		clientCreator: defaultHttpClientCreator,
		stdoutLogger:  log.New(os.Stdout, "", log.LstdFlags),
		stderrLogger:  log.New(os.Stderr, "", log.LstdFlags),
	}
}

// parseConnectionString converts a path string into an url.URL.
func parseConnectionString(path string) (url.URL, error) {
	var host string

	h, p, err := net.SplitHostPort(path)
	if err != nil {
		if path == "" {
			host = DEFAULT_HOST
		} else {
			host = path
		}
	} else {
		host = h
		_, err = strconv.Atoi(p)
		if err != nil {
			return url.URL{}, fmt.Errorf("invalid port number %q: %s", path, err)
		}
	}

	u := url.URL{
		Scheme: "http",
		Host:   host,
	}

	return u, nil
}

// parseClientConfig initialize the client.config
func parseClientConfig(clc *CommandLineConfig) (*client.Config, error) {
	config := client.NewConfig()

	addr := net.JoinHostPort(clc.Host, strconv.Itoa(clc.Port))
	u, err := parseConnectionString(addr)
	if err != nil {
		return nil, err
	}
	config.URL = u
	config.URL.Host = addr

	if clc.Precision, err = parsePrecision(clc.Precision); err != nil {
		return nil, err
	}
	config.Precision = clc.Precision

	config.UnixSocket = clc.UnixSocket
	config.Username = clc.Username
	config.Password = clc.Password
	config.UnsafeSsl = clc.IgnoreSsl
	return &config, nil
}

// Import processes the specified file in the Config and writes the data to the databases in chunks specified by batchSize
func (ipt *Importer) Import(clc *CommandLineConfig) error {
	config, err := parseClientConfig(clc)
	if err != nil {
		return err
	}
	ipt.precision = config.Precision
	ipt.writeConsistency = config.WriteConsistency
	if clc.Path == "" {
		return fmt.Errorf("execute -import cmd, -path is required")
	}

	// Create a new client and ping it.
	cli, err := ipt.clientCreator(*config)
	if err != nil {
		return fmt.Errorf("could not create client %s", err)
	}
	ipt.client = cli
	if _, _, err = ipt.client.Ping(); err != nil {
		return err
	}

	defer func() {
		if ipt.totalInserts > 0 {
			ipt.stdoutLogger.Printf("Processed %d commands\n", ipt.totalCommands)
			ipt.stdoutLogger.Printf("Processed %d inserts\n", ipt.totalInserts)
			ipt.stdoutLogger.Printf("Failed %d inserts\n", ipt.failedInserts)
		}
	}()

	f, err := os.Open(clc.Path)
	if err != nil {
		return fmt.Errorf("fail to open file %s, %s", clc.Path, err)
	}
	defer f.Close()

	var r = f
	// Get the reader
	scanner := bufio.NewReader(r)

	// Process the DDL
	if err := ipt.processDDL(scanner); err != nil {
		return fmt.Errorf("reading standard input: %s", err)
	}

	// Set up a throttle channel.
	ipt.throttle = time.NewTicker(time.Microsecond) // TODO: to use
	defer ipt.throttle.Stop()

	// Process the DML
	if err := ipt.processDML(scanner); err != nil {
		return fmt.Errorf("reading standard input: %s", err)
	}

	if ipt.failedInserts > 0 {
		plural := " was"
		if ipt.failedInserts > 1 {
			plural = "s were"
		}
		// If we failed any inserts, then return an error with the amount failed inserts.
		return fmt.Errorf("%d point%s not inserted", ipt.failedInserts, plural)
	}

	return nil
}

func (ipt *Importer) processDDL(scanner *bufio.Reader) error {
	for {
		line, err := scanner.ReadString(byte('\n'))
		if err != nil && err != io.EOF {
			return err
		} else if err == io.EOF {
			return nil
		}
		// If DML token find
		if strings.HasPrefix(line, "# DML") {
			return nil
		}
		if strings.HasPrefix(line, "#") {
			continue
		}
		// Skip blank lines
		if strings.TrimSpace(line) == "" {
			continue
		}
		ipt.queryExecutor(line)
	}
}

func (ipt *Importer) processDML(scanner *bufio.Reader) error {
	ipt.startTime = time.Now()
	for {
		line, err := scanner.ReadString(byte('\n'))
		if err != nil && err != io.EOF {
			return err
		} else if err == io.EOF {
			ipt.batchWrite()
			return nil
		}
		if strings.HasPrefix(line, "# CONTEXT-DATABASE:") {
			ipt.batchWrite()
			ipt.database = strings.TrimSpace(strings.Split(line, ":")[1])
			continue
		}
		if strings.HasPrefix(line, "# CONTEXT-RETENTION-POLICY:") {
			ipt.batchWrite()
			ipt.retentionPolicy = strings.TrimSpace(strings.Split(line, ":")[1])
			continue
		}
		if strings.HasPrefix(line, "#") {
			continue
		}
		// Skip blank lines
		if strings.TrimSpace(line) == "" {
			continue
		}
		ipt.batchAccumulator(line)
	}
}

func (ipt *Importer) execute(command string) {
	response, err := ipt.client.QueryContext(context.TODO(), client.Query{Command: command, Database: ipt.database})
	if err != nil {
		ipt.stderrLogger.Printf("error: %s\n", err.Error())
		return
	}
	if err = response.Error(); err != nil {
		ipt.stderrLogger.Printf("error: %s\n", err.Error())
	}
}

func (ipt *Importer) queryExecutor(command string) {
	ipt.totalCommands++
	ipt.execute(command)
}

func (ipt *Importer) batchAccumulator(line string) {
	ipt.batch = append(ipt.batch, line)
	if len(ipt.batch) == batchSize {
		ipt.batchWrite()
	}
}

func (ipt *Importer) batchWrite() {
	// Exit early if there are no points in the batch
	if len(ipt.batch) == 0 {
		return
	}

	// Accumulate the batch size to see how many points we have written this second
	ipt.throttlePointsWritten += len(ipt.batch)

	resp, err := ipt.client.WriteLineProtocol(strings.Join(ipt.batch, "\n"), ipt.database, ipt.retentionPolicy, ipt.precision, ipt.writeConsistency)
	if err != nil {
		ipt.stderrLogger.Println("error writing batch: ", err)
		ipt.stderrLogger.Println(strings.Join(ipt.batch, "\n"))
		ipt.failedInserts += len(ipt.batch)
	} else if resp != nil && resp.Error() != nil {
		ipt.stderrLogger.Println("error writing batch: ", resp.Error())
		ipt.stderrLogger.Println(strings.Join(ipt.batch, "\n"))
		ipt.stderrLogger.Println("error writing response: ")
		ipt.stderrLogger.Println(resp.MarshalJSON())
		ipt.failedInserts += len(ipt.batch)
	} else {
		ipt.totalInserts += len(ipt.batch)
	}
	ipt.throttlePointsWritten = 0

	// Clear the batch and record the number of processed points.
	ipt.batch = ipt.batch[:0]
	// Give some status feedback every 100000 lines processed
	processed := ipt.totalInserts + ipt.failedInserts
	if processed%100000 == 0 {
		since := time.Since(ipt.startTime)
		pps := float64(processed) / since.Seconds()
		ipt.stdoutLogger.Printf("Processed %d lines.  Time elapsed: %s.  Points per second (PPS): %d", processed, since.String(), int64(pps))
	}
}
