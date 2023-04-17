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

package geminicli

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/influxdb/client"
)

// Importer is the importer used for importing data
type Importer struct {
	client                *client.Client
	config                Config
	batch                 []string
	totalInserts          int
	failedInserts         int
	totalCommands         int
	throttlePointsWritten int
	startTime             time.Time
	endTime               time.Time
	throttle              *time.Ticker
	database              string
	retentionPolicy       string

	stderrLogger *log.Logger
	stdoutLogger *log.Logger
}

// NewImporter will return an initialized Importer struct
func NewImporter(config Config) *Importer {
	config.UserAgent = fmt.Sprintf("openGemini importer/%s", CLIENT_VERSION)
	return &Importer{
		config:       config,
		batch:        make([]string, 0, batchSize),
		stdoutLogger: log.New(os.Stdout, "", log.LstdFlags),
		stderrLogger: log.New(os.Stderr, "", log.LstdFlags),
	}
}

type Config struct {
	Version          string
	Path             string
	Precision        string
	WriteConsistency string

	client.Config
}

// InitConfig is used to initialize the config
func (c *CommandLineConfig) InitConfig() error {
	addr := net.JoinHostPort(c.Host, strconv.Itoa(c.Port))
	u, err := parseConnectionString(addr)
	if err != nil {
		return fmt.Errorf("[ERR] %s", err)
	}

	c.ImportConfig.URL = u
	c.ImportConfig.URL.Host = addr
	c.ClientConfig.URL = u
	c.ClientConfig.URL.Host = addr
	c.ClientConfig.Username = c.Username
	c.ClientConfig.Password = c.Password

	return nil
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

func (c *CommandLineConfig) Run() error {
	config := c.ImportConfig
	config.Config = c.ClientConfig

	i := NewImporter(config)
	if err := i.Import(); err != nil {
		err = fmt.Errorf("ERROR: %s", err)
		return err
	}
	return nil
}

// Import processes the specified file in the Config and writes the data to the databases in chunks specified by batchSize
func (i *Importer) Import() error {
	// Create a new client and ping it.
	cli, err := client.NewClient(i.config.Config)
	if err != nil {
		return fmt.Errorf("could not create client %s", err)
	}
	i.client = cli
	if _, _, e := i.client.Ping(); e != nil {
		return fmt.Errorf("failed to connect to %s", i.client.Addr())
	}

	if i.config.Path == "" {
		return fmt.Errorf("file path required")
	}

	defer func() {
		if i.totalInserts > 0 {
			i.stdoutLogger.Printf("Processed %d commands\n", i.totalCommands)
			i.stdoutLogger.Printf("Processed %d inserts\n", i.totalInserts)
			i.stdoutLogger.Printf("Failed %d inserts\n", i.failedInserts)
		}
	}()

	f, err := os.Open(i.config.Path)
	if err != nil {
		return fmt.Errorf("fail to open file %s, %s", i.config.Path, err)
	}
	defer f.Close()

	var r = f
	// Get the reader
	scanner := bufio.NewReader(r)

	// Process the DDL
	if err := i.processDDL(scanner); err != nil {
		return fmt.Errorf("reading standard input: %s", err)
	}

	// Set up a throttle channel.
	i.throttle = time.NewTicker(time.Microsecond)
	defer i.throttle.Stop()

	// Prime the last write
	i.endTime = time.Now()

	// Process the DML
	if err := i.processDML(scanner); err != nil {
		return fmt.Errorf("reading standard input: %s", err)
	}

	if i.failedInserts > 0 {
		plural := " was"
		if i.failedInserts > 1 {
			plural = "s were"
		}
		// If we failed any inserts, then return an error with the amount failed inserts.
		return fmt.Errorf("%d point%s not inserted", i.failedInserts, plural)
	}

	return nil
}

func (i *Importer) processDDL(scanner *bufio.Reader) error {
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
		i.queryExecutor(line)
	}
}

func (i *Importer) processDML(scanner *bufio.Reader) error {
	i.startTime = time.Now()
	for {
		line, err := scanner.ReadString(byte('\n'))
		if err != nil && err != io.EOF {
			return err
		} else if err == io.EOF {
			i.batchWrite()
			return nil
		}
		if strings.HasPrefix(line, "# CONTEXT-DATABASE:") {
			i.batchWrite()
			i.database = strings.TrimSpace(strings.Split(line, ":")[1])
		}
		if strings.HasPrefix(line, "# CONTEXT-RETENTION-POLICY:") {
			i.batchWrite()
			i.retentionPolicy = strings.TrimSpace(strings.Split(line, ":")[1])
		}
		if strings.HasPrefix(line, "#") {
			continue
		}
		// Skip blank lines
		if strings.TrimSpace(line) == "" {
			continue
		}
		i.batchAccumulator(line)
	}
}

func (i *Importer) execute(command string) {
	response, err := i.client.Query(client.Query{Command: command, Database: i.database})
	if err != nil {
		i.stderrLogger.Printf("error: %s\n", err)
		return
	}
	if err := response.Error(); err != nil {
		i.stderrLogger.Printf("error: %s\n", response.Error())
	}
}

func (i *Importer) queryExecutor(command string) {
	i.totalCommands++
	i.execute(command)
}

func (i *Importer) batchAccumulator(line string) {
	i.batch = append(i.batch, line)
	if len(i.batch) == batchSize {
		i.batchWrite()
	}
}

func (i *Importer) batchWrite() {
	// Exit early if there are no points in the batch
	if len(i.batch) == 0 {
		return
	}

	// Accumulate the batch size to see how many points we have written this second
	i.throttlePointsWritten += len(i.batch)

	err := i.WriteLineProtocol(strings.Join(i.batch, "\n"))
	if err != nil {
		i.stderrLogger.Println("error writing batch: ", err)
		i.stderrLogger.Println(strings.Join(i.batch, "\n"))
		i.failedInserts += len(i.batch)
	} else {
		i.totalInserts += len(i.batch)
	}
	i.throttlePointsWritten = 0
	i.endTime = time.Now()

	// Clear the batch and record the number of processed points.
	i.batch = i.batch[:0]
	// Give some status feedback every 100000 lines processed
	processed := i.totalInserts + i.failedInserts
	if processed%100000 == 0 {
		since := time.Since(i.startTime)
		pps := float64(processed) / since.Seconds()
		i.stdoutLogger.Printf("Processed %d lines.  Time elapsed: %s.  Points per second (PPS): %d", processed, since.String(), int64(pps))
	}
}

// WriteLineProtocol takes a string with line returns to delimit each write
func (i *Importer) WriteLineProtocol(data string) error {
	u := i.config.URL
	u.Path = path.Join(u.Path, "write")
	r := strings.NewReader(data)

	req, err := http.NewRequest("POST", u.String(), r)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "")
	req.Header.Set("User-Agent", i.config.UserAgent)
	if i.config.Config.Username != "" {
		req.SetBasicAuth(i.config.Config.Username, i.config.Config.Password)
	}
	params := req.URL.Query()
	params.Set("db", i.database)
	params.Set("rp", i.retentionPolicy)
	params.Set("precision", i.config.Precision)
	params.Set("consistency", i.config.WriteConsistency)
	req.URL.RawQuery = params.Encode()

	httpClient := &http.Client{Timeout: i.config.Config.Timeout}
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		err := fmt.Errorf(string(body))
		return fmt.Errorf("error writing: %s", err)
	}

	return nil
}
