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

package collector

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nxadm/tail"
	"github.com/openGemini/openGemini/lib/atomic"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/crypto"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/metrics"
	"github.com/openGemini/openGemini/lib/statisticsPusher/pusher"
	"github.com/openGemini/openGemini/lib/util"
	"go.uber.org/zap"
)

const (
	HttpTimeout = 10 * time.Second
)

// Easy to test modify
var (
	MinBatchSize       = 100
	ReportFrequency    = 5 * time.Second
	ReportLogFrequency = 10 * time.Second
	WaitRotationEnd    = 10 * time.Second
	MaxRetryTimes      = 20
	ignoreKeys         = map[string]struct{}{
		"errno":    {},
		"hostname": {},
		"time":     {},
		"level":    {},
		"caller":   {},
	}
)

type JobType int

const (
	ErrLogHistoryType JobType = iota
	ErrLogCurrentType
)

const (
	ErrLogMst = "err_log"
)

// HTTPClient interface
type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type ReportJob struct {
	storeDatabase  string
	storeRP        string
	storeDuration  time.Duration
	rbAddress      []string
	rbAddressIndex int64
	writeHeader    http.Header
	queryHeader    http.Header
	gzipped        bool
	protocol       string

	Client HTTPClient
	done   chan struct{}
	logger *logger.Logger
	Hook   func()

	errLogLock    sync.RWMutex // lock for errLogStat
	errLogHistory string
	errLogStat    map[string]string // {"sql1.error": "2022-06-29T09:01:08.270895676+08:00"}

	compress   bool // is metric file compressed
	reportStat *ReportStat

	replicaN       int
	mux            *sync.RWMutex
	indexModuleMap map[string][]*metrics.ModuleIndex
}

func NewReportJob(logger *logger.Logger, c *config.TSMonitor, gzipped bool, errLogHistory string) *ReportJob {
	r := &c.ReportConfig
	q := &c.QueryConfig
	m := &c.MonitorConfig
	protocol := "http"
	var defaultClient = http.DefaultClient
	if c.ReportConfig.HTTPSEnabled {
		protocol = "https"
		tr := &http.Transport{
			TLSClientConfig: config.NewTLSConfig(true),
		}
		defaultClient = &http.Client{
			Transport: tr,
		}
	}

	writeHeader := http.Header{}
	writeHeader.Set("Authorization", "Basic "+basicAuth(r.Username, crypto.Decrypt(r.Password)))

	queryHeader := http.Header{}
	queryHeader.Set("Authorization", "Basic "+basicAuth(q.Username, crypto.Decrypt(q.Password)))
	queryHeader.Add("Content-Type", "application/x-www-form-urlencoded")
	if r.ReplicaN <= 0 {
		logger.Error("ts-monitor report config replicaN = 0, use replicaN = 1")
		r.ReplicaN = 1
	}
	if r.ReplicaN > 1 && r.ReplicaN%2 == 0 {
		logger.Error("ts-monitor report config replicaN > 1 add %2 = 0, use replicaN = 1")
		r.ReplicaN = 1
	}
	logger.Info("ts-monitor new reportJob", zap.Int("replicaN", r.ReplicaN))
	mr := &ReportJob{
		storeDatabase:  r.Database,
		storeRP:        r.Rp,
		storeDuration:  time.Duration(r.RpDuration),
		rbAddress:      strings.Split(r.Address, ","),
		writeHeader:    writeHeader,
		protocol:       protocol,
		queryHeader:    queryHeader,
		compress:       m.Compress,
		gzipped:        gzipped,
		done:           make(chan struct{}),
		logger:         logger,
		errLogHistory:  errLogHistory,
		errLogStat:     make(map[string]string),
		reportStat:     NewReportStat(),
		replicaN:       r.ReplicaN,
		mux:            new(sync.RWMutex),
		indexModuleMap: make(map[string][]*metrics.ModuleIndex),
	}
	mr.Client = defaultClient
	return mr
}

func (rb *ReportJob) CreateDatabase() error {
	data := url.Values{}
	cmd := fmt.Sprintf("CREATE DATABASE %s with duration %s replication %d name %s", rb.storeDatabase, rb.storeDuration, rb.replicaN, rb.storeRP)
	data.Set("q", cmd)
	buf := data.Encode()
	path := "/query"
	if err := rb.retryEver(path, rb.queryHeader, buf, HttpTimeout); err != nil {
		return err
	}
	return nil
}

func basicAuth(username, password string) string {
	auth := username + ":" + password
	return base64.StdEncoding.EncodeToString([]byte(auth))
}

func (rb *ReportJob) WriteData(buf string) error {
	path := "/write"
	return rb.retryEver(path, rb.writeHeader, buf, 0)
}

func (rb *ReportJob) switchAddress() {
	atomic.SetModInt64AndADD(&rb.rbAddressIndex, 1, int64(len(rb.rbAddress)))
	rb.logger.Info("switch address", zap.Int64("address index: ", rb.rbAddressIndex))
}

func checkConnectionError(err error) bool {
	if strings.Contains(err.Error(), "connection reset by peer") ||
		strings.Contains(err.Error(), "connection refused") ||
		strings.Contains(err.Error(), "read message type: EOF") ||
		strings.Contains(err.Error(), "max message size") ||
		strings.Contains(err.Error(), "write: connection timed out") {
		return true
	}
	return false
}

func (rb *ReportJob) retryEver(path string, headers http.Header, buf string, httpTimeout time.Duration) error {
	url := rb.genUrl(path)
	tries := 0
	var timeout <-chan time.Time
	if httpTimeout > 0 {
		timeout = time.After(httpTimeout)
	}
	body := strings.NewReader(buf)
	for {
		if timeout != nil {
			// exit if we're timeout
			select {
			case <-timeout:
				return errors.New("request timeout")
			default:
				// we're still reporting, continue on
			}
		}
		req, _ := http.NewRequest(http.MethodPost, url, body)
		req.Header = headers
		resp, err := rb.Client.Do(req)
		var bytesBody []byte
		if err == nil {
			if resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusOK {
				_ = resp.Body.Close()
				break
			} else {
				bytesBody, _ = io.ReadAll(resp.Body)
				_ = resp.Body.Close()
			}
		} else {
			rb.switchAddress()
			url = rb.genUrl(path)
			if checkConnectionError(err) {
				time.Sleep(time.Second)
				continue
			}
		}
		tries++
		if tries > MaxRetryTimes {
			break
		}
		rb.logger.Info("monitor report retry", zap.String("url", url), zap.Int("tries", tries), zap.ByteString("body", bytesBody), zap.Error(err))
		time.Sleep(100 * time.Millisecond)
		body.Reset(buf)
	}
	return nil
}

func (rb *ReportJob) genUrl(path string) string {
	return fmt.Sprintf("%s://%s%s?db=%s&rp=%s", rb.protocol, rb.rbAddress[rb.rbAddressIndex], path, rb.storeDatabase, rb.storeRP)
}

func (rb *ReportJob) StartErrLogJob(filename, errLogPath string, jobType JobType) error {
	switch jobType {
	case ErrLogHistoryType:
		return rb.reportHistoryErrLog(filename)
	case ErrLogCurrentType:
		return rb.reportCurrentErrLog(filename, errLogPath)
	}
	return nil
}

func (rb *ReportJob) ReportMetric(filename string) error {
	config := &pusher.FileConfig{Path: filename}
	config.Parser()

	lines := make(chan []byte)
	defer close(lines)
	done := make(chan struct{})
	go func() {
		err := rb.tail(filename, lines)
		if err != nil {
			if !rb.reportStat.TryAgain(filename) {
				rb.reportStat.Delete(filename)
				_ = fileops.RenameFile(filename, filename+suffixInvalid)
			}
			rb.logger.Error("tail failed", zap.Error(err), zap.String("file", filename))
		} else {
			rb.reportStat.Delete(filename)
		}
		close(done)
	}()

	ticker := time.NewTicker(ReportFrequency)
	defer ticker.Stop()

	batch := 0
	var buf bytes.Buffer

	for {
		select {
		case line := <-lines:
			ticker.Reset(ReportFrequency)
			if len(line) == 0 {
				continue
			}

			batch += bytes.Count(line, []byte{'\n'})
			buf.Write(line)
			buf.WriteByte('\n')

			pusher.PutBuffer(line)
			rb.parseIndex(line)
			if err := rb.postData(&buf, &batch, MinBatchSize); err != nil {
				return err
			}
		case <-ticker.C:
			if err := rb.postData(&buf, &batch, 0); err != nil {
				return err
			}
		case <-done:
			return rb.postData(&buf, &batch, 0)
		case <-rb.done:
			rb.logger.Info("report job stopped", zap.String("file", filename))
			return nil
		}
	}
}

func (rb *ReportJob) parseIndex(line []byte) {
	var (
		moduleName  string
		metricSlice = make([]*metrics.ModuleIndex, 0)
	)

	lines := strings.Split(string(line), "\n")
	t := time.Now()

	//Data format of each row: {table name},{labels} {indicator data} {timestamp}
	for _, value := range lines {
		m := metrics.ModuleIndex{
			LabelValues: make(map[string]string),
			MetricsMap:  make(map[string]interface{}),
			Timestamp:   t,
		}
		parts := strings.Split(value, " ")
		if len(parts) != 3 {
			rb.logger.Error("invalid metric line:", zap.String("line", value))
			continue
		}
		tmp := strings.Split(parts[0], ",")
		if len(tmp) != 2 {
			rb.logger.Error("invalid metric line:", zap.String("line", value))
			continue
		}
		moduleName = tmp[0]
		// parse labels
		labels := tmp[1:]
		for _, label := range labels {
			labelKv := strings.Split(label, "=")
			m.LabelValues[labelKv[0]] = labelKv[1]
		}
		// parse index
		indexes := strings.Split(parts[1], ",")
		for _, index := range indexes {
			indexKv := strings.Split(index, "=")
			if len(indexKv) != 2 {
				rb.logger.Error("invalid metric index:", zap.String("index", index))
				continue
			}
			value, err := strconv.ParseFloat(indexKv[1], 64)
			if err != nil {
				rb.logger.Error("parse index failed", zap.String("file", moduleName), zap.String("index", index), zap.Error(err))
				m.MetricsMap[indexKv[0]] = indexKv[1]
			} else {
				m.MetricsMap[indexKv[0]] = value
			}
		}
		metricSlice = append(metricSlice, &m)
	}
	rb.mux.Lock()
	rb.indexModuleMap[moduleName] = metricSlice
	rb.mux.Unlock()
}

func (rb *ReportJob) GetIndexModuleMap() map[string][]*metrics.ModuleIndex {
	rb.mux.RLock()
	defer rb.mux.RUnlock()
	return rb.indexModuleMap
}

func (rb *ReportJob) postData(buf *bytes.Buffer, batch *int, minSize int) error {
	if *batch <= minSize {
		return nil
	}

	if err := rb.WriteData(buf.String()); err != nil {
		return err
	}

	*batch = 0
	buf.Reset()
	return nil
}

func (rb *ReportJob) tail(filename string, result chan []byte) error {
	stat, statErr := os.Stat(filename)
	if statErr != nil {
		return statErr
	}

	history := false
	timeout := pusher.DefaultTailTimeout
	if stat.ModTime().Add(time.Hour).Before(time.Now()) {
		history = true
		timeout = time.Minute
	}

	st := pusher.NewSnappyTail(timeout, rb.compress)
	defer st.Close()

	err := st.Tail(filename, func(data []byte) {
		defer func() {
			_ = recover()
		}()

		result <- data
	})

	if err == io.EOF || (history && errno.Equal(err, errno.WatchFileTimeout)) {
		rb.logger.Info("tail file finish, remove it", zap.String("file", filename))
		_ = fileops.Remove(filename)
		return nil
	}

	return err
}

func ignoreLine(line []byte, errnoMap map[int]string) bool {
	// ignore no errno line
	if !bytes.ContainsAny(line, "errno") {
		return true
	}
	// ignore no hostname line
	if !bytes.ContainsAny(line, "hostname") {
		return true
	}
	reg := regexp.MustCompile(`.*?"errno":"(\d+)"`)
	if reg == nil {
		return true
	}
	result := reg.FindStringSubmatch(string(line))
	if len(result) < 2 {
		return true
	}
	errno, _ := strconv.Atoi(result[1])
	if _, ok := errnoMap[errno]; ok {
		return true
	}
	return false
}

func (rb *ReportJob) reportHistoryErrLog(filename string) error {
	file, _ := os.Open(path.Clean(filename))
	defer util.MustClose(file)
	archive, err := gzip.NewReader(file)
	if err != nil {
		return err
	}
	defer util.MustClose(archive)
	reader := bufio.NewReader(archive)

	// reduce the log reporting.
	// A maximum of one point can be reported for each error code in each batch.
	errnoMap := make(map[int]string)

	for {
		line, _, err := reader.ReadLine()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if ignoreLine(line, errnoMap) {
			continue
		}
		errno, data := rb.parseErrLine(line, "", false, nil)
		if data == "" {
			continue
		}
		errnoMap[errno] = data
	}
	var buf bytes.Buffer
	for _, data := range errnoMap {
		buf.WriteString(data)
		buf.WriteByte('\n')
	}

	if len(errnoMap) > 0 {
		if err = rb.WriteData(buf.String()); err != nil {
			return err
		}
		buf.Reset()
	}
	return nil
}

func (rb *ReportJob) reportCurrentErrLog(filename, errLogPath string) error {
	tailConfig := tail.Config{
		Follow:    true,
		MustExist: true,
		Logger:    tail.DiscardingLogger,
	}
	filePrefix := strings.TrimSuffix(filepath.Base(filename), filepath.Ext(filename))
	t, err := tail.TailFile(filename, tailConfig)
	if err != nil {
		return err
	}
	defer t.Cleanup()

	isLogRotation := make(chan struct{})
	go rb.checkLogRotation(filePrefix, errLogPath, isLogRotation)

	// reduce the log reporting.
	// A maximum of one point can be reported for each error code in each batch.
	errnoMap := make(map[int]string)

	ticker := time.NewTicker(ReportLogFrequency)
	defer ticker.Stop()

	var buf bytes.Buffer
	for {
		select {
		case line, ok := <-t.Lines:
			if !ok {
				continue
			}
			if line.Err != nil {
				rb.logger.Error("tail error", zap.Error(line.Err), zap.String("filename", filename))
				return line.Err
			}
			if ignoreLine([]byte(line.Text), errnoMap) {
				continue
			}
			errno, data := rb.parseErrLine([]byte(line.Text), filePrefix, true, nil)
			if data == "" {
				continue
			}
			errnoMap[errno] = data
			continue
		case <-ticker.C:
			if len(errnoMap) > 0 {
				for _, data := range errnoMap {
					buf.WriteString(data)
					buf.WriteByte('\n')
				}
				if err = rb.WriteData(buf.String()); err != nil {
					return err
				}
				buf.Reset()
				errnoMap = make(map[int]string)
				ticker.Reset(ReportFrequency)
				continue
			}
			continue
		case <-isLogRotation:
			if err = t.Stop(); err != nil {
				rb.logger.Error("stop tail error log error", zap.Error(err), zap.String("filename", filename))
			}
			return nil
		case <-rb.done:
			rb.logger.Info("report job stopped", zap.String("file", filename))
			return nil
		}
	}
}

func (rb *ReportJob) parseErrLine(line []byte, filePrefix string, isCurrent bool, ignoreTimeFunc func(line []byte) bool) (int, string) {
	if len(line) == 0 {
		return 0, ""
	}
	if ignoreTimeFunc != nil && ignoreTimeFunc(line) {
		return 0, ""
	}

	data := make(map[string]interface{})
	err := json.Unmarshal(line, &data)
	if err != nil {
		rb.logger.Error("parse line err", zap.Error(err), zap.ByteString("line", line))
		return 0, ""
	}
	tm := data["time"].(string)
	t, _ := time.Parse(time.RFC3339Nano, tm)
	ts := t.UnixNano()
	rb.saveCurrentErrLog(filePrefix, isCurrent, tm)

	errno, _ := strconv.Atoi(data["errno"].(string))
	hostname, _ := data["hostname"].(string)
	tags := fmt.Sprintf("%s,errno=%s,hostname=%s", ErrLogMst, data["errno"], hostname)

	var field []string
	var repeated float64
	for k, v := range data {
		if _, ok := ignoreKeys[k]; ok {
			continue
		} else if k == "repeated" {
			repeated = v.(float64)
			continue
		}
		if _, ok := v.(string); ok {
			v = strings.ReplaceAll(v.(string), "\n", "###")
		}
		field = append(field, fmt.Sprintf("%s:%v", k, v))
	}
	fields := strings.Join(field, ";")
	return errno, fmt.Sprintf(`%s msg="%s",repeated=%0.fi %d`, tags, fields, repeated, ts)
}

func (rb *ReportJob) saveCurrentErrLog(filePrefix string, isCurrent bool, time string) {
	if isCurrent {
		rb.errLogLock.Lock()
		rb.errLogStat[filePrefix] = time
		rb.errLogLock.Unlock()
	}
}

func (rb *ReportJob) checkLogRotation(name, errLogPath string, isLogRotation chan struct{}) {
	for {
		errLogHist := readErrLogHistory(rb.errLogHistory)
		gzFiles, _ := filepath.Glob(filepath.Join(errLogPath, name+"*log.gz"))
		for _, filename := range gzFiles {
			if _, ok := errLogHist[filename]; !ok {
				rb.logger.Info("log rotation happened", zap.String("filename", filename))
				isLogRotation <- struct{}{}
				return
			}
		}
		select {
		case <-rb.done:
			return
		default:
		}
		time.Sleep(WaitRotationEnd)
	}
}

func (rb *ReportJob) Close() {
	close(rb.done)
}
