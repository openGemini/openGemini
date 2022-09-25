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

package logger_test

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type LogLine struct {
	Level  string
	Msg    string
	Time   string
	Caller string
	Errno  string
}

func initLogger(t *testing.T, level zapcore.Level) (string, string) {
	dir := os.TempDir()
	filename := dir + "/sql.log"
	errFile := dir + "/sql.error.log"
	removeFile(t, filename)
	removeFile(t, errFile)

	conf := config.NewLogger(config.AppSql)
	conf.Path = dir
	conf.Level = level

	logger.InitLogger(conf)

	return filename, errFile
}

func TestLogger(t *testing.T) {
	filename, errFile := initLogger(t, zapcore.DebugLevel)

	errMessage := fmt.Sprintf("test error. %d", time.Now().UnixNano())
	infoMessage := fmt.Sprintf("test info. %d", time.Now().UnixNano())

	lg := logger.GetLogger()
	lg.Info(infoMessage)
	lg.Error(errMessage)
	logger.CloseLogger()

	assertFileStat(t, filename)
	assertFileStat(t, errFile)

	assertFileContents(t, filename, 2, []string{"info", "error"}, []string{infoMessage, errMessage})
	assertFileContents(t, errFile, 1, []string{"error"}, []string{errMessage})
}

func TestNewLogger(t *testing.T) {
	stat := make(map[string]int64)
	wg := sync.WaitGroup{}
	wg.Add(1)
	logger.SetErrnoStatHandler(func(s string) {
		stat[s]++
		wg.Done()
	})

	filename, _ := initLogger(t, zapcore.DebugLevel)
	errno.SetNode(errno.NodeSql)
	lg := logger.NewLogger(errno.ModuleUnknown)
	lg.SetModule(errno.ModuleNetwork)

	var obwg sync.WaitGroup
	lg.GetSuppressLogger().ApplyObserver(func(_ *logger.SuppressLog) {
		obwg.Done()
	})

	err := errno.NewError(errno.RecoverPanic, 1, "127.0.0.1:8400")
	messages := []string{"some error with errno", "some error with no errno", "some debug", "some info", "some warn"}
	lg.With(zap.String("color", "red"))

	obwg.Add(5)
	lg.Error(messages[0], zap.String("hostname", "localhost"), zap.Error(err))
	lg.Error(messages[1], zap.String("hostname", "localhost"), zap.Error(errors.New("error")))
	lg.Debug(messages[2])
	lg.Info(messages[3])
	lg.Warn(messages[4])
	obwg.Wait()

	logger.CloseLogger()

	assertFileStat(t, filename)
	assertFileContents(t, filename, len(messages),
		[]string{"error", "error", "debug", "info", "warn"},
		messages)

	logs, readErr := readLog(filename)
	if !assert.NoError(t, readErr) {
		return
	}

	expErrno := fmt.Sprintf("%d%02d%d%04d",
		errno.NodeSql, errno.ModuleNetwork, errno.LevelFatal, errno.RecoverPanic)

	wg.Wait()
	assert.Equal(t, int64(1), stat[expErrno], "stat errno failed")
	assert.Equal(t, expErrno, logs[0].Errno, "incorrect errno")
	assert.Equal(t, "", logs[1].Errno, "incorrect errno, exp empty")
}

func TestLoggerLevel(t *testing.T) {
	filename, _ := initLogger(t, zapcore.DebugLevel-1)
	errno.SetNode(errno.NodeSql)
	lg := logger.NewLogger(errno.ModuleUnknown)
	lg.SetModule(errno.ModuleNetwork)

	var obwg sync.WaitGroup
	lg.GetSuppressLogger().ApplyObserver(func(_ *logger.SuppressLog) {
		obwg.Done()
	})

	messages := []string{"some debug", "some info", "some warn"}

	obwg.Add(3)
	lg.Debug(messages[0])
	lg.Info(messages[1])
	lg.Warn(messages[2])
	obwg.Wait()

	logger.CloseLogger()

	assertFileStat(t, filename)
	assertFileContents(t, filename, len(messages)-1,
		[]string{"info", "warn"},
		messages[1:])
}

func TestZapLogger(t *testing.T) {
	lg := logger.NewLogger(errno.ModuleUnknown)

	other := lg.GetZapLogger()
	assert.NotEmpty(t, other)
}

func removeFile(t *testing.T, file string) {
	_ = os.Remove(file)
}

func assertSize(t *testing.T, logs []*LogLine, size int) {
	if len(logs) != size {
		t.Fatalf("incorrect number of log lines, exp: %d, got: %d", size, len(logs))
	}
}

func assertFileContents(t *testing.T, file string, size int, levels []string, messages []string) {
	logs, err := readLog(file)
	if err != nil {
		t.Fatalf("%v", err)
	}
	assertSize(t, logs, size)

	for i, line := range logs {
		if line.Level != levels[i] {
			t.Fatalf("Level value in line %d is incorrect, exp %s, got: %s",
				i+1, levels[i], line.Level)
		}
	}

	for i, line := range logs {
		if line.Msg != messages[i] {
			t.Fatalf("Msg value in line %d is incorrect, exp %s, got: %s",
				i+1, messages[i], line.Msg)
		}
	}
}

func assertFileStat(t *testing.T, file string) {
	if _, err := os.Stat(file); err != nil {
		t.Fatalf("%s not exists", file)
	}
}

func readLog(file string) ([]*LogLine, error) {
	fp, err := os.OpenFile(file, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer fp.Close()

	reader := bufio.NewReader(fp)
	var ret []*LogLine
	for {
		line, _, err := reader.ReadLine()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		log := &LogLine{}
		if err := json.Unmarshal(line, log); err != nil {
			return nil, err
		}

		ret = append(ret, log)
	}
	return ret, nil
}

func TestSuppressLogger(t *testing.T) {
	stat := make(map[string]int64)
	wg := sync.WaitGroup{}
	wg.Add(1)
	logger.SetErrnoStatHandler(func(s string) {
		stat[s]++
		wg.Done()
	})

	filename, _ := initLogger(t, zapcore.DebugLevel)
	errno.SetNode(errno.NodeSql)
	lg := logger.NewLogger(errno.ModuleUnknown)
	lg.SetModule(errno.ModuleNetwork)

	var obwg sync.WaitGroup
	lg.GetSuppressLogger().ApplyObserver(func(_ *logger.SuppressLog) {
		obwg.Done()
	})

	err := errno.NewError(errno.RecoverPanic, 1, "127.0.0.1:8400")
	messages := []string{"some error with errno", "some error with no errno", "some debug", "some info", "some warn"}
	lg.With(zap.String("color", "red"))

	obwg.Add(5)
	for i := 0; i < 10; i++ {
		lg.Error(messages[0], zap.String("hostname", "localhost"), zap.Error(err))
	}
	for i := 0; i < 10; i++ {
		lg.Error(messages[1], zap.String("hostname", "localhost"), zap.Error(errors.New("error")))
	}
	for i := 0; i < 10; i++ {
		lg.Debug(messages[2])
	}
	for i := 0; i < 10; i++ {
		lg.Info(messages[3])
	}
	for i := 0; i < 10; i++ {
		lg.Warn(messages[4])
	}
	obwg.Wait()

	logger.CloseLogger()

	assertFileStat(t, filename)
	assertFileContents(t, filename, len(messages),
		[]string{"error", "error", "debug", "info", "warn"},
		messages)

	logs, readErr := readLog(filename)
	if !assert.NoError(t, readErr) {
		return
	}

	expErrno := fmt.Sprintf("%d%02d%d%04d",
		errno.NodeSql, errno.ModuleNetwork, errno.LevelFatal, errno.RecoverPanic)

	wg.Wait()
	assert.Equal(t, int64(1), stat[expErrno], "stat errno failed")
	assert.Equal(t, expErrno, logs[0].Errno, "incorrect errno")
	assert.Equal(t, "", logs[1].Errno, "incorrect errno, exp empty")
}
