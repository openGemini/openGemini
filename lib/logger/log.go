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

package logger

import (
	"fmt"
	"time"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/crypto"
	"github.com/openGemini/openGemini/lib/util"
	"go.etcd.io/etcd/raft/v3"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

var logger *zap.Logger

var hooks []*lumberjack.Logger

var Alevel zap.AtomicLevel

func SetLevel(lev string) error {
	var l zapcore.Level
	if err := l.UnmarshalText([]byte(lev)); err != nil {
		return err
	}
	Alevel.SetLevel(l)

	return nil
}

var initHandler func(*zap.Logger)

var level zapcore.Level

func init() {
	InitLogger(config.NewLogger(config.AppSingle))
}

func SetInitLoggerHandler(handler func(*zap.Logger)) {
	initHandler = handler
}

func InitLogger(conf config.Logger) {
	level = conf.Level
	logger = getLogger(conf, conf.GetApp())
	if initHandler != nil {
		initHandler(logger)
	}
	util.SetLogger(logger)
	crypto.SetLogger(logger)
	fmt.Printf("%s init logger, conf: %+v\n", time.Now().String(), conf)
}

func GetLogger() *zap.Logger {
	return logger
}

func SetLogger(zapLogger *zap.Logger) {
	logger = zapLogger
	if initHandler != nil {
		initHandler(logger)
	}
}

func CloseLogger() {
	_ = logger.Sync()
	closeHooks()
}

func getLogger(conf config.Logger, name string) *zap.Logger {
	hookNormal := conf.NewLumberjackLogger(name)
	hookError := conf.NewLumberjackLogger(makeErrFileName(name))
	hooks = append(hooks, hookNormal, hookError)

	encoder := newEncoder()

	logLevel := rewriteLevel(conf.Level)

	levelError := zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
		return lvl >= zapcore.ErrorLevel
	})

	Alevel = zap.NewAtomicLevel()
	Alevel.SetLevel(logLevel)

	core := zapcore.NewTee(
		zapcore.NewCore(encoder, zapcore.AddSync(hookNormal), Alevel),
		zapcore.NewCore(encoder, zapcore.AddSync(hookError), levelError),
	)

	return zap.New(core, zap.AddCaller(), zap.Development())
}

func rewriteLevel(level zapcore.Level) zapcore.Level {
	if level < zap.DebugLevel || level > zap.FatalLevel {
		level = zap.InfoLevel
	}

	return level
}

func makeErrFileName(fileName string) string {
	return fmt.Sprintf("%s.error", fileName)
}

func closeHooks() {
	if len(hooks) == 0 {
		return
	}
	for _, h := range hooks {
		_ = h.Close()
	}
	hooks = nil
}

func newEncoder() zapcore.Encoder {
	// log format
	encoderConfig := zapcore.EncoderConfig{
		TimeKey:        "time",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.RFC3339NanoTimeEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder, //zapcore.FullCallerEncoder,
		EncodeName:     zapcore.FullNameEncoder,
	}

	return zapcore.NewJSONEncoder(encoderConfig)
}

var srLogger raft.Logger

func InitSrLogger(conf config.Logger) {
	level = conf.Level
	zapLogger := getLogger(conf, config.DefaultStoreRaftLoggerName)
	fmt.Printf("%s init sr logger, conf: %+v\n", time.Now().String(), conf)
	srLogger = &SugarLogger{zapLogger.Sugar()}
}

func GetSrLogger() raft.Logger {
	return srLogger
}

type SugarLogger struct {
	*zap.SugaredLogger
}

func (sl *SugarLogger) Warning(v ...interface{}) {
	sl.Warn(v...)
}

func (sl *SugarLogger) Warningf(format string, v ...interface{}) {
	sl.Warnf(format, v...)
}

func (sl *SugarLogger) Panic(args ...interface{}) {
	sl.Error(args...)
}

func (sl *SugarLogger) Panicf(template string, args ...interface{}) {
	sl.Errorf(template, args...)
}
