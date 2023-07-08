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

package logger

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/influxdata/influxdb/toml"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/crypto"
	"github.com/openGemini/openGemini/lib/util"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	lumberjack "gopkg.in/natefinch/lumberjack.v2"
)

var logger *zap.Logger

var hooks []*lumberjack.Logger

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
	logger = getLogger(conf)
	if initHandler != nil {
		initHandler(logger)
	}
	util.SetLogger(logger)
	crypto.SetLogger(logger)
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

func getLogger(conf config.Logger) *zap.Logger {
	maxSize := rewriteMaxSize(conf.MaxSize)
	level := rewriteLevel(conf.Level)
	hookNormal := newHook(conf, maxSize, conf.GetFileName())
	hookError := newHook(conf, maxSize, makeErrFileName(conf.GetFileName()))
	hooks = append(hooks, hookNormal, hookError)

	encoder := newEncoder()

	levelNormal := zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
		return lvl >= level
	})
	levelError := zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
		return lvl >= zapcore.ErrorLevel
	})
	core := zapcore.NewTee(
		zapcore.NewCore(encoder, zapcore.AddSync(hookNormal), levelNormal),
		zapcore.NewCore(encoder, zapcore.AddSync(hookError), levelError),
	)

	return zap.New(core, zap.AddCaller(), zap.Development())
}

func rewriteMaxSize(size toml.Size) int {
	maxSize := int(size)
	if maxSize < 1024*1024 {
		maxSize = 1
	} else {
		maxSize = maxSize / (1024 * 1024)
	}
	return maxSize
}

func rewriteLevel(level zapcore.Level) zapcore.Level {
	if level < zap.DebugLevel || level > zap.FatalLevel {
		level = zap.InfoLevel
	}

	return level
}

func newHook(conf config.Logger, maxSize int, fileName string) *lumberjack.Logger {
	hook := &lumberjack.Logger{
		Filename:   fileName,
		MaxSize:    maxSize,
		MaxBackups: conf.MaxNum,
		Compress:   conf.CompressEnabled,
		MaxAge:     conf.MaxAge,
	}
	return hook
}

func makeErrFileName(fileName string) string {
	ext := filepath.Ext(fileName)
	name := strings.TrimSuffix(filepath.Base(fileName), ext)
	return fmt.Sprintf("%s/%s.error%s", filepath.Dir(fileName), name, ext)
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
