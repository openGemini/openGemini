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
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/openGemini/openGemini/lib/errno"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type SuppressLog struct {
	Node     errno.Node
	Module   errno.Module
	Level    zapcore.Level
	Message  string
	Fields   []zap.Field
	Frame    runtime.Frame
	Repeated uint64
	Checked  uint64
}

func NewSuppressLog(n errno.Node, m errno.Module, level zapcore.Level, message string, fields []zap.Field, frame runtime.Frame) *SuppressLog {
	return &SuppressLog{
		Node:     n,
		Module:   m,
		Level:    level,
		Message:  message,
		Fields:   fields,
		Frame:    frame,
		Repeated: 1,
		Checked:  1,
	}
}

type LogSuppressor struct {
	suppressLog *SuppressLog
	mutex       sync.Mutex
	callerSkip  int
}

func NewLogSuppressor() *LogSuppressor {
	return &LogSuppressor{
		suppressLog: nil,
		callerSkip:  1,
	}
}

func (ls *LogSuppressor) ForceIndulge() *SuppressLog {
	ls.mutex.Lock()
	defer ls.mutex.Unlock()

	spillingLog := ls.suppressLog
	ls.suppressLog = nil
	return spillingLog
}

func (ls *LogSuppressor) Indulge() *SuppressLog {
	ls.mutex.Lock()
	defer ls.mutex.Unlock()

	spillingLog := ls.suppressLog
	if spillingLog == nil {
		return nil
	}

	if spillingLog.Checked == spillingLog.Repeated {
		ls.suppressLog = nil
		return spillingLog
	}

	ls.suppressLog.Checked = ls.suppressLog.Repeated
	return nil
}

func (ls *LogSuppressor) Suppress(n errno.Node, m errno.Module, level zapcore.Level, message string, fields []zap.Field) *SuppressLog {
	const skipOffset = 2

	frame, ok := ls.getCallerFrame(ls.callerSkip + skipOffset)
	PC := uintptr(0)

	if ok {
		PC = frame.PC
	}

	ls.mutex.Lock()
	defer ls.mutex.Unlock()

	spillingLog := ls.suppressLog
	if spillingLog != nil && spillingLog.Frame.PC == PC {
		spillingLog.Level = level
		spillingLog.Message = message
		spillingLog.Fields = fields
		spillingLog.Repeated++
		return nil
	}

	ls.suppressLog = NewSuppressLog(n, m, level, message, fields, frame)
	return spillingLog
}

func (ls *LogSuppressor) getCallerFrame(skip int) (frame runtime.Frame, ok bool) {
	const skipOffset = 2 // skip getCallerFrame and Callers

	pc := make([]uintptr, 1)
	numFrames := runtime.Callers(skip+skipOffset, pc)
	if numFrames < 1 {
		return
	}

	frame, _ = runtime.CallersFrames(pc).Next()
	return frame, frame.PC != 0
}

type SuppressLogger struct {
	logger        *zap.Logger
	logSuppressor *LogSuppressor

	timer   *time.Timer
	timeout time.Duration
	// how many round to force indulge supress log
	forceIndulge int

	closed    chan struct{}
	closeOnce sync.Once

	loggingObserver func(*SuppressLog)
	notifyHandler   func(*SuppressLogger, *SuppressLog)
	observerMutex   sync.Mutex
}

func NewSuppressLogger() *SuppressLogger {
	logger := &SuppressLogger{
		logger:        GetLogger().WithOptions(zap.AddCallerSkip(2)),
		logSuppressor: NewLogSuppressor(),
		timer:         nil,
		// check in every 3 seconds
		timeout: 3 * time.Second,
		// default round is 5
		forceIndulge:    5,
		notifyHandler:   (*SuppressLogger).defaultNotify,
		loggingObserver: nil,
		closed:          make(chan struct{}),
	}

	go logger.run()

	return logger
}

func (l *SuppressLogger) run() {
	l.timer = time.NewTimer(l.timeout)
	round := 0
	for {
		l.timer.Reset(l.timeout)
		select {
		case <-l.timer.C:
			round++
			var log *SuppressLog
			if round >= l.forceIndulge {
				round = 0
				log = l.logSuppressor.ForceIndulge()
			} else {
				log = l.logSuppressor.Indulge()
			}
			l.suppressLogging(log)
		case <-l.closed:
			return
		}
	}
}

func (l *SuppressLogger) suppressLogging(logs ...*SuppressLog) {
	for _, log := range logs {
		if log == nil {
			continue
		}

		switch log.Level {
		case zap.DebugLevel:
			l.debug(log)
		case zap.InfoLevel:
			l.info(log)
		case zap.WarnLevel:
			l.warn(log)
		case zap.ErrorLevel:
			l.error(log)
		case zap.DPanicLevel:
			l.dpanic(log)
		case zap.PanicLevel:
			l.panic(log)
		case zap.FatalLevel:
			l.fatal(log)
		}
		l.notifyHandler(l, log)
	}
}

func (l *SuppressLogger) ApplyObserver(observer func(*SuppressLog)) {
	l.observerMutex.Lock()
	defer l.observerMutex.Unlock()
	l.loggingObserver = observer
	l.notifyHandler = (*SuppressLogger).notify
}

func (l *SuppressLogger) notify(log *SuppressLog) {
	l.observerMutex.Lock()
	defer l.observerMutex.Unlock()
	if l.loggingObserver != nil {
		l.loggingObserver(log)
	}
}

func (l *SuppressLogger) defaultNotify(log *SuppressLog) {

}

func (l *SuppressLogger) Close() {
	l.closeOnce.Do(func() {
		close(l.closed)
		_ = l.logger.Sync()
	})
}

func (l *SuppressLogger) With(fields ...zap.Field) *SuppressLogger {
	l.logger.With(fields...)
	return l
}

func (l *SuppressLogger) Error(n errno.Node, m errno.Module, msg string, fields ...zap.Field) {
	log := l.logSuppressor.Suppress(n, m, zap.ErrorLevel, msg, fields)
	if log == nil {
		return
	}
	l.suppressLogging(log)
}

func (l *SuppressLogger) error(log *SuppressLog) {
	fields := l.rewriteFields(log.Fields, log.Node, log.Module)
	fields = l.addSuppressField(fields, log)
	l.logger.Error(log.Message, fields...)
}

func (l *SuppressLogger) Info(msg string, fields ...zap.Field) {
	log := l.logSuppressor.Suppress(errno.NodeServer, errno.ModuleUnknown, zap.InfoLevel, msg, fields)
	if log == nil {
		return
	}
	l.suppressLogging(log)
}

func (l *SuppressLogger) info(log *SuppressLog) {
	fields := l.addSuppressField(log.Fields, log)
	l.logger.Info(log.Message, fields...)
}

func (l *SuppressLogger) Warn(msg string, fields ...zap.Field) {
	log := l.logSuppressor.Suppress(errno.NodeServer, errno.ModuleUnknown, zap.WarnLevel, msg, fields)
	if log == nil {
		return
	}
	l.suppressLogging(log)
}

func (l *SuppressLogger) warn(log *SuppressLog) {
	fields := l.addSuppressField(log.Fields, log)
	l.logger.Warn(log.Message, fields...)
}

func (l *SuppressLogger) Debug(msg string, fields ...zap.Field) {
	log := l.logSuppressor.Suppress(errno.NodeServer, errno.ModuleUnknown, zap.DebugLevel, msg, fields)
	if log == nil {
		return
	}
	l.suppressLogging(log)
}

func (l *SuppressLogger) debug(log *SuppressLog) {
	fields := l.addSuppressField(log.Fields, log)
	l.logger.Debug(log.Message, fields...)
}

func (l *SuppressLogger) dpanic(log *SuppressLog) {
	fields := l.addSuppressField(log.Fields, log)
	l.logger.DPanic(log.Message, fields...)
}

func (l *SuppressLogger) panic(log *SuppressLog) {
	fields := l.addSuppressField(log.Fields, log)
	l.logger.Panic(log.Message, fields...)
}

func (l *SuppressLogger) fatal(log *SuppressLog) {
	fields := l.addSuppressField(log.Fields, log)
	l.logger.Fatal(log.Message, fields...)
}

func (l *SuppressLogger) GetZapLogger() *zap.Logger {
	return l.logger.WithOptions(zap.WithCaller(true))
}

func (l *SuppressLogger) SetZapLogger(lg *zap.Logger) *SuppressLogger {
	l.logger = lg.WithOptions(zap.WithCaller(false))
	return l
}

func (l *SuppressLogger) addSuppressField(fields []zapcore.Field, log *SuppressLog) []zap.Field {
	//add location field
	entries := strings.Split(log.Frame.File, "/")
	location := ""
	if len(entries) > 1 {
		location = fmt.Sprintf("%s/%s", entries[len(entries)-2], entries[len(entries)-1])
	} else {
		location = entries[len(entries)-1]
	}
	location = fmt.Sprintf("%s:%d", location, log.Frame.Line)
	fields = append(fields, zap.String("location", location))

	//add repeated field
	fields = append(fields, zap.Uint64("repeated", log.Repeated))

	return fields
}

func (l *SuppressLogger) rewriteFields(fields []zap.Field, n errno.Node, m errno.Module) []zap.Field {
	size := len(fields)
	for i := 0; i < size; i++ {
		if fields[i].Key != "error" {
			continue
		}

		tmp, ok := fields[i].Interface.(*errno.Error)
		if !ok || tmp == nil {
			continue
		}

		code := l.makeErrno(tmp, n, m)
		stat(code)

		fields = append(fields, zap.String("errno", code))
		if tmp.Level().LogStack() {
			fields = append(fields, zap.String("stack", string(tmp.Stack())))
		}
		return fields
	}

	return fields
}

func (l *SuppressLogger) makeErrno(err *errno.Error, n errno.Node, m errno.Module) string {
	level := err.Level() % (errno.LevelFatal + 1)
	module := err.Module()
	if module == errno.ModuleUnknown {
		module = m
	}

	return fmt.Sprintf("%d%02d%d%04d", n, module, level, err.Errno())
}

var (
	singletonLogger *SuppressLogger
	once            sync.Once
)

func GetSuppressLogger() *SuppressLogger {
	once.Do(func() {
		singletonLogger = NewSuppressLogger()
		SetInitLoggerHandler(func(logger *zap.Logger) {
			singletonLogger.SetZapLogger(logger)
		})
	})
	return singletonLogger
}

type Logger struct {
	logger *SuppressLogger
	node   errno.Node
	module errno.Module
}

var loggerPool sync.Map

func NewLogger(module errno.Module) *Logger {
	l, ok := loggerPool.Load(module)
	if ok {
		log, _ := l.(*Logger)
		return log
	}
	// ignore concurrent situation, repeat store same module logger
	log := &Logger{
		logger: GetSuppressLogger(),
		node:   errno.GetNode(),
		module: module,
	}
	loggerPool.Store(module, log)
	return log
}

func (l *Logger) With(fields ...zap.Field) *Logger {
	l.logger.With(fields...)
	return l
}

func (l *Logger) SetModule(m errno.Module) {
	l.module = m
}

func (l *Logger) Error(msg string, fields ...zap.Field) {
	l.logger.Error(l.node, l.module, msg, fields...)
}

func (l *Logger) Info(msg string, fields ...zap.Field) {
	l.logger.Info(msg, fields...)
}

func (l *Logger) Warn(msg string, fields ...zap.Field) {
	l.logger.Warn(msg, fields...)
}

func (l *Logger) Debug(msg string, fields ...zap.Field) {
	if level > zapcore.DebugLevel {
		return
	}
	l.logger.Debug(msg, fields...)
}

func (l *Logger) GetZapLogger() *zap.Logger {
	return l.logger.GetZapLogger()
}

func (l *Logger) SetZapLogger(lg *zap.Logger) *Logger {
	tmp := l.logger.SetZapLogger(lg)
	l.logger = tmp
	return l
}

func (l *Logger) GetSuppressLogger() *SuppressLogger {
	return l.logger
}

func (l *Logger) IsDebugLevel() bool {
	return level == zap.DebugLevel
}
