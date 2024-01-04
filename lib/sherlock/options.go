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

package sherlock

import (
	"time"

	"github.com/openGemini/openGemini/lib/logger"
)

type configureType int

const (
	Memory configureType = iota
	CPU
	Goroutine
)

func (t configureType) string() string {
	switch t {
	case CPU:
		return "cpu"
	case Memory:
		return "mem"
	case Goroutine:
		return "goroutine"
	default:
		return "unknown"
	}
}

// check type to profile name, just align to pprof
var type2name = map[configureType]string{
	Memory:    "heap",
	CPU:       "cpu",
	Goroutine: "goroutine",
}

type options struct {
	logger *logger.Logger

	CPUMaxPercent int // if cpuUsage > CPUMaxPercent, disable dump

	*dumpOptions

	// interval for dump loop, default 10s
	MonitorInterval time.Duration

	memOpts *commonOption
	cpuOpts *commonOption
	grtOpts *grtOptions
}

const (
	defaultInterval = 10 * time.Second
	defaultDumpPath = "/tmp"
	defaultMaxNum   = 32
	defaultMaxAge   = 7
)

func newOptions() *options {
	opts := &options{
		memOpts: newMemOptions(),
		cpuOpts: newCPUOptions(),
		grtOpts: newGrtOptions(),

		MonitorInterval: defaultInterval,
		dumpOptions: &dumpOptions{
			dumpPath: defaultDumpPath,
			maxNum:   defaultMaxNum,
			maxAge:   defaultMaxAge,
		},
	}
	return opts
}

type Option func(opts *options)

// WithMonitorInterval set: interval
func WithMonitorInterval(interval time.Duration) Option {
	return func(opts *options) {
		opts.MonitorInterval = interval
	}
}

// WithCPUMax : set the CPUMaxPercent parameter as max
func WithCPUMax(max int) Option {
	return func(opts *options) {
		opts.CPUMaxPercent = max
	}
}

// WithSavePath set the profile export path
func WithSavePath(dumpPath string) Option {
	return func(opts *options) {
		opts.dumpPath = dumpPath
	}
}

// WithMaxNum set the maximum number of old profile files to retain
func WithMaxNum(num int) Option {
	return func(opts *options) {
		opts.maxNum = num
	}
}

// WithMaxAge set the maximum number of days to retain old profile files based on the
// timestamp encoded in their filename
func WithMaxAge(age int) Option {
	return func(opts *options) {
		opts.maxAge = age
	}
}

func WithLogger(log *logger.Logger) Option {
	return func(opts *options) {
		opts.logger = log
	}
}

// dumpOptions contains configuration about dump file.
type dumpOptions struct {
	dumpPath string
	maxNum   int
	maxAge   int
}

type commonOption struct {
	Enable bool

	TriggerMin  int
	TriggerDiff int
	TriggerAbs  int

	CoolDown time.Duration
}

func newCommonOption(triggerMin, triggerDiff, triggerAbs int, coolDown time.Duration) *commonOption {
	return &commonOption{
		Enable:      false,
		TriggerMin:  triggerMin,
		TriggerDiff: triggerDiff,
		TriggerAbs:  triggerAbs,
		CoolDown:    coolDown,
	}
}

func (co *commonOption) Set(min, diff, abs int, coolDown time.Duration) {
	co.TriggerMin, co.TriggerDiff, co.TriggerAbs, co.CoolDown = min, diff, abs, coolDown
}

const (
	defaultMemTriggerMin  = 10 // 10%
	defaultMemTriggerDiff = 25 // 25%
	defaultMemTriggerAbs  = 80 // 80%

	defaultCoolDown = time.Minute
)

// newMemOptions
// enable the heap dumper, should dump if one of the following requirements is matched
//  1. memory usage > TriggerMin && memory usage diff > TriggerDiff
//  2. memory usage > TriggerAbs
func newMemOptions() *commonOption {
	return newCommonOption(
		defaultMemTriggerMin,
		defaultMemTriggerDiff,
		defaultMemTriggerAbs,
		defaultCoolDown,
	)
}

// WithMemRule set the memory rule options.
func WithMemRule(min int, diff int, abs int, coolDown time.Duration) Option {
	return func(opts *options) {
		opts.memOpts.Set(min, diff, abs, coolDown)
	}
}

// GetMemOpts return a copy of commonOption.
func (o *options) GetMemOpts() commonOption {
	return *o.memOpts
}

const (
	defaultCPUTriggerMin   = 10               // 10%
	defaultCPUTriggerDiff  = 25               // 25%
	defaultCPUTriggerAbs   = 70               // 70%
	defaultCPUSamplingTime = 10 * time.Second // collect 10s cpu profile
)

// newCPUOptions
// enable the cpu dumper, should dump if one of the following requirements is matched
// in percent
//  1. cpu usage > CPUTriggerMin && cpu usage diff > CPUTriggerDiff
//  2. cpu usage > CPUTriggerAbs
func newCPUOptions() *commonOption {
	return newCommonOption(
		defaultCPUTriggerMin,
		defaultCPUTriggerDiff,
		defaultCPUTriggerAbs,
		defaultCoolDown,
	)
}

// WithCPURule set the cpu rule options.
func WithCPURule(min int, diff int, abs int, coolDown time.Duration) Option {
	return func(opts *options) {
		opts.cpuOpts.Set(min, diff, abs, coolDown)
	}
}

// GetCPUOpts return a copy of commonOption
func (o *options) GetCPUOpts() commonOption {
	return *o.cpuOpts
}

type grtOptions struct {
	*commonOption
	GoroutineTriggerMaxNum int // goroutine trigger max in number
}

const (
	defaultGoroutineTriggerMin  = 3000   // 3000 goroutines
	defaultGoroutineTriggerDiff = 20     // 20%  diff
	defaultGoroutineTriggerAbs  = 200000 // 200k goroutines
	defaultGoroutineCoolDown    = time.Minute * 10
)

// enable the goroutine dumper, should dump if one of the following requirements is matched
//  1. goroutine_num > TriggerMin && goroutine_num < GoroutineTriggerNumMax && goroutine diff percent > TriggerDiff
//  2. goroutine_num > GoroutineTriggerNumAbsNum && goroutine_num < GoroutineTriggerNumMax
func newGrtOptions() *grtOptions {
	comm := newCommonOption(defaultGoroutineTriggerMin, defaultGoroutineTriggerDiff, defaultGoroutineTriggerAbs, defaultGoroutineCoolDown)
	return &grtOptions{commonOption: comm}
}

// WithGrtRule set the goroutine rule options.
func WithGrtRule(min int, diff int, abs int, max int, coolDown time.Duration) Option {
	return func(opts *options) {
		opts.grtOpts.Set(min, diff, abs, coolDown)
		opts.grtOpts.GoroutineTriggerMaxNum = max
	}
}

// GetGrtOpts return a copy of commonOption
func (o *options) GetGrtOpts() grtOptions {
	return *o.grtOpts
}
