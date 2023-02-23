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
	"bytes"
	"fmt"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"time"

	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/shirou/gopsutil/v3/mem"
	"go.uber.org/zap"
)

type Sherlock struct {
	opts *options

	// stats
	collectCount    int
	cpuTriggerCount int
	memTriggerCount int
	grtTriggerCount int

	cpuCoolDownTime time.Time
	memCoolDownTime time.Time
	grtCoolDownTime time.Time

	// metrics stats
	memStats *MetricCircle
	cpuStats *MetricCircle
	grtStats *MetricCircle

	// collector
	collectFn func(cpuCore int, memoryLimit uint64) (int, int, int, error)

	closed int64

	mu sync.Mutex
}

// New creates a sherlock dumper.
func New(opts ...Option) *Sherlock {
	sherlock := &Sherlock{
		opts:      newOptions(),
		collectFn: collectMetrics,
		closed:    1, // init close the sherlock before start
	}

	for _, opt := range opts {
		opt(sherlock.opts)
	}

	return sherlock
}

// Set sets sherlock's optional after initialing.
func (s *Sherlock) Set(opts ...Option) {
	for _, opt := range opts {
		opt(s.opts)
	}
}

// EnableCPUDump enables the cpu dump.
func (s *Sherlock) EnableCPUDump() {
	s.opts.cpuOpts.Enable = true
}

// DisableCPUDump disables the cpu dump.
func (s *Sherlock) DisableCPUDump() {
	s.opts.cpuOpts.Enable = false
}

// EnableMemDump enables the mem dump.
func (s *Sherlock) EnableMemDump() {
	s.opts.memOpts.Enable = true
}

// DisableMemDump disables the mem dump.
func (s *Sherlock) DisableMemDump() {
	s.opts.memOpts.Enable = false
}

// EnableGrtDump enables the goroutine dump.
func (s *Sherlock) EnableGrtDump() {
	s.opts.grtOpts.Enable = true
}

// DisableGrtDump disables the goroutine dump.
func (s *Sherlock) DisableGrtDump() {
	s.opts.grtOpts.Enable = false
}

// Start starts the dump loop of sherlock.
func (s *Sherlock) Start() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !atomic.CompareAndSwapInt64(&s.closed, 1, 0) {
		s.opts.logger.Error("Sherlock has started, please don't start it again")
		return
	}
	s.opts.logger.Info("sherlock is starting")

	go s.startDumpLoop()
}

// Stop the dump loop of sherlock.
func (s *Sherlock) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !atomic.CompareAndSwapInt64(&s.closed, 0, 1) {
		//nolint
		s.opts.logger.Error("Sherlock has stop, please don't stop it again.")
		return
	}
}

const minMetricsBeforeDump = 10

func (s *Sherlock) startDumpLoop() {
	// init previous cool down time
	now := time.Now()
	s.cpuCoolDownTime = now
	s.memCoolDownTime = now
	s.grtCoolDownTime = now

	// init metrics Circle
	s.cpuStats = newMetricCircle(minMetricsBeforeDump)
	s.memStats = newMetricCircle(minMetricsBeforeDump)
	s.grtStats = newMetricCircle(minMetricsBeforeDump)

	// dump loop
	ticker := time.NewTicker(s.opts.MonitorInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if atomic.LoadInt64(&s.closed) == 1 {
				s.opts.logger.Info("[Sherlock] dump loop stopped")
				return
			}

			cpuCore := cpu.GetCpuNum()
			if cpuCore == 0 {
				s.opts.logger.Error("[Sherlock] get CPU core failed", zap.Int("core", cpuCore))
				return
			}

			memoryLimit, err := s.getMemoryLimit()
			if memoryLimit == 0 || err != nil {
				s.opts.logger.Error("[Sherlock] get memory limit failed", zap.Uint64("memory", memoryLimit), zap.Error(err))
				return
			}

			cpuN, memN, gNum, err := s.collectFn(cpuCore, memoryLimit)
			if err != nil {
				s.opts.logger.Error("[Sherlock] failed to collect metrics usage", zap.Error(err))
				continue
			}

			s.cpuStats.push(cpuN)
			s.memStats.push(memN)
			s.grtStats.push(gNum)

			s.collectCount++
			if s.collectCount < minMetricsBeforeDump {
				continue
			}

			if err = s.enableDump(cpuN); err != nil {
				s.opts.logger.Info("[Sherlock] unable to dump", zap.Error(err))
				continue
			}

			s.memCheckAndDump(memN)
			s.cpuCheckAndDump(cpuN)
			s.goroutineCheckAndDump(gNum)
		}
	}
}

// mem check start.
func (s *Sherlock) memCheckAndDump(mem int) {
	memOpts := s.opts.GetMemOpts()
	if !memOpts.Enable {
		return
	}

	if s.memCoolDownTime.After(time.Now()) {
		s.opts.logger.Debug("[Sherlock] mem dump is in cooldown")
		return
	}

	if triggered := s.memProfile(mem, memOpts); triggered {
		s.memCoolDownTime = time.Now().Add(memOpts.CoolDown)
		s.memTriggerCount++
	}
}

func (s *Sherlock) memProfile(rss int, c commonOption) bool {
	match, _ := matchRule(s.memStats, rss, c.TriggerMin, c.TriggerDiff, c.TriggerAbs)
	if !match {
		s.opts.logger.Info("[Sherlock] not match memory dump rule",
			zap.Int("config_min", c.TriggerMin), zap.Int("config_diff", c.TriggerDiff), zap.Int("config_abs", c.TriggerAbs),
			zap.Int("current", rss), zap.Ints("previous", s.memStats.sequentialData()))
		return false
	}

	s.opts.logger.Info("[Sherlock] dump memory profile",
		zap.Int("config_min", c.TriggerMin), zap.Int("config_diff", c.TriggerDiff), zap.Int("config_abs", c.TriggerAbs),
		zap.Int("current", rss), zap.Ints("previous", s.memStats.sequentialData()))

	var buf bytes.Buffer
	// write to binary gz profile
	err := pprof.Lookup("heap").WriteTo(&buf, 0) // nolint
	if err != nil {
		return false
	}

	s.writeProfileDataToFile(buf, Memory)
	return true
}

// mem check end.

// cpu check start.
func (s *Sherlock) cpuCheckAndDump(cpu int) {
	cpuOpts := s.opts.GetCPUOpts()
	if !cpuOpts.Enable {
		return
	}

	if s.cpuCoolDownTime.After(time.Now()) {
		s.opts.logger.Debug("[Sherlock] cpu dump is in cooldown")
		return
	}
	// cpuOpts is a struct, no escape.
	if triggered := s.cpuProfile(cpu, cpuOpts); triggered {
		s.cpuCoolDownTime = time.Now().Add(cpuOpts.CoolDown)
		s.cpuTriggerCount++
	}
}

func (s *Sherlock) cpuProfile(curCPUUsage int, c commonOption) bool {
	match, _ := matchRule(s.cpuStats, curCPUUsage, c.TriggerMin, c.TriggerDiff, c.TriggerAbs)
	if !match {
		// let user know why this should not dump
		s.opts.logger.Info("[Sherlock] not match cpu dump rule",
			zap.Int("config_min", c.TriggerMin), zap.Int("config_diff", c.TriggerDiff), zap.Int("config_abs", c.TriggerAbs),
			zap.Int("current", curCPUUsage), zap.Ints("previous", s.cpuStats.sequentialData()))
		return false
	}

	s.opts.logger.Info("[Sherlock] dump cpu profile",
		zap.Int("config_min", c.TriggerMin), zap.Int("config_diff", c.TriggerDiff), zap.Int("config_abs", c.TriggerAbs),
		zap.Int("current", curCPUUsage), zap.Ints("previous", s.cpuStats.sequentialData()))

	bf, filename, err := createAndGetFileInfo(s.opts.dumpPath, CPU)
	if err != nil {
		s.opts.logger.Error("[Sherlock] failed to create cpu profile file", zap.Error(err))
		return false
	}
	defer bf.Close() // nolint

	err = pprof.StartCPUProfile(bf)
	if err != nil {
		s.opts.logger.Error("[Sherlock] failed to profile cpu", zap.Error(err))
		return false
	}

	time.Sleep(defaultCPUSamplingTime)
	pprof.StopCPUProfile()
	s.opts.logger.Info("[Sherlock] profile write to file successfully", zap.String("type", "cpu"), zap.String("filename", filename))
	return true
}

// cpu check end.

// goroutine check start.
func (s *Sherlock) goroutineCheckAndDump(gNum int) {
	// get a copy instead of locking it
	grtOpts := s.opts.GetGrtOpts()
	if !grtOpts.Enable {
		return
	}

	if s.grtCoolDownTime.After(time.Now()) {
		s.opts.logger.Debug("[Sherlock] goroutine dump is in cooldown")
		return
	}

	if triggered := s.goroutineProfile(gNum, grtOpts); triggered {
		s.grtCoolDownTime = time.Now().Add(grtOpts.CoolDown)
		s.grtTriggerCount++
	}
}
func (s *Sherlock) goroutineProfile(gNum int, c grtOptions) bool {
	match, _ := matchRule(s.grtStats, gNum, c.TriggerMin, c.TriggerDiff, c.TriggerAbs)
	if c.GoroutineTriggerMaxNum > 0 && gNum >= c.GoroutineTriggerMaxNum {
		//gNum is too big, preventing system crashes
		match = false
	}
	if !match {
		// let user know why this should not dump
		s.opts.logger.Info("[Sherlock] not match goroutine dump rule",
			zap.Int("config_min", c.TriggerMin), zap.Int("config_diff", c.TriggerDiff), zap.Int("config_abs", c.TriggerAbs),
			zap.Int("current", gNum), zap.Ints("previous", s.cpuStats.sequentialData()))
		return false
	}
	s.opts.logger.Info("[Sherlock] dump goroutine profile",
		zap.Int("config_min", c.TriggerMin), zap.Int("config_diff", c.TriggerDiff), zap.Int("config_abs", c.TriggerAbs),
		zap.Int("current", gNum), zap.Ints("previous", s.cpuStats.sequentialData()))

	var buf bytes.Buffer
	err := pprof.Lookup("goroutine").WriteTo(&buf, 0) // nolint
	if err != nil {
		return false
	}

	s.writeProfileDataToFile(buf, Goroutine)
	return true
}

// goroutine check end.

func (s *Sherlock) writeProfileDataToFile(data bytes.Buffer, dumpType configureType) {
	filename, err := writeFile(data, dumpType, s.opts.dumpOptions)
	if err != nil {
		s.opts.logger.Error("[Sherlock] failed to write profile to file", zap.String("filename", filename), zap.Error(err))
		return
	}
	s.opts.logger.Info("[Sherlock] profile write to file successfully", zap.String("type", check2name[dumpType]), zap.String("filename", filename))
	return
}

func (s *Sherlock) getMemoryLimit() (uint64, error) {
	// vm
	vm, err := mem.VirtualMemory()
	if err != nil {
		return 0, err
	}
	return vm.Total, nil
}

// enableDump disables the trigger dump when the cpu usage is too high.
func (s *Sherlock) enableDump(cpuUsed int) (err error) {
	if s.opts.CPUMaxPercent != 0 && cpuUsed >= s.opts.CPUMaxPercent {
		return fmt.Errorf("current cpu used percent [%v] is greater than the CPUMaxPercent [%v]", cpuUsed, s.opts.CPUMaxPercent)
	}
	return nil
}
