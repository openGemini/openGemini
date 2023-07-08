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

package collector

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/host"
	"github.com/shirou/gopsutil/v3/mem"
	"go.uber.org/zap"
)

var (
	collectFrequency     = 10 * time.Second
	metricFlushFrequency = 4 * time.Second
	nodeMst              = "system"
)

const (
	psStore = "ps -ef | grep -iE \"ts-store.*-config\" | grep -v grep"
	psSql   = "ps -ef | grep -iE \"ts-sql.*-config\" | grep -v grep"
	psMeta  = "ps -ef | grep -iE \"ts-meta.*-config\" | grep -v grep"

	DuIndex = "du -al --max-depth=1 %s/data/*/*/*/index | grep index$"

	running int64 = 1
	killed  int64 = 2
)

type NodeCollector struct {
	done chan struct{}

	conf     *config.MonitorMain
	Reporter *ReportJob
	logger   *logger.Logger

	nodeMetric *nodeMetrics
}

type nodeMetrics struct {
	mu sync.RWMutex

	Uptime int64 // system running duration(e.g., 963389), unit is s

	CpuNum   int64   // number of CPU cores(e.g., 32)
	CpuUsage float64 // cpu usage of the node(e.g., 70), unit is %

	// vm
	MemSize        int64   // memory capacity (e.g., 5000), unit is MB
	MemInUse       int64   // memory used, exclude cache and buffer (e.g., 3000), unit is MB
	MemCacheBuffer int64   // memory cache and buffer (e.g., 500), unit is MB
	MemUsage       float64 // memory usage: MemInUse/MemSize*100 (e.g., 60.0), unit is %

	// process
	StorePid    int64 // store process pid
	StoreStatus int64 // store process status, seek running or killed
	SqlPid      int64
	SqlStatus   int64
	MetaPid     int64
	MetaStatus  int64

	// disk
	DiskSize  int64   // rated disk capacity (e.g., 5000), unit is MB
	DiskUsed  int64   // used disk capacity (e.g., 3000), unit is MB
	DiskUsage float64 // Disk usage (e.g., 60.0), unit is %

	// Second disk
	AuxDiskSize  int64
	AuxDiskUsed  int64
	AuxDiskUsage float64

	// Index path Used
	IndexUsed int64 // Disk space used by Indexes (e.g., 6324), unit is MB
}

func NewNodeCollector(logger *logger.Logger, conf *config.MonitorMain) *NodeCollector {
	return &NodeCollector{
		done:       make(chan struct{}),
		conf:       conf,
		logger:     logger,
		nodeMetric: &nodeMetrics{},
	}
}

func (nc *NodeCollector) Start() {
	_, err := os.Stat(nc.conf.DiskPath)
	if err != nil {
		nc.logger.Error("DiskPath is invalid", zap.Error(err))
		return
	}
	nc.logger.Info("start NodeCollector")

	go nc.collect()
	go nc.report()
}

func (nc *NodeCollector) report() {
	ticker := time.NewTicker(collectFrequency)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			point := nc.formatPoint()
			if err := nc.Reporter.WriteData(point); err != nil {
				nc.logger.Error("report node metrics error", zap.Error(err))
			}
		case <-nc.done:
			nc.logger.Info("collect node metrics closed")
			return
		}
	}
}

func (nc *NodeCollector) collect() {
	ctx := context.Background()
	for {
		time.Sleep(metricFlushFrequency)
		err := nc.collectBasic(ctx)
		if err != nil {
			nc.logger.Error("collect basic metrics error", zap.Error(err))
			continue
		}
		err = nc.collectIndexUsed()
		if err != nil {
			nc.logger.Error("collect index used metrics error", zap.Error(err))
			continue
		}
	}

}

func (nc *NodeCollector) collectBasic(ctx context.Context) error {
	uptime, err := host.UptimeWithContext(ctx)
	if err != nil {
		return err
	}
	nc.nodeMetric.mu.Lock()
	nc.nodeMetric.Uptime = int64(uptime)
	nc.nodeMetric.mu.Unlock()

	nc.nodeMetric.mu.Lock()
	nc.nodeMetric.CpuNum = int64(runtime.NumCPU())
	nc.nodeMetric.mu.Unlock()

	nc.nodeMetric.mu.Lock()
	nc.nodeMetric.Uptime = int64(uptime)
	nc.nodeMetric.mu.Unlock()

	// cpu
	for i := 0; i < 100; i++ {
		cpuPercent, err := cpu.PercentWithContext(ctx, 0, false)
		if err != nil {
			return err
		}
		if len(cpuPercent) == 0 {
			continue
		}
		nc.nodeMetric.mu.Lock()
		nc.nodeMetric.CpuUsage = cpuPercent[0]
		nc.nodeMetric.mu.Unlock()
		break
	}

	// vm
	vm, err := mem.VirtualMemoryWithContext(ctx)
	if err != nil {
		return err
	}
	nc.nodeMetric.mu.Lock()
	nc.nodeMetric.MemSize = int64(vm.Total) / 1024 / 1024
	nc.nodeMetric.MemInUse = int64(vm.Used) / 1024 / 1024
	nc.nodeMetric.MemCacheBuffer = int64(vm.Cached+vm.Buffers) / 1024 / 1024
	nc.nodeMetric.MemUsage = float64(nc.nodeMetric.MemInUse) / float64(nc.nodeMetric.MemSize) * 100.0
	nc.nodeMetric.mu.Unlock()

	// process
	var status int64
	if strings.Contains(nc.conf.Process, "ts-store") {
		res, err := exec.Command("/bin/sh", "-c", psStore).Output()
		var StorePid int
		if err != nil {
			status = killed
		} else {
			status = running
			StorePid, _ = strconv.Atoi(strings.Fields(strings.TrimSpace(string(res)))[1])
		}
		nc.nodeMetric.mu.Lock()
		nc.nodeMetric.StorePid = int64(StorePid)
		nc.nodeMetric.StoreStatus = status
		nc.nodeMetric.mu.Unlock()
	}
	if strings.Contains(nc.conf.Process, "ts-sql") {
		res, err := exec.Command("/bin/sh", "-c", psSql).Output()
		var SqlPid int
		if err != nil {
			status = killed
		} else {
			status = running
			SqlPid, _ = strconv.Atoi(strings.Fields(strings.TrimSpace(string(res)))[1])
		}
		nc.nodeMetric.mu.Lock()
		nc.nodeMetric.SqlPid = int64(SqlPid)
		nc.nodeMetric.SqlStatus = status
		nc.nodeMetric.mu.Unlock()
	}
	if strings.Contains(nc.conf.Process, "ts-meta") {
		res, err := exec.Command("/bin/sh", "-c", psMeta).Output()
		var MetaPid int
		if err != nil {
			status = killed
		} else {
			status = running
			MetaPid, _ = strconv.Atoi(strings.Fields(strings.TrimSpace(string(res)))[1])
		}
		nc.nodeMetric.mu.Lock()
		nc.nodeMetric.MetaPid = int64(MetaPid)
		nc.nodeMetric.MetaStatus = status
		nc.nodeMetric.mu.Unlock()
	}

	// disk
	diskInfo, err := disk.UsageWithContext(ctx, nc.conf.DiskPath)
	if err != nil {
		return err
	}
	nc.nodeMetric.mu.Lock()
	nc.nodeMetric.DiskSize = int64(diskInfo.Total) / 1024 / 1024
	nc.nodeMetric.DiskUsed = int64(diskInfo.Used) / 1024 / 1024
	nc.nodeMetric.DiskUsage = diskInfo.UsedPercent
	nc.nodeMetric.mu.Unlock()

	// Second
	auxdiskInfo, err := disk.UsageWithContext(ctx, nc.conf.AuxDiskPath)
	if err != nil {
		return err
	}
	nc.nodeMetric.mu.Lock()
	nc.nodeMetric.AuxDiskSize = int64(auxdiskInfo.Total) / 1024 / 1024
	nc.nodeMetric.AuxDiskUsed = int64(auxdiskInfo.Used) / 1024 / 1024
	nc.nodeMetric.AuxDiskUsage = auxdiskInfo.UsedPercent
	nc.nodeMetric.mu.Unlock()
	return nil
}

func (nc *NodeCollector) collectIndexUsed() error {
	files, err := filepath.Glob(fmt.Sprintf("%s/data/*/*/*/index", nc.conf.DiskPath))
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return nil
	}
	duCmd := fmt.Sprintf(DuIndex, path.Clean(nc.conf.DiskPath))
	// #nosec
	res, err := exec.Command("/bin/sh", "-c", duCmd).Output()
	if err != nil {
		return err
	}
	fields := bytes.Fields(res)
	var indexUsed int64
	for i, v := range fields {
		if i%2 != 0 {
			continue
		}
		used, _ := strconv.Atoi(strings.TrimSpace(string(v))) // KB
		indexUsed += int64(used)
	}
	nc.nodeMetric.mu.Lock()
	nc.nodeMetric.IndexUsed = indexUsed / 1024
	nc.nodeMetric.mu.Unlock()
	return nil
}

var metricFields = []string{
	"Uptime=%d",
	"CpuNum=%d",
	"CpuUsage=%.2f",
	"MemSize=%d",
	"MemInUse=%d",
	"MemCacheBuffer=%d",
	"MemUsage=%.2f",
	"StorePid=%d",
	"StoreStatus=%d",
	"SqlPid=%d",
	"SqlStatus=%d",
	"MetaPid=%d",
	"MetaStatus=%d",
	"DiskSize=%d",
	"DiskUsed=%d",
	"DiskUsage=%.2f",
	"AuxDiskSize=%d",
	"AuxDiskUsed=%d",
	"AuxDiskUsage=%.2f",
	"IndexUsed=%d",
}

func (nc *NodeCollector) formatPoint() string {
	nc.nodeMetric.mu.RLock()
	defer nc.nodeMetric.mu.RUnlock()

	// The order of fields is important.
	field := fmt.Sprintf(
		strings.Join(metricFields, ","),
		nc.nodeMetric.Uptime,
		nc.nodeMetric.CpuNum,
		nc.nodeMetric.CpuUsage,
		nc.nodeMetric.MemSize,
		nc.nodeMetric.MemInUse,
		nc.nodeMetric.MemCacheBuffer,
		nc.nodeMetric.MemUsage,
		nc.nodeMetric.StorePid,
		nc.nodeMetric.StoreStatus,
		nc.nodeMetric.SqlPid,
		nc.nodeMetric.SqlStatus,
		nc.nodeMetric.MetaPid,
		nc.nodeMetric.MetaStatus,
		nc.nodeMetric.DiskSize,
		nc.nodeMetric.DiskUsed,
		nc.nodeMetric.DiskUsage,
		nc.nodeMetric.AuxDiskSize,
		nc.nodeMetric.AuxDiskUsed,
		nc.nodeMetric.AuxDiskUsage,
		nc.nodeMetric.IndexUsed,
	)
	return fmt.Sprintf("%s,host=%s %s", nodeMst, nc.conf.Host, field)
}

func (nc *NodeCollector) Close() {
	close(nc.done)
}
