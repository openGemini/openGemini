// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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

package dict

import (
	"os"
	"strings"
)

const DefaultSep = ","

var defaultDict = &Dict{valMap: make(map[string]int)}

// TSBS devops fields
var tsbsDevopsDict = "usage_guest,usage_guest_nice,usage_idle,usage_iowait,usage_irq,usage_nice,usage_softirq," +
	"usage_steal,usage_system,usage_user,inodes_free,inodes_total,inodes_used,io_time," +
	"read_bytes,read_time,reads,write_bytes,write_time,writes,boot_time,context_switches,disk_pages_in," +
	"disk_pages_out,interrupts,processes_forked,available,available_percent,buffered,buffered_percent,float," +
	"cached,free,total,used,used_percent,bytes_recv,bytes_sent,drop_in,drop_out,err_in,err_out," +
	"packets_recv,packets_sent,accepts,active,handled,reading,requests,waiting,writing,blk_read_time,blk_write_time," +
	"blks_hit,blks_read,conflicts,deadlocks,numbackends,temp_bytes,temp_files,tup_deleted,tup_fetched,tup_inserted," +
	"tup_returned,tup_updated,xact_commit,xact_rollback,connected_clients,connected_slaves,evicted_keys," +
	"expired_keys,instantaneous_input_kbps,instantaneous_ops_per_sec,instantaneous_output_kbps,keyspace_hits," +
	"keyspace_misses,latest_fork_usec,master_repl_offset,mem_fragmentation_ratio,pubsub_channels,pubsub_patterns," +
	"rdb_changes_since_last_save,repl_backlog_active,repl_backlog_histlen,repl_backlog_size,sync_full," +
	"sync_partial_err,sync_partial_ok,total_connections_received,uptime_in_seconds,used_cpu_sys,used_cpu_sys_children," +
	"used_cpu_user,used_cpu_user_children,used_memory,used_memory_lua,used_memory_peak,used_memory_rss"

func init() {
	defaultDict.Load(strings.Split(tsbsDevopsDict, DefaultSep))
	defaultDict.Load([]string{"time"})
}

func DefaultDict() *Dict {
	return defaultDict
}

type Dict struct {
	values []string
	valMap map[string]int
}

func (d *Dict) LoadFromFiles(sep string, files ...string) error {
	for i := range files {
		if err := d.loadFromFile(sep, files[i]); err != nil {
			return err
		}
	}
	return nil
}

func (d *Dict) loadFromFile(sep string, file string) error {
	buf, err := os.ReadFile(file)
	if err != nil {
		return err
	}

	d.Load(strings.Split(string(buf), sep))
	return nil
}

func (d *Dict) Load(s []string) {
	for i := range s {
		d.Add(s[i])
	}
}

func (d *Dict) Add(val string) {
	_, ok := d.valMap[val]
	if ok {
		return
	}

	d.values = append(d.values, val)
	d.valMap[val] = len(d.values) - 1
}

func (d *Dict) GetID(v string) int {
	id, ok := d.valMap[v]
	if !ok {
		return -1
	}
	return id
}

func (d *Dict) GetValue(idx int) string {
	if idx >= len(d.values) {
		return ""
	}

	return d.values[idx]
}
