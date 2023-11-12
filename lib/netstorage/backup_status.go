package netstorage

import (
	"sync/atomic"
)

type BackupStatus struct {
	readyBackupFilesTotal   int64
	alreadyBackupFilesTotal int64
	Shards                  map[string][]string
	Index                   map[string][]string
	Wal                     []string
	Logs                    map[string]interface{}
}

func (s *BackupStatus) AlreadyFileIncrement() {
	atomic.AddInt64(&s.alreadyBackupFilesTotal, 1)
}

func (s *BackupStatus) ReadyFileAdd(delta int64) {
	atomic.AddInt64(&s.readyBackupFilesTotal, delta)
}

func (s *BackupStatus) Clear() {
	atomic.StoreInt64(&s.readyBackupFilesTotal, 0)
	atomic.StoreInt64(&s.alreadyBackupFilesTotal, 0)
	s.Logs = make(map[string]interface{})
}

func (s *BackupStatus) Progress() int64 {
	readyFiles := atomic.LoadInt64(&s.readyBackupFilesTotal)
	alreadyFiles := atomic.LoadInt64(&s.alreadyBackupFilesTotal)
	return alreadyFiles / readyFiles
}

func NewBackupStatus() *BackupStatus {
	return &BackupStatus{
		readyBackupFilesTotal:   0,
		alreadyBackupFilesTotal: 0,
		Shards:                  make(map[string][]string),
		Index:                   make(map[string][]string),
		Logs:                    make(map[string]interface{}),
	}
}
