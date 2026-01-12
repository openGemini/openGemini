// Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.
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

package engine

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/engine/index/tsi"
	"github.com/openGemini/openGemini/lib/backup"
	"github.com/openGemini/openGemini/lib/errno"
	meta "github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/util"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"go.uber.org/zap"
)

type BackupStatus string

const (
	Success     BackupStatus = "backup success"
	Failed      BackupStatus = "backup field"
	InProgress  BackupStatus = "backup in progress"
	NotBackedUp BackupStatus = "not backed up"
)

type Backup struct {
	Name          string
	time          int64
	BackupLogInfo *backup.BackupLogInfo
	Engine        *EngineImpl
	IsAborted     bool
	Status        BackupStatus

	IsInc           bool
	IsRemote        bool
	OnlyBackupMater bool
	BackupPath      string
	DataBases       []string
}

func (s *Backup) RunBackupData() error {
	s.time = time.Now().UnixNano()
	dbPtIds := s.Engine.GetAllDBPtIds()
	ch := make(chan struct{})
	var wg sync.WaitGroup

	result := &backup.BackupResult{
		Result: "backup success",
		Time:   s.time, Databases: make(map[string]struct{}),
		DataDir: s.Engine.dataPath,
		WalDir:  s.Engine.walPath,
	}
	wg.Add(1)
	go execTicker(ch, s.BackupPath, &wg)
	defer func() {
		close(ch)
		wg.Wait()
		if r := recover(); r != nil {
			err := errno.NewError(errno.RecoverPanic, r)
			log.Error(err.Error())
			result.Result = err.Error()
			s.Status = Failed
		} else {
			s.Status = Success
		}

		if err := backup.WriteResultFile(result, s.BackupPath, backup.ResultLog); err != nil {
			log.Error(err.Error())
		}
	}()

	if len(s.DataBases) == 0 {
		for dbName, pts := range dbPtIds {
			if err := s.TraversePts(dbName, pts); err != nil {
				result.Result = fmt.Sprintf("backup failed, error: %s", err.Error())
				return err
			}
		}
	} else {
		dbMap := make(map[string]struct{})
		for _, dbName := range s.DataBases {
			pts, ok := dbPtIds[dbName]
			if !ok {
				continue
			}
			if _, ok := dbMap[dbName]; ok {
				continue
			}
			dbMap[dbName] = struct{}{}
			if err := s.TraversePts(dbName, pts); err != nil {
				result.Result = fmt.Sprintf("backup failed, error: %s", err.Error())
				return err
			}
		}
		result.Databases = dbMap
	}

	return nil
}

func (s *Backup) TraversePts(dbName string, pts []uint32) error {
	for _, ptId := range pts {
		err := s.BackupPt(dbName, ptId)
		if err != nil {
			s.Status = Failed
			return err
		}
	}
	return nil
}

func (s *Backup) BackupPt(dbName string, ptId uint32) error {
	backupPath := filepath.Join(s.BackupPath, backup.DataBackupDir)
	metaClient := s.Engine.metaClient
	p, err := s.Engine.getPartition(dbName, ptId, true)
	if err != nil {
		return err
	}
	p.mu.RLock()
	defer func() {
		p.unref()
		p.mu.RUnlock()
	}()

	var peersPtIDMap map[uint32]*NodeInfo
	if s.OnlyBackupMater && metaClient.DBRepGroups(dbName) != nil {
		if s.Engine.metaClient.IsMasterPt(ptId, dbName) {
			peersPtIDMap, err = GenShardDirPath(metaClient, dbName, ptId)
			if err != nil {
				return err
			}
		} else {
			return nil
		}
	}

	shardIds := p.ShardIds(nil)
	for _, id := range shardIds {
		sh := p.Shard(id)
		opened := sh.IsOpened()
		err := sh.OpenAndEnable(s.Engine.metaClient)

		if err != nil {
			return err
		}
		if s.IsInc {
			s.BackupLogInfo = &backup.BackupLogInfo{}
			if err := backup.ReadBackupLogFile(filepath.Join(sh.GetDataPath(), backup.BackupLogPath, backup.FullBackupLog), s.BackupLogInfo); err != nil {
				log.Info("backupLog file not exist", zap.Error(err))
			}
			if err := s.IncBackup(sh, backupPath, p.path, peersPtIDMap); err != nil {
				return err
			}
		} else {
			if err := s.FullBackup(sh, backupPath, p.path, peersPtIDMap); err != nil {
				return err
			}
		}
		if !opened {
			sh.FreeSequencer()
		}
	}

	// backup index
	for _, ib := range p.indexBuilder {
		if err := BackupIndex(ib, backupPath); err != nil {
			return err
		}
	}

	// backup wal
	if p.node == nil {
		return nil
	}
	storage := p.node.GetStorage()
	entryLog := storage.GetRaftEntryLog()
	walStorage := storage.GetDir()
	walDir, _ := filepath.Split(walStorage)
	walBackupPath := filepath.Join(s.BackupPath, backup.WalBackupDir)
	dstPath := filepath.Join(walBackupPath, walDir)
	storage.Lock()
	entryLog.FileRLock()
	defer func() {
		storage.UnLock()
		entryLog.FileRUnLock()
	}()
	if err := backup.FolderCopy(walDir, dstPath); err != nil {
		log.Error("backup wal file error", zap.Error(err))
		return err
	}

	return nil
}

func (s *Backup) FullBackup(sh Shard, dataPath, nodePath string, peersPtIDMap map[uint32]*NodeInfo) error {
	t := sh.GetTableStore()
	logPath := sh.GetDataPath()
	fileListMap := make(map[string][][]string)

	fileList := t.GetAllMstList()

	for _, name := range fileList {
		if fileListMap[name] != nil {
			continue
		}
		fileList, err := s.FullBackupTableFile(sh.GetDataPath(), t, peersPtIDMap, name, true, nodePath, dataPath)
		if err != nil {
			return err
		}
		if len(fileList) > 0 {
			fileListMap[name] = fileList
		}

	}

	if len(fileListMap) > 0 {
		backupLog := &backup.BackupLogInfo{
			FullBackupTime: s.time,
			FileListMap:    fileListMap,
		}
		content, err := json.MarshalIndent(&backupLog, "", "\t")
		if err != nil {
			return err
		}
		if err := backup.WriteBackupLogFile(content, logPath, backup.FullBackupLog); err != nil {
			return err
		}
		if err := backup.WriteBackupLogFile(content, filepath.Join(dataPath, logPath), backup.FullBackupLog); err != nil {
			return err
		}
	}

	return nil
}

func (s *Backup) IncBackup(sh Shard, dataPath, nodePath string, peersPtIDMap map[uint32]*NodeInfo) error {
	t := sh.GetTableStore()
	logPath := sh.GetDataPath()

	fileList := t.GetAllMstList()
	addFileListMap := make(map[string][][]string, 0)
	delFileListMap := make(map[string][][]string, 0)
	for _, name := range fileList {
		if addFileListMap[name] != nil || delFileListMap[name] != nil {
			continue
		}
		aList, dList, err := s.IncBackupTableFile(sh.GetDataPath(), t, peersPtIDMap, name, true, nodePath, dataPath)
		if err != nil {
			return err
		}
		if len(aList) > 0 {
			addFileListMap[name] = aList
		}
		if len(dList) > 0 {
			delFileListMap[name] = dList
		}
	}

	if len(addFileListMap) > 0 || len(delFileListMap) > 0 {
		incBackupLog := &backup.IncBackupLogInfo{
			AddFileListMap: addFileListMap,
			DelFileListMap: delFileListMap,
		}
		content, err := json.MarshalIndent(&incBackupLog, "", "\t")
		if err != nil {
			return err
		}
		incBackupLogPath := filepath.Join(dataPath, logPath)
		if err := backup.WriteBackupLogFile(content, incBackupLogPath, backup.IncBackupLog); err != nil {
			return err
		}
		if err := backup.WriteBackupLogFile(content, logPath, backup.IncBackupLog); err != nil {
			return err
		}
	}

	return nil
}

func (s *Backup) FullBackupTableFile(shardPath string, t immutable.TablesStore, peersPtIDMap map[uint32]*NodeInfo, name string, isOrder bool, nodePath, outPath string) ([][]string, error) {
	orderfiles, unOrderFiles, _ := t.GetBothFilesRef(name, false, util.TimeRange{}, nil)
	if len(unOrderFiles) > 0 {
		orderfiles = append(orderfiles, unOrderFiles...)
	}
	csFiles, ok := t.GetCSFilesRef(name)
	if ok {
		if len(csFiles) > 0 {
			orderfiles = append(orderfiles, csFiles...)
		}
	}
	if len(orderfiles) == 0 {
		return nil, nil
	}
	defer func() {
		immutable.UnrefFiles(orderfiles...)
	}()

	fileList := make([][]string, 0, len(orderfiles))
	for _, f := range orderfiles {
		if s.IsAborted {
			return nil, fmt.Errorf("backup aborted")
		}
		if err := copyTableFile(f, nil, shardPath, peersPtIDMap, nodePath, outPath, &fileList); err != nil {
			return fileList, err
		}
	}
	if len(csFiles) > 0 {
		countPath := filepath.Join(filepath.Dir(csFiles[0].Path()), immutable.CountBinFile)
		fileList = append(fileList, []string{countPath})
		if err := retryFileCopy(countPath, filepath.Join(outPath, countPath)); err != nil {
			log.Error("backup colstore count file error", zap.Error(err))
			return fileList, err
		}
	}

	return fileList, nil
}

func copyTableFile(f immutable.TSSPFile, seen map[string]bool, shardPath string, peersPtIDMap map[uint32]*NodeInfo, nodePath, outPath string, fileList *[][]string) error {
	f.RefFileReader()
	defer func() {
		f.UnrefFileReader()
	}()
	fullPath := f.Path()
	if seen != nil && seen[fullPath] {
		seen[fullPath] = false
		return nil
	}
	fileListItem := GenPeerPtFilePath(shardPath, peersPtIDMap, nodePath, fullPath)
	*fileList = append(*fileList, fileListItem)
	dstPath := filepath.Join(outPath, fullPath)
	if err := retryFileCopy(fullPath, dstPath); err != nil {
		log.Error("backup file error", zap.Error(err))
		return err
	}
	if f.GetPkInfo() != nil {
		// backup colstore index files
		idxPath := strings.Replace(fullPath, immutable.TsspFileSuffix, colstore.IndexFileSuffix, -1)
		*fileList = append(*fileList, []string{idxPath})
		if err := retryFileCopy(idxPath, filepath.Join(outPath, idxPath)); err != nil {
			log.Error("backup colstore index file error", zap.Error(err))
			return err
		}
	}
	if f.GetSkipIndexInfo() != nil {
		for _, s := range f.GetSkipIndexInfo() {
			*fileList = append(*fileList, []string{s.Name()})
			if err := retryFileCopy(s.Name(), filepath.Join(outPath, s.Name())); err != nil {
				log.Error("backup colstore index file error", zap.Error(err))
				return err
			}
		}
	}
	return nil
}

func retryFileCopy(srcPath, dstPath string) error {
	dstPath = strings.Replace(dstPath, ".init", "", -1)
	err := backup.FileCopy(srcPath, dstPath)
	if err == nil {
		return nil
	}
	srcPath += ".init"
	return backup.FileCopy(srcPath, dstPath)
}

func (s *Backup) IncBackupTableFile(shardPath string, t immutable.TablesStore, peersPtIDMap map[uint32]*NodeInfo, name string, isOrder bool, nodePath, outPath string) ([][]string, [][]string, error) {
	orderfiles, unOrderFiles, _ := t.GetBothFilesRef(name, false, util.TimeRange{}, nil)
	if len(unOrderFiles) > 0 {
		orderfiles = append(orderfiles, unOrderFiles...)
	}
	csFiles, ok := t.GetCSFilesRef(name)
	if ok {
		if len(csFiles) > 0 {
			orderfiles = append(orderfiles, csFiles...)
		}
	}
	if len(orderfiles) == 0 {
		return nil, nil, nil
	}
	defer func() {
		immutable.UnrefFiles(orderfiles...)
	}()

	seen := make(map[string]bool)
	oldFileList := s.BackupLogInfo.FileListMap[name]
	for _, f := range oldFileList {
		seen[f[0]] = true
	}
	addFileList := make([][]string, 0)
	deleteFileList := make([][]string, 0)

	for _, f := range orderfiles {
		if s.IsAborted {
			return nil, nil, fmt.Errorf("backup aborted")
		}
		if err := copyTableFile(f, seen, shardPath, peersPtIDMap, nodePath, outPath, &addFileList); err != nil {
			return addFileList, deleteFileList, err
		}
	}

	if len(csFiles) > 0 {
		countPath := filepath.Join(filepath.Dir(csFiles[0].Path()), immutable.CountBinFile)
		addFileList = append(addFileList, []string{countPath})
		deleteFileList = append(addFileList, []string{countPath})
		if err := retryFileCopy(countPath, filepath.Join(outPath, countPath)); err != nil {
			log.Error("backup colstore count file error", zap.Error(err))
			return addFileList, deleteFileList, err
		}
	}

	for f, v := range seen {
		if v {
			deleteFileListItem := GenPeerPtFilePath(shardPath, peersPtIDMap, nodePath, f)
			deleteFileList = append(deleteFileList, deleteFileListItem)
		}
	}

	return addFileList, deleteFileList, nil
}

func BackupIndex(ib *tsi.IndexBuilder, backupPath string) error {
	ib.RLock()
	defer ib.RUnlock()
	indexPath := ib.Path()
	dstPath := filepath.Join(backupPath, indexPath)
	if err := backup.FolderCopy(indexPath, dstPath); err != nil {
		log.Error("backup index file error", zap.Error(err))
		return err
	}
	return nil
}

type NodeInfo struct {
	shardId      uint64
	indexId      uint64
	nodeId       uint32
	rpName       string
	startTime    time.Time
	endTime      time.Time
	shardDirName string
}

func (n *NodeInfo) GenShardDirName() {
	name := strconv.Itoa(int(n.shardId)) + pathSeparator + strconv.Itoa(int(meta2.MarshalTime(n.startTime))) +
		pathSeparator + strconv.Itoa(int(meta2.MarshalTime(n.endTime))) +
		pathSeparator + strconv.Itoa(int(n.indexId))

	n.shardDirName = filepath.Join(strconv.Itoa(int(n.nodeId)), n.rpName, name)
}

func GenShardDirPath(metaClient meta.MetaClient, dbName string, ptId uint32) (map[uint32]*NodeInfo, error) {
	rpGroups := metaClient.DBRepGroups(dbName)
	// get all the peers Pt ID
	peersPtIDMap := make(map[uint32]*NodeInfo, 0)
	for _, g := range rpGroups {
		if ptId == g.ID {
			for _, p := range g.Peers {
				peersPtIDMap[p.ID] = &NodeInfo{}
			}
		}
	}
	dbInfo, err := metaClient.Database(dbName)
	if err != nil {
		return peersPtIDMap, err
	}
	for rpName, rp := range dbInfo.RetentionPolicies {
		for _, shGroup := range rp.ShardGroups {
			for _, sh := range shGroup.Shards {
				if len(sh.Owners) == 0 {
					return peersPtIDMap, fmt.Errorf("shard owner don't exist")
				}
				if nodeInfo, ok := peersPtIDMap[sh.Owners[0]]; ok {
					nodeInfo.nodeId = sh.Owners[0]
					nodeInfo.shardId = sh.ID
					nodeInfo.rpName = rpName
					nodeInfo.startTime = shGroup.StartTime
					nodeInfo.endTime = shGroup.EndTime
				}
			}
		}
		for _, idxGroup := range rp.IndexGroups {
			for _, idx := range idxGroup.Indexes {
				if len(idx.Owners) == 0 {
					return peersPtIDMap, fmt.Errorf("shard owner don't exist")
				}
				if nodeInfo, ok := peersPtIDMap[idx.Owners[0]]; ok {
					nodeInfo.indexId = idx.ID
				}
			}
		}
	}
	for _, p := range peersPtIDMap {
		p.GenShardDirName()
	}
	return peersPtIDMap, nil
}

func GenPeerPtFilePath(shardPath string, peersPtIDMap map[uint32]*NodeInfo, nodePath, fullPath string) []string {
	fileListItem := make([]string, 0, len(peersPtIDMap)+1)
	fileListItem = append(fileListItem, fullPath)
	shFilePath := strings.Replace(fullPath, shardPath, "", -1)
	basicPath, _ := filepath.Split(nodePath)
	for _, p := range peersPtIDMap {
		fileListItem = append(fileListItem, filepath.Join(basicPath, p.shardDirName, shFilePath))
	}
	return fileListItem
}

func execTicker(ch chan struct{}, path string, wg *sync.WaitGroup) {
	t := time.NewTicker(1 * time.Minute)
	defer func() {
		t.Stop()
		wg.Done()
	}()
	for {
		select {
		case <-t.C:
			result := time.Now().String()
			result = fmt.Sprintf("%s: Backing up...", result)
			_ = backup.WriteBackupLogFile([]byte(result), path, backup.ResultLog)
		case <-ch:
			return
		}
	}
}
