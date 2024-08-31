/*
Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.

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

package engine

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/lib/backup"
	meta "github.com/openGemini/openGemini/lib/metaclient"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"go.uber.org/zap"
)

type Backup struct {
	Name            string
	time            int64
	IsInc           bool
	IsRemote        bool
	OnlyBackupMater bool
	BackupPath      string
	BackupLogInfo   *backup.BackupLogInfo
	Engine          *Engine
}

func (s *Backup) RunBackupData() error {
	s.time = time.Now().UnixNano()
	dbPtIds := s.Engine.GetDBPtIds()

	for dbName, pts := range dbPtIds {
		for _, ptId := range pts {
			err := s.BackupPt(dbName, ptId)
			if err != nil {
				return err
			}
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
	defer p.unref()

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
		if s.IsInc {
			s.BackupLogInfo = &backup.BackupLogInfo{}
			if err := backup.ReadBackupLogFile(filepath.Join(sh.GetDataPath(), backup.BackupLogPath, backup.FullBackupLog), s.BackupLogInfo); err != nil {
				return err
			}
			if err := s.IncBackup(sh, backupPath, p.path, peersPtIDMap); err != nil {
				return err
			}
		} else {
			if err := s.FullBackup(sh, backupPath, p.path, peersPtIDMap); err != nil {
				return err
			}
		}
	}

	// backup index
	for _, ib := range p.indexBuilder {
		indexPath := ib.Path()
		dstPath := filepath.Join(backupPath, indexPath)
		if err := backup.FolderCopy(indexPath, dstPath); err != nil {
			log.Error("backup index file error", zap.Error(err))
			return err
		}
	}
	return nil
}

func (s *Backup) FullBackup(sh Shard, dataPath, nodePath string, peersPtIDMap map[uint32]*NodeInfo) error {
	t := sh.GetTableStore()
	logPath := sh.GetDataPath()
	orderFileListMap := make(map[string][][]string)
	outOfOrderFileListMap := make(map[string][][]string)

	orderList := t.GetMstList(true)

	for _, name := range orderList {
		fileList, err := s.FullBackupTableFile(sh, t, peersPtIDMap, name, true, nodePath, dataPath)
		if err != nil {
			return err
		}
		if len(fileList) > 0 {
			orderFileListMap[name] = fileList
		}

	}
	outOfOrderList := t.GetMstList(false)
	for _, name := range outOfOrderList {
		fileList, err := s.FullBackupTableFile(sh, t, peersPtIDMap, name, false, nodePath, dataPath)
		if err != nil {
			return err
		}
		if len(fileList) > 0 {
			outOfOrderFileListMap[name] = fileList
		}
	}

	backupLog := &backup.BackupLogInfo{
		FullBackupTime:        s.time,
		OrderFileListMap:      orderFileListMap,
		OutOfOrderFileListMap: outOfOrderFileListMap,
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
	return nil
}

func (s *Backup) IncBackup(sh Shard, dataPath, nodePath string, peersPtIDMap map[uint32]*NodeInfo) error {
	t := sh.GetTableStore()
	logPath := sh.GetDataPath()

	orderList := t.GetMstList(true)
	addOrderFileListMap := make(map[string][][]string, 0)
	delOrderFileListMap := make(map[string][][]string, 0)
	for _, name := range orderList {
		aList, dList, err := s.IncBackupTableFile(sh, t, peersPtIDMap, name, true, nodePath, dataPath)
		if err != nil {
			return err
		}
		if len(aList) > 0 {
			addOrderFileListMap[name] = aList
		}
		if len(dList) > 0 {
			delOrderFileListMap[name] = dList
		}
	}
	outOfOrderList := t.GetMstList(false)
	addOutOfOrderFileListMap := make(map[string][][]string, 0)
	delOutOfOrderFileListMap := make(map[string][][]string, 0)
	for _, name := range outOfOrderList {
		aList, dList, err := s.IncBackupTableFile(sh, t, peersPtIDMap, name, false, nodePath, dataPath)
		if err != nil {
			return err
		}
		if len(aList) > 0 {
			addOutOfOrderFileListMap[name] = aList
		}
		if len(dList) > 0 {
			delOutOfOrderFileListMap[name] = dList
		}
	}

	if len(addOrderFileListMap) > 0 || len(delOrderFileListMap) > 0 || len(addOutOfOrderFileListMap) > 0 || len(delOutOfOrderFileListMap) > 0 {
		incBackupLog := &backup.IncBackupLogInfo{
			AddOrderFileListMap:      addOrderFileListMap,
			DelOrderFileListMap:      delOrderFileListMap,
			AddOutOfOrderFileListMap: addOutOfOrderFileListMap,
			DelOutOfOrderFileListMap: delOutOfOrderFileListMap,
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

func (s *Backup) FullBackupTableFile(sh Shard, t immutable.TablesStore, peersPtIDMap map[uint32]*NodeInfo, name string, isOrder bool, nodePath, outPath string) ([][]string, error) {
	files, ok := t.GetTSSPFiles(name, isOrder)
	if !ok {
		return nil, nil
	}
	defer func() {
		immutable.UnrefFilesReader(files.Files()...)
		immutable.UnrefFiles(files.Files()...)
	}()

	fileList := make([][]string, len(files.Files()))
	for i, f := range files.Files() {
		fullPath := f.Path()
		fileListItem := make([]string, 0, len(peersPtIDMap)+1)
		fileListItem = append(fileListItem, fullPath)
		shFilePath := strings.Replace(fullPath, sh.GetDataPath(), "", -1)
		basicPath, _ := filepath.Split(nodePath)
		for _, p := range peersPtIDMap {
			fileListItem = append(fileListItem, filepath.Join(basicPath, p.shardDirName, shFilePath))
		}

		fileList[i] = fileListItem
		dstPath := filepath.Join(outPath, fullPath)
		if err := backup.FileCopy(fullPath, dstPath); err != nil {
			log.Error("backup file error", zap.Error(err))
			return fileList, err
		}
	}

	return fileList, nil
}

func (s *Backup) IncBackupTableFile(sh Shard, t immutable.TablesStore, peersPtIDMap map[uint32]*NodeInfo, name string, isOrder bool, nodePath, outPath string) ([][]string, [][]string, error) {
	files, ok := t.GetTSSPFiles(name, isOrder)
	if !ok {
		return nil, nil, nil
	}
	defer func() {
		immutable.UnrefFilesReader(files.Files()...)
		immutable.UnrefFiles(files.Files()...)
	}()

	seen := make(map[string]bool)
	oldFileList := s.BackupLogInfo.OrderFileListMap[name]
	for _, f := range oldFileList {
		seen[f[0]] = true
	}
	addFileList := make([][]string, 0)
	deleteFileList := make([][]string, 0)

	for _, f := range files.Files() {
		fullPath := f.Path()
		if seen[fullPath] {
			seen[fullPath] = false
			continue
		}
		addFileListItem := GenPeerPtFilePath(sh, peersPtIDMap, nodePath, fullPath)
		addFileList = append(addFileList, addFileListItem)
		dstPath := filepath.Join(outPath, fullPath)
		if err := backup.FileCopy(fullPath, dstPath); err != nil {
			return addFileList, deleteFileList, err
		}
	}

	for f, v := range seen {
		if v {
			deleteFileListItem := GenPeerPtFilePath(sh, peersPtIDMap, nodePath, f)
			deleteFileList = append(deleteFileList, deleteFileListItem)
		}
	}

	return addFileList, deleteFileList, nil
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

func GenPeerPtFilePath(sh Shard, peersPtIDMap map[uint32]*NodeInfo, nodePath, fullPath string) []string {
	fileListItem := make([]string, 0, len(peersPtIDMap)+1)
	fileListItem = append(fileListItem, fullPath)
	shFilePath := strings.Replace(fullPath, sh.GetDataPath(), "", -1)
	basicPath, _ := filepath.Split(nodePath)
	for _, p := range peersPtIDMap {
		fileListItem = append(fileListItem, filepath.Join(basicPath, p.shardDirName, shFilePath))
	}
	return fileListItem
}
