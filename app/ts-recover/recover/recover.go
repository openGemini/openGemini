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

package recover

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/openGemini/openGemini/lib/backup"
	"github.com/openGemini/openGemini/lib/config"
	fileops "github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
)

const (
	FullAndIncRecoverMode = "1"
	FullRecoverMode       = "2"
	RecoverMeta           = "3"
	GenNodeInfoMap        = "4"

	decimal = 10
)

type RecoverOptions struct {
	RecoverMode        string
	DataDir            string
	MetaDir            string
	WalDir             string
	FullBackupDataPath string
	IncBackupDataPath  string
	Force              bool
	// leader meta address
	Host        string
	SSL         bool
	InsecureTLS bool

	SrcNode     uint64
	DstNode     uint64
	NodeInfoMap *backup.NodeInfoMap
}

type RecoverFunc func(name, path string) error

func (r *RecoverOptions) BackupRecover() error {
	if r.FullBackupDataPath == "" {
		return fmt.Errorf("`missing required parameter: fullBackupDataPath")
	}
	if r.RecoverMode == FullAndIncRecoverMode && r.IncBackupDataPath == "" {
		return fmt.Errorf("`missing required parameter: incBackupDataPath")
	}
	var err error
	var dbs []string

	switch r.RecoverMode {
	case FullAndIncRecoverMode, FullRecoverMode:
		isInc := r.RecoverMode == FullAndIncRecoverMode
		dbs, err = r.getDatabasesFromData(isInc)
		if err != nil {
			return err
		}
		if !isInc {
			err = r.getNodeMap()
			if err != nil {
				return err
			}
			if r.NodeInfoMap != nil {
				return r.rewriteRecover()
			}
		}

		err = r.runRecover(isInc, dbs)
	case RecoverMeta:
		backupPath := r.FullBackupDataPath
		if r.IncBackupDataPath != "" {
			backupPath = r.IncBackupDataPath
		}
		dbs = r.getDatabasesFromMeta(backupPath)
		err = r.recoverMeta(backupPath, dbs)
	case GenNodeInfoMap:
		if r.SrcNode == r.DstNode {
			return fmt.Errorf("wrong SrcNode or DstNode,SrcNode: %d,DstNode: %d", r.SrcNode, r.DstNode)
		}
		err = r.genMap()
	default:
		return errors.New("wrong recover mode")
	}

	if err != nil {
		return err
	}

	return nil
}

func (r *RecoverOptions) rewriteRecover() error {
	// recover data
	fullBackupDataPath := filepath.Join(r.FullBackupDataPath, backup.DataBackupDir, r.DataDir)
	err := traversePath(fullBackupDataPath, TypeData, r.recoverDB(r.NodeInfoMap))
	if err != nil {
		return err
	}

	// recover wal
	fullBackupWalPath := filepath.Join(r.FullBackupDataPath, backup.WalBackupDir, r.WalDir)
	return traversePath(fullBackupWalPath, TypeWal, r.recoverDB(r.NodeInfoMap))
}

func (r *RecoverOptions) runRecover(isInc bool, dbs []string) error {
	if err := r.recoverData(dbs, isInc); err != nil {
		return err
	}
	if err := r.recoverWal(dbs, isInc); err != nil {
		return err
	}

	var backupPath string
	if isInc {
		backupPath = r.IncBackupDataPath
	} else {
		backupPath = r.FullBackupDataPath
	}
	return r.recoverMeta(backupPath, dbs)
}

func (r *RecoverOptions) recoverData(dbs []string, isInc bool) error {
	dataPath := r.DataDir
	// check clear data path
	if len(dbs) > 0 {
		for _, db := range dbs {
			p := filepath.Join(dataPath, db)
			_, err := fileops.Stat(p)
			if !r.Force && err == nil {
				return fmt.Errorf("target database file exist,db : %s.if you still recover,please use --force", db)
			}
			if err := fileops.RemoveAll(p); err != nil {
				return err
			}
		}
	} else {
		if err := fileops.RemoveAll(dataPath); err != nil {
			return err
		}
	}

	copyFunc := r.copyWithFull
	if isInc {
		copyFunc = r.copyWithFullAndInc
	}

	// recover full_backup
	fullBackupDataPath := filepath.Join(r.FullBackupDataPath, backup.DataBackupDir, dataPath)
	if _, err := fileops.Stat(fullBackupDataPath); err != nil {
		return fmt.Errorf("backupDataPath empty: %s", fullBackupDataPath)
	}
	if err := r.traversalBackupLogFile(fullBackupDataPath, copyFunc, isInc, false); err != nil {
		return err
	}

	if !isInc {
		return nil
	}

	// recover inc_backup
	incBackupDataPath := filepath.Join(r.IncBackupDataPath, backup.DataBackupDir, dataPath)
	if _, err := fileops.Stat(incBackupDataPath); err != nil {
		return fmt.Errorf("incBackupDataPath empty: %s", incBackupDataPath)
	}
	if err := r.traversalBackupLogFile(incBackupDataPath, r.copyWithInc, isInc, true); err != nil {
		return err
	}

	return nil
}

func (r *RecoverOptions) recoverWal(dbs []string, isInc bool) error {
	if len(dbs) > 0 {
		for _, db := range dbs {
			p := filepath.Join(r.WalDir, db)
			_, err := fileops.Stat(p)
			if !r.Force && err == nil {
				return fmt.Errorf("target database file exist,db : %s.if you still recover,please use --force", db)
			}
			if err := fileops.RemoveAll(p); err != nil {
				return err
			}
		}
	} else {
		if err := fileops.RemoveAll(r.WalDir); err != nil {
			return err
		}
	}
	backupPath := r.FullBackupDataPath
	if isInc {
		backupPath = r.IncBackupDataPath
	}
	p := filepath.Join(backupPath, backup.WalBackupDir, r.WalDir)
	if _, err := fileops.Stat(p); err != nil {
		return nil
	}
	var fds []fs.FileInfo
	var err error

	if fds, err = fileops.ReadDir(p); err != nil {
		return err
	}
	for _, fd := range fds {
		srcPath := filepath.Join(p, fd.Name())
		dstPath := filepath.Join(r.WalDir, fd.Name())
		if err := backup.FolderMove(srcPath, dstPath); err != nil {
			return err
		}
	}
	return nil
}

func (r *RecoverOptions) recoverMeta(backupPath string, dbs []string) error {
	backupMetaPath := filepath.Join(backupPath, backup.MetaBackupDir)

	// while single or multi databases backup,then run logic recover
	if len(dbs) > 0 {
		if _, err := fileops.Stat(backupMetaPath); err != nil {
			return nil
		}
		buf, err := fileops.ReadFile(filepath.Join(backupMetaPath, backup.MetaInfo))
		if err != nil {
			return err
		}
		return r.sendRequestToMeta(dbs, string(buf))
	}

	// while all database backup,then run physics recover
	err := fileops.RemoveAll(r.MetaDir)
	if err != nil {
		return err
	}

	if _, err := fileops.Stat(backupMetaPath); err != nil {
		return nil
	}

	if err := fileops.RemoveAll(filepath.Join(backupPath, backup.MetaInfo)); err != nil {
		return err
	}

	return backup.FolderMove(backupMetaPath, r.MetaDir)
}

func (r *RecoverOptions) sendRequestToMeta(dbs []string, metaData string) error {
	protocol := "http"
	if r.SSL {
		protocol = "https"
	}
	urlValues := url.Values{}
	urlValues.Add(backup.DataBases, strings.Join(dbs, ","))
	urlValues.Add(backup.MetaData, metaData)

	Url, err := url.Parse(fmt.Sprintf("%s://%s/recoverMeta", protocol, r.Host))
	if err != nil {
		return err
	}
	Url.RawQuery = urlValues.Encode()

	transport := &http.Transport{}
	client := &http.Client{
		Transport: transport,
	}
	if r.InsecureTLS {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}
	res, err := client.PostForm(Url.String(), urlValues)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	return resolveResponseError(res)
}

func resolveResponseError(resp *http.Response) error {
	if resp.StatusCode == http.StatusOK {
		return nil
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("recover meta request error,read response body faild,%s", err.Error())
	}

	return fmt.Errorf("recover meta error,%s", string(b))
}

func (r *RecoverOptions) traversalBackupLogFile(path string, fn RecoverFunc, isInc, incBackup bool) error {
	var err error
	var fds []fs.FileInfo

	if _, err = fileops.Stat(path); err != nil {
		return err
	}
	if fds, err = fileops.ReadDir(path); err != nil {
		return err
	}

	for _, fd := range fds {
		srcfp := filepath.Join(path, fd.Name())

		if fd.IsDir() {
			if fd.Name() == config.IndexFileDirectory && (!isInc || incBackup) {
				outPath := strings.Replace(srcfp, filepath.Join(r.FullBackupDataPath, backup.DataBackupDir), "", -1)
				if err := backup.FolderMove(srcfp, outPath); err != nil {
					return err
				}
				continue
			}
			if err = r.traversalBackupLogFile(srcfp, fn, isInc, incBackup); err != nil {
				return err
			}
			continue
		}
		if err = fn(fd.Name(), srcfp); err != nil {
			return err
		}
	}
	return nil
}

func (r *RecoverOptions) copyWithFull(name, path string) error {
	if name != backup.FullBackupLog {
		return nil
	}
	backupLog := &backup.BackupLogInfo{}
	if err := backup.ReadBackupLogFile(path, backupLog); err != nil {
		return err
	}

	basicPath := filepath.Join(r.FullBackupDataPath, backup.DataBackupDir)
	for _, fileList := range backupLog.FileListMap {
		for _, files := range fileList {
			srcPath := filepath.Join(basicPath, files[0])
			for _, f := range files {
				if err := backup.FileMove(srcPath, f); err != nil {
					return err
				}
			}
		}
	}

	if r.NodeInfoMap != nil {
		return nil
	}
	// recover backup log
	outPath := strings.Replace(path, filepath.Join(r.FullBackupDataPath, backup.DataBackupDir), "", -1)
	return backup.FileMove(path, outPath)
}

func (r *RecoverOptions) copyWithFullAndInc(name, fullPath string) error {
	if name != backup.FullBackupLog {
		return nil
	}
	p, _ := filepath.Split(strings.Replace(fullPath, r.FullBackupDataPath, r.IncBackupDataPath, -1))
	incPath := filepath.Join(p, backup.IncBackupLog)
	incBackupLog := &backup.IncBackupLogInfo{}
	_, err := fileops.Stat(incPath)
	if err != nil {
		err = r.copyWithFull(name, fullPath)
		return err
	}
	if err := backup.ReadBackupLogFile(incPath, incBackupLog); err != nil {
		return err
	}

	backupLog := &backup.BackupLogInfo{}
	if err := backup.ReadBackupLogFile(fullPath, backupLog); err != nil {
		return err
	}

	err = r.mergeFileList(backupLog.FileListMap, incBackupLog.DelFileListMap)
	if err != nil {
		return err
	}

	if r.NodeInfoMap != nil {
		return nil
	}
	// recover backup log
	outPath := strings.Replace(fullPath, filepath.Join(r.FullBackupDataPath, backup.DataBackupDir), "", -1)
	return backup.FileMove(fullPath, outPath)
}

func (r *RecoverOptions) copyWithInc(name, incPath string) error {
	if name != backup.IncBackupLog {
		return nil
	}
	backupLog := &backup.IncBackupLogInfo{}
	if err := backup.ReadBackupLogFile(incPath, backupLog); err != nil {
		return err
	}

	basicPath := filepath.Join(r.IncBackupDataPath, backup.DataBackupDir)
	for _, fileList := range backupLog.AddFileListMap {
		for _, files := range fileList {
			srcPath := filepath.Join(basicPath, files[0])
			for _, f := range files {
				if err := backup.FileMove(srcPath, f); err != nil {
					return err
				}
			}
		}
	}

	if r.NodeInfoMap != nil {
		return nil
	}
	// recover log file
	outPath := strings.Replace(incPath, filepath.Join(r.IncBackupDataPath, backup.DataBackupDir), "", -1)

	return backup.FileMove(incPath, outPath)
}

func (r *RecoverOptions) mergeFileList(listMap, delListMap map[string][][]string) error {
	fullBasicPath := filepath.Join(r.FullBackupDataPath, backup.DataBackupDir)
	for name, fileList := range listMap {
		dListSeen := make(map[string]bool)
		if dLists, ok := delListMap[name]; ok {
			for _, files := range dLists {
				dListSeen[files[0]] = true
			}
		}
		for _, files := range fileList {
			// delete
			if dListSeen[files[0]] {
				continue
			}
			srcPath := filepath.Join(fullBasicPath, files[0])
			for _, f := range files {
				if err := backup.FileMove(srcPath, f); err != nil {
					return err
				}
			}
		}

	}
	return nil
}

func (r *RecoverOptions) getDatabasesFromMeta(backupPath string) []string {
	databases := make([]string, 0)
	path := fileops.Join(backupPath, backup.BackupLogPath, backup.ResultLog)

	if _, err := fileops.Stat(path); err != nil {
		fmt.Println(err)
		return databases
	}
	fullRes := &backup.BackupResult{}
	if err := backup.ReadBackupLogFile(path, fullRes); err != nil {
		fmt.Println(err)
		return databases
	}
	if r.MetaDir == "" {
		r.MetaDir = fullRes.MetaDir
	}

	for db := range fullRes.Databases {
		databases = append(databases, db)
	}
	return databases
}

func (r *RecoverOptions) getDatabasesFromData(isInc bool) ([]string, error) {
	path := fileops.Join(r.FullBackupDataPath, backup.BackupLogPath, backup.ResultLog)
	if _, err := fileops.Stat(path); err != nil {
		return nil, err
	}
	fullRes := &backup.BackupResult{}
	if err := backup.ReadBackupLogFile(path, fullRes); err != nil {
		return nil, err
	}
	if r.MetaDir == "" {
		r.MetaDir = fullRes.MetaDir
	}
	if r.DataDir == "" {
		r.DataDir = fullRes.DataDir
	}
	r.DataDir = filepath.Join(r.DataDir, config.DataDirectory)
	r.WalDir = filepath.Join(fullRes.WalDir, config.WalDirectory)
	databases := make([]string, 0, len(fullRes.Databases))
	if !isInc {
		for db := range fullRes.Databases {
			databases = append(databases, db)
		}
		return databases, nil
	}

	path = fileops.Join(r.IncBackupDataPath, backup.BackupLogPath, backup.ResultLog)
	if _, err := fileops.Stat(path); err != nil {
		return nil, err
	}
	incRes := &backup.BackupResult{}
	if err := backup.ReadBackupLogFile(path, incRes); err != nil {
		return nil, err
	}

	if fullRes.Time > incRes.Time {
		return nil, errors.New("fullBackup time should earlier than incBackup")
	}

	if len(fullRes.Databases) != len(incRes.Databases) {
		return nil, errors.New("databases not equal in full Backup and inc Backup")
	}

	for db := range fullRes.Databases {
		if _, ok := incRes.Databases[db]; !ok {
			return nil, errors.New("databases not equal in full Backup and inc Backup")
		}
		databases = append(databases, db)
	}

	return databases, nil
}

func (r *RecoverOptions) getNodeMap() error {
	path := fileops.Join(r.FullBackupDataPath, backup.BackupLogPath, backup.NodeMapInfo)
	if _, err := fileops.Stat(path); err != nil {
		return nil
	}
	nodeInfoMap := &backup.NodeInfoMap{}
	if err := backup.ReadBackupLogFile(path, nodeInfoMap); err != nil {
		return err
	}
	r.NodeInfoMap = nodeInfoMap
	return nil
}

func (r *RecoverOptions) genMap() error {
	backupMetaPath := filepath.Join(r.FullBackupDataPath, backup.MetaBackupDir)
	if _, err := fileops.Stat(backupMetaPath); err != nil {
		return errors.New("backupMetaPath not found")
	}
	metaInfo, err := getMetaInfo(backupMetaPath)
	if err != nil {
		return err
	}

	info, err := r.genNodeInfoMap(metaInfo)
	if err != nil {
		return err
	}
	content, err := json.MarshalIndent(info, "", "\t")
	if err != nil {
		return err
	}

	return backup.WriteBackupLogFile(content, r.FullBackupDataPath, backup.NodeMapInfo)
}

func getMetaInfo(p string) (*meta.Data, error) {
	buf, err := fileops.ReadFile(filepath.Join(p, backup.MetaInfo))
	if err != nil {
		return nil, err
	}
	metaData := &meta.Data{}
	err = json.Unmarshal(buf, metaData)
	if err != nil {
		return nil, err
	}
	return metaData, nil
}

func (r *RecoverOptions) genNodeInfoMap(metaInfo *meta.Data) (*backup.NodeInfoMap, error) {
	n := &backup.NodeInfoMap{
		SrcNode: r.SrcNode,
		DstNode: r.DstNode,
		PtMap:   make(map[string]map[string]string),
		DbMap:   make(map[string]map[string]*backup.RpInfoMap),
	}
	genPtMap(metaInfo, n)

	for dbName, db := range metaInfo.Databases {
		ptInfo, ok := n.PtMap[dbName]
		if !ok {
			return n, fmt.Errorf("DBName not found. db: %s", dbName)
		}
		n.DbMap[dbName] = make(map[string]*backup.RpInfoMap)
		for rpName, rp := range db.RetentionPolicies {
			rpInfoMap := &backup.RpInfoMap{
				ShardMap: make(map[string]string),
				IndexMap: make(map[string]string),
			}
			n.DbMap[dbName][rpName] = rpInfoMap
			if err := genShardMap(rp, ptInfo, rpInfoMap); err != nil {
				return n, fmt.Errorf("%w,db: %s,rp: %s", err, dbName, rpName)
			}
			if err := genIndexMap(rp, ptInfo, rpInfoMap); err != nil {
				return n, fmt.Errorf("%w,db: %s,rp: %s", err, dbName, rpName)
			}
		}
	}
	return n, nil
}

func genPtMap(metaInfo *meta.Data, n *backup.NodeInfoMap) {
	for dbName, dbPt := range metaInfo.PtView {
		ptInfoMap := make(map[string]string)
		n.PtMap[dbName] = ptInfoMap
		ptGroup := make(map[uint32][]meta.PtInfo)
		for _, pt := range dbPt {
			_, ok := ptGroup[pt.RGID]
			if !ok {
				ptGroup[pt.RGID] = make([]meta.PtInfo, 0)
			}
			ptGroup[pt.RGID] = append(ptGroup[pt.RGID], pt)
		}
		for _, g := range ptGroup {
			var srcPtID, dstPtID string
			for _, pt := range g {
				if pt.Owner.NodeID == n.SrcNode {
					srcPtID = strconv.FormatUint(uint64(pt.PtId), decimal)
				} else if pt.Owner.NodeID == n.DstNode {
					dstPtID = strconv.FormatUint(uint64(pt.PtId), decimal)
				}
			}
			if srcPtID == "" || dstPtID == "" {
				continue
			}
			ptInfoMap[srcPtID] = dstPtID
		}
	}
}

func genShardMap(rp *meta.RetentionPolicyInfo, ptInfo map[string]string, rpInfoMap *backup.RpInfoMap) error {
	for _, sh := range rp.ShardGroups {
		ptShardMap := make(map[string]meta.ShardInfo, len(sh.Shards))
		for _, s := range sh.Shards {
			if len(s.Owners) != 1 {
				return fmt.Errorf("wrong shard owners,shardID: %d", s.ID)
			}
			ptID := strconv.FormatUint(uint64(s.Owners[0]), decimal)
			ptShardMap[ptID] = s
		}
		for srcPt, dstPt := range ptInfo {
			srcShardID := strconv.FormatUint(ptShardMap[srcPt].ID, decimal)
			dstShardID := strconv.FormatUint(ptShardMap[dstPt].ID, decimal)
			rpInfoMap.ShardMap[srcShardID] = dstShardID
		}
	}
	return nil
}

func genIndexMap(rp *meta.RetentionPolicyInfo, ptInfo map[string]string, rpInfoMap *backup.RpInfoMap) error {
	for _, idx := range rp.IndexGroups {
		ptIndexMap := make(map[string]meta.IndexInfo, len(idx.Indexes))
		for _, s := range idx.Indexes {
			if len(s.Owners) != 1 {
				return fmt.Errorf("wrong shard owners,shardID: %d", s.ID)
			}
			ptID := strconv.FormatUint(uint64(s.Owners[0]), decimal)
			ptIndexMap[ptID] = s
		}
		for srcPt, dstPt := range ptInfo {
			srcIndexID := strconv.FormatUint(ptIndexMap[srcPt].ID, decimal)
			dstIndexID := strconv.FormatUint(ptIndexMap[dstPt].ID, decimal)
			rpInfoMap.IndexMap[srcIndexID] = dstIndexID
		}
	}
	return nil
}
