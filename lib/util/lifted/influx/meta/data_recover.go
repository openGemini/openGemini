package meta

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
import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/openGemini/openGemini/lib/logger"
	"go.uber.org/zap"
)

type DataRecover struct {
}
type DirNode struct {
	Name     string
	Path     string
	Children []*DirNode
}

func (dataRecover *DataRecover) RecoverMeta(targetDir string, data *Data) {
	// build dir tree
	root, err := BuildDirTree(targetDir)
	if err != nil {
		logger.GetLogger().Error("build tree err", zap.Error(err))
		return
	}
	m := make(map[string]*DatabaseInfo)

	PtView := make(map[string]DBPtInfos)
	var pis []PtInfo

	var truncatedAt = time.Unix(1, 0)
	var database = ""
	TraverseDirTree(root, 0, func(node *DirNode, depth int) {

		name := node.Name
		if depth == 1 {
			database = name
			m[database] = &DatabaseInfo{
				Name: database,
			}
		}
		if depth == 3 {
			m[database].DefaultRetentionPolicy = name
			m[database].RetentionPolicies = make(map[string]*RetentionPolicyInfo)
			m[database].RetentionPolicies[name] = &RetentionPolicyInfo{
				Name:        name,
				ReplicaN:    1,
				Duration:    0,
				MarkDeleted: false,
				ShardGroups: []ShardGroupInfo{},
			}
		}

	})
	var shardId = 1
	var rp = ""
	// depth: The hierarchy of the directory of the data file
	TraverseDirTree(root, 0, func(node *DirNode, depth int) {

		name := node.Name
		if depth == 1 {
			database = name
		}
		var ptId = 0
		if depth == 2 {
			num, _ := strconv.ParseInt(node.Name, 10, 32)
			ptId = int(num)
		}

		var shardGroupInfos []ShardGroupInfo
		var sgds []ShardInfo
		if depth == 3 {
			rp = node.Name
		}
		if depth == 4 {
			if name != "index" {
				startTime, endTime, indexId := doHandleShards(name, err)
				nodes := data.DataNodes
				ptId, pis, sgds = doHandleNodes(nodes, ptId, pis, shardId, indexId, sgds)
				shard := ShardGroupInfo{
					ID:          uint64(ptId),
					StartTime:   startTime,
					EndTime:     endTime,
					Shards:      sgds,
					TruncatedAt: truncatedAt,
					EngineType:  0,
				}

				shardGroupInfos = append(shardGroupInfos, shard)
				m[database].RetentionPolicies[rp].ShardGroups = DeduplicateShardGroups(shardGroupInfos)
			}
		}
		delete(m[database].RetentionPolicies, "")
		delete(m, "")
		delete(PtView, "")
		duplicates := RemoveDuplicates(pis)
		PtView[database] = duplicates
	})

	data.Databases = m
	data.PtView = PtView

}

func doHandleNodes(nodes []DataNode, ptId int, pis []PtInfo, shardId int, indexId int, sgds []ShardInfo) (int, []PtInfo, []ShardInfo) {
	for nd := range nodes {
		id := nodes[nd].ID
		po := PtOwner{
			NodeID: id,
		}
		pi := PtInfo{
			Owner:  po,
			Status: 0,
			PtId:   uint32(ptId),
			Ver:    1,
		}
		ptId = ptId + 1
		pis = append(pis, pi)
		uint32s := []uint32{}
		oss := append(uint32s, uint32(ptId-1))
		sd := ShardInfo{
			ID:         uint64(shardId),
			Owners:     oss,
			MarkDelete: false,
			Tier:       2,
			IndexID:    uint64(indexId),
		}
		shardId++
		sgds = append(sgds, sd)
	}
	return ptId, pis, sgds
}

func doHandleShards(name string, err error) (time.Time, time.Time, int) {
	shards := SplitByMultiple(name)
	if len(shards) < 5 {
		logger.GetLogger().Error("shard dir name err", zap.Error(err))
		return time.Time{}, time.Time{}, 0
	}
	atoi, err := strconv.Atoi(shards[1])
	if err != nil {
		logger.GetLogger().Error("shard dir transfor err", zap.Error(err))
		return time.Time{}, time.Time{}, 0
	}
	startTime := time.Unix(0, int64(atoi))
	atoH, err := strconv.Atoi(shards[2])
	if err != nil {
		logger.GetLogger().Error("shard dir transfor err", zap.Error(err))
		return time.Time{}, time.Time{}, 0
	}
	endTime := time.Unix(0, int64(atoH))
	indexId, err := strconv.Atoi(shards[3])
	if err != nil {
		logger.GetLogger().Error("shard dir transfor err", zap.Error(err))
		return time.Time{}, time.Time{}, 0
	}
	return startTime, endTime, indexId
}

func TraverseDirTree(node *DirNode, depth int, callback func(*DirNode, int)) {

	callback(node, depth)

	for _, child := range node.Children {
		TraverseDirTree(child, depth+1, callback)
	}
}
func BuildDirTree(rootPath string) (*DirNode, error) {

	absRoot, err := filepath.Abs(rootPath)
	if err != nil {
		return nil, fmt.Errorf("Obtain the absolute path: %w", err)
	}

	rootNode := &DirNode{
		Name: filepath.Base(absRoot),
		Path: absRoot,
	}

	// build tree
	if err := AddChildren(rootNode); err != nil {
		return nil, err
	}

	return rootNode, nil
}

func AddChildren(parent *DirNode) error {

	dir, err := os.Open(parent.Path)
	if err != nil {
		return fmt.Errorf("The directory cannot be opened. %s: %w", parent.Path, err)
	}
	defer dir.Close()

	entries, err := dir.ReadDir(-1)
	if err != nil {
		return fmt.Errorf("Failed to read the directory %s: %w", parent.Path, err)
	}

	for _, entry := range entries {
		link, err := IsDirOrLink(entry.Name())
		if err != nil {
			logger.GetLogger().Error("check dir is dir", zap.Error(err))
		}
		if !entry.IsDir() && !link {
			continue
		}

		childPath := filepath.Join(parent.Path, entry.Name())
		childNode := &DirNode{
			Name: entry.Name(),
			Path: childPath,
		}

		if err := AddChildren(childNode); err != nil {
			return err
		}

		parent.Children = append(parent.Children, childNode)
	}

	return nil
}

// IsDir determines whether the path is a directory (automatically parses symbolic links)
func IsDirOrLink(path string) (bool, error) {
	fileInfo, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, fmt.Errorf("The path does not exist: %w", err)
		}
		return false, fmt.Errorf("The file information cannot be obtained: %w", err)
	}

	return fileInfo.IsDir(), nil
}

func IsDirWithFollow(path string, resolveSymlink bool) (bool, error) {
	var fileInfo os.FileInfo
	var err error

	if resolveSymlink {
		fileInfo, err = os.Stat(path)
	} else {
		fileInfo, err = os.Lstat(path)
	}

	if err != nil {
		return false, fmt.Errorf("The file information cannot be obtained: %w", err)
	}

	return fileInfo.IsDir(), nil
}

func IsTargetDir(path string) (bool, error) {

	target, err := os.Readlink(path)
	if err != nil {

		if errors.Is(err, os.ErrNotExist) {
			return false, fmt.Errorf("The path does not exist: %w", err)
		}
		return false, fmt.Errorf("The link cannot be read: %w", err)
	}

	if !filepath.IsAbs(target) {
		target = filepath.Join(filepath.Dir(path), target)
	}

	return IsDirWithFollow(target, true)
}

func SplitByMultiple(input string) []string {
	return strings.FieldsFunc(input, func(r rune) bool {
		return r == '_' || r == '-'
	})
}

func DeduplicateShardGroups(groups []ShardGroupInfo) []ShardGroupInfo {
	seen := make(map[int64]struct{})
	result := make([]ShardGroupInfo, 0, len(groups))

	for _, group := range groups {
		if _, exists := seen[int64(group.ID)]; !exists {
			seen[int64(group.ID)] = struct{}{}
			result = append(result, group)
		}
	}

	return result
}

func RemoveDuplicates[T comparable](slice []T) []T {
	seen := make(map[T]struct{})
	result := make([]T, 0, len(slice))

	for _, item := range slice {
		if _, exists := seen[item]; !exists {
			seen[item] = struct{}{}
			result = append(result, item)
		}
	}
	return result
}
