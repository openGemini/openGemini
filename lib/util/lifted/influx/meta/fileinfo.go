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

package meta

import (
	"database/sql"
	"time"

	proto2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta/proto"
	"github.com/openGemini/openGemini/lib/util/lifted/protobuf/proto"
	_ "modernc.org/sqlite"
)

const INITSQL = `
CREATE TABLE IF NOT EXISTS files (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    sequence         BIGINT NOT NULL,
    level            INT NOT NULL,
    merge            INT NOT NULL,
    extent           INT NOT NULL,
    mst_id           INT NOT NULL,
    shard_id         INT NOT NULL,
    deleted_at       BIGINT DEFAULT 0,
    created_at       BIGINT NOT NULL,
    min_time         BIGINT NOT NULL,
    max_time         BIGINT NOT NULL,
    row_count        BIGINT NOT NULL,
    file_size_bytes  BIGINT NOT NULL,

    CONSTRAINT files_unique
        UNIQUE(mst_id, shard_id, sequence, level, extent, merge)
);
`

type FileInfo struct {
	Sequence      uint64
	Level         uint16
	Merge         uint16
	Extent        uint16
	MstID         uint64
	ShardID       uint64
	DeletedAt     int64
	CreatedAt     int64
	MinTime       int64
	MaxTime       int64
	RowCount      int64
	FileSizeBytes int64
}

func (fileInfo *FileInfo) Marshal() *proto2.FileInfo {
	var fileInfoProto proto2.FileInfo
	fileInfoProto.Sequence = proto.Uint64(fileInfo.Sequence)
	fileInfoProto.Level = proto.Uint32(uint32(fileInfo.Level))
	fileInfoProto.Merge = proto.Uint32(uint32(fileInfo.Merge))
	fileInfoProto.Extent = proto.Uint32(uint32(fileInfo.Extent))
	fileInfoProto.MstID = proto.Uint64(fileInfo.MstID)
	fileInfoProto.ShardID = proto.Uint64(fileInfo.ShardID)
	fileInfoProto.DeletedAt = proto.Int64(fileInfo.DeletedAt)
	fileInfoProto.CreatedAt = proto.Int64(fileInfo.CreatedAt)
	fileInfoProto.MinTime = proto.Int64(fileInfo.MinTime)
	fileInfoProto.MaxTime = proto.Int64(fileInfo.MaxTime)
	fileInfoProto.RowCount = proto.Int64(fileInfo.RowCount)
	fileInfoProto.FileSizeBytes = proto.Int64(fileInfo.FileSizeBytes)
	return &fileInfoProto
}

func (fileInfo *FileInfo) Unmarshal(fileInfoProto *proto2.FileInfo) {
	fileInfo.Sequence = fileInfoProto.GetSequence()
	fileInfo.Level = uint16(fileInfoProto.GetLevel())
	fileInfo.Merge = uint16(fileInfoProto.GetMerge())
	fileInfo.Extent = uint16(fileInfoProto.GetExtent())
	fileInfo.MstID = fileInfoProto.GetMstID()
	fileInfo.ShardID = fileInfoProto.GetShardID()
	fileInfo.DeletedAt = fileInfoProto.GetDeletedAt()
	fileInfo.CreatedAt = fileInfoProto.GetCreatedAt()
	fileInfo.MinTime = fileInfoProto.GetMinTime()
	fileInfo.MaxTime = fileInfoProto.GetMaxTime()
	fileInfo.RowCount = fileInfoProto.GetRowCount()
	fileInfo.FileSizeBytes = fileInfoProto.GetFileSizeBytes()
}

type SQLiteWrapper struct {
	database *sql.DB // client for sqlite database
}

func NewSQLiteWrapper(path string) (*SQLiteWrapper, error) {
	database, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	_, err = database.Exec(INITSQL)
	if err != nil {
		return nil, err
	}
	return &SQLiteWrapper{database}, nil
}

func (sqlite *SQLiteWrapper) Close() (err error) {
	if sqlite.database == nil {
		return nil
	}
	err = sqlite.database.Close()
	return
}

// for Ingester and Compactor to insert files.
func (sqlite *SQLiteWrapper) InsertFiles(files []FileInfo, transaction *sql.Tx) error {
	var err error
	stmt := `INSERT INTO
			 files(mst_id, shard_id, sequence, level, extent, merge, min_time, max_time, created_at, row_count, file_size_bytes)
			 values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	var insert *sql.Stmt
	if transaction != nil {
		insert, err = transaction.Prepare(stmt)
	} else {
		insert, err = sqlite.database.Prepare(stmt)
	}

	if err != nil {
		return err
	}
	for _, file := range files {
		_, err = insert.Exec(file.MstID, file.ShardID, file.Sequence,
			file.Level, file.Extent, file.Merge,
			file.MinTime, file.MaxTime, file.CreatedAt, file.RowCount, file.FileSizeBytes)
		if err != nil {
			return err
		}
	}
	return err
}

// for Querier and Compactor to get files by measurement ID and shard ID.
func (sqlite *SQLiteWrapper) QueryFiles(mstID uint64, shardID uint64) ([]FileInfo, error) {
	var err error
	stmt :=
		`SELECT mst_id, shard_id, sequence, level, extent, merge, min_time, max_time, created_at, row_count, file_size_bytes
		FROM files WHERE mst_id=? AND shard_id=? AND deleted_at = 0`
	query, err := sqlite.database.Prepare(stmt)
	if err != nil {
		return nil, err
	}
	queryResult, err := query.Query(mstID, shardID)
	if err != nil {
		return nil, err
	}

	// todo: optimize later
	var files []FileInfo

	defer queryResult.Close()
	for queryResult.Next() {
		var file FileInfo
		err = queryResult.Scan(&file.MstID, &file.ShardID, &file.Sequence, &file.Level, &file.Extent, &file.Merge,
			&file.MinTime, &file.MaxTime, &file.CreatedAt, &file.RowCount, &file.FileSizeBytes)
		if err != nil {
			return nil, err
		}
		files = append(files, file)
	}
	return files, err
}

// for Compactor to update files(mark delete).
func (sqlite *SQLiteWrapper) UpdateFiles(filesID []uint64, transaction *sql.Tx) error {
	var err error
	stmt := "UPDATE files SET deleted_at = ? WHERE id = ?"
	deleteAt := time.Now().UnixNano()
	var update *sql.Stmt
	if transaction != nil {
		update, err = transaction.Prepare(stmt)
	} else {
		update, err = sqlite.database.Prepare(stmt)
	}

	if err != nil {
		return err
	}
	for _, file := range filesID {
		_, err = update.Exec(deleteAt, file)
		if err != nil {
			return err
		}
	}
	return err
}

// for Compactor(Garbage Collection) to delete "mark-deleted" files.
func (sqlite *SQLiteWrapper) DeleteFiles(filesID []uint64) error {
	var err error
	stmt := "DELETE FROM files WHERE id = ?"
	update, err := sqlite.database.Prepare(stmt)
	if err != nil {
		return err
	}
	for _, file := range filesID {
		_, err = update.Exec(file)
		if err != nil {
			return err
		}
	}
	return err
}

// for Retention Policies to delete files by Shard ID,
func (sqlite *SQLiteWrapper) DeleteFilesByShard(shardID []uint64) error {
	var err error
	stmt := "DELETE FROM files WHERE shard_id = ?"
	update, err := sqlite.database.Prepare(stmt)
	if err != nil {
		return err
	}
	for _, file := range shardID {
		_, err = update.Exec(file)
		if err != nil {
			return err
		}
	}
	return err
}

// for Compactor to delete files and insert files in one transaction.
func (sqlite *SQLiteWrapper) CompactFiles(deleteFiles []uint64, insertFiles []FileInfo) error {
	var err error
	transaction, err := sqlite.database.Begin()
	if err != nil {
		return err
	}
	err = sqlite.UpdateFiles(deleteFiles, transaction)
	if err != nil {
		transaction.Rollback()
		return err
	}
	err = sqlite.InsertFiles(insertFiles, transaction)
	if err != nil {
		transaction.Rollback()
		return err
	}
	err = transaction.Commit()
	if err != nil {
		return err
	}
	return nil
}
