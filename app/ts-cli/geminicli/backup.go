/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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

package geminicli

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/executor/spdy"
	"github.com/openGemini/openGemini/engine/executor/spdy/transport"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/netstorage"
	meta2 "github.com/openGemini/openGemini/open_src/influx/meta"
)

type BackupConfig struct {
	MetaAddr string // ts-meta user-face addr, eg, "127.0.0.1:8091"
	Storage  string // the url where backup storage, eg, "sftp://path/to/prefix", // TODO sftp/s3/gcs/azblob
	SftpHost string // the sftp storage host, eg, "127.0.0.1"
	SftpPort int    // the sftp storage port, eg, "22"
	SftpUser string // the sftp storage user, eg, "root"
	SftpPass string //  the sftp storage password, eg, "root"
}

// HTTPClient interface
type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type BackupEntry struct {
	conf       *BackupConfig // back config from command line flags
	HttpClient HTTPClient
	data       *meta2.Data
}

func NewBackupEntry(conf *BackupConfig) *BackupEntry {
	return &BackupEntry{
		conf:       conf,
		data:       &meta2.Data{},
		HttpClient: http.DefaultClient,
	}
}

type UserInfo struct {
	Id string `json:"id"`

	Name string `json:"name"`
}

func (be *BackupEntry) Initialize() error {
	//TODO: HTTPS
	metaUrl := "http://" + be.conf.MetaAddr + "/getdata"
	fmt.Printf("try to connect ts-meta to get datanodes info by %s\n", metaUrl)
	req, _ := http.NewRequest(http.MethodGet, metaUrl, nil)
	resp, err := be.HttpClient.Do(req)
	var bytesBody []byte
	if err != nil {
		return err
	}
	if resp.StatusCode == http.StatusOK {
		bytesBody, _ = io.ReadAll(resp.Body)
		_ = resp.Body.Close()
	} else {
		fmt.Println("error")
	}
	if err = json.Unmarshal(bytesBody, be.data); err != nil {
		return err
	}
	fmt.Printf("get datanodes info, total %d nodes:\n", len(be.data.DataNodes))
	for i := range be.data.DataNodes {
		fmt.Println(be.data.DataNodes[i].TCPHost)
	}
	return nil
}

func (be *BackupEntry) Backup() error {
	if len(be.conf.Storage) <= 6 {
		return fmt.Errorf("invalid storage url: %s", be.conf.Storage)
	}
	remotePath := be.conf.Storage[6:]
	for i := range be.data.DataNodes {
		r := netstorage.NewRequester(0, nil, nil)
		err := r.InitWithNode(&be.data.DataNodes[i])
		if err != nil {
			return err
		}
		cb := &BackupCallback{}
		backupRequest := &netstorage.BackupRequest{
			StorageType:    be.conf.Storage,
			SftpHost:       be.conf.SftpHost,
			SftpPort:       be.conf.SftpPort,
			SftpUser:       be.conf.SftpUser,
			SftpPassword:   be.conf.SftpPass,
			SftpRemotePath: remotePath,
		}
		fmt.Printf("start to backup for %s to sftp server path of %s\n", be.data.DataNodes[i].TCPHost, remotePath)
		err = r.Request(spdy.BackupRecoverRequest, backupRequest, cb)
		if err != nil {
			return err
		}
	}
	fmt.Printf("Congratulations🎉🎉🎉! Backup cluster data successfully!\n")
	return nil
}

type BackupCallback struct {
	data *netstorage.BackupResponse
}

func (c *BackupCallback) Handle(data interface{}) error {
	msg, ok := data.(*netstorage.BackupResponse)
	if !ok {
		return executor.NewInvalidTypeError("*netstorage.BackupResponse", data)
	}

	c.data = msg
	if c.data.Code == 0 {
		return nil
	} else if c.data.ErrCode != 0 {
		err := errno.NewError(c.data.ErrCode)
		err.SetMessage(c.data.Message)
		return err
	}
	return errors.New(c.data.Message)
}

func (c *BackupCallback) GetCodec() transport.Codec {
	return &netstorage.BackupResponse{}
}
