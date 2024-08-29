// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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

package fileops

import (
	"net/http"
	"time"

	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	"github.com/openGemini/openGemini/lib/cache"
)

const (
	ObsClientCacheSize = 1024
	ObsClientCacheTTL  = 24 * time.Hour
)

var obsClientCache = cache.NewCache(ObsClientCacheSize, ObsClientCacheTTL)

type clientEntry struct {
	key    string
	client ObsClient
	time   time.Time
}

func newClientEntry(client ObsClient) *clientEntry {
	return &clientEntry{client: client}
}

func updateClientFunc(old, new cache.Entry) bool {
	return true
}

func (c *clientEntry) SetTime(time time.Time) {
	c.time = time
}

func (c *clientEntry) GetTime() time.Time {
	return c.time
}

func (c *clientEntry) SetValue(value interface{}) {
	c.client = value.(ObsClient)
}

func (c *clientEntry) GetValue() interface{} {
	return c.client
}

func (c *clientEntry) GetKey() string {
	return c.key
}

func (c *clientEntry) Size() int64 {
	var size int64
	size += 8  // key
	size += 8  // client
	size += 24 // time
	return size
}

func PutObsClient(conf *obsConf, client ObsClient) {
	obsClientCache.Put(conf.cacheKey(), newClientEntry(client), updateClientFunc)
}

func GetObsClient(conf *obsConf) (ObsClient, error) {
	entry, ok := obsClientCache.Get(conf.cacheKey())
	if !ok {
		newClient, err := newObsClient(conf)
		if err != nil {
			return nil, err
		}
		obsClientCache.Put(conf.cacheKey(), newClientEntry(newClient), updateClientFunc)
		return newClient, nil
	}
	return entry.(*clientEntry).client, nil
}

type ObsClient interface {
	ListBuckets(input *obs.ListBucketsInput) (output *obs.ListBucketsOutput, err error)
	ListObjects(input *obs.ListObjectsInput) (output *obs.ListObjectsOutput, err error)
	GetObject(input *obs.GetObjectInput) (output *obs.GetObjectOutput, err error)
	DeleteObject(input *obs.DeleteObjectInput) (output *obs.DeleteObjectOutput, err error)
	DeleteObjects(input *obs.DeleteObjectsInput) (output *obs.DeleteObjectsOutput, err error)
	ModifyObject(input *obs.ModifyObjectInput) (output *obs.ModifyObjectOutput, err error)
	PutObject(input *obs.PutObjectInput) (output *obs.PutObjectOutput, err error)
	GetObjectMetadata(input *obs.GetObjectMetadataInput) (output *obs.GetObjectMetadataOutput, err error)
	RenameFile(input *obs.RenameFileInput) (*obs.RenameFileOutput, error)
	IsObsFile(input *obs.HeadObjectInput) (output *obs.BaseModel, err error)
	Do(r *http.Request) (*http.Response, error)
}

type obsConf struct {
	ak       string
	sk       string
	endpoint string
	bucket   string
}

func (c *obsConf) cacheKey() string {
	return c.ak + "|" + c.endpoint + "|" + c.bucket
}

type obsClient struct {
	obsClient  *obs.ObsClient
	httpClient *http.Client
	obsConf    *obsConf
}

func newObsClient(conf *obsConf) (ObsClient, error) {
	client, err := obs.New(conf.ak, conf.sk, conf.endpoint, obs.WithMaxRetryCount(3))
	if err != nil {
		return nil, err
	}
	httpClient := &http.Client{
		Timeout: time.Duration(30) * time.Second,
	}
	return &obsClient{
		obsClient:  client,
		httpClient: httpClient,
		obsConf:    conf,
	}, nil
}

func (o *obsClient) ListBuckets(input *obs.ListBucketsInput) (*obs.ListBucketsOutput, error) {
	return o.obsClient.ListBuckets(input)
}

func (o *obsClient) ListObjects(input *obs.ListObjectsInput) (*obs.ListObjectsOutput, error) {
	return o.obsClient.ListObjects(input)
}

func (o *obsClient) GetObject(input *obs.GetObjectInput) (*obs.GetObjectOutput, error) {
	return o.obsClient.GetObject(input)
}

func (o *obsClient) DeleteObject(input *obs.DeleteObjectInput) (*obs.DeleteObjectOutput, error) {
	return o.obsClient.DeleteObject(input)
}

func (o *obsClient) DeleteObjects(input *obs.DeleteObjectsInput) (output *obs.DeleteObjectsOutput, err error) {
	return o.obsClient.DeleteObjects(input)
}

func (o *obsClient) ModifyObject(input *obs.ModifyObjectInput) (*obs.ModifyObjectOutput, error) {
	return o.obsClient.ModifyObject(input)
}

func (o *obsClient) PutObject(input *obs.PutObjectInput) (*obs.PutObjectOutput, error) {
	return o.obsClient.PutObject(input)
}

func (o *obsClient) GetObjectMetadata(input *obs.GetObjectMetadataInput) (*obs.GetObjectMetadataOutput, error) {
	return o.obsClient.GetObjectMetadata(input)
}

func (o *obsClient) Do(r *http.Request) (*http.Response, error) {
	return o.httpClient.Do(r)
}

func (o *obsClient) RenameFile(input *obs.RenameFileInput) (output *obs.RenameFileOutput, err error) {
	return o.obsClient.RenameFile(input)
}

func (o *obsClient) IsObsFile(input *obs.HeadObjectInput) (output *obs.BaseModel, err error) {
	return o.obsClient.HeadObject(input)
}
