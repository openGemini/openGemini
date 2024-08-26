// Copyright Huawei Cloud Computing Technologies Co., Ltd.
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
	"bytes"
	"fmt"
	"io"
	"io/fs"
	"mime"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	OBS "github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/request"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"go.uber.org/zap"
)

const (
	ObsReadRetryTimes  = 3
	ObsWriteRetryTimes = 3
)

type obsFile struct {
	key, fullPath string
	client        ObsClient
	conf          *obsConf
	offset        int64
	flag          int
}

func (o *obsFile) Close() error {
	o.offset = 0
	return o.Sync()
}

func (o *obsFile) Read(dst []byte) (int, error) {
	n, err := o.ReadAt(dst, o.offset)
	if err != nil {
		return 0, err
	}
	o.offset += int64(len(dst))
	return n, nil
}

func (o *obsFile) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case 0:
		o.offset = offset
	case 1:
		o.offset += offset
	default:
		return o.offset, fmt.Errorf("invalid whence(2) for obs file")
	}
	return o.offset, nil
}

func (o *obsFile) Write(src []byte) (int, error) {
	size := len(src)
	if size <= 0 {
		return 0, fmt.Errorf("write bytes length must be positive")
	}
	var err error
	for i := 0; i < ObsWriteRetryTimes; i++ {
		// the Body of ModifyObjectInput will be cleared after each send, so it need to place inside the loop body
		modifyObjectInput := &obs.ModifyObjectInput{
			Bucket:        o.conf.bucket,
			Key:           o.key,
			Position:      o.offset,
			Body:          bytes.NewReader(src),
			ContentLength: int64(len(src)),
		}
		_, err = o.client.ModifyObject(modifyObjectInput)
		if err == nil {
			break
		}
		logger.GetLogger().Error("retry append to object", zap.Error(err), zap.Int64("pos", modifyObjectInput.Position),
			zap.Int("data size", size), zap.String("write stack", string(debug.Stack())))
	}
	if err != nil {
		return 0, fmt.Errorf("obsClient.ModifyObject failed, error: %v", err)
	}
	o.offset += int64(size)
	return size, nil
}

func (o *obsFile) ReadAt(dst []byte, off int64) (int, error) {
	size := len(dst)
	if size <= 0 || off < 0 {
		return 0, fmt.Errorf("invalid read size[%v] or offset[%v]", size, off)
	}
	getObjectInput := &obs.GetObjectInput{
		RangeStart: off,
		RangeEnd:   off + int64(size) - 1,
	}
	getObjectInput.Bucket = o.conf.bucket
	getObjectInput.Key = o.key
	if getObjectOutput, err := o.client.GetObject(getObjectInput); err != nil {
		return 0, fmt.Errorf("obsClient.GetObject failed, error: %v", err)
	} else {
		_, err = io.ReadFull(getObjectOutput.Body, dst)
		if err != nil {
			return 0, err
		}
	}
	return size, nil
}

func (o *obsFile) StreamReadBatch(offsets []int64, sizes []int64, minBlockSize int64, c chan *request.StreamReader, rangSize int, isStat bool) {
	defer close(c)
	rangeRequests, err := NewObsReadRequest(offsets, sizes, minBlockSize, rangSize)
	if err != nil {
		c <- &request.StreamReader{Err: err}
		return
	}
	readLen := o.StreamReadRangeRequests(0, c, rangeRequests, rangSize)
	// collect obs read data size
	if isStat && config.GetProductType() == config.LogKeeper {
		statistics.NewLogKeeperStatistics().AddTotalObsReadDataSize(readLen)
		item := statistics.NewLogKeeperStatItem(o.GetRepoAndStreamId())
		atomic.AddInt64(&item.ObsReadDataSize, readLen)
		statistics.NewLogKeeperStatistics().Push(item)
	}
}

func (o *obsFile) GetRepoAndStreamId() (repoId, streamId string) {
	index := make([]int, 5)
	order := 0
	// o.key: ../data/repoId/0/streamId/1_1709510400000000000_1709596800000000000_1/columnstore/test_0000/xxx
	for i, v := range o.key {
		if v == '/' {
			index[order] = i
			order++
		}
		if order == 5 {
			break
		}
	}
	if order != 5 {
		return
	}
	return o.key[index[1]+1 : index[2]], o.key[index[3]+1 : index[4]]
}

type retryCtx struct {
	retries          map[int64]int64
	mutex            sync.RWMutex
	chErrorCloseOnce sync.Once
	chError          chan int
	totalReadLen     int64
	retryTimes       int32
}

func newRetryCtx(retryTimes int32) *retryCtx {
	return &retryCtx{
		retries:    make(map[int64]int64),
		chError:    make(chan int),
		retryTimes: retryTimes,
	}
}

func (r *retryCtx) empty() bool {
	return len(r.retries) == 0
}

func (r *retryCtx) insertRange(readers []*request.StreamReader) {
	r.mutex.Lock()
	for _, reader := range readers {
		r.retries[reader.Offset] = int64(len(reader.Content))
	}
	r.mutex.Unlock()
}

func (r *retryCtx) insertRangeFromReaderMap(readerMap map[int64][]*request.StreamReader) {
	for _, readers := range readerMap {
		r.insertRange(readers)
	}
}

func (r *retryCtx) addTotalReadLen(len int64) {
	atomic.AddInt64(&r.totalReadLen, len)
}

func (r *retryCtx) sendReadErr(err error, c chan *request.StreamReader) {
	logger.GetLogger().Error(fmt.Sprintf("obsClient.StreamReadMultiRange http request failed, error: %v", err))
	select {
	case c <- &request.StreamReader{Err: errno.NewError(errno.OBSClientRead, fmt.Sprintf("obsClient.StreamReadMultiRange http request failed, error: %v", err))}:
		r.chErrorCloseOnce.Do(func() {
			close(r.chError)
		})
	case <-r.chError:
		return
	}
}

func (r *retryCtx) dealReadErr(err error, c chan *request.StreamReader, readerMap map[int64][]*request.StreamReader) {
	if r.retryTimes >= ObsReadRetryTimes {
		r.sendReadErr(err, c)
	} else {
		r.insertRangeFromReaderMap(readerMap)
	}
}

func (r *retryCtx) dealReadErrForReaders(err error, c chan *request.StreamReader, readers []*request.StreamReader) {
	if r.retryTimes >= ObsReadRetryTimes {
		r.sendReadErr(err, c)
	} else {
		r.insertRange(readers)
	}
}

func (o *obsFile) newRequest(objUrl string, rangeRequest *RangeRequest) (*http.Request, error) {
	req, err := http.NewRequest(http.MethodGet, objUrl, nil)
	if err != nil {
		logger.GetLogger().Error("obsClient.StreamReadMultiRange new request failed, url is: " + objUrl)
		return nil, err
	}
	dateStr := time.Now().UTC().Format(time.RFC1123)
	req.Header = http.Header{}
	req.Header.Add("Authorization", headerSignature(o.conf.ak, o.conf.sk, o.conf.bucket, http.MethodGet, dateStr, o.key))
	req.Header.Add("Date", dateStr)
	req.Header.Add("Range", "bytes="+rangeRequest.GetRangeString())
	return req, nil
}

func (o *obsFile) getStreamReaders(rangeHeader string, rangeRequest *RangeRequest) []*request.StreamReader {
	hyphenPos := strings.Index(rangeHeader, "-")
	partStart, err := strconv.Atoi(rangeHeader[6:hyphenPos])
	if err != nil {
		logger.GetLogger().Error("obsClient.StreamReadMultiRange parse response header failed, header is: " + rangeHeader)
	}
	return rangeRequest.readMap[int64(partStart)]
}

func (o *obsFile) StreamReadRangeRequests(retryTimes int32, c chan *request.StreamReader,
	rangeRequests []*RangeRequest, obsRangeSize int) int64 {
	var toReadLen int64 = 0
	wg := &sync.WaitGroup{}
	retryRange := newRetryCtx(retryTimes)
	objUrl := o.buildObsEndpoint()
	for _, rangeRequest := range rangeRequests {
		wg.Add(1)
		toReadLen += rangeRequest.length
		go func(rangeRequest *RangeRequest, retryInfo *retryCtx, waitGroup *sync.WaitGroup) {
			defer waitGroup.Done()
			req, err := o.newRequest(objUrl, rangeRequest)
			if err != nil {
				return
			}
			if resp, err := o.client.Do(req); err != nil {
				retryInfo.dealReadErr(err, c, rangeRequest.readMap)
				return
			} else if strings.ToLower(resp.Header.Get("Content-Type")) == "binary/octet-stream" {
				readers := o.getStreamReaders(resp.Header.Get("Content-range"), rangeRequest)
				for i, v := range readers {
					readLen, err := io.ReadFull(resp.Body, v.Content)
					if err != nil {
						retryInfo.dealReadErrForReaders(err, c, readers[i:])
						return
					}
					select {
					case c <- v:
						retryInfo.addTotalReadLen(int64(readLen))
						continue
					case <-retryInfo.chError:
						return
					}
				}
			} else if strings.HasPrefix(strings.ToLower(resp.Header.Get("Content-Type")), "multipart/") {
				_, params, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
				if err != nil {
					logger.GetLogger().Error("obsClient.StreamReadMultiRange parse response header failed, Content-Type is: " + resp.Header.Get("Content-Type"))
				}
				mr := multipart.NewReader(resp.Body, params["boundary"])
				for part, err := mr.NextPart(); err == nil; part, err = mr.NextPart() {
					readers := o.getStreamReaders(part.Header.Get("Content-range"), rangeRequest)
					for i, v := range readers {
						readLen, err := io.ReadFull(part, v.Content)
						if err != nil {
							retryInfo.dealReadErrForReaders(err, c, readers[i:])
							return
						}
						select {
						case c <- v:
							retryInfo.addTotalReadLen(int64(readLen))
							continue
						case <-retryInfo.chError:
							return
						}
					}
				}
			} else {
				body, _ := io.ReadAll(resp.Body)
				logger.GetLogger().Error(fmt.Sprintf("obsClient.StreamReadMultiRange read body failed, resp body is illegal, body: " + string(body)))
				retryInfo.dealReadErr(err, c, rangeRequest.readMap)
				return
			}
		}(rangeRequest, retryRange, wg)
	}
	wg.Wait()
	if retryRange.empty() {
		return retryRange.totalReadLen
	}
	if retryTimes < ObsReadRetryTimes {
		rangeRequests = NewObsRetryReadRequest(retryRange.retries, obsRangeSize)
		toReadLen += o.StreamReadRangeRequests(retryTimes+1, c, rangeRequests, obsRangeSize)
		return toReadLen
	}
	logger.GetLogger().Error("obsClient.StreamReadMultiRange read retry times reach limit")
	return toReadLen
}

func (o *obsFile) buildObsEndpoint() string {
	return fmt.Sprintf("http://%s.%s/%s", o.conf.bucket, o.conf.endpoint, o.key)
}

func (o *obsFile) Name() string {
	return o.fullPath
}

func (o *obsFile) Truncate(size int64) error {
	if size < 0 {
		return fmt.Errorf("truncate size must be positive")
	}
	url := fmt.Sprintf("http://%s.%s/%s", o.conf.bucket, o.conf.endpoint, o.key)
	extension := fmt.Sprintf("?length=%d&truncate", size)
	req, err := http.NewRequest(http.MethodPut, url+extension, nil)
	if err != nil {
		return err
	}
	req.Header = http.Header{}
	dateStr := time.Now().UTC().Format(time.RFC1123)
	req.Header.Add("Authorization", headerSignature(o.conf.ak, o.conf.sk, o.conf.bucket, http.MethodPut, dateStr, o.key+extension))
	req.Header.Add("Date", dateStr)
	_, err = o.client.Do(req)
	if err != nil {
		return err
	}
	if o.offset > size {
		o.offset = size
	}
	return nil
}

func (o *obsFile) Sync() error {
	return nil
}

func (o *obsFile) Stat() (os.FileInfo, error) {
	getObjectMetadataInput := &obs.GetObjectMetadataInput{
		Bucket: o.conf.bucket,
		Key:    o.key,
	}
	if meta, err := o.client.GetObjectMetadata(getObjectMetadataInput); err != nil {
		return nil, fmt.Errorf("obsClient.GetObjectMetadata failed, error: %v", err)
	} else {
		return &obsFileInfo{
			name:         o.key,
			size:         meta.ContentLength,
			lastModified: meta.LastModified,
		}, nil
	}
}

func (o *obsFile) Size() (int64, error) {
	if o.flag&os.O_APPEND != 0 {
		return o.offset, nil
	}

	getObjectMetadataInput := &obs.GetObjectMetadataInput{
		Bucket: o.conf.bucket,
		Key:    o.key,
	}
	if meta, err := o.client.GetObjectMetadata(getObjectMetadataInput); err != nil {
		return 0, fmt.Errorf("obsClient.GetObjectMetadata failed, error: %v", err)
	} else {
		return meta.ContentLength, nil
	}
}

func headerSignature(ak string, sk string, bucketName string, httpMethod string, date string, objectKey string) string {
	var sb strings.Builder
	sb.WriteString(httpMethod)
	sb.WriteString("\n\n\n")
	sb.WriteString(date)
	sb.WriteString("\n")
	sb.WriteString("/")
	sb.WriteString(bucketName)
	sb.WriteString("/")
	sb.WriteString(objectKey)
	return "OBS " + ak + ":" + obs.Base64Encode(obs.HmacSha1([]byte(sk), []byte(sb.String())))
}

func (o *obsFile) SyncUpdateLength() error {
	return o.Sync()
}

func (o *obsFile) Fd() uintptr {
	return uintptr(unsafe.Pointer(o))
}

type obsFileInfo struct {
	name         string
	size         int64
	lastModified time.Time
}

func (o *obsFileInfo) Name() string {
	return o.name
}

func (o *obsFileInfo) Size() int64 {
	return o.size
}

func (o *obsFileInfo) Mode() fs.FileMode {
	return os.ModeIrregular
}

func (o *obsFileInfo) ModTime() time.Time {
	return o.lastModified
}

func (o *obsFileInfo) IsDir() bool {
	return strings.HasSuffix(o.name, "/")
}

func (o *obsFileInfo) Sys() any {
	return nil
}

type obsFs struct {
}

func NewObsFs() VFS {
	return &obsFs{}
}

func (o *obsFs) prepare(path string) (*obsConf, string, ObsClient, error) {
	conf, key, err := o.parseObsConf(path)
	if err != nil {
		return nil, "", nil, fmt.Errorf("parse obs config failed [%s]", path)
	}
	client, err := GetObsClient(conf)
	if err != nil {
		return nil, "", nil, err
	}
	return conf, key, client, nil
}

func (o *obsFs) parseObsConf(oriPath string) (*obsConf, string, error) {
	endpoint, ak, sk, bucket, basePath, err := decodeObsPath(oriPath)
	if err != nil {
		return nil, "", fmt.Errorf("cannot parse base path for obs")
	}
	conf := &obsConf{}
	conf.endpoint = endpoint
	conf.ak = ak
	conf.sk = sk
	conf.bucket = bucket
	return conf, basePath, nil
}

func (o *obsFs) Open(path string, opt ...FSOption) (File, error) {
	return o.OpenFile(path, os.O_RDWR, fs.ModePerm, opt...)
}

func (o *obsFs) OpenFile(path string, flag int, perm os.FileMode, opt ...FSOption) (File, error) {
	if flag&os.O_TRUNC != 0 && (flag&os.O_RDWR != 0 || flag&os.O_WRONLY != 0) {
		return Create(path, opt...)
	}
	conf, key, client, err := o.prepare(path)
	if err != nil {
		return nil, err
	}
	input := &obs.GetObjectMetadataInput{
		Bucket: conf.bucket,
		Key:    key,
	}

	meta, err := client.GetObjectMetadata(input)
	if err != nil {
		if err.(obs.ObsError).StatusCode == 404 && flag&os.O_CREATE > 0 { // object not found
			putObjectInput := &obs.PutObjectInput{
				Body: bytes.NewReader([]byte{}),
			}
			putObjectInput.Bucket = conf.bucket
			putObjectInput.Key = key
			_, err = client.PutObject(putObjectInput)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}
	var offset int64
	if flag&os.O_APPEND > 0 && meta != nil {
		offset = meta.ContentLength
	}

	fd := &obsFile{
		fullPath: path,
		key:      key,
		client:   client,
		conf:     conf,
		offset:   offset,
		flag:     flag,
	}
	return fd, nil
}

func (o *obsFs) Create(path string, opt ...FSOption) (File, error) {
	conf, key, client, err := o.prepare(path)
	if err != nil {
		return nil, err
	}
	input := &obs.PutObjectInput{
		Body: bytes.NewReader([]byte{}),
	}
	input.Bucket = conf.bucket
	input.Key = key
	_, err = client.PutObject(input)
	if err != nil {
		return nil, err
	}
	fd := &obsFile{
		fullPath: path,
		key:      key,
		client:   client,
		conf:     conf,
		offset:   0,
	}
	return fd, nil
}

func (o *obsFs) CreateV1(path string, opt ...FSOption) (File, error) {
	return o.Create(path, opt...)
}

func (o *obsFs) Remove(path string, opt ...FSOption) error {
	conf, key, client, err := o.prepare(path)
	if err != nil {
		return err
	}
	delInput := &obs.DeleteObjectInput{
		Bucket: conf.bucket,
		Key:    key,
	}
	_, err = client.DeleteObject(delInput)
	if err != nil {
		return err
	}
	return nil
}

func (o *obsFs) RemoveLocal(path string, _ ...FSOption) error {
	return o.Remove(path)
}

func (o *obsFs) RemoveLocalEnabled(obsOptValid bool) bool {
	return true
}

func (o *obsFs) RemoveAll(path string, opt ...FSOption) error {
	conf, key, client, err := o.prepare(path)
	if err != nil {
		return err
	}
	listInput := &obs.ListObjectsInput{
		Bucket: conf.bucket,
	}
	listInput.Prefix = o.NormalizeDirPath(key)
	objs, err := client.ListObjects(listInput)
	if err != nil {
		return err
	}
	sort.Slice(objs.Contents, func(i, j int) bool {
		return objs.Contents[i].Key > objs.Contents[j].Key
	})
	for _, obj := range objs.Contents {
		delInput := &obs.DeleteObjectInput{
			Bucket: conf.bucket,
			Key:    obj.Key,
		}
		_, err = client.DeleteObject(delInput)
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *obsFs) Mkdir(path string, perm os.FileMode, opt ...FSOption) error {
	return o.MkdirAll(path, perm, opt...)
}

func (o *obsFs) MkdirAll(path string, perm os.FileMode, opt ...FSOption) error {
	conf, key, client, err := o.prepare(path)
	if err != nil {
		return err
	}
	input := &obs.PutObjectInput{
		Body: bytes.NewReader([]byte{}),
	}
	input.Bucket = conf.bucket
	input.Key = o.NormalizeDirPath(key)
	_, err = client.PutObject(input)
	if err != nil {
		return err
	}
	return nil
}

func (o *obsFs) NormalizeDirPath(path string) string {
	i := 0
	for path[i] == '/' {
		i++
	}
	return filepath.Clean(path[i:]) + "/"
}

func (o *obsFs) ReadDir(path string) ([]os.FileInfo, error) {
	conf, key, client, err := o.prepare(path)
	if err != nil {
		return nil, err
	}
	listInput := &obs.ListObjectsInput{
		Bucket: conf.bucket,
	}
	listInput.Prefix = o.NormalizeDirPath(key)
	objs, err := client.ListObjects(listInput)
	if err != nil {
		return nil, err
	}
	infos := make([]os.FileInfo, len(objs.Contents))
	for i, obj := range objs.Contents {
		infos[i] = &obsFileInfo{
			name:         obj.Key,
			size:         obj.Size,
			lastModified: obj.LastModified,
		}
	}
	return infos, nil
}

func (o *obsFs) Glob(pattern string) ([]string, error) {
	conf, key, client, err := o.prepare(pattern)
	if err != nil {
		return nil, err
	}
	listInput := &obs.ListObjectsInput{
		Bucket: conf.bucket,
	}
	listInput.Prefix = key
	objs, err := client.ListObjects(listInput)
	if err != nil {
		return nil, err
	}
	names := make([]string, len(objs.Contents))
	for i, obj := range objs.Contents {
		names[i] = obj.Key
	}
	return names, nil
}

func (o *obsFs) RenameFile(oldPath, newPath string, opt ...FSOption) error {
	conf, key, client, err := o.prepare(oldPath)
	if err != nil {
		return err
	}

	input := &obs.RenameFileInput{
		Bucket:       conf.bucket,
		Key:          key,
		NewObjectKey: key[:len(key)-len(OBS.ObsFileTmpSuffix)],
	}

	_, err = client.RenameFile(input)
	return err
}

func (o *obsFs) IsObsFile(path string) (bool, error) {
	conf, key, client, err := o.prepare(path)
	if err != nil {
		return false, err
	}

	input := &obs.HeadObjectInput{
		Bucket: conf.bucket,
		Key:    key,
	}

	output, _ := client.IsObsFile(input)
	return output == nil, nil
}

func (o *obsFs) GetOBSTmpFileName(path string, obsOption *OBS.ObsOptions) string {
	return path + OBS.ObsFileSuffix + OBS.ObsFileTmpSuffix
}

func (o *obsFs) Stat(path string) (os.FileInfo, error) {
	conf, key, client, err := o.prepare(path)
	if err != nil {
		return nil, err
	}
	input := &obs.GetObjectMetadataInput{
		Bucket: conf.bucket,
		Key:    key,
	}
	meta, err := client.GetObjectMetadata(input)
	if err != nil {
		return nil, err
	}
	return &obsFileInfo{
		name:         key,
		size:         meta.ContentLength,
		lastModified: meta.LastModified,
	}, nil
}

func (o *obsFs) WriteFile(filename string, data []byte, perm os.FileMode, opt ...FSOption) error {
	conf, key, client, err := o.prepare(filename)
	if err != nil {
		return err
	}
	input := &obs.PutObjectInput{
		Body: bytes.NewReader(data),
	}
	input.ContentLength = int64(len(data))
	input.Bucket = conf.bucket
	input.Key = key
	_, err = client.PutObject(input)
	if err != nil {
		return err
	}
	return nil
}

func (o *obsFs) ReadFile(filename string, opt ...FSOption) ([]byte, error) {
	conf, key, client, err := o.prepare(filename)
	if err != nil {
		return nil, err
	}
	getObjectInput := &obs.GetObjectInput{}
	getObjectInput.Bucket = conf.bucket
	getObjectInput.Key = key
	if getObjectOutput, err := client.GetObject(getObjectInput); err != nil {
		return nil, fmt.Errorf("obsClient.GetObject failed, error: %v", err)
	} else {
		dst := make([]byte, getObjectOutput.ContentLength)
		_, err = io.ReadFull(getObjectOutput.Body, dst)
		if err != nil {
			return nil, err
		}
		return dst, nil
	}
}

func (o *obsFs) CopyFile(srcFile, dstFile string, opt ...FSOption) (written int64, err error) {
	content, err := o.ReadFile(srcFile, opt...)
	if err != nil {
		return 0, err
	}
	err = o.WriteFile(dstFile, content, os.ModePerm, opt...)
	if err != nil {
		return 0, err
	}
	return int64(len(content)), nil
}

func (o *obsFs) CreateTime(name string) (*time.Time, error) {
	info, err := o.Stat(name)
	if err != nil {
		return nil, err
	}
	modTime := info.ModTime()
	return &modTime, nil
}

func (o *obsFs) Truncate(name string, size int64, opt ...FSOption) error {
	fd, err := o.Open(name, opt...)
	if err != nil {
		return err
	}
	err = fd.Truncate(size)
	if err != nil {
		return err
	}
	return fd.Close()
}

func (o *obsFs) CopyFileFromDFVToOBS(srcPath, dstPath string, opt ...FSOption) error {
	_, err := o.CopyFile(srcPath, dstPath, opt...)
	return err
}

func (o *obsFs) CreateV2(name string, opt ...FSOption) (File, error) {
	return o.OpenFile(name, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0640)
}

func (o *obsFs) GetAllFilesSizeInPath(path string) (int64, int64, int64, error) {
	fi, err := o.Stat(path)
	if err != nil {
		return 0, 0, 0, err
	}
	size := fi.Size()
	return size, 0, 0, nil
}

func (o *obsFs) DecodeRemotePathToLocal(path string) (string, error) {
	_, key, _, err := o.prepare(path)
	if err != nil {
		return "", err
	}
	key = key[strings.Index(key, "/"):]
	return key[:len(key)-len(OBS.ObsFileSuffix)], nil
}
