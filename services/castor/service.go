/*
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.

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
package castor

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/services"
)

var service *Service

type Service struct {
	services.Base

	Config config.Castor
	mu     sync.Mutex
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	clientPool chan *castorCli
	clientCnt  map[string]*int32

	dataChan        chan *data
	dataRetryChan   chan *data
	dataFailureChan chan *data
	resultChan      chan array.Record
	responseChanMap sync.Map

	alive bool
}

// GetService return service instance
func GetService() *Service {
	return service
}

// NewService return an new service instance
func NewService(c config.Castor) *Service {
	if !c.Enabled {
		return &Service{Config: c}
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := &Service{
		Config:          c,
		ctx:             ctx,
		cancel:          cancel,
		clientPool:      make(chan *castorCli, c.ConnPoolSize*len(c.PyWorkerAddr)),
		dataChan:        make(chan *data, chanBufferSize),
		dataRetryChan:   make(chan *data, chanBufferSize),
		dataFailureChan: make(chan *data, chanBufferSize),
		resultChan:      make(chan array.Record, chanBufferSize),
		responseChanMap: sync.Map{},
	}
	s.clientCnt = make(map[string]*int32, len(c.PyWorkerAddr))
	for _, addr := range c.PyWorkerAddr {
		s.clientCnt[addr] = new(int32)
	}
	s.Init("castor", 0, s.handle)
	return s
}

// Open start service
func (s *Service) Open() error {
	if !s.Config.Enabled {
		return nil
	}

	if err := s.Base.Open(); err != nil {
		return err
	}
	s.wg.Add(4)
	go s.monitorConn()
	go s.handleResult()
	go s.sendData()
	go s.recordFailure()
	s.alive = true
	service = s
	return nil
}

func (s *Service) handle() {}

func (s *Service) monitorConn() {
	ticker := time.NewTicker(connCheckInterval)
	defer func() {
		s.wg.Done()
		ticker.Stop()
	}()

	for {
		select {
		case <-ticker.C:
			s.fillUpConnPool()
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *Service) recordFailure() {
	defer s.wg.Done()
	for {
		select {
		case data, ok := <-s.dataFailureChan:
			if !ok {
				return
			}
			s.Logger.Error(errno.NewError(errno.FailToProcessData, data.size()).Error())
			data.release()
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *Service) fillUpConnPool() {
	for addr, cnt := range s.clientCnt {
		for i := int(atomic.LoadInt32(cnt)); i < s.Config.ConnPoolSize; i++ {
			time.Sleep(time.Second)
			cli, err := newClient(addr, s.Logger, &chanSet{
				dataRetryChan:   s.dataRetryChan,
				dataFailureChan: s.dataFailureChan,
				resultChan:      s.resultChan,
				clientPool:      s.clientPool,
			}, cnt)
			if err != nil {
				s.Logger.Warn(errno.NewError(errno.FailToFillUpConnPool, err).Error())
				continue
			}
			s.clientPool <- cli
		}
	}
}

func (s *Service) sendData() {
	defer s.wg.Done()
	for {
		select {
		case data, ok := <-s.dataChan:
			if !ok {
				return
			}
			cli, err := s.getClient()
			if err != nil {
				if errno.Equal(err, errno.ClientQueueClosed) {
					s.Logger.Error(errno.NewError(errno.FailToProcessData, data.size()).Error())
					return
				}
				s.Logger.Warn(err.Error())
				s.dataRetryChan <- data
				continue
			}
			cli.writeChan <- data
		case data, ok := <-s.dataRetryChan:
			if !ok {
				return
			}
			cli, err := s.getClient()
			if err != nil {
				if errno.Equal(err, errno.ClientQueueClosed) {
					s.Logger.Error(errno.NewError(errno.FailToProcessData, data.size()).Error())
					return
				}
				s.dataFailureChan <- data
				continue
			}
			cli.writeChan <- data
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *Service) getClient() (*castorCli, *errno.Error) {
	timer := time.NewTimer(getCliTimeout)
	defer timer.Stop()
	var cli *castorCli
	var ok bool
GETCLI:
	for {
		select {
		case <-timer.C:
			break GETCLI
		case cli, ok = <-s.clientPool:
			if !ok {
				return nil, errno.NewError(errno.ClientQueueClosed)
			}
			break GETCLI
		}
	}
	if cli == nil {
		return nil, errno.NewError(errno.NoAvailableClient)
	}
	if !cli.alive {
		return nil, errno.NewError(errno.ConnectionBroken)
	}
	return cli, nil
}

func (s *Service) handleResult() {
	defer s.wg.Done()
	for {
		select {
		case res, ok := <-s.resultChan:
			if !ok {
				return
			}
			if err := s.dispatchResult(res); err != nil {
				res.Release()
				s.Logger.Error(err.Error())
			}
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *Service) dispatchResult(rec array.Record) *errno.Error {
	id, err := GetMetaValueFromRecord(rec, string(TaskID))
	if err != nil {
		return err
	}

	nodeResultChan, exist := s.responseChanMap.Load(id)
	if !exist {
		return errno.NewError(errno.ResponseTimeout)
	}
	ch, ok := nodeResultChan.(*respChan)
	if !ok {
		return errno.NewError(errno.TypeAssertFail)
	}
	ch.mu.Lock()
	defer ch.mu.Unlock()
	if !ch.alive {
		return errno.NewError(errno.ResponseTimeout)
	}
	ch.C <- rec
	return nil
}

// HandleData will send data to computation node through internal client
func (s *Service) HandleData(record array.Record) {
	s.dataChan <- newData(record)
}

// ResultChan store result from computation node
func (s *Service) ResultChan() chan<- array.Record {
	return s.resultChan
}

// Close release internal client and close channel, service will mark as not alive, any castor query will not be processed
func (s *Service) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.alive {
		return nil
	}
	s.alive = false
	service = nil
	s.cancel()
	s.wg.Wait()

	// close all client
	for len(s.clientPool) > 0 {
		cli := <-s.clientPool
		cli.close()
	}
	close(s.clientPool)

	// clear out remaining data
	for len(s.dataChan) > 0 {
		s.dataFailureChan <- <-s.dataChan
	}
	close(s.dataChan)
	for len(s.dataRetryChan) > 0 {
		s.dataFailureChan <- <-s.dataRetryChan
	}
	close(s.dataRetryChan)
	for len(s.resultChan) > 0 {
		s.dataFailureChan <- newData(<-s.resultChan)
	}
	close(s.resultChan)
	for len(s.dataFailureChan) > 0 {
		data := <-s.dataFailureChan
		s.Logger.Error(errno.NewError(errno.FailToProcessData, data.size()).Error())
		data.release()
	}
	close(s.dataFailureChan)

	s.responseChanMap = sync.Map{}
	if err := s.Base.Close(); err != nil {
		return err
	}
	s.Logger.Info("shutdown castor service")
	if l := s.Logger.GetSuppressLogger(); l != nil {
		l.Close()
	}
	return nil
}

// IsAlive return service running status
func (s *Service) IsAlive() bool {
	return s.alive
}

// RegisterResultChan register an channel with id, response from computation node will be dispatched according to id
func (s *Service) RegisterResultChan(id string, ch *respChan) {
	s.responseChanMap.Store(id, ch)
}

// DeregisterResultChan deregister an channel with id, response from computation node will be dropped if correspond id not found
func (s *Service) DeregisterResultChan(id string) {
	s.responseChanMap.Delete(id)
}
