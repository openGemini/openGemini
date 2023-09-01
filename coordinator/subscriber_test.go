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

package coordinator

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/influxdata/influxdb/toml"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/open_src/influx/meta"
	assert2 "github.com/stretchr/testify/assert"
)

type MockSubscriberClient struct {
	dest string
}

func (c *MockSubscriberClient) Send(db, rp string, lineProtocol []byte) error {
	return nil
}

func (c *MockSubscriberClient) Destination() string {
	return c.dest
}

func (c *MockSubscriberClient) SendColumn(db, rp, mst string, record array.Record) error {
	panic("implement me")
}

func (c *MockSubscriberClient) Close() {
	panic("implement me")
}

func TestAllWriter(t *testing.T) {
	destinations := []string{"http://127.0.0.1:8086", "http://127.0.0.1:8087", "http://127.0.0.1:8088"}
	clients := make([]Client, 3)
	for i, dest := range destinations {
		clients[i] = &MockSubscriberClient{dest}
	}
	w := AllWriter{NewBaseWriter("db0", "rp0", "sub0", clients, logger.NewLogger(errno.ModuleCoordinator))}

	ch := make(chan *WriteRequest, 3)
	w.ch = ch

	line := "cpu_load,host=\"server-01\",region=\"west_cn\" value=75.31"
	w.Write([]byte(line))
	for i := 0; i < 3; i++ {
		wr := <-ch
		assert2.Equal(t, wr.Client, i)
		assert2.Equal(t, string(wr.LineProtocol), line)
	}

	select {
	case <-ch:
		assert2.Error(t, errors.New("more write request in channel than expected"))
	default:
	}
	close(ch)
}

func TestAnyWriter(t *testing.T) {
	destinations := []string{"http://127.0.0.1:8086", "http://127.0.0.1:8087", "https://127.0.0.1:8088"}
	clients := make([]Client, 3)
	for i, dest := range destinations {
		clients[i] = &MockSubscriberClient{dest}
	}

	w := RoundRobinWriter{BaseWriter: NewBaseWriter("db0", "rp0", "sub0", clients, logger.NewLogger(errno.ModuleCoordinator))}
	ch := make(chan *WriteRequest, 1)
	w.ch = ch

	line := "cpu_load,host=\"server-01\",region=\"west_cn\" value=75.31"
	for i := 0; i < 6; i++ {
		w.Write([]byte(line))
		wr := <-ch
		assert2.Equal(t, wr.Client, (i+1)%3)
		assert2.Equal(t, string(wr.LineProtocol), line)
		select {
		case <-ch:
			assert2.Error(t, errors.New("more write request in channel than expected"))
		default:
		}
	}
	close(ch)
}

func JudgeSame(dbis map[string]*meta.DatabaseInfo, writers map[string]map[string][]SubscriberWriter) error {
	for _, dbi := range dbis {
		for _, rpi := range dbi.RetentionPolicies {
			subscriptions := rpi.Subscriptions
			writer, ok := writers[dbi.Name][rpi.Name]
			if !ok && len(subscriptions) == 0 {
				continue
			}
			if !ok {
				return fmt.Errorf("there should be %d writers of %s.%s, but got none", len(subscriptions), dbi.Name, rpi.Name)
			}
			if len(subscriptions) != len(writer) {
				return fmt.Errorf("there should be %d writers of %s.%s, but got %d", len(subscriptions), dbi.Name, rpi.Name, len(writer))
			}
			// allow different order, so we need to construct maps to compare
			subscriptionMap := make(map[string]meta.SubscriptionInfo)
			for _, sub := range subscriptions {
				subscriptionMap[sub.Name] = sub
			}
			writerMap := make(map[string]SubscriberWriter)
			for _, w := range writer {
				writerMap[w.Name()] = w
			}
			for name, sub := range subscriptionMap {
				w, ok := writerMap[name]
				if !ok {
					return fmt.Errorf("database info has subscription %s.%s.%s, but writers hasn't", dbi.Name, rpi.Name, name)
				}
				if _, ok := w.(*AllWriter); ok && sub.Mode == "ANY" {
					return fmt.Errorf("subscription %s.%s.%s type not match, should be ANY but got ALL", dbi.Name, rpi.Name, name)
				}
				if _, ok := w.(*RoundRobinWriter); ok && sub.Mode == "ALL" {
					return fmt.Errorf("subscription %s.%s.%s type not match, should be ALL but got ANY", dbi.Name, rpi.Name, name)
				}
				clients := w.Clients()
				if len(sub.Destinations) != len(clients) {
					return fmt.Errorf("subscription %s.%s.%s has %d destinations, but writer has %d destinations",
						dbi.Name, rpi.Name, name, len(sub.Destinations), len(clients))
				}
				for i := 0; i < len(sub.Destinations); i++ {
					if sub.Destinations[i] != clients[i].Destination() {
						return fmt.Errorf("subscription %s.%s.%s destination mismatch %s %s",
							dbi.Name, rpi.Name, name, sub.Destinations[i], clients[i].Destination())
					}
				}
			}
		}
	}
	return nil
}

type MockSubscriberMetaClient struct {
	databases         map[string]*meta.DatabaseInfo
	maxSubscriptionID uint64
}

func (c *MockSubscriberMetaClient) Databases() map[string]*meta.DatabaseInfo {
	return c.databases
}

func (c *MockSubscriberMetaClient) Database(db string) (*meta.DatabaseInfo, error) {
	dbi, ok := c.databases[db]
	if !ok {
		return nil, fmt.Errorf("database %s not exist", db)
	}
	return dbi, nil
}

func (c *MockSubscriberMetaClient) GetMaxSubscriptionID() uint64 {
	return c.maxSubscriptionID
}

func (c *MockSubscriberMetaClient) WaitForDataChanged() chan struct{} {
	ch := make(chan struct{})
	defer close(ch)
	return ch
}

func (c *MockSubscriberMetaClient) CreateSubscription(db, rp, name, mode string, destinations []string) {
	dbi, ok := c.databases[db]
	if !ok {
		if c.databases == nil {
			c.databases = make(map[string]*meta.DatabaseInfo)
		}
		dbi = &meta.DatabaseInfo{Name: db}
		c.databases[db] = dbi
	}
	rpi, ok := dbi.RetentionPolicies[rp]
	if !ok {
		if dbi.RetentionPolicies == nil {
			dbi.RetentionPolicies = make(map[string]*meta.RetentionPolicyInfo)
		}
		rpi = &meta.RetentionPolicyInfo{Name: rp, Subscriptions: make([]meta.SubscriptionInfo, 0)}
		dbi.RetentionPolicies[rp] = rpi
	}
	sbi := meta.SubscriptionInfo{Name: name, Mode: mode, Destinations: destinations}
	rpi.Subscriptions = append(rpi.Subscriptions, sbi)
	c.maxSubscriptionID++
}

func (c *MockSubscriberMetaClient) DropSubscription(db, rp, name string) error {
	dbi, ok := c.databases[db]
	if !ok {
		return fmt.Errorf("database %s not exist", db)
	}
	rpi, ok := dbi.RetentionPolicies[rp]
	if !ok {
		return fmt.Errorf("retention policy %s.%s not exist", db, rp)
	}
	for i := 0; i < len(rpi.Subscriptions); i++ {
		if rpi.Subscriptions[i].Name == name {
			rpi.Subscriptions = append(rpi.Subscriptions[:i], rpi.Subscriptions[i+1:]...)
			c.maxSubscriptionID++
			return nil
		}
	}
	return fmt.Errorf("subscription %s.%s.%s not exist", db, rp, name)
}

func TestInitWriter(t *testing.T) {
	client := &MockSubscriberMetaClient{databases: make(map[string]*meta.DatabaseInfo)}
	client.CreateSubscription("db0", "rp0", "sub0", "ALL", []string{"http://127.0.0.1:8086"})
	client.CreateSubscription("db0", "rp0", "sub1", "ANY", []string{"http://127.0.0.2:8086", "https://127.0.0.3:8086"})
	client.CreateSubscription("db1", "rp1", "sub0", "ALL", []string{"http://127.0.0.1:8086"})

	conf := config.NewSubscriber()
	s := &SubscriberManager{
		writers:    make(map[string]map[string][]SubscriberWriter),
		metaclient: client,
		config:     &conf,
	}
	s.WithLogger(logger.NewLogger(errno.ModuleUnknown))
	s.InitWriters()
	err := JudgeSame(client.databases, s.writers)
	assert2.NoError(t, err)
}

func TestUpdateWriter(t *testing.T) {
	client := &MockSubscriberMetaClient{databases: make(map[string]*meta.DatabaseInfo)}
	client.CreateSubscription("db0", "rp0", "sub0", "ALL", []string{"http://127.0.0.1:8086"})
	client.CreateSubscription("db0", "rp0", "sub1", "ANY", []string{"http://127.0.0.2:8086", "https://127.0.0.3:8086"})

	conf := config.NewSubscriber()
	s := &SubscriberManager{
		writers:    make(map[string]map[string][]SubscriberWriter),
		metaclient: client,
		config:     &conf,
	}
	s.WithLogger(logger.NewLogger(errno.ModuleUnknown))
	s.InitWriters()
	err := JudgeSame(client.databases, s.writers)
	assert2.NoError(t, err)

	// test add new subscriptions
	client.CreateSubscription("db1", "rp1", "sub0", "ALL", []string{"http://127.0.0.1:8086"})
	client.CreateSubscription("db1", "rp2", "sub0", "ANY", []string{"https://127.0.0.1:8086"})
	client.CreateSubscription("db0", "rp0", "sub2", "ALL", []string{"http://127.0.0.1:8086", "http://127.0.0.2:8087"})
	s.UpdateWriters()
	err = JudgeSame(client.databases, s.writers)
	assert2.NoError(t, err)

	// test remove subscriptions
	err = client.DropSubscription("db0", "rp0", "sub0")
	assert2.NoError(t, err)
	err = client.DropSubscription("db1", "rp2", "sub0")
	assert2.NoError(t, err)
	s.UpdateWriters()
	err = JudgeSame(client.databases, s.writers)
	assert2.NoError(t, err)

	// test remove subscriptions and add new subscriptions
	client.CreateSubscription("db0", "rp0", "sub3", "ALL", []string{"http://127.0.0.2:8086", "http://127.0.0.2:8087"})
	client.CreateSubscription("db0", "rp0", "sub4", "ANY", []string{"http://127.0.0.2:8086", "http://127.0.0.2:8087"})
	err = client.DropSubscription("db0", "rp0", "sub2")
	assert2.NoError(t, err)
	client.CreateSubscription("db1", "rp1", "sub3", "ALL", []string{"http://127.0.0.2:8086", "http://127.0.0.2:8087"})
	err = client.DropSubscription("db1", "rp1", "sub0")
	assert2.NoError(t, err)
	s.UpdateWriters()
	err = JudgeSame(client.databases, s.writers)
	assert2.NoError(t, err)

	// test re-create dropped subscription
	client.CreateSubscription("db1", "rp2", "sub0", "ANY", []string{"https://127.0.0.1:8086"})
	s.UpdateWriters()
	err = JudgeSame(client.databases, s.writers)
	assert2.NoError(t, err)
}

func TestUpdate(t *testing.T) {
	client := &MockSubscriberMetaClient{databases: make(map[string]*meta.DatabaseInfo)}
	conf := config.NewSubscriber()
	s := &SubscriberManager{
		writers:    make(map[string]map[string][]SubscriberWriter),
		metaclient: client,
		config:     &conf,
	}
	s.WithLogger(logger.NewLogger(errno.ModuleUnknown))

	go s.Update()
	client.CreateSubscription("db0", "rp0", "sub0", "ANY", []string{"http://127.0.0.2:8086", "https://127.0.0.3:8086"})
	client.CreateSubscription("db1", "rp1", "sub1", "ALL", []string{"http://127.0.0.2:8086", "https://127.0.0.3:8086"})
	time.Sleep(time.Millisecond * 100)
	err := JudgeSame(client.databases, s.writers)
	assert2.NoError(t, err)

	_ = client.DropSubscription("db0", "rp0", "sub1")
	_ = client.DropSubscription("db1", "rp1", "sub1")
	time.Sleep(time.Millisecond * 100)
	err = JudgeSame(client.databases, s.writers)
	assert2.NoError(t, err)
	s.StopAllWriters()
}

func TestSendWriteRequest(t *testing.T) {
	type Request struct {
		db           string
		rp           string
		lineProtocol []byte
	}
	ch := make(chan Request, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("/write", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wr := Request{db: r.URL.Query().Get("db"), rp: r.URL.Query().Get("rp")}
		wr.lineProtocol, _ = ioutil.ReadAll(r.Body)
		ch <- wr
		w.WriteHeader(http.StatusNoContent)
	}))
	server1 := httptest.NewServer(mux)
	defer server1.Close()
	server2 := httptest.NewTLSServer(mux)
	defer server2.Close()
	// close server3, and the subscriberManager shouldn't crash
	server3 := httptest.NewServer(mux)
	server3.Close()

	client := &MockSubscriberMetaClient{databases: make(map[string]*meta.DatabaseInfo)}
	client.CreateSubscription("db0", "rp0", "sub0", "ALL", []string{server1.URL, server2.URL, server3.URL})
	client.CreateSubscription("db1", "rp1", "sub0", "ANY", []string{server1.URL, server2.URL})

	conf := config.NewSubscriber()
	conf.InsecureSkipVerify = true
	conf.HTTPTimeout = toml.Duration(time.Second)
	s := &SubscriberManager{
		writers:    make(map[string]map[string][]SubscriberWriter),
		metaclient: client,
		config:     &conf,
	}
	s.WithLogger(logger.NewLogger(errno.ModuleUnknown))
	s.InitWriters()
	line := "cpu_load,host=\"server-01\",region=\"west_cn\" value=75.3"

	// test ALL mode
	for i := 0; i < 5; i++ {
		s.Send("db0", "rp0", []byte(line))
	}

	for i := 0; i < 10; i++ {
		r := <-ch
		assert2.Equal(t, r.db, "db0")
		assert2.Equal(t, r.rp, "rp0")
		assert2.Equal(t, string(r.lineProtocol), line)
	}

	time.Sleep(time.Second)
	select {
	case <-ch:
		assert2.Error(t, errors.New("more write request than expected"))
	default:
	}

	// test any mode
	dbi, ok := client.databases["db1"]
	if !ok {
		t.Error("fail to find db1")
	}
	dbi.DefaultRetentionPolicy = "rp1"
	for i := 0; i < 5; i++ {
		s.Send("db1", "", []byte(line))
	}

	for i := 0; i < 5; i++ {
		r := <-ch
		assert2.Equal(t, r.db, "db1")
		assert2.Equal(t, r.rp, "rp1")
		assert2.Equal(t, string(r.lineProtocol), line)
	}

	time.Sleep(time.Second)
	select {
	case <-ch:
		assert2.Error(t, errors.New("more write request than expected"))
	default:
	}
	s.StopAllWriters()
}
