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

package immutable

import (
	"sync"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/fileops"
)

type MockNameWriterCloser struct{}

func (MockNameWriterCloser) Write(p []byte) (int, error) { return len(p), nil }
func (MockNameWriterCloser) Close() error                { return nil }
func (MockNameWriterCloser) Name() string                { return "" }
func (MockNameWriterCloser) Read([]byte) (int, error)    { return 0, nil }

func TestCompactLimitWriter(t *testing.T) {
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()

		fd := &MockNameWriterCloser{}
		SetCompactLimit(1*1024*1024, 1*1024*1024+512*1024)
		lw := NewLimitWriter(fd, compWriteLimiter)

		b := make([]byte, 1*1024*1024)
		b[0] = 1
		b[len(b)-1] = 1

		for j := 0; j < 2; j++ {
			start := time.Now()
			var wn int
			for i := 0; i < 10; i++ {
				n, err := lw.Write(b)
				if err != nil {
					t.Fatal(err)
				}
				wn += n
			}
			d := time.Since(start)
			sec := int(d / time.Second)
			speed := float64(wn) / float64(sec) / 1024 / 1024
			t.Logf("copact speed: %.03fMB/s", speed)
			SetCompactLimit(2*1024*1024, 2*1024*1024+512*1024)
			time.Sleep(time.Second)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		fd := &MockNameWriterCloser{}
		SetSnapshotLimit(1*1024*1024, 2*1024*1024)
		lw := NewLimitWriter(fd, snapshotWriteLimiter)

		b := make([]byte, 2*1024*1024)
		b[0] = 1
		b[len(b)-1] = 1

		for j := 0; j < 2; j++ {
			start := time.Now()
			var wn int
			for i := 0; i < 10; i++ {
				n, err := lw.Write(b)
				if err != nil {
					t.Fatal(err)
				}
				wn += n
			}
			d := time.Since(start)
			sec := int(d / time.Second)
			speed := float64(wn) / float64(sec) / 1024 / 1024
			t.Logf("snapshot speed: %.03fMB/s", speed)
			SetSnapshotLimit(2*1024*1024, 3*1024*1024)
			time.Sleep(time.Second)
		}
	}()

	wg.Wait()
}

type nameWriterCloser struct{}

func (nameWriterCloser) Name() string                { return "" }
func (nameWriterCloser) Write(p []byte) (int, error) { return len(p), nil }
func (nameWriterCloser) Close() error                { return nil }
func (nameWriterCloser) Read(b []byte) (int, error)  { return len(b), nil }

func TestLimitWriter(t *testing.T) {
	var buf [32]byte
	lt := NewLimiter(10, 20)

	l := NewLimitWriter(nameWriterCloser{}, nil)
	if _, err := l.Write(buf[:]); err != nil {
		t.Fatal(err)
	}

	l = NewLimitWriter(nameWriterCloser{}, lt)
	_, err := l.Write(buf[:25])
	if err != nil {
		t.Fatal(err)
	}

	if l.Name() != "" {
		t.Fail()
	}

	if rn, err := l.Read(buf[:]); err != nil || rn != len(buf) {
		t.Fatal("read fail")
	}

	if err = l.Close(); err != nil {
		t.Fail()
	}
}

func TestSnapshotLimit(t *testing.T) {
	name := "/tmp/000000002-0001-0000.tssp.init"
	_ = fileops.Remove(name)
	defer fileops.Remove(name)
	fd := &mockFile{
		WriteFn: func(p []byte) (n int, err error) { return len(p), nil },
		CloseFn: func() error { return nil },
		NameFn:  func() string { return name },
	}
	SetSnapshotLimit(64*1024*1024, 70*1024*1024)
	lockPath := ""
	lw := newFileWriter(fd, false, false, &lockPath)
	var buf [1024 * 1024]byte

	start := time.Now()
	for i := 0; i < 512; i++ {
		if _, err := lw.WriteData(buf[:]); err != nil {
			t.Fatal(err)
		}
	}
	d := time.Since(start)
	burst := int(snapshotWriteLimiter.Burst()) / 1024 / 1024
	limit := int(snapshotWriteLimiter.Limit()) / 1024 / 1024
	if limit != 64 || burst != 70 {
		t.Fatalf("set limit rate fail, exp:[64, 70], but:[%v, %v]", limit, burst)
	}
	speed := int(float64(512*1024*1024) / d.Seconds() / 1024 / 1024)
	if speed < limit || speed > burst {
		t.Fatalf("write rate error, exp 64 <= speed <= 70, but speed = %v", speed)
	}

	// no limit
	SetSnapshotLimit(0, 0)
	lw = newFileWriter(fd, false, false, &lockPath)
	start = time.Now()
	for i := 0; i < 512; i++ {
		if _, err := lw.WriteData(buf[:]); err != nil {
			t.Fatal(err)
		}
	}
	d = time.Since(start)
	speed = int(float64(512*1024*1024) / d.Seconds() / 1024 / 1024)
	if speed < burst {
		t.Fatalf("write rate error, exp > 70, but speed = %v", speed)
	}
}
