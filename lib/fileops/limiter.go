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

package fileops

import (
	"context"
	"io"
	"time"

	"golang.org/x/time/rate"
)

var (
	BackgroundReadLimiter                 = NewLimiter(64*1024*1024, 64*1024*1024)
	ctx                   context.Context = context.Background()
)

type Limiter interface {
	SetBurst(newBurst int)
	SetLimit(newLimit rate.Limit)
	WaitN(ctx context.Context, n int) (err error)
	Limit() rate.Limit
	Burst() int
}

type NameReadWriterCloser interface {
	Name() string
	io.ReadWriteCloser
}

func NewLimiter(bytesPerSec, burstLimit int) Limiter {
	l := rate.NewLimiter(rate.Limit(bytesPerSec), burstLimit)
	l.AllowN(time.Now(), burstLimit)
	return l
}

type LimitWriter struct {
	w       NameReadWriterCloser
	limiter Limiter
	ctx     context.Context
}

func NewLimitWriter(w NameReadWriterCloser, l Limiter) NameReadWriterCloser {
	return &LimitWriter{
		w:       w,
		limiter: l,
		ctx:     context.Background(),
	}
}

func (w *LimitWriter) Write(p []byte) (int, error) {
	if w.limiter == nil {
		return w.w.Write(p)
	}

	buf := p
	var n int
	for len(buf) > 0 {
		writeN := len(buf)
		if writeN > w.limiter.Burst() {
			writeN = w.limiter.Burst()
		}

		wn, err := w.w.Write(buf[:writeN])
		if err != nil {
			return n, err
		}

		n += wn
		buf = buf[wn:]

		if err = w.limiter.WaitN(w.ctx, wn); err != nil {
			return n, err
		}
	}

	return n, nil
}

func (w *LimitWriter) Close() error {
	return w.w.Close()
}

func (w *LimitWriter) Name() string {
	return w.w.Name()
}

func (w *LimitWriter) Read(b []byte) (int, error) {
	return w.w.Read(b)
}

func SetBackgroundReadLimiter(limiter int) {
	BackgroundReadLimiter.SetLimit(rate.Limit(limiter))
	BackgroundReadLimiter.SetBurst(limiter)
}

func BackGroundReaderWait(n int) error {
	if err := BackgroundReadLimiter.WaitN(ctx, n); err != nil {
		return err
	}
	return nil
}
