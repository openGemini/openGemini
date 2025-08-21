// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this File except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fence

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"os"
	"sync"

	"github.com/golang/geo/s2"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/logger"
	stat "github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/util"
	"go.uber.org/zap"
)

type Manager struct {
	fences *util.SyncMap[string, *Fence]

	fence2Cell *util.SyncMap[string, *util.SyncMap[s2.CellID, struct{}]]
	cell2fence *util.SyncMap[s2.CellID, *util.SyncMap[string, *Fence]]

	Fd fileops.File
	mu sync.RWMutex
}

func (gm *Manager) Open() error {
	v, err := fileops.OpenFile(config.GetStoreConfig().Fence.FenceFilePath, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		logger.GetLogger().Error("open fence Fd err", zap.Error(err))
		return err
	}
	gm.Fd = v
	return nil

}

func (gm *Manager) Close() error {
	if gm.Fd != nil {
		err := gm.Fd.Close()
		if err != nil {
			return err
		}
	}
	return nil

}

func newCellMap() *util.SyncMap[s2.CellID, struct{}] {
	return util.NewSyncMap[s2.CellID, struct{}]()
}

func newFenceMap() *util.SyncMap[string, *Fence] {
	return util.NewSyncMap[string, *Fence]()
}

var manager *Manager

func init() {
	manager = &Manager{
		fences:     util.NewSyncMap[string, *Fence](),
		fence2Cell: util.NewSyncMap[string, *util.SyncMap[s2.CellID, struct{}]](),
		cell2fence: util.NewSyncMap[s2.CellID, *util.SyncMap[string, *Fence]](),
	}
}

type PointMatchCtx struct {
	ids    []string
	fences []*Fence
}

func (ctx *PointMatchCtx) Reset() {
	ctx.ids = ctx.ids[:0]
	clear(ctx.fences)
	ctx.fences = ctx.fences[:0]
}

func ManagerIns() *Manager {
	return manager
}

func (gm *Manager) Add(fence *Fence) {
	stat.NewFence().FenceNumber.Incr()
	gm.fences.Store(fence.ID, fence)

	cells := fence.Geometry.Cells()
	subCell, _ := gm.fence2Cell.LoadOrStore(fence.ID, newCellMap)

	for _, cell := range cells {
		stat.NewFence().CellNumber.Incr()
		subCell.Store(cell, struct{}{})
		subFence, _ := gm.cell2fence.LoadOrStore(cell, newFenceMap)
		subFence.Store(fence.ID, fence)
	}
}
func (gm *Manager) reloadFenceFile() error {
	fences, err := ReadFencedFile()
	for _, fence := range fences {
		_, ok := gm.fences.Load(fence.ID)
		if ok {
			continue
		}
		gm.Add(fence)
	}
	if err != nil {
		logger.GetLogger().Error("reload fence Fd err", zap.Error(err))
	}

	if gm.Fd != nil {
		err := gm.Fd.Close()
		if err != nil {
			return err
		}
	}
	err = fileops.Remove(config.GetStoreConfig().Fence.FenceFilePath)
	if err != nil {
		logger.GetLogger().Error("remove fence Fd err", zap.Error(err))
	}
	err = gm.Open()
	if err != nil {
		return err
	}

	for _, fence := range fences {
		err := gm.WriteFenceFile(fence, false)
		if err != nil {
			logger.GetLogger().Error("reload fence Fd err", zap.Error(err))
		}
	}
	return nil
}

func (gm *Manager) WriteFenceFile(fence *Fence, isDel bool) error {

	file := gm.Fd
	data := fence.Serialize()

	record := make([]byte, 0, 1+len(data))
	if isDel {
		record = append(record, 1)
	} else {
		record = append(record, 0)
	}
	record = append(record, data...)

	lengthBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(lengthBytes, uint16(len(record)))
	if _, err := file.Write(lengthBytes); err != nil {
		return err
	}
	gm.mu.Lock()
	defer gm.mu.Unlock()
	_, err := file.Write(record)
	return err

}

func ReadFencedFile() ([]*Fence, error) {
	file, err := fileops.OpenFile(config.GetStoreConfig().Fence.FenceFilePath, os.O_RDONLY|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	defer func(file fileops.File) {
		err := file.Close()
		if err != nil {
			logger.GetLogger().Error("read fencefile close file err", zap.Error(err))
		}
	}(file)

	reader := bufio.NewReader(file)
	var results []*Fence
	activeIndex := make(map[string]int)

	for {
		lenBuf, err := reader.Peek(2)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		recordLen := binary.BigEndian.Uint16(lenBuf)
		_, err = reader.Discard(2)
		if err != nil {
			return nil, err
		}

		record := make([]byte, recordLen)
		if _, err := io.ReadFull(reader, record); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		deletedFlag := record[0]
		id, data, err := ParseId(record[1:])
		if err != nil {
			return nil, err
		}

		if deletedFlag == 0 {

			fence, err := ParseFenceById(id, data)
			if err != nil {
				return nil, err
			}
			results = append(results, fence)
			activeIndex[fence.ID] = len(results) - 1
		} else {

			if idx, exists := activeIndex[id]; exists {

				lastIdx := len(results) - 1
				results[idx] = results[lastIdx]

				activeIndex[results[lastIdx].ID] = idx

				results = results[:lastIdx]

				delete(activeIndex, id)
			}
		}
	}

	return results, nil
}

func ParseFenceById(id string, data []byte) (*Fence, error) {
	if len(data) < 2 {
		return &Fence{}, errors.New("invalid data format")
	}

	typeLen := binary.BigEndian.Uint16(data[:2])
	if len(data) < int(2+typeLen+16) {
		return &Fence{}, errors.New("invalid data format")
	}

	typ := string(data[2 : 2+typeLen])

	latData := data[2+typeLen : 10+typeLen]
	lonData := data[10+typeLen : 18+typeLen]
	radiusData := data[18+typeLen : 26+typeLen]

	lat := math.Float64frombits(binary.BigEndian.Uint64(latData))
	lon := math.Float64frombits(binary.BigEndian.Uint64(lonData))
	radius := math.Float64frombits(binary.BigEndian.Uint64(radiusData))

	return &Fence{
		ID:       id,
		Typ:      typ,
		Geometry: &Circle{lat: lat, lon: lon, radius: radius},
	}, nil
}

func ParseId(data []byte) (id string, remaining []byte, err error) {
	if len(data) < 2 {
		return "", nil, errors.New("invalid data format")
	}

	idLen := binary.BigEndian.Uint16(data[:2])
	if len(data) < int(2+idLen) {
		return "", nil, errors.New("invalid data format")
	}

	id = string(data[2 : 2+idLen])
	remaining = data[2+idLen:]
	return id, remaining, nil
}

func (gm *Manager) DeleteFenceByID(fenceId string) error {

	fence := &Fence{ID: fenceId}
	return gm.WriteFenceFile(fence, true)
}

func (gm *Manager) RemoveFenceMap(fenceId string) {
	gm.fences.Remove(fenceId)
	cellIdMap, _ := gm.fence2Cell.Load(fenceId)
	if cellIdMap == nil {
		return
	}
	var cellids []s2.CellID
	cellIdMap.Range(func(cellId s2.CellID, s struct{}) {
		cellids = append(cellids, cellId)
	})
	if len(cellids) == 0 {
		return
	}
	for cellId := range cellids {
		load, _ := gm.cell2fence.Load(cellids[cellId])
		load.Remove(fenceId)
	}

	gm.fence2Cell.Remove(fenceId)

}

func (gm *Manager) MatchPoint(ctx *PointMatchCtx, lat, lon float64) []string {
	statFence := stat.NewFence()
	statFence.FenceCheckTotal.Incr()

	point := s2.LatLngFromDegrees(lat, lon)
	cells, ok := gm.cell2fence.Load(GetLatLonCell(point))
	if !ok {
		return nil
	}

	ctx.Reset()
	statFence.MaybeInFenceTotal.Incr()
	cells.Range(func(id string, f *Fence) {
		ctx.fences = append(ctx.fences, f)
	})

	for _, fence := range ctx.fences {
		if fence.Geometry.Contains(point) {
			ctx.ids = append(ctx.ids, fence.ID)
		}
	}

	if len(ctx.ids) > 0 {
		statFence.InFenceTotal.Incr()
	}

	return ctx.ids
}

func (gm *Manager) Get(id string) (*Fence, bool) {
	return gm.fences.Load(id)
}
