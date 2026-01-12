/*
Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.

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

package executor

type TDdcmCycleManagerBase interface {
	AddNewLevel() int
	Copy() TDdcmCycleManagerBase
}

type TDdcmCycleManagerWdm struct {
	CyclesNumberExponent       float64
	CurrentNumCyclesFractional float64
	CurrentNumCycles           int
	CurrentCyclePos            int
	CurrentCycleLength         int
	CurrentPosInCycle          int
}

func NewTDdcmCycleManagerWdm(cyclesNumberExponent float64, firstCycleLength int) *TDdcmCycleManagerWdm {
	return &TDdcmCycleManagerWdm{
		CyclesNumberExponent:       cyclesNumberExponent,
		CurrentNumCyclesFractional: 1,
		CurrentNumCycles:           1,
		CurrentCyclePos:            0,
		CurrentCycleLength:         firstCycleLength,
		CurrentPosInCycle:          (-1),
	}
}

func (ddcmw *TDdcmCycleManagerWdm) AddNewLevel() int {
	if ddcmw.CurrentPosInCycle < ddcmw.CurrentCycleLength-1 {
		ddcmw.CurrentPosInCycle++
	} else if ddcmw.CurrentCyclePos < ddcmw.CurrentNumCycles-1 {
		ddcmw.CurrentCyclePos++
		ddcmw.CurrentPosInCycle = 0
	} else {
		ddcmw.CurrentNumCyclesFractional *= ddcmw.CyclesNumberExponent
		ddcmw.CurrentNumCycles = int(ddcmw.CurrentNumCyclesFractional)
		ddcmw.CurrentCyclePos = 0
		ddcmw.CurrentCycleLength++
		ddcmw.CurrentPosInCycle = 0
	}
	return ddcmw.CurrentCycleLength - ddcmw.CurrentPosInCycle - 1
}

func (ddcmw *TDdcmCycleManagerWdm) Copy() TDdcmCycleManagerBase {
	return &TDdcmCycleManagerWdm{
		CyclesNumberExponent:       ddcmw.CyclesNumberExponent,
		CurrentNumCyclesFractional: ddcmw.CurrentNumCyclesFractional,
		CurrentNumCycles:           ddcmw.CurrentNumCycles,
		CurrentCyclePos:            ddcmw.CurrentCyclePos,
		CurrentCycleLength:         ddcmw.CurrentCycleLength,
		CurrentPosInCycle:          ddcmw.CurrentPosInCycle,
	}
}
