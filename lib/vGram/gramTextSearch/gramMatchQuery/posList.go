package gramMatchQuery

import (
	"github.com/openGemini/openGemini/lib/utils"
)

type PosList struct {
	sid      utils.SeriesId
	posArray []uint16
}

func (p *PosList) Sid() utils.SeriesId {
	return p.sid
}

func (p *PosList) SetSid(sid utils.SeriesId) {
	p.sid = sid
}

func (p *PosList) PosArray() []uint16 {
	return p.posArray
}

func (p *PosList) SetPosArray(posArray []uint16) {
	p.posArray = posArray
}

func NewPosList(sid utils.SeriesId, posArray []uint16) PosList {
	return PosList{
		sid:      sid,
		posArray: posArray,
	}
}
