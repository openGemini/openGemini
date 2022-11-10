package gramMatchQuery

import (
	"github.com/openGemini/openGemini/lib/utils"
)

type ResList struct {
	sid      utils.SeriesId
	posArray []uint16
}

func NewResList(sid utils.SeriesId, posArray []uint16) ResList {
	return ResList{
		sid:      sid,
		posArray: posArray,
	}
}
