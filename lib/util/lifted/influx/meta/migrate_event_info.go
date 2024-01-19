package meta

import (
	metaProto "github.com/openGemini/openGemini/lib/util/lifted/influx/meta/proto"
	"github.com/openGemini/openGemini/lib/util/lifted/protobuf/proto"
)

type MigrateEventInfo struct {
	eventId     string
	eventType   int
	opId        uint64
	pt          *DbPtInfo
	currState   int
	preState    int
	src         uint64
	dest        uint64
	aliveConnId uint64
}

func NewMigrateEventInfo(eventId string, eventType int, pt *DbPtInfo, dest uint64, aliveConnId uint64) *MigrateEventInfo {
	return &MigrateEventInfo{
		eventId:     eventId,
		eventType:   eventType,
		pt:          pt,
		dest:        dest,
		aliveConnId: aliveConnId,
	}
}

func (m *MigrateEventInfo) GetSrc() uint64 {
	return m.src
}

func (m *MigrateEventInfo) SetSrc(src uint64) {
	m.src = src
}

func (m *MigrateEventInfo) SetDest(dst uint64) {
	m.dest = dst
}

func (m *MigrateEventInfo) GetDst() uint64 {
	return m.dest
}

func (m *MigrateEventInfo) GetEventType() int {
	return m.eventType
}

func (m *MigrateEventInfo) GetPtInfo() *DbPtInfo {
	return m.pt
}

func (m *MigrateEventInfo) GetOpId() uint64 {
	return m.opId
}

func (m *MigrateEventInfo) GetCurrentState() int {
	return m.currState
}

func (m *MigrateEventInfo) SetCurrentState(state int) {
	m.currState = state
}

func (m *MigrateEventInfo) GetPreState() int {
	return m.preState
}

func (m *MigrateEventInfo) SetPreState(state int) {
	m.preState = state
}

func (m *MigrateEventInfo) GetAliveConnId() uint64 {
	return m.aliveConnId
}

func (m *MigrateEventInfo) marshal() *metaProto.MigrateEventInfo {
	pb := &metaProto.MigrateEventInfo{
		EventId:     proto.String(m.eventId),
		EventType:   proto.Int(m.eventType),
		OpId:        proto.Uint64(m.opId),
		Pti:         m.pt.Marshal(),
		CurrState:   proto.Int(m.currState),
		PreState:    proto.Int(m.preState),
		Dest:        proto.Uint64(m.dest),
		Src:         proto.Uint64(m.src),
		AliveConnId: proto.Uint64(m.aliveConnId),
	}
	return pb
}

func (m *MigrateEventInfo) unmarshal(pb *metaProto.MigrateEventInfo) {
	m.eventId = pb.GetEventId()
	m.eventType = int(pb.GetEventType())
	m.opId = pb.GetOpId()
	m.pt = &DbPtInfo{}
	m.pt.Unmarshal(pb.GetPti())
	m.preState = int(pb.GetCurrState())
	m.currState = int(pb.GetCurrState())
	m.src = pb.GetSrc()
	m.dest = pb.GetDest()
	m.aliveConnId = pb.GetAliveConnId()
}

func (m *MigrateEventInfo) Clone() *MigrateEventInfo {
	other := *m
	other.pt = &(*(m.pt))
	return &other
}
