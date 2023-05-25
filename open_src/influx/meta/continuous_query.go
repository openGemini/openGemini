package meta

import (
	"github.com/gogo/protobuf/proto"
	proto2 "github.com/openGemini/openGemini/open_src/influx/meta/proto"
)

// ContinuousQueryInfo represents metadata about a continuous query.
type ContinuousQueryInfo struct {
	// Name of the continuous query to be created.
	Name string

	// String corresponding to continuous query statement
	Query string

	// Mark whether this cq has been deleted
	MarkDeleted bool
}

// NewContinuousQueryInfo returns a new instance of ContinuousQueryInfo
// with default values.
func NewContinuousQueryInfo() *ContinuousQueryInfo {
	return &ContinuousQueryInfo{
		Name:        "",
		Query:       "",
		MarkDeleted: false,
	}
}

// Apply applies a specification to the continuous query info.
func (cqi *ContinuousQueryInfo) Apply(spec *ContinuousQuerySpec) *ContinuousQueryInfo {
	cq := &ContinuousQueryInfo{
		Name:        cqi.Name,
		Query:       cqi.Query,
		MarkDeleted: cqi.MarkDeleted,
	}

	if spec.Name != "" {
		cq.Name = spec.Name
	}

	if spec.Query != "" {
		cq.Query = spec.Query
	}

	return cq
}

// Marshal serializes to a protobuf representation.
func (cqi *ContinuousQueryInfo) Marshal() *proto2.ContinuousQueryInfo {
	pb := &proto2.ContinuousQueryInfo{
		Name:        proto.String(cqi.Name),
		Query:       proto.String(cqi.Query),
		MarkDeleted: proto.Bool(cqi.MarkDeleted),
	}

	return pb
}

// unmarshal deserializes from a protobuf representation.
func (cqi *ContinuousQueryInfo) unmarshal(pb *proto2.ContinuousQueryInfo) {
	cqi.Name = pb.GetName()
	cqi.Query = pb.GetQuery()
	cqi.MarkDeleted = pb.GetMarkDeleted()
}

// Clone returns a deep copy of cqi.
func (cqi ContinuousQueryInfo) Clone() *ContinuousQueryInfo {
	other := cqi
	return &other
}

func (cqi *ContinuousQueryInfo) CheckSpecValid() error {
	// Benevor TODO : what need to check

	return nil
}

// Determine if the actions of two cqs are exactly the same.
func (cqi *ContinuousQueryInfo) EqualsAnotherCq(other *ContinuousQueryInfo) bool {
	return cqi.Query == other.Query
}

type ContinuousQuerySpec struct {
	// Name of the continuous query to be created.
	Name string

	// String corresponding to continuous query statement
	Query string
}

// NewContinuousQueryInfoBySpec creates a new continuous query info from the specification.
func (s *ContinuousQuerySpec) NewContinuousQueryInfoBySpec() *ContinuousQueryInfo {
	return NewContinuousQueryInfo().Apply(s)
}
