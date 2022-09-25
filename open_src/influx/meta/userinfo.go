package meta

/*
Copyright (c) 2013-2016 Errplane Inc.
This code is originally from: https://github.com/influxdata/influxdb/blob/1.7/services/meta/data.go
*/

import (
	"github.com/gogo/protobuf/proto"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/query"
	originql "github.com/influxdata/influxql"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	proto2 "github.com/openGemini/openGemini/open_src/influx/meta/proto"
)

var _ query.FineAuthorizer = (*UserInfo)(nil)

// UserInfo represents metadata about a GetUser in the system.
type UserInfo struct {
	// User's name.
	Name string

	// Hashed password.
	Hash string

	// Whether the GetUser is an admin, i.e. allowed to do everything.
	Admin bool

	//whether the GetUser is rwuser, rwuser just do not operation GetUser request. all DB operation can do
	Rwuser bool

	// Map of database name to granted privilege.
	Privileges map[string]originql.Privilege
}

type User interface {
	// AuthorizeDatabase indicates whether the given Privilege is authorized on the database with the given name.
	AuthorizeDatabase(p originql.Privilege, name string) bool

	// AuthorizeQuery returns an error if the query cannot be executed
	AuthorizeQuery(database string, query *influxql.Query) error

	query.FineAuthorizer
	ID() string
	AuthorizeUnrestricted() bool
}

func (u *UserInfo) ID() string {
	return u.Name
}

// AuthorizeDatabase returns true if the GetUser is authorized for the given privilege on the given database.
func (u *UserInfo) AuthorizeDatabase(privilege originql.Privilege, database string) bool {
	if u.Admin || u.Rwuser || privilege == originql.NoPrivileges {
		return true
	}
	p, ok := u.Privileges[database]
	return ok && (p == privilege || p == originql.AllPrivileges)
}

func (u *UserInfo) IsOpen() bool {
	return true
}

// AuthorizeSeriesRead is used to limit access per-series (enterprise only)
func (u *UserInfo) AuthorizeSeriesRead(database string, measurement []byte, tags models.Tags) bool {
	return true
}

// AuthorizeSeriesWrite is used to limit access per-series (enterprise only)
func (u *UserInfo) AuthorizeSeriesWrite(database string, measurement []byte, tags models.Tags) bool {
	return true
}

// AuthorizeUnrestricted allows admins to shortcut access checks.
func (u *UserInfo) AuthorizeUnrestricted() bool {
	return u.Admin
}

// clone returns a deep copy of si.
func (u UserInfo) clone() UserInfo {
	other := u

	if u.Privileges != nil {
		other.Privileges = make(map[string]originql.Privilege)
		for k, v := range u.Privileges {
			other.Privileges[k] = v
		}
	}

	return other
}

// marshal serializes to a protobuf representation.
func (u UserInfo) marshal() *proto2.UserInfo {
	pb := &proto2.UserInfo{
		Name:   proto.String(u.Name),
		Hash:   proto.String(u.Hash),
		Admin:  proto.Bool(u.Admin),
		RwUser: proto.Bool(u.Rwuser),
	}

	for database, privilege := range u.Privileges {
		pb.Privileges = append(pb.Privileges, &proto2.UserPrivilege{
			Database:  proto.String(database),
			Privilege: proto.Int32(int32(privilege)),
		})
	}

	return pb
}

// unmarshal deserializes from a protobuf representation.
func (u *UserInfo) unmarshal(pb *proto2.UserInfo) {
	u.Name = pb.GetName()
	u.Hash = pb.GetHash()
	u.Admin = pb.GetAdmin()
	u.Rwuser = pb.GetRwUser()

	if len(pb.Privileges) > 0 {
		u.Privileges = make(map[string]originql.Privilege)
	}

	for _, p := range pb.GetPrivileges() {
		u.Privileges[p.GetDatabase()] = originql.Privilege(p.GetPrivilege())
	}
}
