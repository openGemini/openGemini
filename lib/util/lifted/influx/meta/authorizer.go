package meta

/*
Copyright (c) 2013-2016 Errplane Inc.
This code is originally from: https://github.com/influxdata/influxdb/blob/1.7/services/meta/query_authorizer.go

2022.01.23 add internal user rwuser authorize
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
*/

import (
	"fmt"

	originql "github.com/influxdata/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
)

// rwusers can execute show users
// rwusers can create none sys users with partition privilege, whether provided with all privileges or not
// rwusers can not drop sys users and rwuser
// rwusers can set password for none sys users
// rwusers can grant [READ,WRITE,ALL] ON <db> TO <user> for none sys users
// rwusers can revoke [READ,WRITE,ALL] ON <db> TO <user> for none sys users
// rwusers can show grants for none sys users
// rwusers can not drop _internal
func (u *UserInfo) AuthorizeQueryForRwUser(database string, query *influxql.Query) error {

	// Check each statement in the query.
	for _, stmt := range query.Statements {
		switch stmtType := stmt.(type) {
		case *influxql.ShowUsersStatement:
			continue
		case *influxql.CreateUserStatement:
			if stmtType.Admin == true {
				stmtType.Admin = false
			}
			continue
		case *influxql.DropUserStatement:
			if stmtType.Name != "rwuser" {
				continue
			}
		case *influxql.SetPasswordUserStatement:
			if u.Name != "rwuser" && stmtType.Name == "rwuser" {
				return &ErrAuthorize{
					Query:    query,
					User:     u.Name,
					Database: database,
					Message:  fmt.Sprintf("statement '%s', requires rwuser privilege", stmt),
				}
			}
			continue
		case *influxql.GrantStatement:
			continue
		case *influxql.RevokeStatement:
			continue
		case *influxql.ShowGrantsForUserStatement:
			continue
		case *influxql.DropDatabaseStatement:
			if stmtType.Name == "_internal" {
				return &ErrAuthorize{
					Query:    query,
					User:     u.Name,
					Database: database,
					Message:  fmt.Sprintf("statement '%s', requires admin privilege", stmt),
				}
			}
		default:
		}
		// Get the privileges required to execute the statement.
		privs, err := stmt.RequiredPrivileges()
		if err != nil {
			return err
		}

		// Make sure the GetUser has the privileges required to execute
		// each statement.
		for _, p := range privs {
			if !p.Rwuser {
				//  rwuser privilege cannot be run stmt.
				return &ErrAuthorize{
					Query:    query,
					User:     u.Name,
					Database: database,
					Message:  fmt.Sprintf("statement '%s', requires admin privilege", stmt),
				}
			}
		}
	}
	return nil
}

func (u *UserInfo) AuthorizeQuery(database string, query *influxql.Query) error {

	// Admin privilege allows the GetUser to execute all statements.
	if u.Admin {
		return nil
	}
	if u.Rwuser {
		return u.AuthorizeQueryForRwUser(database, query)
	}
	// Check each statement in the query.
	for _, stmt := range query.Statements {
		// Get the privileges required to execute the statement.
		privs, err := stmt.RequiredPrivileges()
		if err != nil {
			return err
		}

		// Make sure the GetUser has the privileges required to execute
		// each statement.
		for _, p := range privs {
			if p.Admin {
				// Admin privilege already checked so statement requiring admin
				// privilege cannot be run.
				return &ErrAuthorize{
					Query:    query,
					User:     u.Name,
					Database: database,
					Message:  fmt.Sprintf("statement '%s', requires admin privilege", stmt),
				}
			}

			// Use the db name specified by the statement or the db
			// name passed by the caller if one wasn't specified by
			// the statement.
			db := p.Name
			if db == "" {
				db = database
			}
			if !u.AuthorizeDatabase(originql.Privilege(p.Privilege), db) {
				return &ErrAuthorize{
					Query:    query,
					User:     u.Name,
					Database: database,
					Message:  fmt.Sprintf("statement '%s', requires %s on %s", stmt, p.Privilege.String(), db),
				}
			}
		}
	}
	return nil
}

// ErrAuthorize represents an authorization error.
type ErrAuthorize struct {
	Query    *influxql.Query
	User     string
	Database string
	Message  string
}

// Error returns the text of the error.
func (e ErrAuthorize) Error() string {
	if e.User == "" {
		return fmt.Sprint(e.Message)
	}
	return fmt.Sprintf("%s not authorized to execute %s", e.User, e.Message)
}
