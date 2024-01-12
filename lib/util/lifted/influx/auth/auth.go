package auth

import (
	"fmt"

	originql "github.com/influxdata/influxql"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
)

// QueryAuthorizer determines whether a user is authorized to execute a given query.
type QueryAuthorizer struct {
	Client *metaclient.Client
}

// NewQueryAuthorizer returns a new instance of QueryAuthorizer.
func NewQueryAuthorizer(c *metaclient.Client) *QueryAuthorizer {
	return &QueryAuthorizer{
		Client: c,
	}
}

// AuthorizeQuery authorizes u to execute q on database.
// Database can be "" for queries that do not require a database.
// If no user is provided it will return an error unless the query's first statement is to create
// a root user.
func (a *QueryAuthorizer) AuthorizeQuery(u meta.User, query *influxql.Query, database string) error {
	// Special case if no users exist.
	if n := a.Client.UserCount(); n == 0 {
		// Ensure there is at least one statement.
		if len(query.Statements) > 0 {
			// First statement in the query must create a user with admin privilege.
			cu, ok := query.Statements[0].(*influxql.CreateUserStatement)
			if ok && cu.Admin {
				return nil
			}
		}
		return &meta.ErrAuthorize{
			Query:    query,
			Database: database,
			Message:  "create admin user first or disable authentication",
		}
	}

	if u == nil {
		return &meta.ErrAuthorize{
			Query:    query,
			Database: database,
			Message:  "no user provided",
		}
	}

	return u.AuthorizeQuery(database, query)
}

func (a *QueryAuthorizer) AuthorizeDatabase(u meta.User, priv originql.Privilege, database string) error {
	if u == nil {
		return &meta.ErrAuthorize{
			Database: database,
			Message:  "no user provided",
		}
	}

	if !u.AuthorizeDatabase(priv, database) {
		return &meta.ErrAuthorize{
			Database: database,
			Message:  fmt.Sprintf("user %q, requires %s for database %q", u.ID(), priv.String(), database),
		}
	}

	return nil
}

type WriteAuthorizer struct {
	Client *metaclient.Client
}

// NewWriteAuthorizer returns a new instance of WriteAuthorizer.
func NewWriteAuthorizer(c *metaclient.Client) *WriteAuthorizer {
	return &WriteAuthorizer{Client: c}
}

// AuthorizeWrite returns nil if the user has permission to write to the database.
func (a WriteAuthorizer) AuthorizeWrite(username, database string) error {
	u, err := a.Client.User(username)
	if err != nil || u == nil || !u.AuthorizeDatabase(originql.WritePrivilege, database) {
		return &meta.ErrAuthorize{
			Database: database,
			Message:  fmt.Sprintf("%s not authorized to write to %s", username, database),
		}
	}
	return nil
}
