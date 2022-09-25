package meta

/*
Copyright (c) 2013-2016 Errplane Inc.
This code is originally from: https://github.com/influxdata/influxdb/blob/1.7/services/meta/errors.go

2022.01.23 Add ErrConflictWithIo,ErrDBPTClose, etc.
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
*/

import (
	"errors"
	"fmt"

	"github.com/openGemini/openGemini/lib/errno"
)

var (
	// ErrStoreOpen is returned when opening an already open store.
	ErrStoreOpen = errors.New("store already open")

	// ErrStoreClosed is returned when closing an already closed store.
	ErrStoreClosed = errors.New("raft store already closed")

	ErrClientInited = errors.New("aready inited")

	ErrClientClosed = errors.New("client already closed")

	ErrConflictWithIo = errors.New("conflict with io")

	ErrDBPTClose = errors.New("DBPT is being closing")
)

var (
	// ErrNodeExists is returned when creating an already existing node.
	ErrNodeExists = errors.New("node already exists")

	// ErrNodeNotFound is returned when mutating a node that doesn't exist.
	ErrNodeNotFound = errors.New("node not found")

	// ErrNodesRequired is returned when at least one node is required for an operation.
	// This occurs when creating a shard group.
	ErrNodesRequired = errors.New("at least one node required")

	// ErrNodeIDRequired is returned when using a zero node id.
	ErrNodeIDRequired = errors.New("node id must be greater than 0")

	// ErrDatabaseExists is returned when creating an already existing database.
	ErrDatabaseExists = errors.New("database already exists")

	// ErrDatabaseNotExists is returned when operating on a not existing database.
	ErrDatabaseNotExists = errors.New("database does not exist")

	// ErrDatabaseNameRequired is returned when creating a database without a name.
	ErrDatabaseNameRequired = errors.New("database name required")

	// ErrInvalidName is returned when attempting to create a database or retention policy with an invalid name
	ErrInvalidName = errors.New("invalid name")

	// ErrNodeUnableToDropFinalNode is returned if the node being dropped is the last
	// node in the cluster
	ErrNodeUnableToDropFinalNode = errors.New("unable to drop the final node in a cluster")

	ErrInvalidPtView     = errors.New("invalid ptView number")
	ErrDataViewBootStrap = errors.New("cluster is bootstrapping for initial data view")
	ErrDuplicateShardKey = errors.New("duplicate shard key")
	ErrInvalidShardKey   = errors.New("invalid shard key")
)

var (
	// ErrRetentionPolicyExists is returned when creating an already existing policy.
	ErrRetentionPolicyExists = errors.New("retention policy already exists")

	ErrRetentionPolicyIsBeingDelete = errors.New("retention policy is being delete")

	// ErrRetentionPolicyDefault is returned when attempting a prohibited operation
	// on a default retention policy.
	ErrRetentionPolicyDefault = errors.New("retention policy is default")

	// ErrRetentionPolicyRequired is returned when a retention policy is required
	// by an operation, but a nil policy was passed.
	ErrRetentionPolicyRequired = errors.New("retention policy required")

	// ErrRetentionPolicyNameRequired is returned when creating a policy without a name.
	ErrRetentionPolicyNameRequired = errors.New("retention policy name required")

	ErrMeasurementNameRequired = errors.New("measurement name required")

	ErrShardKeyRequired = errors.New("shard key required")

	ErrMeasurementExists = errors.New("measurement already exists")

	ErrMeasurementIsBeingDelete = errors.New("measurement is being delete")

	// ErrRetentionPolicyNameExists is returned when renaming a policy to
	// the same name as another existing policy.
	ErrRetentionPolicyNameExists = errors.New("retention policy name already exists")

	// ErrRetentionPolicyDurationTooLow is returned when updating a retention
	// policy that has a duration lower than the allowed minimum.
	ErrRetentionPolicyDurationTooLow = fmt.Errorf("retention policy duration must be at least %s", MinRetentionPolicyDuration)

	// ErrRetentionPolicyConflict is returned when creating a retention policy conflicts
	// with an existing policy.
	ErrRetentionPolicyConflict = errors.New("retention policy conflicts with an existing policy")

	// ErrIncompatibleDurations is returned when creating or updating a
	// retention policy that has a duration lower than the current shard
	// duration.
	ErrIncompatibleDurations = errors.New("retention policy duration must be greater than the shard duration")

	// ErrReplicationFactorTooLow is returned when the replication factor is not in an
	// acceptable range.
	ErrReplicationFactorTooLow = errors.New("replication factor must be greater than 0")

	ErrIncompatibleHotDurations = errors.New("retention policy hot duration must be greater than the shard duration and lower than the duration")
	// ErrIncompatibleWarmDurations is returned when creating or updating a
	// retention policy that has a warm duration lower than the current shard duration
	// or greater than the current duration.
	ErrIncompatibleWarmDurations = errors.New("retention policy warm duration must be greater than the shard duration and lower than the duration")

	ErrIncompatibleIndexGroupDuration = errors.New("retention policy index group duration must be greater than the shard duration and lower than the duration")

	// ErrIncompatibleShardGroupDurations is returned when creating or updating a
	// retention policy that has a warm duration not equal n * shard duration
	ErrIncompatibleShardGroupDurations = errors.New("retention policy hot duration/warm duration/index duration should be equal n * shard duration and n>=1")
)

var (
	// ErrShardGroupExists is returned when creating an already existing shard group.
	ErrShardGroupExists = errors.New("shard group already exists")

	// ErrShardGroupNotFound is returned when mutating a shard group that doesn't exist.
	ErrShardGroupNotFound = errors.New("shard group not found")

	// ErrShardNotReplicated is returned if the node requested to be dropped has
	// the last copy of a shard present and the force keyword was not used
	ErrShardNotReplicated = errors.New("shard not replicated")

	ErrIndexGroupNotFound = errors.New("index group not found")

	ErrMeasurementNotFound = errno.NewError(errno.ErrMeasurementNotFound)
)

var (
	// ErrContinuousQueryExists is returned when creating an already existing continuous query.
	ErrContinuousQueryExists = errors.New("continuous query already exists")

	// ErrContinuousQueryNotFound is returned when removing a continuous query that doesn't exist.
	ErrContinuousQueryNotFound = errors.New("continuous query not found")

	// ErrSameContinuosQueryName is returned when creating an already existing continuous query name.
	ErrSameContinuosQueryName = errors.New("continuous query name already exists")
)

var (
	// ErrSubscriptionExists is returned when creating an already existing subscription.
	ErrSubscriptionExists = errors.New("subscription already exists")

	// ErrSubscriptionNotFound is returned when removing a subscription that doesn't exist.
	ErrSubscriptionNotFound = errors.New("subscription not found")
)

// ErrInvalidSubscriptionURL is returned when the subscription's destination URL is invalid.
func ErrInvalidSubscriptionURL(url string) error {
	return fmt.Errorf("invalid subscription URL: %s", url)
}

var (
	// ErrUserExists is returned when creating an already existing GetUser.
	ErrUserExists = errors.New("user already exists")

	// ErrUserNotFound is returned when mutating a GetUser that doesn't exist.
	ErrUserNotFound = errors.New("user not found")

	// ErrUserLocked is returned when a user that is locked.
	ErrUserLocked = errors.New("user is locked")

	// ErrUserForbidden is returned when create the second admin user.
	ErrUserForbidden = errors.New("admin user is existed, forbidden to create new admin user")

	// ErrGrantAdmin is to forbidden grant or revoke privileges
	ErrGrantOrRevokeAdmin = errors.New("forbidden to grant or revoke privileges, because only one admin is allowed for the database")

	// ErrUserDropSelf is returned when delete the only admin
	ErrUserDropSelf = errors.New("forbidden to delete admin user")

	// ErrPwdUsed is returned when use an old password
	ErrPwdUsed = errors.New("the password is the same as the old one, please enter a new password")

	// ErrHashedLength is returned when hashed length err.
	ErrHashedLength         = errors.New("hashedSecret too short to be a hashed password")
	ErrMismatchedHashAndPwd = errors.New("hashedPassword is not the hash of the given password")
	ErrUnsupportedVer       = errors.New("do not support the hash version")

	// ErrUsernameRequired is returned when creating a GetUser without a username.
	ErrUsernameRequired = errors.New("username required")

	// ErrAuthenticate is returned when authentication fails.
	ErrAuthenticate = errors.New("authentication failed")

	ErrFieldTypeConflict = errors.New("field type conflict")

	ErrUnsupportCommand = errors.New("unsupported command")

	ErrCommandTimeout = errors.New("execute command timeout")

	ErrStorageNodeNotReady = errors.New("storage node has not open")
)

// ErrRetentionPolicyNotFound indicates that the named retention policy could
// not be found in the database.
func ErrRetentionPolicyNotFound(name string) error {
	return fmt.Errorf("retention policy not found: %s", name)
}

func ErrShardGroupAlreadyReSharding(id uint64) error {
	return fmt.Errorf("shard group already reSharding: %d", id)
}

func ErrShardingTypeNotEqual(rp, existType, inputType string) error {
	return fmt.Errorf("sharding type are not equal in %s exist type %s inputType %s", rp, existType, inputType)
}

func ErrInvalidTierType(tier, minTier, maxTier uint64) error {
	return fmt.Errorf("invalid tier type %d, tier type range should between %d and %d", tier, minTier, maxTier)
}
