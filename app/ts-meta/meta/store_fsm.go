/*
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.

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

package meta

import (
	"fmt"
	"io"
	"io/ioutil"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/hashicorp/raft"
	originql "github.com/influxdata/influxql"
	meta2 "github.com/openGemini/openGemini/open_src/influx/meta"
	proto2 "github.com/openGemini/openGemini/open_src/influx/meta/proto"
	"go.uber.org/zap"
)

// storeFSM represents the finite state machine used by Store to interact with Raft.
type storeFSM Store

func (fsm *storeFSM) ApplyBatch(logs []*raft.Log) []interface{} {
	s := (*Store)(fsm)
	var dataChanged bool
	s.mu.Lock()
	defer s.mu.Unlock()
	ret := make([]interface{}, len(logs))
	for i := range logs {
		switch logs[i].Type {
		case raft.LogCommand:
		default:
			continue
		}
		var cmd proto2.Command
		if err := proto.Unmarshal(logs[i].Data, &cmd); err != nil {
			panic(fmt.Errorf("cannot marshal command: %s err %+v", string(logs[i].Data), err))
		}
		fsm.Logger.Info(fmt.Sprintf("BatchApply log term %d index %d type %d", logs[i].Term, logs[i].Index, int32(cmd.GetType())))
		ret[i] = fsm.executeCmd(cmd)
		if ret[i] == nil {
			dataChanged = true
		}
	}

	fsm.data.Term = logs[len(logs)-1].Term
	fsm.data.Index = logs[len(logs)-1].Index

	if dataChanged {
		close(s.dataChanged)
		s.dataChanged = make(chan struct{})
	}

	return ret
}

func (fsm *storeFSM) Apply(l *raft.Log) interface{} {
	var cmd proto2.Command
	if err := proto.Unmarshal(l.Data, &cmd); err != nil {
		panic(fmt.Errorf("cannot marshal command: %x", l.Data))
	}

	// Lock the store.
	s := (*Store)(fsm)
	s.mu.Lock()
	defer s.mu.Unlock()
	fsm.Logger.Info(fmt.Sprintf("Apply log term %d index %d type %d", l.Term, l.Index, int32(cmd.GetType())))
	err := fsm.executeCmd(cmd)

	// Copy term and index to new metadata.
	fsm.data.Term = l.Term
	fsm.data.Index = l.Index

	if err != nil {
		return err
	}
	// signal that the data changed
	close(s.dataChanged)
	s.dataChanged = make(chan struct{})

	return err
}

func (fsm *storeFSM) executeCmd(cmd proto2.Command) interface{} {
	switch cmd.GetType() {
	case proto2.Command_CreateDatabaseCommand:
		return fsm.applyCreateDatabaseCommand(&cmd)
	case proto2.Command_DropDatabaseCommand:
		return fsm.applyDropDatabaseCommand(&cmd)
	case proto2.Command_CreateRetentionPolicyCommand:
		return fsm.applyCreateRetentionPolicyCommand(&cmd)
	case proto2.Command_DropRetentionPolicyCommand:
		return fsm.applyDropRetentionPolicyCommand(&cmd)
	case proto2.Command_SetDefaultRetentionPolicyCommand:
		return fsm.applySetDefaultRetentionPolicyCommand(&cmd)
	case proto2.Command_UpdateRetentionPolicyCommand:
		return fsm.applyUpdateRetentionPolicyCommand(&cmd)
	case proto2.Command_CreateShardGroupCommand:
		return fsm.applyCreateShardGroupCommand(&cmd)
	case proto2.Command_DeleteShardGroupCommand:
		return fsm.applyDeleteShardGroupCommand(&cmd)
	case proto2.Command_CreateSubscriptionCommand:
		return fsm.applyCreateSubscriptionCommand(&cmd)
	case proto2.Command_DropSubscriptionCommand:
		return fsm.applyDropSubscriptionCommand(&cmd)
	case proto2.Command_CreateUserCommand:
		return fsm.applyCreateUserCommand(&cmd)
	case proto2.Command_DropUserCommand:
		return fsm.applyDropUserCommand(&cmd)
	case proto2.Command_UpdateUserCommand:
		return fsm.applyUpdateUserCommand(&cmd)
	case proto2.Command_SetPrivilegeCommand:
		return fsm.applySetPrivilegeCommand(&cmd)
	case proto2.Command_SetAdminPrivilegeCommand:
		return fsm.applySetAdminPrivilegeCommand(&cmd)
	case proto2.Command_SetDataCommand:
		return fsm.applySetDataCommand(&cmd)
	case proto2.Command_CreateMetaNodeCommand:
		return fsm.applyCreateMetaNodeCommand(&cmd)
	case proto2.Command_DeleteMetaNodeCommand:
		return fsm.applyDeleteMetaNodeCommand(&cmd, (*Store)(fsm))
	case proto2.Command_SetMetaNodeCommand:
		return fsm.applySetMetaNodeCommand(&cmd)
	case proto2.Command_CreateDataNodeCommand:
		return fsm.applyCreateDataNodeCommand(&cmd)
	case proto2.Command_DeleteDataNodeCommand:
		return fsm.applyDeleteDataNodeCommand(&cmd)
	case proto2.Command_MarkDatabaseDeleteCommand:
		return fsm.applyMarkDatabaseDeleteCommand(&cmd)
	case proto2.Command_UpdateShardOwnerCommand:
		return fsm.applyUpdateShardOwnerCommand(&cmd)
	case proto2.Command_MarkRetentionPolicyDeleteCommand:
		return fsm.applyMarkRetentionPolicyDeleteCommand(&cmd)
	case proto2.Command_CreateMeasurementCommand:
		return fsm.applyCreateMeasurementCommand(&cmd)
	case proto2.Command_ReShardingCommand:
		return fsm.applyReShardingCommand(&cmd)
	case proto2.Command_UpdateSchemaCommand:
		return fsm.applyUpdateSchemaCommand(&cmd)
	case proto2.Command_AlterShardKeyCmd:
		return fsm.applyAlterShardKeyCommand(&cmd)
	case proto2.Command_PruneGroupsCommand:
		return fsm.applyPruneGroupsCommand(&cmd)
	case proto2.Command_MarkMeasurementDeleteCommand:
		return fsm.applyMarkMeasurementDeleteCommand(&cmd)
	case proto2.Command_DropMeasurementCommand:
		return fsm.applyDropMeasurementCommand(&cmd)
	case proto2.Command_DeleteIndexGroupCommand:
		return fsm.applyDeleteIndexGroupCommand(&cmd)
	case proto2.Command_UpdateShardInfoTierCommand:
		return fsm.applyUpdateShardInfoTierCommand(&cmd)
	case proto2.Command_UpdateNodeStatusCommand:
		return fsm.applyUpdateNodeStatusCommand(&cmd)
	case proto2.Command_CreateEventCommand:
		return fsm.applyCreateEventCommand(&cmd)
	case proto2.Command_UpdateEventCommand:
		return fsm.applyUpdateEventCommand(&cmd)
	case proto2.Command_UpdatePtInfoCommand:
		return fsm.applyUpdatePtInfoCommand(&cmd)
	case proto2.Command_RemoveEventCommand:
		return fsm.applyRemoveEvent(&cmd)
	default:
		panic(fmt.Errorf("cannot apply command: %x", cmd.GetType()))
	}
}

func (fsm *storeFSM) applyReShardingCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_ReShardingCommand_Command)
	v := ext.(*proto2.ReShardingCommand)
	info := &meta2.ReShardingInfo{
		Database:     v.GetDatabase(),
		Rp:           v.GetRpName(),
		ShardGroupID: v.GetShardGroupID(),
		SplitTime:    v.GetSplitTime(),
		Bounds:       v.GetShardBounds(),
	}
	return fsm.data.ReSharding(info)
}

func (fsm *storeFSM) applyUpdateSchemaCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_UpdateSchemaCommand_Command)
	v := ext.(*proto2.UpdateSchemaCommand)
	return fsm.data.UpdateSchema(v.GetDatabase(), v.GetRpName(), v.GetMeasurement(), v.GetFieldToCreate())
}

func (fsm *storeFSM) applyAlterShardKeyCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_AlterShardKeyCmd_Command)
	v := ext.(*proto2.AlterShardKeyCmd)
	return fsm.data.AlterShardKey(v.GetDBName(), v.GetRpName(), v.GetName(), v.GetSki())
}

func (fsm *storeFSM) applyMarkMeasurementDeleteCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_MarkMeasurementDeleteCommand_Command)
	v := ext.(*proto2.MarkMeasurementDeleteCommand)
	return fsm.data.MarkMeasurementDelete(v.GetDatabase(), v.GetPolicy(), v.GetMeasurement())
}

func (fsm *storeFSM) applyDropMeasurementCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_DropMeasurementCommand_Command)
	v := ext.(*proto2.DropMeasurementCommand)
	return fsm.data.DropMeasurement(v.GetDatabase(), v.GetPolicy(), v.GetMeasurement())
}

func (fsm *storeFSM) applyCreateDatabaseCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_CreateDatabaseCommand_Command)
	v := ext.(*proto2.CreateDatabaseCommand)

	s := (*Store)(fsm)
	var rp *meta2.RetentionPolicyInfo
	rpi := v.GetRetentionPolicy()
	if rpi != nil {
		rp = &meta2.RetentionPolicyInfo{
			Name:               rpi.GetName(),
			ReplicaN:           int(rpi.GetReplicaN()),
			Duration:           time.Duration(rpi.GetDuration()),
			ShardGroupDuration: time.Duration(rpi.GetShardGroupDuration()),
			HotDuration:        time.Duration(rpi.GetHotDuration()),
			WarmDuration:       time.Duration(0), // FIXME DO NOT SUPPORT WARM DURATION
			IndexGroupDuration: time.Duration(rpi.GetIndexGroupDuration())}
	} else if s.config.RetentionAutoCreate {
		replicaN := len(fsm.data.DataNodes)
		if replicaN > maxAutoCreatedRetentionPolicyReplicaN {
			replicaN = maxAutoCreatedRetentionPolicyReplicaN
		} else if replicaN < 1 {
			replicaN = 1
		}

		// Create a retention policy.
		rp = meta2.NewRetentionPolicyInfo(autoCreateRetentionPolicyName)
		rp.ReplicaN = replicaN
		rp.Duration = autoCreateRetentionPolicyPeriod
	}
	err := fsm.data.CreateDatabase(v.GetName(), rp, v.GetSki())
	fsm.Logger.Info("apply create database", zap.Error(err))
	return err
}

func (fsm *storeFSM) applyMarkDatabaseDeleteCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_MarkDatabaseDeleteCommand_Command)
	v := ext.(*proto2.MarkDatabaseDeleteCommand)
	return fsm.data.MarkDatabaseDelete(v.GetName())
}

func (fsm *storeFSM) applyDropDatabaseCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_DropDatabaseCommand_Command)
	v := ext.(*proto2.DropDatabaseCommand)

	dbi := fsm.data.Database(v.GetName())
	if dbi == nil {
		return nil
	}
	fsm.data.DropDatabase(v.GetName())

	return nil
}

func (fsm *storeFSM) applyCreateMeasurementCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_CreateMeasurementCommand_Command)
	v := ext.(*proto2.CreateMeasurementCommand)
	return fsm.data.CreateMeasurement(v.GetDBName(), v.GetRpName(), v.GetName(), v.GetSki(), v.GetIR())
}

func (fsm *storeFSM) applyCreateRetentionPolicyCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_CreateRetentionPolicyCommand_Command)
	v := ext.(*proto2.CreateRetentionPolicyCommand)
	pb := v.GetRetentionPolicy()

	rpi := &meta2.RetentionPolicyInfo{
		Name:               pb.GetName(),
		ReplicaN:           int(pb.GetReplicaN()),
		Duration:           time.Duration(pb.GetDuration()),
		ShardGroupDuration: time.Duration(pb.GetShardGroupDuration()),
		HotDuration:        time.Duration(pb.GetHotDuration()),
		WarmDuration:       time.Duration(pb.GetWarmDuration()),
		IndexGroupDuration: time.Duration(pb.GetIndexGroupDuration()),
	}

	return fsm.data.CreateRetentionPolicy(v.GetDatabase(), rpi, v.GetDefaultRP())
}

func (fsm *storeFSM) applyDropRetentionPolicyCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_DropRetentionPolicyCommand_Command)
	v := ext.(*proto2.DropRetentionPolicyCommand)
	return fsm.data.DropRetentionPolicy(v.GetDatabase(), v.GetName())
}

func (fsm *storeFSM) applyMarkRetentionPolicyDeleteCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_MarkRetentionPolicyDeleteCommand_Command)
	v := ext.(*proto2.MarkRetentionPolicyDeleteCommand)
	return fsm.data.MarkRetentionPolicyDelete(v.GetDatabase(), v.GetName())
}

func (fsm *storeFSM) applySetDefaultRetentionPolicyCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_SetDefaultRetentionPolicyCommand_Command)
	v := ext.(*proto2.SetDefaultRetentionPolicyCommand)
	return fsm.data.SetDefaultRetentionPolicy(v.GetDatabase(), v.GetName())
}

func (fsm *storeFSM) applyUpdateRetentionPolicyCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_UpdateRetentionPolicyCommand_Command)
	v := ext.(*proto2.UpdateRetentionPolicyCommand)

	// Create update object.
	rpu := meta2.RetentionPolicyUpdate{Name: v.NewName}
	rpu.Duration = meta2.GetDuration(v.Duration)
	rpu.HotDuration = meta2.GetDuration(v.HotDuration)
	rpu.WarmDuration = nil // FIXME DO NOT SUPPORT WARM DURATION
	rpu.IndexGroupDuration = meta2.GetDuration(v.IndexGroupDuration)
	rpu.ShardGroupDuration = meta2.GetDuration(v.ShardGroupDuration)
	if v.ReplicaN != nil {
		value := int(v.GetReplicaN())
		rpu.ReplicaN = &value
	}
	return fsm.data.UpdateRetentionPolicy(v.GetDatabase(), v.GetName(), &rpu, v.GetMakeDefault())
}

func (fsm *storeFSM) applyCreateShardGroupCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_CreateShardGroupCommand_Command)
	v := ext.(*proto2.CreateShardGroupCommand)
	return fsm.data.CreateShardGroup(v.GetDatabase(), v.GetPolicy(), time.Unix(0, v.GetTimestamp()), v.GetShardTier())
}

func (fsm *storeFSM) applyDeleteShardGroupCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_DeleteShardGroupCommand_Command)
	v := ext.(*proto2.DeleteShardGroupCommand)
	return fsm.data.DeleteShardGroup(v.GetDatabase(), v.GetPolicy(), v.GetShardGroupID())
}

func (fsm *storeFSM) applyDeleteIndexGroupCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_DeleteIndexGroupCommand_Command)
	v := ext.(*proto2.DeleteIndexGroupCommand)
	return fsm.data.DeleteIndexGroup(v.GetDatabase(), v.GetPolicy(), v.GetIndexGroupID())
}

func (fsm *storeFSM) applyPruneGroupsCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_PruneGroupsCommand_Command)
	v := ext.(*proto2.PruneGroupsCommand)
	return fsm.data.PruneGroups(v.GetShardGroup())
}

func (fsm *storeFSM) applyCreateSubscriptionCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_CreateSubscriptionCommand_Command)
	v := ext.(*proto2.CreateSubscriptionCommand)
	return fsm.data.CreateSubscription(v.GetDatabase(), v.GetRetentionPolicy(), v.GetName(), v.GetMode(), v.GetDestinations())
}

func (fsm *storeFSM) applyDropSubscriptionCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_DropSubscriptionCommand_Command)
	v := ext.(*proto2.DropSubscriptionCommand)
	return fsm.data.DropSubscription(v.GetDatabase(), v.GetRetentionPolicy(), v.GetName())
}

func (fsm *storeFSM) applyCreateUserCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_CreateUserCommand_Command)
	v := ext.(*proto2.CreateUserCommand)
	err := fsm.data.CreateUser(v.GetName(), v.GetHash(), v.GetAdmin(), v.GetRwUser())
	fsm.Logger.Info("apply create user command", zap.String("userID", v.GetName()), zap.Error(err))
	return err
}

func (fsm *storeFSM) applyDropUserCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_DropUserCommand_Command)
	v := ext.(*proto2.DropUserCommand)
	err := fsm.data.DropUser(v.GetName())
	fsm.Logger.Info("apply drop user command", zap.String("userID", v.GetName()), zap.Error(err))
	return err
}

func (fsm *storeFSM) applyUpdateUserCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_UpdateUserCommand_Command)
	v := ext.(*proto2.UpdateUserCommand)
	return fsm.data.UpdateUser(v.GetName(), v.GetHash())
}

func (fsm *storeFSM) applySetPrivilegeCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_SetPrivilegeCommand_Command)
	v := ext.(*proto2.SetPrivilegeCommand)
	err := fsm.data.SetPrivilege(v.GetUsername(), v.GetDatabase(), originql.Privilege(v.GetPrivilege()))
	fsm.Logger.Info("apply set privilege command", zap.String("userID", v.GetUsername()),
		zap.String("db", v.GetDatabase()), zap.Int32("privilege", v.GetPrivilege()), zap.Error(err))
	return err
}

func (fsm *storeFSM) applySetAdminPrivilegeCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_SetAdminPrivilegeCommand_Command)
	v := ext.(*proto2.SetAdminPrivilegeCommand)
	err := fsm.data.SetAdminPrivilege(v.GetUsername(), v.GetAdmin())
	fsm.Logger.Info("apply set admin privilege command", zap.String("userID", v.GetUsername()), zap.Error(err))
	return err
}

func (fsm *storeFSM) applySetDataCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_SetDataCommand_Command)
	v := ext.(*proto2.SetDataCommand)

	// Overwrite data.
	fsm.data = &meta2.Data{}
	fsm.data.Unmarshal(v.GetData())

	return nil
}

func (fsm *storeFSM) applyCreateMetaNodeCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_CreateMetaNodeCommand_Command)
	v := ext.(*proto2.CreateMetaNodeCommand)

	err := fsm.data.CreateMetaNode(v.GetHTTPAddr(), v.GetRPCAddr(), v.GetTCPAddr())
	if err != nil {
		return err
	}
	fsm.data.ClusterID = v.GetRand()
	return nil
}

func (fsm *storeFSM) applySetMetaNodeCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_SetMetaNodeCommand_Command)
	v := ext.(*proto2.SetMetaNodeCommand)
	err := fsm.data.SetMetaNode(v.GetHTTPAddr(), v.GetRPCAddr(), v.GetTCPAddr())
	if err != nil {
		return err
	}

	// If the cluster ID hasn't been set then use the command's random number.
	if fsm.data.ClusterID == 0 {
		fsm.data.ClusterID = v.GetRand()
	}

	return nil
}

func (fsm *storeFSM) applyDeleteMetaNodeCommand(cmd *proto2.Command, s *Store) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_DeleteMetaNodeCommand_Command)
	v := ext.(*proto2.DeleteMetaNodeCommand)
	return fsm.data.DeleteMetaNode(v.GetID())
}

func (fsm *storeFSM) applyCreateDataNodeCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_CreateDataNodeCommand_Command)
	v := ext.(*proto2.CreateDataNodeCommand)

	dataNode := fsm.data.DataNodeByHttpHost(v.GetHTTPAddr())
	if dataNode != nil {
		return nil
	}

	err, _ := fsm.data.CreateDataNode(v.GetHTTPAddr(), v.GetTCPAddr())
	return err
}

func (fsm *storeFSM) applyDeleteDataNodeCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_DeleteDataNodeCommand_Command)
	v := ext.(*proto2.DeleteDataNodeCommand)
	return fsm.data.DeleteDataNode(v.GetID())
}

func (fsm *storeFSM) Snapshot() (raft.FSMSnapshot, error) {
	s := (*Store)(fsm)
	s.mu.Lock()
	defer s.mu.Unlock()

	return &storeFSMSnapshot{Data: fsm.data.Clone()}, nil
}

func (fsm *storeFSM) applyUpdateShardOwnerCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_UpdateShardOwnerCommand_Command)
	v := ext.(*proto2.UpdateShardOwnerCommand)
	return fsm.data.UpdateShardOwnerId(v.GetDbName(), v.GetRpName(), uint64(v.GetShardId()), uint64(v.GetOwnerId()))
}

func (fsm *storeFSM) applyUpdateShardInfoTierCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_UpdateShardInfoTierCommand_Command)
	v := ext.(*proto2.UpdateShardInfoTierCommand)
	return fsm.data.UpdateShardInfoTier(v.GetShardID(), v.GetTier(), v.GetDbName(), v.GetRpName())
}

func (fsm *storeFSM) applyUpdateNodeStatusCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_UpdateNodeStatusCommand_Command)
	v := ext.(*proto2.UpdateNodeStatusCommand)
	return fsm.data.UpdateNodeStatus(v.GetID(), v.GetStatus(), v.GetLtime(), v.GetGossipAddr())
}

func (fsm *storeFSM) applyCreateEventCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_CreateEventCommand_Command)
	v := ext.(*proto2.CreateEventCommand)
	fsm.data.CreateMigrateEvent(v.GetEventInfo())
	return nil
}

func (fsm *storeFSM) applyUpdateEventCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_UpdateEventCommand_Command)
	v := ext.(*proto2.UpdateEventCommand)
	return fsm.data.UpdateMigrateEvent(v.GetEventInfo())
}

func (fsm *storeFSM) applyUpdatePtInfoCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_UpdatePtInfoCommand_Command)
	v := ext.(*proto2.UpdatePtInfoCommand)
	return fsm.data.UpdatePtInfo(v.GetDb(), v.GetPt(), v.GetOwnerNode(), v.GetStatus())
}

func (fsm *storeFSM) applyRemoveEvent(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_RemoveEventCommand_Command)
	v := ext.(*proto2.RemoveEventCommand)
	return fsm.data.RemoveEventInfo(v.GetEventId())
}

func (fsm *storeFSM) Restore(r io.ReadCloser) error {
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}

	data := &meta2.Data{}
	if err = data.UnmarshalBinary(b); err != nil {
		return err
	}

	fsm.data = data
	fsm.cacheData = data.Clone()
	fsm.cacheDataBytes = b

	return nil
}
