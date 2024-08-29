// Copyright Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package meta

import (
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/hashicorp/raft"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	proto2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta/proto"
	"github.com/openGemini/openGemini/lib/util/lifted/protobuf/proto"
	"go.uber.org/zap"
)

// storeFSM represents the finite state machine used by Store to interact with Raft.
type storeFSM Store

func (fsm *storeFSM) ApplyBatch(logs []*raft.Log) []interface{} {
	s := (*Store)(fsm)
	var dataChanged bool
	s.mu.Lock()
	defer s.mu.Unlock()
	if fsm.UseIncSyncData {
		s.cacheMu.Lock()
		defer s.cacheMu.Unlock()
	}
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
		if cmd.GetType() != proto2.Command_VerifyDataNodeCommand {
			fsm.Logger.Info(fmt.Sprintf("BatchApply log term %d index %d type %d", logs[i].Term, logs[i].Index, int32(cmd.GetType())))
		}
		ret[i] = fsm.executeCmd(cmd)
		if ret[i] == nil {
			dataChanged = true
			fsm.data.AddCmdAsOpToOpMap(cmd, logs[i].Index)
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
	if fsm.UseIncSyncData {
		s.cacheMu.Lock()
		defer s.cacheMu.Unlock()
	}
	fsm.Logger.Info(fmt.Sprintf("Apply log term %d index %d type %d", l.Term, l.Index, int32(cmd.GetType())))
	err := fsm.executeCmd(cmd)

	// Copy term and index to new metadata.
	fsm.data.Term = l.Term
	fsm.data.Index = l.Index

	if err != nil {
		return err
	}
	fsm.data.AddCmdAsOpToOpMap(cmd, l.Index)
	// signal that the data changed
	close(s.dataChanged)
	s.dataChanged = make(chan struct{})

	return err
}

var applyFunc = map[proto2.Command_Type]func(fsm *storeFSM, cmd *proto2.Command) interface{}{
	proto2.Command_CreateDatabaseCommand:            applyCreateDatabase,
	proto2.Command_DropDatabaseCommand:              applyDropDatabase,
	proto2.Command_CreateRetentionPolicyCommand:     applyCreateRetentionPolicy,
	proto2.Command_DropRetentionPolicyCommand:       applyDropRetentionPolicy,
	proto2.Command_SetDefaultRetentionPolicyCommand: applySetDefaultRetentionPolicy,
	proto2.Command_UpdateRetentionPolicyCommand:     applyUpdateRetentionPolicy,
	proto2.Command_CreateShardGroupCommand:          applyCreateShardGroup,
	proto2.Command_DeleteShardGroupCommand:          applyDeleteShardGroup,
	proto2.Command_CreateSubscriptionCommand:        applyCreateSubscription,
	proto2.Command_DropSubscriptionCommand:          applyDropSubscription,
	proto2.Command_CreateUserCommand:                applyCreateUser,
	proto2.Command_DropUserCommand:                  applyDropUser,
	proto2.Command_UpdateUserCommand:                applyUpdateUser,
	proto2.Command_SetPrivilegeCommand:              applySetPrivilege,
	proto2.Command_SetAdminPrivilegeCommand:         applySetAdminPrivilege,
	proto2.Command_SetDataCommand:                   applySetData,
	proto2.Command_CreateMetaNodeCommand:            applyCreateMetaNode,
	proto2.Command_DeleteMetaNodeCommand:            applyDeleteMetaNode,
	proto2.Command_SetMetaNodeCommand:               applySetMetaNode,
	proto2.Command_CreateDataNodeCommand:            applyCreateDataNode,
	proto2.Command_CreateSqlNodeCommand:             applyCreateSqlNode,
	proto2.Command_DeleteDataNodeCommand:            applyDeleteDataNode,
	proto2.Command_MarkDatabaseDeleteCommand:        applyMarkDatabaseDelete,
	proto2.Command_MarkRetentionPolicyDeleteCommand: applyMarkRetentionPolicyDelete,
	proto2.Command_CreateMeasurementCommand:         applyCreateMeasurement,
	proto2.Command_ReShardingCommand:                applyReSharding,
	proto2.Command_UpdateSchemaCommand:              applyUpdateSchema,
	proto2.Command_AlterShardKeyCmd:                 applyAlterShardKey,
	proto2.Command_PruneGroupsCommand:               applyPruneGroups,
	proto2.Command_MarkMeasurementDeleteCommand:     applyMarkMeasurementDelete,
	proto2.Command_DropMeasurementCommand:           applyDropMeasurement,
	proto2.Command_DeleteIndexGroupCommand:          applyDeleteIndexGroup,
	proto2.Command_UpdateShardInfoTierCommand:       applyUpdateShardInfoTier,
	proto2.Command_UpdateNodeStatusCommand:          applyUpdateNodeStatus,
	proto2.Command_UpdateSqlNodeStatusCommand:       applyUpdateSqlNodeStatus,
	proto2.Command_CreateEventCommand:               applyCreateEvent,
	proto2.Command_UpdateEventCommand:               applyUpdateEvent,
	proto2.Command_UpdatePtInfoCommand:              applyUpdatePtInfo,
	proto2.Command_RemoveEventCommand:               applyRemoveEvent,
	proto2.Command_CreateDownSamplePolicyCommand:    applyCreateDownSample,
	proto2.Command_DropDownSamplePolicyCommand:      applyDropDownSample,
	proto2.Command_CreateDbPtViewCommand:            applyCreateDbPtView,
	proto2.Command_UpdateShardDownSampleInfoCommand: applyUpdateShardDownSampleInfo,
	proto2.Command_MarkTakeoverCommand:              applyMarkTakeover,
	proto2.Command_MarkBalancerCommand:              applyMarkBalancer,
	proto2.Command_CreateStreamCommand:              applyCreateStream,
	proto2.Command_DropStreamCommand:                applyDropStream,
	proto2.Command_VerifyDataNodeCommand:            applyVerifyDataNode,
	proto2.Command_ExpandGroupsCommand:              applyExpandGroups,
	proto2.Command_UpdatePtVersionCommand:           applyUpdatePtVersion,
	proto2.Command_RegisterQueryIDOffsetCommand:     applyRegisterQueryIDOffset,
	proto2.Command_CreateContinuousQueryCommand:     applyCreateContinuousQuery,
	proto2.Command_ContinuousQueryReportCommand:     applyContinuousQueryReport,
	proto2.Command_DropContinuousQueryCommand:       applyDropContinuousQuery,
	proto2.Command_NotifyCQLeaseChangedCommand:      applyNotifyCQLeaseChanged,
	proto2.Command_SetNodeSegregateStatusCommand:    applySetNodeSegregateStatusCommand,
	proto2.Command_RemoveNodeCommand:                applyRemoveNodeCommand,
	proto2.Command_UpdateReplicationCommand:         applyUpdateReplicationCommand,
	proto2.Command_UpdateMeasurementCommand:         applyUpdateMeasurement,
	proto2.Command_UpdateNodeTmpIndexCommand:        applyUpdateNodeTmpIndexCommand,
	proto2.Command_InsertFilesCommand:               applyInsertFilesCommand,
}

func applyCreateDatabase(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return fsm.applyCreateDatabaseCommand(cmd)
}

func applyDropDatabase(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return fsm.applyDropDatabaseCommand(cmd)
}

func applyCreateRetentionPolicy(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return fsm.applyCreateRetentionPolicyCommand(cmd)
}

func applyDropRetentionPolicy(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return fsm.applyDropRetentionPolicyCommand(cmd)
}

func applySetDefaultRetentionPolicy(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return fsm.applySetDefaultRetentionPolicyCommand(cmd)
}

func applyUpdateRetentionPolicy(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return fsm.applyUpdateRetentionPolicyCommand(cmd)
}

func applyCreateShardGroup(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return fsm.applyCreateShardGroupCommand(cmd)
}

func applyDeleteShardGroup(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return fsm.applyDeleteShardGroupCommand(cmd)
}

func applyCreateSubscription(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return fsm.applyCreateSubscriptionCommand(cmd)
}

func applyDropSubscription(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return fsm.applyDropSubscriptionCommand(cmd)
}

func applyCreateUser(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return fsm.applyCreateUserCommand(cmd)
}

func applyDropUser(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return fsm.applyDropUserCommand(cmd)
}

func applyUpdateUser(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return fsm.applyUpdateUserCommand(cmd)
}

func applySetPrivilege(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return fsm.applySetPrivilegeCommand(cmd)
}

func applySetAdminPrivilege(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return fsm.applySetAdminPrivilegeCommand(cmd)
}

func applySetData(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return fsm.applySetDataCommand(cmd)
}

func applyCreateMetaNode(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return fsm.applyCreateMetaNodeCommand(cmd)
}

func applyDeleteMetaNode(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return fsm.applyDeleteMetaNodeCommand(cmd, (*Store)(fsm))
}

func applySetMetaNode(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return fsm.applySetMetaNodeCommand(cmd)
}

func applyCreateDataNode(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return fsm.applyCreateDataNodeCommand(cmd)
}

func applyCreateSqlNode(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return fsm.applyCreateSqlNodeCommand(cmd)
}

func applyDeleteDataNode(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return fsm.applyDeleteDataNodeCommand(cmd)
}

func applyMarkDatabaseDelete(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return fsm.applyMarkDatabaseDeleteCommand(cmd)
}

func applyMarkRetentionPolicyDelete(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return fsm.applyMarkRetentionPolicyDeleteCommand(cmd)
}

func applyCreateMeasurement(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return fsm.applyCreateMeasurementCommand(cmd)
}

func applyReSharding(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return fsm.applyReShardingCommand(cmd)
}

func applyUpdateSchema(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return fsm.applyUpdateSchemaCommand(cmd)
}

func applyAlterShardKey(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return fsm.applyAlterShardKeyCommand(cmd)
}

func applyPruneGroups(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return fsm.applyPruneGroupsCommand(cmd)
}

func applyMarkMeasurementDelete(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return fsm.applyMarkMeasurementDeleteCommand(cmd)
}

func applyDropMeasurement(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return fsm.applyDropMeasurementCommand(cmd)
}

func applyDeleteIndexGroup(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return fsm.applyDeleteIndexGroupCommand(cmd)
}

func applyUpdateShardInfoTier(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return fsm.applyUpdateShardInfoTierCommand(cmd)
}

func applyUpdateNodeStatus(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return fsm.applyUpdateNodeStatusCommand(cmd)
}

func applyUpdateSqlNodeStatus(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return fsm.applyUpdateSqlNodeStatusCommand(cmd)
}

func applyCreateEvent(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return fsm.applyCreateEventCommand(cmd)
}

func applyUpdateEvent(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return fsm.applyUpdateEventCommand(cmd)
}

func applyUpdatePtInfo(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return fsm.applyUpdatePtInfoCommand(cmd)
}

func applyRemoveEvent(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return fsm.applyRemoveEvent(cmd)
}

func applyCreateDownSample(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return fsm.applyCreateDownSampleCommand(cmd)
}

func applyDropDownSample(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return fsm.applyDropDownSampleCommand(cmd)
}

func applyCreateDbPtView(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return fsm.applyCreateDbPtViewCommand(cmd)
}

func applyUpdateShardDownSampleInfo(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return fsm.applyUpdateShardDownSampleInfoCommand(cmd)
}

func applyMarkTakeover(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return fsm.applyMarkTakeoverCommand(cmd)
}

func applyMarkBalancer(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return fsm.applyMarkBalancerCommand(cmd)
}

func applyCreateStream(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return fsm.applyCreateStream(cmd)
}

func applyDropStream(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return fsm.applyDropStream(cmd)
}

func applyVerifyDataNode(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return nil
}

func applyExpandGroups(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return fsm.applyExpandGroupsCommand(cmd)
}

func applyUpdatePtVersion(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return fsm.applyUpdatePtVersionCommand(cmd)
}

func applyRegisterQueryIDOffset(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return fsm.applyRegisterQueryIDOffsetCommand(cmd)
}

func applyCreateContinuousQuery(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return fsm.applyCreateContinuousQueryCommand(cmd)
}

func applyContinuousQueryReport(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return fsm.applyContinuousQueryReportCommand(cmd)
}

func applyDropContinuousQuery(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return fsm.applyDropContinuousQueryCommand(cmd)
}

func applyNotifyCQLeaseChanged(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return fsm.applyNotifyCQLeaseChangedCommand(cmd)
}

func applySetNodeSegregateStatusCommand(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return fsm.applySetNodeSegregateStatusCommand(cmd)
}

func applyRemoveNodeCommand(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return fsm.applyRemoveNodeCommand(cmd)
}

func applyUpdateReplicationCommand(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return fsm.applyUpdateReplicationCommand(cmd)
}

func applyUpdateMeasurement(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return fsm.applyUpdateMeasurementCommand(cmd)
}

func applyUpdateNodeTmpIndexCommand(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return fsm.applyUpdateNodeTmpIndexCommand(cmd)
}

func applyInsertFilesCommand(fsm *storeFSM, cmd *proto2.Command) interface{} {
	return fsm.applyInsertFilesCommand(cmd)
}

func (fsm *storeFSM) executeCmd(cmd proto2.Command) interface{} {
	if handler, ok := applyFunc[cmd.GetType()]; ok {
		return handler(fsm, &cmd)
	}
	fsm.Logger.Error("cannot apply command: %x", zap.Int32("typ", int32(cmd.GetType())))
	return nil
}

func (fsm *storeFSM) applyReShardingCommand(cmd *proto2.Command) interface{} {
	return meta2.ApplyReSharding(fsm.data, cmd)
}

func (fsm *storeFSM) applyUpdateSchemaCommand(cmd *proto2.Command) interface{} {
	return meta2.ApplyUpdateSchema(fsm.data, cmd)
}

func (fsm *storeFSM) applyAlterShardKeyCommand(cmd *proto2.Command) interface{} {
	return meta2.ApplyAlterShardKey(fsm.data, cmd)
}

func (fsm *storeFSM) applyMarkMeasurementDeleteCommand(cmd *proto2.Command) interface{} {
	return meta2.ApplyMarkMeasurementDelete(fsm.data, cmd)
}

func (fsm *storeFSM) applyDropMeasurementCommand(cmd *proto2.Command) interface{} {
	return meta2.ApplyDropMeasurement(fsm.data, cmd)
}

func (fsm *storeFSM) applyCreateDbPtViewCommand(cmd *proto2.Command) interface{} {
	return meta2.ApplyCreateDbPtViewCommand(fsm.data, cmd)
}

func (fsm *storeFSM) applyCreateDatabaseCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_CreateDatabaseCommand_Command)
	v, ok := ext.(*proto2.CreateDatabaseCommand)
	if !ok {
		panic(fmt.Errorf("%s is not a CreateDatabaseCommand", ext))
	}

	s := (*Store)(fsm)
	var rp *meta2.RetentionPolicyInfo
	rpi := v.GetRetentionPolicy()
	repN := v.GetReplicaNum()
	if repN == 0 {
		repN = 1
	}
	if rpi != nil {
		rp = &meta2.RetentionPolicyInfo{
			Name:               rpi.GetName(),
			ReplicaN:           int(rpi.GetReplicaN()),
			Duration:           time.Duration(rpi.GetDuration()),
			ShardGroupDuration: time.Duration(rpi.GetShardGroupDuration()),
			HotDuration:        time.Duration(rpi.GetHotDuration()),
			WarmDuration:       time.Duration(rpi.GetWarmDuration()),
			IndexGroupDuration: time.Duration(rpi.GetIndexGroupDuration())}
	} else if s.config.RetentionAutoCreate {
		// Create a retention policy.
		rp = meta2.NewRetentionPolicyInfo(autoCreateRetentionPolicyName)
		rp.ReplicaN = int(repN)
		rp.Duration = autoCreateRetentionPolicyPeriod
		rp.WarmDuration = autoCreateRetentionPolicyWarmPeriod
		if fsm.UseIncSyncData {
			pbRN := uint32(rp.ReplicaN)
			v.RetentionPolicy = &proto2.RetentionPolicyInfo{
				Name:               &rp.Name,
				ReplicaN:           &pbRN,
				Duration:           (*int64)(&rp.Duration),
				ShardGroupDuration: (*int64)(&rp.ShardGroupDuration),
				HotDuration:        (*int64)(&rp.HotDuration),
				WarmDuration:       (*int64)(&rp.WarmDuration),
				IndexGroupDuration: (*int64)(&rp.IndexGroupDuration),
			}
		}
	}

	err := fsm.data.CreateDatabase(v.GetName(), rp, v.GetSki(), v.GetEnableTagArray(), repN, v.GetOptions())
	fsm.Logger.Info("apply create database", zap.Error(err))
	return err
}

func (fsm *storeFSM) applyMarkDatabaseDeleteCommand(cmd *proto2.Command) interface{} {
	return meta2.ApplyMarkDatabaseDelete(fsm.data, cmd)
}

func (fsm *storeFSM) applyDropDatabaseCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_DropDatabaseCommand_Command)
	v, ok := ext.(*proto2.DropDatabaseCommand)
	if !ok {
		panic(fmt.Errorf("%s is not a DropDatabaseCommand", ext))
	}
	dbi := fsm.data.Database(v.GetName())
	if dbi == nil {
		return nil
	}
	// remove all deleted continuous query names from fsm.cqNames
	// notify all sql nodes that CQ has been changed
	if len(dbi.ContinuousQueries) > 0 {
		s := (*Store)(fsm)
		s.cqLock.Lock()
		for cqName := range dbi.ContinuousQueries {
			n := sort.SearchStrings(s.cqNames, cqName)
			if n < len(s.cqNames) {
				s.cqNames = append(s.cqNames[:n], s.cqNames[n+1:]...)
			}
		}
		s.scheduleCQsWithoutLock(cqDropped)
		s.cqLock.Unlock()
		fsm.data.MaxCQChangeID++
	}
	fsm.data.DropDatabase(v.GetName())

	return nil
}

func (fsm *storeFSM) applyCreateMeasurementCommand(cmd *proto2.Command) interface{} {
	return meta2.ApplyCreateMeasurement(fsm.data, cmd)
}

func (fsm *storeFSM) applyCreateRetentionPolicyCommand(cmd *proto2.Command) interface{} {
	return meta2.ApplyCreateRetentionPolicy(fsm.data, cmd)
}

func (fsm *storeFSM) applyCreateContinuousQueryCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_CreateContinuousQueryCommand_Command)
	v, ok := ext.(*proto2.CreateContinuousQueryCommand)
	if !ok {
		panic(fmt.Errorf("%s is not a CreateContinuousQueryCommand", ext))
	}

	if err := fsm.data.CreateContinuousQuery(v.GetDatabase(), v.GetName(), v.GetQuery()); err != nil {
		return err
	}
	s := (*Store)(fsm)
	s.handleCQCreated(v.GetName())
	return nil
}

func (fsm *storeFSM) applyContinuousQueryReportCommand(cmd *proto2.Command) interface{} {
	return meta2.ApplyContinuousQueryReport(fsm.data, cmd)
}

func (fsm *storeFSM) applyDropContinuousQueryCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_DropContinuousQueryCommand_Command)
	v, ok := ext.(*proto2.DropContinuousQueryCommand)
	if !ok {
		panic(fmt.Errorf("%s is not a DropContinuousQueryCommand", ext))
	}
	changed, err := fsm.data.DropContinuousQuery(v.GetName(), v.GetDatabase())
	if err != nil || !changed {
		return err
	}

	s := (*Store)(fsm)
	s.handleCQDropped(v.GetName())
	return nil
}

// applyNotifyCQLeaseChangedCommand notify all sql that cq lease has been changed.
func (fsm *storeFSM) applyNotifyCQLeaseChangedCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_NotifyCQLeaseChangedCommand_Command)
	_, ok := ext.(*proto2.NotifyCQLeaseChangedCommand)
	if !ok {
		panic(fmt.Errorf("%s is not a NotifyCQLeaseChangedCommand", ext))
	}
	fsm.data.MaxCQChangeID++
	return nil
}

func (fsm *storeFSM) applyDropRetentionPolicyCommand(cmd *proto2.Command) interface{} {
	return meta2.ApplyDropRetentionPolicy(fsm.data, cmd)
}

func (fsm *storeFSM) applyMarkRetentionPolicyDeleteCommand(cmd *proto2.Command) interface{} {
	return meta2.ApplyMarkRetentionPolicyDelete(fsm.data, cmd)
}

func (fsm *storeFSM) applySetDefaultRetentionPolicyCommand(cmd *proto2.Command) interface{} {
	return meta2.ApplySetDefaultRetentionPolicy(fsm.data, cmd)
}

func (fsm *storeFSM) applyUpdateRetentionPolicyCommand(cmd *proto2.Command) interface{} {
	return meta2.ApplyUpdateRetentionPolicy(fsm.data, cmd)
}

func (fsm *storeFSM) applyCreateShardGroupCommand(cmd *proto2.Command) interface{} {
	return meta2.ApplyCreateShardGroup(fsm.data, cmd)
}

func (fsm *storeFSM) applyDeleteShardGroupCommand(cmd *proto2.Command) interface{} {
	return meta2.ApplyDeleteShardGroup(fsm.data, cmd)
}

func (fsm *storeFSM) applyDeleteIndexGroupCommand(cmd *proto2.Command) interface{} {
	return meta2.ApplyDeleteIndexGroup(fsm.data, cmd)
}

func (fsm *storeFSM) applyPruneGroupsCommand(cmd *proto2.Command) interface{} {
	return meta2.ApplyPruneGroups(fsm.data, cmd)
}

func (fsm *storeFSM) applyCreateSubscriptionCommand(cmd *proto2.Command) interface{} {
	return meta2.ApplyCreateSubscription(fsm.data, cmd)
}

func (fsm *storeFSM) applyDropSubscriptionCommand(cmd *proto2.Command) interface{} {
	return meta2.ApplyDropSubscription(fsm.data, cmd)
}

func (fsm *storeFSM) applyCreateUserCommand(cmd *proto2.Command) interface{} {
	return meta2.ApplyCreateUser(fsm.data, cmd)
}

func (fsm *storeFSM) applyDropUserCommand(cmd *proto2.Command) interface{} {
	return meta2.ApplyDropUser(fsm.data, cmd)
}

func (fsm *storeFSM) applyUpdateUserCommand(cmd *proto2.Command) interface{} {
	return meta2.ApplyUpdateUser(fsm.data, cmd)
}

func (fsm *storeFSM) applySetPrivilegeCommand(cmd *proto2.Command) interface{} {
	return meta2.ApplySetPrivilege(fsm.data, cmd)
}

func (fsm *storeFSM) applySetAdminPrivilegeCommand(cmd *proto2.Command) interface{} {
	return meta2.ApplySetAdminPrivilege(fsm.data, cmd)
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
	return meta2.ApplyCreateMetaNode(fsm.data, cmd)
}

func (fsm *storeFSM) applySetMetaNodeCommand(cmd *proto2.Command) interface{} {
	return meta2.ApplySetMetaNode(fsm.data, cmd)
}

func (fsm *storeFSM) applyDeleteMetaNodeCommand(cmd *proto2.Command, s *Store) interface{} {
	return meta2.ApplyDeleteMetaNode(fsm.data, cmd)
}

func (fsm *storeFSM) applyCreateDataNodeCommand(cmd *proto2.Command) interface{} {
	fsm.data.ExpandShardsEnable = fsm.config.ExpandShardsEnable
	return meta2.ApplyCreateDataNode(fsm.data, cmd)
}

func (fsm *storeFSM) applyCreateSqlNodeCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_CreateSqlNodeCommand_Command)
	v, ok := ext.(*proto2.CreateSqlNodeCommand)
	if !ok {
		panic(fmt.Errorf("%s is not a CreateSqlNodeCommand", ext))
	}
	sqlNode := fsm.data.SqlNodeByHttpHost(v.GetHTTPAddr())
	if sqlNode != nil {
		fsm.data.MaxConnID++
		sqlNode.ConnID = fsm.data.MaxConnID
		return nil
	}

	fsm.data.ExpandShardsEnable = fsm.config.ExpandShardsEnable
	_, err := fsm.data.CreateSqlNode(v.GetHTTPAddr(), v.GetGossipAddr())
	return err
}

func (fsm *storeFSM) applyDeleteDataNodeCommand(cmd *proto2.Command) interface{} {
	return meta2.ApplyDeleteDataNode(fsm.data, cmd)
}

func (fsm *storeFSM) Snapshot() (raft.FSMSnapshot, error) {
	s := (*Store)(fsm)
	s.mu.Lock()
	defer s.mu.Unlock()

	return &storeFSMSnapshot{Data: fsm.data.Clone()}, nil
}

func (fsm *storeFSM) applyUpdateShardInfoTierCommand(cmd *proto2.Command) interface{} {
	return meta2.ApplyUpdateShardInfoTier(fsm.data, cmd)
}

func (fsm *storeFSM) applyUpdateNodeStatusCommand(cmd *proto2.Command) interface{} {
	return meta2.ApplyUpdateNodeStatus(fsm.data, cmd)
}

func (fsm *storeFSM) applyUpdateSqlNodeStatusCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_UpdateSqlNodeStatusCommand_Command)
	v, ok := ext.(*proto2.UpdateSqlNodeStatusCommand)
	if !ok {
		panic(fmt.Errorf("%s is not a UpdateSqlNodeStatusCommand", ext))
	}
	return fsm.data.UpdateSqlNodeStatus(v.GetID(), v.GetStatus(), v.GetLtime(), v.GetGossipAddr())
}

func (fsm *storeFSM) applyCreateEventCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_CreateEventCommand_Command)
	v := ext.(*proto2.CreateEventCommand)
	return fsm.data.CreateMigrateEvent(v.GetEventInfo())
}

func (fsm *storeFSM) applyUpdateEventCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_UpdateEventCommand_Command)
	v := ext.(*proto2.UpdateEventCommand)
	return fsm.data.UpdateMigrateEvent(v.GetEventInfo())
}

func (fsm *storeFSM) applyUpdatePtInfoCommand(cmd *proto2.Command) interface{} {
	return meta2.ApplyUpdatePtInfo(fsm.data, cmd)
}

func (fsm *storeFSM) applyRemoveEvent(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_RemoveEventCommand_Command)
	v := ext.(*proto2.RemoveEventCommand)
	return fsm.data.RemoveEventInfo(v.GetEventId())
}

func (fsm *storeFSM) applyCreateStream(cmd *proto2.Command) interface{} {
	return meta2.ApplyCreateStream(fsm.data, cmd)
}

func (fsm *storeFSM) applyDropStream(cmd *proto2.Command) interface{} {
	return meta2.ApplyDropStream(fsm.data, cmd)
}

func (fsm *storeFSM) applyExpandGroupsCommand(cmd *proto2.Command) interface{} {
	fsm.data.ExpandGroups()
	return nil
}

func (fsm *storeFSM) applyUpdatePtVersionCommand(cmd *proto2.Command) interface{} {
	return meta2.ApplyUpdatePtVersion(fsm.data, cmd)
}

func (fsm *storeFSM) Restore(r io.ReadCloser) error {
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}

	data := &meta2.Data{}
	if err = data.UnmarshalBinary(b); err != nil {
		return err
	}
	if fsm.UseIncSyncData {
		data.SetOps(fsm.data)
	}
	fsm.data = data
	fsm.restoreCQNames()

	return nil
}

func (fsm *storeFSM) restoreCQNames() {
	fsm.cqNames = fsm.cqNames[:0]

	fsm.data.WalkDatabases(func(db *meta2.DatabaseInfo) {
		for cqName := range db.ContinuousQueries {
			fsm.cqNames = append(fsm.cqNames, cqName)
		}
	})
	sort.Strings(fsm.cqNames)
}

func (fsm *storeFSM) applyCreateDownSampleCommand(cmd *proto2.Command) interface{} {
	return meta2.ApplyCreateDownSample(fsm.data, cmd)
}

func (fsm *storeFSM) applyDropDownSampleCommand(cmd *proto2.Command) interface{} {
	return meta2.ApplyDropDownSample(fsm.data, cmd)
}

func (fsm *storeFSM) applyUpdateShardDownSampleInfoCommand(cmd *proto2.Command) interface{} {
	return meta2.ApplyUpdateShardDownSampleInfo(fsm.data, cmd)
}

func (fsm *storeFSM) applyMarkTakeoverCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_MarkTakeoverCommand_Command)
	v := ext.(*proto2.MarkTakeoverCommand)

	fsm.data.MarkTakeover(v.GetEnable())
	return nil
}

func (fsm *storeFSM) applyMarkBalancerCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_MarkBalancerCommand_Command)
	v := ext.(*proto2.MarkBalancerCommand)

	fsm.data.MarkBalancer(v.GetEnable())
	return nil
}

func (fsm *storeFSM) applyRegisterQueryIDOffsetCommand(cmd *proto2.Command) interface{} {
	return meta2.ApplyRegisterQueryIDOffset(fsm.data, cmd)
}

func (fsm *storeFSM) applySetNodeSegregateStatusCommand(cmd *proto2.Command) interface{} {
	return meta2.ApplySetNodeSegregateStatus(fsm.data, cmd)
}

func (fsm *storeFSM) applyRemoveNodeCommand(cmd *proto2.Command) interface{} {
	return meta2.ApplyRemoveNode(fsm.data, cmd)
}

func (fsm *storeFSM) applyUpdateReplicationCommand(cmd *proto2.Command) interface{} {
	return meta2.ApplyUpdateReplication(fsm.data, cmd)
}

func (fsm *storeFSM) applyUpdateMeasurementCommand(cmd *proto2.Command) interface{} {
	return meta2.ApplyUpdateMeasurement(fsm.data, cmd)
}

func (fsm *storeFSM) applyUpdateNodeTmpIndexCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_UpdateNodeTmpIndexCommand_Command)
	v, ok := ext.(*proto2.UpdateNodeTmpIndexCommand)
	if !ok {
		panic(fmt.Errorf("%s is not a UpdateNodeTmpIndexCommand", ext))
	}
	return fsm.data.UpdateNodeTmpIndex(v.GetRole(), v.GetIndex(), v.GetNodeId())
}

func (fsm *storeFSM) applyInsertFilesCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_InsertFilesCommand_Command)
	v, ok := ext.(*proto2.InsertFilesCommand)
	if !ok {
		panic(fmt.Errorf("%s is not a InsertFilesCommand", ext))
	}
	if fsm.data.SQLite == nil {
		return fmt.Errorf("not support to InsertFiles while SQLite is not initialized")
	}
	fileNum := len(v.GetFileInfos())
	if fileNum == 0 {
		return nil
	}
	fileInfos := make([]meta2.FileInfo, fileNum)
	for i, file := range v.GetFileInfos() {
		fileInfos[i].Unmarshal(file)
	}

	return fsm.data.SQLite.InsertFiles(fileInfos, nil)
}
