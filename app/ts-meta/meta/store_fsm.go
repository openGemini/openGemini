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
	"sort"
	"time"

	"github.com/hashicorp/raft"
	originql "github.com/influxdata/influxql"
	"github.com/openGemini/openGemini/lib/config"
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

func (fsm *storeFSM) executeCmd(cmd proto2.Command) interface{} {
	if handler, ok := applyFunc[cmd.GetType()]; ok {
		return handler(fsm, &cmd)
	}
	panic(fmt.Errorf("cannot apply command: %x", cmd.GetType()))
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
	v, ok := ext.(*proto2.UpdateSchemaCommand)
	if !ok {
		panic(fmt.Errorf("%s is not a UpdateSchemaCommand", ext))
	}
	return fsm.data.UpdateSchema(v.GetDatabase(), v.GetRpName(), v.GetMeasurement(), v.GetFieldToCreate())
}

func (fsm *storeFSM) applyAlterShardKeyCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_AlterShardKeyCmd_Command)
	v, ok := ext.(*proto2.AlterShardKeyCmd)
	if !ok {
		panic(fmt.Errorf("%s is not a AlterShardKeyCmd", ext))
	}
	return fsm.data.AlterShardKey(v.GetDBName(), v.GetRpName(), v.GetName(), v.GetSki())
}

func (fsm *storeFSM) applyMarkMeasurementDeleteCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_MarkMeasurementDeleteCommand_Command)
	v, ok := ext.(*proto2.MarkMeasurementDeleteCommand)
	if !ok {
		panic(fmt.Errorf("%s is not a MarkMeasurementDeleteCommand", ext))
	}
	return fsm.data.MarkMeasurementDelete(v.GetDatabase(), v.GetPolicy(), v.GetMeasurement())
}

func (fsm *storeFSM) applyDropMeasurementCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_DropMeasurementCommand_Command)
	v, ok := ext.(*proto2.DropMeasurementCommand)
	if !ok {
		panic(fmt.Errorf("%s is not a DropMeasurementCommand", ext))
	}
	return fsm.data.DropMeasurement(v.GetDatabase(), v.GetPolicy(), v.GetMeasurement())
}

func (fsm *storeFSM) applyCreateDbPtViewCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_CreateDbPtViewCommand_Command)
	v, ok := ext.(*proto2.CreateDbPtViewCommand)
	if !ok {
		panic(fmt.Errorf("%s is not a CreateDbPtViewCommand", ext))
	}

	if err := fsm.data.CreateDBPtView(v.GetDbName()); err != nil {
		return err
	}

	return fsm.data.CreateReplication(v.GetDbName(), v.GetReplicaNum())
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
	}

	err := fsm.data.CreateDatabase(v.GetName(), rp, v.GetSki(), v.GetEnableTagArray(), repN, v.GetOptions())
	fsm.Logger.Info("apply create database", zap.Error(err))
	return err
}

func (fsm *storeFSM) applyMarkDatabaseDeleteCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_MarkDatabaseDeleteCommand_Command)
	v, ok := ext.(*proto2.MarkDatabaseDeleteCommand)
	if !ok {
		panic(fmt.Errorf("%s is not a MarkDatabaseDeleteCommand", ext))
	}
	return fsm.data.MarkDatabaseDelete(v.GetName())
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
	ext, _ := proto.GetExtension(cmd, proto2.E_CreateMeasurementCommand_Command)
	v, ok := ext.(*proto2.CreateMeasurementCommand)
	if !ok {
		panic(fmt.Errorf("%s is not a CreateMeasurementCommand", ext))
	}
	return fsm.data.CreateMeasurement(v.GetDBName(), v.GetRpName(), v.GetName(), v.GetSki(), v.GetIR(), config.EngineType(v.GetEngineType()),
		v.GetColStoreInfo(), v.GetSchemaInfo(), v.GetOptions())
}

func (fsm *storeFSM) applyCreateRetentionPolicyCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_CreateRetentionPolicyCommand_Command)
	v, ok := ext.(*proto2.CreateRetentionPolicyCommand)
	if !ok {
		panic(fmt.Errorf("%s is not a CreateRetentionPolicyCommand", ext))
	}
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
	ext, _ := proto.GetExtension(cmd, proto2.E_ContinuousQueryReportCommand_Command)
	v, ok := ext.(*proto2.ContinuousQueryReportCommand)
	if !ok {
		panic(fmt.Errorf("%s is not a ContinuousQueryReportCommand", ext))
	}

	return fsm.data.BatchUpdateContinuousQueryStat(v.GetCQStates())
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
	if v.WarmDuration != nil {
		value := time.Duration(v.GetWarmDuration())
		rpu.WarmDuration = &value
	}
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
	return fsm.data.CreateShardGroup(v.GetDatabase(), v.GetPolicy(), time.Unix(0, v.GetTimestamp()), v.GetShardTier(),
		config.EngineType(v.GetEngineType()), v.GetVersion())
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
	return fsm.data.PruneGroups(v.GetShardGroup(), v.GetID())
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
		fsm.data.MaxConnID++
		dataNode.ConnID = fsm.data.MaxConnID
		return nil
	}

	fsm.data.ExpandShardsEnable = fsm.config.ExpandShardsEnable
	_, err := fsm.data.CreateDataNode(v.GetHTTPAddr(), v.GetTCPAddr(), v.GetRole())
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
	return fsm.data.CreateMigrateEvent(v.GetEventInfo())
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

func (fsm *storeFSM) applyCreateStream(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_CreateStreamCommand_Command)
	v := ext.(*proto2.CreateStreamCommand)
	pb := v.GetStreamInfo()

	rpi := &meta2.StreamInfo{}
	rpi.Unmarshal(pb)

	return fsm.data.CreateStream(rpi)
}

func (fsm *storeFSM) applyDropStream(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_DropStreamCommand_Command)
	v := ext.(*proto2.DropStreamCommand)
	name := v.GetName()

	return fsm.data.DropStream(name)
}

func (fsm *storeFSM) applyExpandGroupsCommand(cmd *proto2.Command) interface{} {
	fsm.data.ExpandGroups()
	return nil
}

func (fsm *storeFSM) applyUpdatePtVersionCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_UpdatePtVersionCommand_Command)
	v, ok := ext.(*proto2.UpdatePtVersionCommand)
	if !ok {
		panic(fmt.Errorf("%s is not a UpdatePtVersionCommand", ext))
	}
	_ = fsm.data.UpdatePtVersion(v.GetDb(), v.GetPt())
	return nil
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
	ext, _ := proto.GetExtension(cmd, proto2.E_CreateDownSamplePolicyCommand_Command)
	v := ext.(*proto2.CreateDownSamplePolicyCommand)
	pb := v.GetDownSamplePolicyInfo()

	rpi := &meta2.DownSamplePolicyInfo{}
	rpi.Unmarshal(pb)
	if rpi.IsNil() {
		rpi = nil
	}

	return fsm.data.CreateDownSamplePolicy(v.GetDatabase(), v.GetName(), rpi)
}

func (fsm *storeFSM) applyDropDownSampleCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_DropDownSamplePolicyCommand_Command)
	v := ext.(*proto2.DropDownSamplePolicyCommand)

	fsm.data.DropDownSamplePolicy(v.GetDatabase(), v.GetRpName(), v.GetDropAll())
	return nil
}

func (fsm *storeFSM) applyUpdateShardDownSampleInfoCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_UpdateShardDownSampleInfoCommand_Command)
	v := ext.(*proto2.UpdateShardDownSampleInfoCommand)

	ident := &meta2.ShardIdentifier{}
	ident.Unmarshal(v.GetIdent())
	return fsm.data.UpdateShardDownSampleInfo(ident)
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
	ext, _ := proto.GetExtension(cmd, proto2.E_RegisterQueryIDOffsetCommand_Command)
	v, ok := ext.(*proto2.RegisterQueryIDOffsetCommand)
	if !ok {
		panic(fmt.Errorf("%s is not a RegisterQueryIDOffsetCommand", ext))
	}
	err := fsm.data.RegisterQueryIDOffset(meta2.SQLHost(v.GetHost()))
	return err
}

func (fsm *storeFSM) applySetNodeSegregateStatusCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_SetNodeSegregateStatusCommand_Command)
	v, ok := ext.(*proto2.SetNodeSegregateStatusCommand)
	if !ok {
		panic(fmt.Errorf("%s is not a SetNodeSegregateStatusCommand", ext))
	}
	nodeIds := v.GetNodeIds()
	status := v.GetStatus()
	fsm.data.SetSegregateNodeStatus(status, nodeIds)
	return nil
}

func (fsm *storeFSM) applyRemoveNodeCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_RemoveNodeCommand_Command)
	v, ok := ext.(*proto2.RemoveNodeCommand)
	if !ok {
		panic(fmt.Errorf("%s is not a RemoveNodeCommand", ext))
	}
	nodeIds := v.GetNodeIds()
	fsm.data.RemoveNode(nodeIds)
	return nil
}

func (fsm *storeFSM) applyUpdateReplicationCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_UpdateReplicationCommand_Command)
	v, ok := ext.(*proto2.UpdateReplicationCommand)
	if !ok {
		panic(fmt.Errorf("%s is not a UpdateReplicationCommand", ext))
	}
	db := v.GetDatabase()
	rgId := v.GetRepGroupId()
	peers := v.GetPeers()
	masterId := v.GetMasterId()
	rgStatus := v.GetRgStatus()
	return fsm.data.UpdateReplication(db, rgId, masterId, peers, rgStatus)
}

func (fsm *storeFSM) applyUpdateMeasurementCommand(cmd *proto2.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, proto2.E_UpdateMeasurementCommand_Command)
	v, ok := ext.(*proto2.UpdateMeasurementCommand)
	if !ok {
		panic(fmt.Errorf("%s is not a UpdateMeasurementCommand", ext))
	}
	return fsm.data.UpdateMeasurement(v.GetDb(), v.GetRp(), v.GetMst(), v.GetOptions())
}
