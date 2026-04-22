// Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.
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
	"encoding/json"
	"fmt"
	"sync"
	"time"

	originql "github.com/influxdata/influxql"
	"github.com/openGemini/openGemini/lib/config"
	proto2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta/proto"
	"github.com/openGemini/openGemini/lib/util/lifted/protobuf/proto"
	"go.uber.org/zap"
)

func ApplyCreateRetentionPolicy(data *Data, cmd *proto2.Command) error {
	ext, _ := proto.GetExtension(cmd, proto2.E_CreateRetentionPolicyCommand_Command)
	v, ok := ext.(*proto2.CreateRetentionPolicyCommand)
	if !ok {
		panic(fmt.Errorf("%s is not a CreateRetentionPolicyCommand", ext))
	}
	pb := v.GetRetentionPolicy()

	rpi := &RetentionPolicyInfo{
		Name:               pb.GetName(),
		ReplicaN:           int(pb.GetReplicaN()),
		Duration:           time.Duration(pb.GetDuration()),
		ShardGroupDuration: time.Duration(pb.GetShardGroupDuration()),
		HotDuration:        time.Duration(pb.GetHotDuration()),
		WarmDuration:       time.Duration(pb.GetWarmDuration()),
		IndexColdDuration:  time.Duration(pb.GetIndexColdDuration()),
		IndexGroupDuration: time.Duration(pb.GetIndexGroupDuration()),
		ShardMergeDuration: time.Duration(pb.GetShardMergeDuration()),
	}

	return data.CreateRetentionPolicy(v.GetDatabase(), rpi, v.GetDefaultRP())
}

func ApplyDropRetentionPolicy(data *Data, cmd *proto2.Command) error {
	ext, _ := proto.GetExtension(cmd, proto2.E_DropRetentionPolicyCommand_Command)
	v, ok := ext.(*proto2.DropRetentionPolicyCommand)
	if !ok {
		DataLogger.Error("applyDropRetentionPolicy err")
	}
	return data.DropRetentionPolicy(v.GetDatabase(), v.GetName())
}

func ApplySetDefaultRetentionPolicy(data *Data, cmd *proto2.Command) error {
	ext, _ := proto.GetExtension(cmd, proto2.E_SetDefaultRetentionPolicyCommand_Command)
	v, ok := ext.(*proto2.SetDefaultRetentionPolicyCommand)
	if !ok {
		DataLogger.Error("applySetDefaultRetentionPolicy err")
	}
	return data.SetDefaultRetentionPolicy(v.GetDatabase(), v.GetName())
}

func ApplyUpdateRetentionPolicy(data *Data, cmd *proto2.Command) error {
	ext, _ := proto.GetExtension(cmd, proto2.E_UpdateRetentionPolicyCommand_Command)
	v, ok := ext.(*proto2.UpdateRetentionPolicyCommand)
	if !ok {
		DataLogger.Error("applyUpdateRetentionPolicy err")
	}
	// Create update object.
	rpu := RetentionPolicyUpdate{Name: v.NewName}
	rpu.Duration = GetDuration(v.Duration)
	rpu.HotDuration = GetDuration(v.HotDuration)
	if v.WarmDuration != nil {
		value := time.Duration(v.GetWarmDuration())
		rpu.WarmDuration = &value
	}
	if v.IndexColdDuration != nil {
		value := time.Duration(v.GetIndexColdDuration())
		rpu.IndexColdDuration = &value
	}
	rpu.IndexGroupDuration = GetDuration(v.IndexGroupDuration)
	rpu.ShardGroupDuration = GetDuration(v.ShardGroupDuration)
	if v.ReplicaN != nil {
		value := int(v.GetReplicaN())
		rpu.ReplicaN = &value
	}
	return data.UpdateRetentionPolicy(v.GetDatabase(), v.GetName(), &rpu, v.GetMakeDefault())
}

func ApplyCreateShardGroup(data *Data, cmd *proto2.Command) error {
	ext, _ := proto.GetExtension(cmd, proto2.E_CreateShardGroupCommand_Command)
	v, ok := ext.(*proto2.CreateShardGroupCommand)
	if !ok {
		DataLogger.Error("applyCreateShardGroup err")
	}
	return data.CreateShardGroup(v.GetDatabase(), v.GetPolicy(), time.Unix(0, v.GetTimestamp()), v.GetShardTier(),
		config.EngineType(v.GetEngineType()), v.GetVersion())
}

func ApplyDeleteShardGroup(data *Data, cmd *proto2.Command) error {
	ext, _ := proto.GetExtension(cmd, proto2.E_DeleteShardGroupCommand_Command)
	v, ok := ext.(*proto2.DeleteShardGroupCommand)
	if !ok {
		DataLogger.Error("applyDeleteShardGroup err")
	}
	return data.DeleteShardGroup(v.GetDatabase(), v.GetPolicy(), v.GetShardGroupID(), v.GetDeletedAt(), v.GetDeleteType())
}

func ApplyCreateSubscription(data *Data, cmd *proto2.Command) error {
	ext, _ := proto.GetExtension(cmd, proto2.E_CreateSubscriptionCommand_Command)
	v, ok := ext.(*proto2.CreateSubscriptionCommand)
	if !ok {
		DataLogger.Error("applyCreateSubscription err")
	}
	return data.CreateSubscription(v.GetDatabase(), v.GetRetentionPolicy(), v.GetName(), v.GetMode(), v.GetDestinations())
}

func ApplyDropSubscription(data *Data, cmd *proto2.Command) error {
	ext, _ := proto.GetExtension(cmd, proto2.E_DropSubscriptionCommand_Command)
	v, ok := ext.(*proto2.DropSubscriptionCommand)
	if !ok {
		DataLogger.Error("applyDropSubscription err")
	}
	return data.DropSubscription(v.GetDatabase(), v.GetRetentionPolicy(), v.GetName())
}

func ApplyCreateUser(data *Data, cmd *proto2.Command) error {
	ext, _ := proto.GetExtension(cmd, proto2.E_CreateUserCommand_Command)
	v, ok := ext.(*proto2.CreateUserCommand)
	if !ok {
		DataLogger.Error("applyCreateUser err")
	}
	err := data.CreateUser(v.GetName(), v.GetHash(), v.GetAdmin(), v.GetRwUser())
	DataLogger.Info("apply create user command", zap.String("userID", v.GetName()), zap.Error(err))
	return err
}

func ApplyDropUser(data *Data, cmd *proto2.Command) error {
	ext, _ := proto.GetExtension(cmd, proto2.E_DropUserCommand_Command)
	v, ok := ext.(*proto2.DropUserCommand)
	if !ok {
		DataLogger.Error("applyDropUser err")
	}
	err := data.DropUser(v.GetName())
	DataLogger.Info("apply drop user command", zap.String("userID", v.GetName()), zap.Error(err))
	return err
}

func ApplyUpdateUser(data *Data, cmd *proto2.Command) error {
	ext, _ := proto.GetExtension(cmd, proto2.E_UpdateUserCommand_Command)
	v, ok := ext.(*proto2.UpdateUserCommand)
	if !ok {
		DataLogger.Error("applyUpdateUser err")
	}
	return data.UpdateUser(v.GetName(), v.GetHash())
}

func ApplySetPrivilege(data *Data, cmd *proto2.Command) error {
	ext, _ := proto.GetExtension(cmd, proto2.E_SetPrivilegeCommand_Command)
	v, ok := ext.(*proto2.SetPrivilegeCommand)
	if !ok {
		DataLogger.Error("applySetPrivilege err")
	}
	err := data.SetPrivilege(v.GetUsername(), v.GetDatabase(), originql.Privilege(v.GetPrivilege()))
	DataLogger.Info("apply set privilege command", zap.String("userID", v.GetUsername()),
		zap.String("db", v.GetDatabase()), zap.Int32("privilege", v.GetPrivilege()), zap.Error(err))
	return err
}

func ApplySetAdminPrivilege(data *Data, cmd *proto2.Command) error {
	ext, _ := proto.GetExtension(cmd, proto2.E_SetAdminPrivilegeCommand_Command)
	v, ok := ext.(*proto2.SetAdminPrivilegeCommand)
	if !ok {
		DataLogger.Error("applySetAdminPrivilege err")
	}
	err := data.SetAdminPrivilege(v.GetUsername(), v.GetAdmin())
	DataLogger.Info("apply set admin privilege command", zap.String("userID", v.GetUsername()), zap.Error(err))
	return err
}

func ApplyCreateMetaNode(data *Data, cmd *proto2.Command) error {
	ext, _ := proto.GetExtension(cmd, proto2.E_CreateMetaNodeCommand_Command)
	v, ok := ext.(*proto2.CreateMetaNodeCommand)
	if !ok {
		DataLogger.Error("applyCreateMetaNode err")
	}
	err := data.CreateMetaNode(v.GetHTTPAddr(), v.GetRPCAddr(), v.GetTCPAddr())
	if err != nil {
		return err
	}
	data.ClusterID = v.GetRand()
	return nil
}

func ApplySetMetaNode(data *Data, cmd *proto2.Command) error {
	ext, _ := proto.GetExtension(cmd, proto2.E_SetMetaNodeCommand_Command)
	v, ok := ext.(*proto2.SetMetaNodeCommand)
	if !ok {
		DataLogger.Error("applySetMetaNode err")
	}
	err := data.SetMetaNode(v.GetHTTPAddr(), v.GetRPCAddr(), v.GetTCPAddr())
	if err != nil {
		return err
	}

	// If the cluster ID hasn't been set then use the command's random number.
	if data.ClusterID == 0 {
		data.ClusterID = v.GetRand()
	}

	return nil
}

func ApplyDeleteMetaNode(data *Data, cmd *proto2.Command) error {
	ext, _ := proto.GetExtension(cmd, proto2.E_DeleteMetaNodeCommand_Command)
	v, ok := ext.(*proto2.DeleteMetaNodeCommand)
	if !ok {
		DataLogger.Error("applyDeleteMetaNode err")
	}
	return data.DeleteMetaNode(v.GetID())
}

func ApplyCreateDataNode(data *Data, cmd *proto2.Command) error {
	ext, _ := proto.GetExtension(cmd, proto2.E_CreateDataNodeCommand_Command)
	v, ok := ext.(*proto2.CreateDataNodeCommand)
	if !ok {
		DataLogger.Error("applyCreateDataNode err")
	}
	dataNode := data.DataNodeByHttpHost(v.GetHTTPAddr())
	if dataNode != nil {
		if AzHard == repDisPolicy && v.GetAz() != dataNode.Az {
			return ErrAzChange
		}
		data.MaxConnID++
		dataNode.ConnID = data.MaxConnID
		return nil
	}

	_, err := data.CreateDataNode(v.GetHTTPAddr(), v.GetTCPAddr(), v.GetRole(), v.GetAz())
	return err
}

func ApplyDeleteDataNode(data *Data, cmd *proto2.Command) error {
	ext, _ := proto.GetExtension(cmd, proto2.E_DeleteDataNodeCommand_Command)
	v, ok := ext.(*proto2.DeleteDataNodeCommand)
	if !ok {
		DataLogger.Error("applyDeleteDataNode err")
	}
	return data.DeleteDataNode(v.GetID())
}

func ApplyMarkDatabaseDelete(data *Data, cmd *proto2.Command) error {
	ext, _ := proto.GetExtension(cmd, proto2.E_MarkDatabaseDeleteCommand_Command)
	v, ok := ext.(*proto2.MarkDatabaseDeleteCommand)
	if !ok {
		panic(fmt.Errorf("%s is not a MarkDatabaseDeleteCommand", ext))
	}
	return data.MarkDatabaseDelete(v.GetName())
}

func ApplyMarkRetentionPolicyDelete(data *Data, cmd *proto2.Command) error {
	ext, _ := proto.GetExtension(cmd, proto2.E_MarkRetentionPolicyDeleteCommand_Command)
	v, ok := ext.(*proto2.MarkRetentionPolicyDeleteCommand)
	if !ok {
		DataLogger.Error("applyMarkRetentionPolicyDelete err")
	}
	return data.MarkRetentionPolicyDelete(v.GetDatabase(), v.GetName())
}

func ApplyCreateMeasurement(data *Data, cmd *proto2.Command) error {
	ext, _ := proto.GetExtension(cmd, proto2.E_CreateMeasurementCommand_Command)
	v, ok := ext.(*proto2.CreateMeasurementCommand)
	if !ok {
		panic(fmt.Errorf("%s is not a CreateMeasurementCommand", ext))
	}
	return data.CreateMeasurement(v.GetDBName(), v.GetRpName(), v.GetName(), v.GetSki(), v.GetInitNumOfShards(), v.GetIR(), config.EngineType(v.GetEngineType()),
		v.GetColStoreInfo(), v.GetSchemaInfo(), v.GetOptions())
}

func ApplyReSharding(data *Data, cmd *proto2.Command) error {
	ext, _ := proto.GetExtension(cmd, proto2.E_ReShardingCommand_Command)
	v, ok := ext.(*proto2.ReShardingCommand)
	if !ok {
		DataLogger.Error("applyReSharding err")
	}
	info := &ReShardingInfo{
		Database:     v.GetDatabase(),
		Rp:           v.GetRpName(),
		ShardGroupID: v.GetShardGroupID(),
		SplitTime:    v.GetSplitTime(),
		Bounds:       v.GetShardBounds(),
	}
	return data.ReSharding(info)
}

func ApplyUpdateSchema(data *Data, cmd *proto2.Command) error {
	ext, _ := proto.GetExtension(cmd, proto2.E_UpdateSchemaCommand_Command)
	v, ok := ext.(*proto2.UpdateSchemaCommand)
	if !ok {
		panic(fmt.Errorf("%s is not a UpdateSchemaCommand", ext))
	}

	return data.UpdateSchema(v.GetDatabase(), v.GetRpName(), v.GetMeasurement(), v.GetFieldToCreate())
}

func ApplyAlterShardKey(data *Data, cmd *proto2.Command) error {
	ext, _ := proto.GetExtension(cmd, proto2.E_AlterShardKeyCmd_Command)
	v, ok := ext.(*proto2.AlterShardKeyCmd)
	if !ok {
		panic(fmt.Errorf("%s is not a AlterShardKeyCmd", ext))
	}
	return data.AlterShardKey(v.GetDBName(), v.GetRpName(), v.GetName(), v.GetSki())
}

func ApplyPruneGroups(data *Data, cmd *proto2.Command) error {
	ext, _ := proto.GetExtension(cmd, proto2.E_PruneGroupsCommand_Command)
	v, ok := ext.(*proto2.PruneGroupsCommand)
	if !ok {
		DataLogger.Error("applyPruneGroups err")
	}
	return data.PruneGroups(v.GetShardGroup(), v.GetID())
}

func ApplyMarkMeasurementDelete(data *Data, cmd *proto2.Command) error {
	ext, _ := proto.GetExtension(cmd, proto2.E_MarkMeasurementDeleteCommand_Command)
	v, ok := ext.(*proto2.MarkMeasurementDeleteCommand)
	if !ok {
		panic(fmt.Errorf("%s is not a MarkMeasurementDeleteCommand", ext))
	}
	return data.MarkMeasurementDelete(v.GetDatabase(), v.GetPolicy(), v.GetMeasurement())
}

func ApplyDropMeasurement(data *Data, cmd *proto2.Command) error {
	ext, _ := proto.GetExtension(cmd, proto2.E_DropMeasurementCommand_Command)
	v, ok := ext.(*proto2.DropMeasurementCommand)
	if !ok {
		panic(fmt.Errorf("%s is not a DropMeasurementCommand", ext))
	}
	return data.DropMeasurement(v.GetDatabase(), v.GetPolicy(), v.GetMeasurement())
}

func ApplyDeleteIndexGroup(data *Data, cmd *proto2.Command) error {
	ext, _ := proto.GetExtension(cmd, proto2.E_DeleteIndexGroupCommand_Command)
	v, ok := ext.(*proto2.DeleteIndexGroupCommand)
	if !ok {
		DataLogger.Error("applyDeleteIndexGroup err")
	}
	return data.DeleteIndexGroup(v.GetDatabase(), v.GetPolicy(), v.GetIndexGroupID())
}

func ApplyUpdateShardInfoTier(data *Data, cmd *proto2.Command) error {
	ext, _ := proto.GetExtension(cmd, proto2.E_UpdateShardInfoTierCommand_Command)
	v, ok := ext.(*proto2.UpdateShardInfoTierCommand)
	if !ok {
		DataLogger.Error("applyUpdateShardInfoTier err")
	}
	return data.UpdateShardInfoTier(v.GetShardID(), v.GetTier(), v.GetDbName(), v.GetRpName())
}

func ApplyUpdateIndexInfoTier(data *Data, cmd *proto2.Command) error {
	ext, _ := proto.GetExtension(cmd, proto2.E_UpdateIndexInfoTierCommand_Command)
	v, ok := ext.(*proto2.UpdateIndexInfoTierCommand)
	if !ok {
		DataLogger.Error("applyUpdateIndexInfoTier err")
	}
	return data.UpdateIndexInfoTier(v.GetIndexID(), v.GetTier(), v.GetDbName(), v.GetRpName())
}

func ApplyUpdateNodeStatus(data *Data, cmd *proto2.Command) error {
	ext, _ := proto.GetExtension(cmd, proto2.E_UpdateNodeStatusCommand_Command)
	v, ok := ext.(*proto2.UpdateNodeStatusCommand)
	if !ok {
		DataLogger.Error("applyUpdateNodeStatus err")
	}
	return data.UpdateNodeStatus(v.GetID(), v.GetStatus(), v.GetLtime(), v.GetGossipAddr())
}

func ApplyUpdatePtInfo(data *Data, cmd *proto2.Command) error {
	ext, _ := proto.GetExtension(cmd, proto2.E_UpdatePtInfoCommand_Command)
	v, ok := ext.(*proto2.UpdatePtInfoCommand)
	if !ok {
		DataLogger.Error("applyUpdatePtInfo err")
	}
	return data.UpdatePtInfo(v.GetDb(), v.GetPt(), v.GetOwnerNode(), v.GetStatus())
}

func ApplyCreateDownSample(data *Data, cmd *proto2.Command) error {
	ext, _ := proto.GetExtension(cmd, proto2.E_CreateDownSamplePolicyCommand_Command)
	v, ok := ext.(*proto2.CreateDownSamplePolicyCommand)
	if !ok {
		DataLogger.Error("applyCreateDownSample err")
	}
	pb := v.GetDownSamplePolicyInfo()

	rpi := &DownSamplePolicyInfo{}
	rpi.Unmarshal(pb)
	if rpi.IsNil() {
		rpi = nil
	}
	return data.CreateDownSamplePolicy(v.GetDatabase(), v.GetName(), rpi)
}

func ApplyDropDownSample(data *Data, cmd *proto2.Command) error {
	ext, _ := proto.GetExtension(cmd, proto2.E_DropDownSamplePolicyCommand_Command)
	v, ok := ext.(*proto2.DropDownSamplePolicyCommand)
	if !ok {
		DataLogger.Error("applyDropDownSample err")
	}
	data.DropDownSamplePolicy(v.GetDatabase(), v.GetRpName(), v.GetDropAll())
	return nil
}

func ApplyCreateDbPtViewCommand(data *Data, cmd *proto2.Command) error {
	ext, _ := proto.GetExtension(cmd, proto2.E_CreateDbPtViewCommand_Command)
	v, ok := ext.(*proto2.CreateDbPtViewCommand)
	if !ok {
		panic(fmt.Errorf("%s is not a CreateDbPtViewCommand", ext))
	}
	var err error
	if ok, err = data.CreateDBPtView(v.GetDbName()); err != nil {
		return err
	}
	if ok {
		return data.CreateDBReplication(v.GetDbName(), v.GetReplicaNum())
	}
	return nil
}

func ApplyUpdateShardDownSampleInfo(data *Data, cmd *proto2.Command) error {
	ext, _ := proto.GetExtension(cmd, proto2.E_UpdateShardDownSampleInfoCommand_Command)
	v, ok := ext.(*proto2.UpdateShardDownSampleInfoCommand)
	if !ok {
		DataLogger.Error("applyUpdateShardDownSampleInfo err")
	}
	ident := &ShardIdentifier{}
	ident.Unmarshal(v.GetIdent())
	return data.UpdateShardDownSampleInfo(ident)
}

func ApplyCreateStream(data *Data, cmd *proto2.Command) error {
	ext, _ := proto.GetExtension(cmd, proto2.E_CreateStreamCommand_Command)
	v, ok := ext.(*proto2.CreateStreamCommand)
	if !ok {
		DataLogger.Error("applyCreateStream err")
	}
	pb := v.GetStreamInfo()
	rpi := &StreamInfo{}
	rpi.Unmarshal(pb)

	return data.CreateStream(rpi)
}

func ApplyDropStream(data *Data, cmd *proto2.Command) error {
	ext, _ := proto.GetExtension(cmd, proto2.E_DropStreamCommand_Command)
	v, ok := ext.(*proto2.DropStreamCommand)
	if !ok {
		DataLogger.Error("applyDropStream err")
	}
	name := v.GetName()
	return data.DropStream(name)
}

func ApplyUpdatePtVersion(data *Data, cmd *proto2.Command) error {
	ext, _ := proto.GetExtension(cmd, proto2.E_UpdatePtVersionCommand_Command)
	v, ok := ext.(*proto2.UpdatePtVersionCommand)
	if !ok {
		panic(fmt.Errorf("%s is not a UpdatePtVersionCommand", ext))
	}
	_ = data.UpdatePtVersion(v.GetDb(), v.GetPt())
	return nil
}

func ApplyRegisterQueryIDOffset(data *Data, cmd *proto2.Command) error {
	ext, _ := proto.GetExtension(cmd, proto2.E_RegisterQueryIDOffsetCommand_Command)
	v, ok := ext.(*proto2.RegisterQueryIDOffsetCommand)
	if !ok {
		panic(fmt.Errorf("%s is not a RegisterQueryIDOffsetCommand", ext))
	}
	err := data.RegisterQueryIDOffset(SQLHost(v.GetHost()))
	return err
}

func ApplyContinuousQueryReport(data *Data, cmd *proto2.Command) error {
	ext, _ := proto.GetExtension(cmd, proto2.E_ContinuousQueryReportCommand_Command)
	v, ok := ext.(*proto2.ContinuousQueryReportCommand)
	if !ok {
		panic(fmt.Errorf("%s is not a ContinuousQueryReportCommand", ext))
	}

	return data.BatchUpdateContinuousQueryStat(v.GetCQStates())
}

func ApplySetNodeSegregateStatus(data *Data, cmd *proto2.Command) error {
	ext, _ := proto.GetExtension(cmd, proto2.E_SetNodeSegregateStatusCommand_Command)
	v, ok := ext.(*proto2.SetNodeSegregateStatusCommand)
	if !ok {
		panic(fmt.Errorf("%s is not a SetNodeSegregateStatusCommand", ext))
	}
	nodeIds := v.GetNodeIds()
	status := v.GetStatus()
	data.SetSegregateNodeStatus(status, nodeIds)
	return nil
}

func ApplyRemoveNode(data *Data, cmd *proto2.Command) error {
	ext, _ := proto.GetExtension(cmd, proto2.E_RemoveNodeCommand_Command)
	v, ok := ext.(*proto2.RemoveNodeCommand)
	if !ok {
		panic(fmt.Errorf("%s is not a RemoveNodeCommand", ext))
	}
	nodeIds := v.GetNodeIds()
	data.RemoveNode(nodeIds)
	return nil
}

func ApplyUpdateReplication(data *Data, cmd *proto2.Command, metaTransferLeadership func(string, uint32, uint32, uint32)) error {
	ext, _ := proto.GetExtension(cmd, proto2.E_UpdateReplicationCommand_Command)
	v, ok := ext.(*proto2.UpdateReplicationCommand)
	if !ok {
		panic(fmt.Errorf("%s is not a UpdateReplicationCommand", ext))
	}
	db := v.GetDatabase()
	rgId := v.GetRepGroupId()
	peers := v.GetPeers()
	masterId := v.GetMasterId()
	oldMasterPtId, err := data.UpdateReplication(db, rgId, masterId, peers)
	if metaTransferLeadership != nil && err == nil {
		// TransferLeader and updateMasterPt are updated asynchronously.
		go metaTransferLeadership(db, rgId, oldMasterPtId, masterId)
	}
	return err
}

func ApplyUpdateMeasurement(data *Data, cmd *proto2.Command) error {
	ext, _ := proto.GetExtension(cmd, proto2.E_UpdateMeasurementCommand_Command)
	v, ok := ext.(*proto2.UpdateMeasurementCommand)
	if !ok {
		panic(fmt.Errorf("%s is not a UpdateMeasurementCommand", ext))
	}
	return data.UpdateMeasurement(v.GetDb(), v.GetRp(), v.GetMst(), v.GetOptions())
}

func ApplyReplaceMergeShards(data *Data, cmd *proto2.Command) error {
	ext, _ := proto.GetExtension(cmd, proto2.E_ReplaceMergeShardsCommand_Command)
	v, ok := ext.(*proto2.ReplaceMergeShardsCommand)
	if !ok {
		return fmt.Errorf("%s is not a ReplaceMergeShardsCommand", ext)
	}
	return data.ReplaceMergeShards(v.GetDb(), v.GetRp(), v.GetPtId(), v.ShardId)
}

func ApplyRecoverMetaData(data *Data, cmd *proto2.Command) error {
	ext, _ := proto.GetExtension(cmd, proto2.E_RecoverMetaDataCommand_Command)
	v, ok := ext.(*proto2.RecoverMetaDataCommand)
	if !ok {
		return fmt.Errorf("%s is not a RecoverMetaDataCommand", ext)
	}

	databases := v.GetDatabases()
	m := v.GetMetaData()
	nodeMap := v.GetNodeMap()

	metaData := &Data{}
	err := json.Unmarshal([]byte(m), metaData)
	if err != nil {
		return err
	}

	once := sync.Once{}
	if len(databases) == 0 {
		err = data.RecoverData(metaData, nodeMap)
	} else {
		for _, d := range databases {
			iErr := data.RecoverDataBase(d, metaData, nodeMap)
			if iErr != nil {
				once.Do(func() {
					err = iErr
				})
			}
		}
	}
	return err
}
