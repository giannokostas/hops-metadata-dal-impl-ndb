/*
 * Hops Database abstraction layer for storing the hops metadata in MySQL Cluster
 * Copyright (C) 2015  hops.io
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package io.hops.metadata.ndb;

import io.hops.DalStorageFactory;
import io.hops.StorageConnector;
import io.hops.exception.StorageInitializtionException;
import io.hops.metadata.common.EntityDataAccess;
import io.hops.metadata.election.dal.HdfsLeDescriptorDataAccess;
import io.hops.metadata.election.dal.YarnLeDescriptorDataAccess;
import io.hops.metadata.hdfs.dal.*;
import io.hops.metadata.ndb.dalimpl.election.HdfsLeaderClusterj;
import io.hops.metadata.ndb.dalimpl.election.YarnLeaderClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.*;
import io.hops.metadata.ndb.dalimpl.yarn.*;
import io.hops.metadata.ndb.dalimpl.yarn.capacity.CSLeafQueuesPendingAppsClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.capacity.FiCaSchedulerAppReservedContainersClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.fair.FSSchedulerNodeClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.fair.LocalityLevelClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.fair.PreemptionMapClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.fair.RunnableAppsClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.rmstatestore.*;
import io.hops.metadata.ndb.mysqlserver.MysqlServerConnector;
import io.hops.metadata.yarn.dal.*;
import io.hops.metadata.yarn.dal.capacity.CSLeafQueuesPendingAppsDataAccess;
import io.hops.metadata.yarn.dal.capacity.FiCaSchedulerAppReservedContainersDataAccess;
import io.hops.metadata.yarn.dal.fair.FSSchedulerNodeDataAccess;
import io.hops.metadata.yarn.dal.fair.LocalityLevelDataAccess;
import io.hops.metadata.yarn.dal.fair.PreemptionMapDataAccess;
import io.hops.metadata.yarn.dal.fair.RunnableAppsDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class NdbStorageFactory implements DalStorageFactory {

  private Map<Class, EntityDataAccess> dataAccessMap =
      new HashMap<Class, EntityDataAccess>();

  @Override
  public void setConfiguration(Properties conf)
          throws StorageInitializtionException {
    try {
      ClusterjConnector.getInstance().setConfiguration(conf);
      MysqlServerConnector.getInstance().setConfiguration(conf);
      initDataAccessMap();
    } catch (IOException ex) {
      throw new StorageInitializtionException(ex);
    }
  }

  private void initDataAccessMap() {
    dataAccessMap
            .put(RMStateVersionDataAccess.class, new RMStateVersionClusterJ());
    dataAccessMap
            .put(ApplicationStateDataAccess.class, new ApplicationStateClusterJ());
    dataAccessMap.put(UpdatedNodeDataAccess.class, new UpdatedNodeClusterJ());
    dataAccessMap.put(ApplicationAttemptStateDataAccess.class,
            new ApplicationAttemptStateClusterJ());
    dataAccessMap.put(RanNodeDataAccess.class, new RanNodeClusterJ());
    dataAccessMap
            .put(DelegationTokenDataAccess.class, new DelegationTokenClusterJ());
    dataAccessMap
            .put(SequenceNumberDataAccess.class, new SequenceNumberClusterJ());
    dataAccessMap
            .put(DelegationKeyDataAccess.class, new DelegationKeyClusterJ());
    dataAccessMap
            .put(YarnVariablesDataAccess.class, new YarnVariablesClusterJ());
    dataAccessMap.put(RPCDataAccess.class, new RPCClusterJ());
    dataAccessMap.put(HeartBeatRPCDataAccess.class, new HeartBeatRPCClusterJ());
    dataAccessMap.put(AllocateRPCDataAccess.class, new AllocateRPCClusterJ());
    dataAccessMap.put(QueueMetricsDataAccess.class, new QueueMetricsClusterJ());
    dataAccessMap.put(FiCaSchedulerNodeDataAccess.class,
            new FiCaSchedulerNodeClusterJ());
    dataAccessMap.put(ResourceDataAccess.class, new ResourceClusterJ());
    dataAccessMap.put(NodeDataAccess.class, new NodeClusterJ());
    dataAccessMap.put(ResourceDataAccess.class, new ResourceClusterJ());
    dataAccessMap.put(RMNodeDataAccess.class, new RMNodeClusterJ());
    dataAccessMap.put(RMContextActiveNodesDataAccess.class,
            new RMContextActiveNodesClusterJ());
    dataAccessMap.put(RMContextInactiveNodesDataAccess.class,
            new RMContextInactiveNodesClusterJ());
    dataAccessMap
            .put(ContainerStatusDataAccess.class, new ContainerStatusClusterJ());
    dataAccessMap
            .put(ContainersLogsDataAccess.class, new ContainersLogsClusterJ());
    dataAccessMap
            .put(NodeHBResponseDataAccess.class, new NodeHBResponseClusterJ());
    dataAccessMap.put(UpdatedContainerInfoDataAccess.class,
            new UpdatedContainerInfoClusterJ());
    dataAccessMap.put(ContainerIdToCleanDataAccess.class,
            new ContainerIdToCleanClusterJ());
    dataAccessMap.put(JustLaunchedContainersDataAccess.class,
            new JustLaunchedContainersClusterJ());
    dataAccessMap.put(LaunchedContainersDataAccess.class,
            new LaunchedContainersClusterJ());
    dataAccessMap.put(FinishedApplicationsDataAccess.class,
            new FinishedApplicationsClusterJ());
    dataAccessMap.put(SchedulerApplicationDataAccess.class,
            new SchedulerApplicationClusterJ());
    dataAccessMap.put(FiCaSchedulerAppSchedulingOpportunitiesDataAccess.class,
            new FiCaSchedulerAppSchedulingOpportunitiesClusterJ());
    dataAccessMap.put(FiCaSchedulerAppLastScheduledContainerDataAccess.class,
            new FiCaSchedulerAppLastScheduledContainerClusterJ());
    dataAccessMap.put(FiCaSchedulerAppReservedContainersDataAccess.class,
            new FiCaSchedulerAppReservedContainersClusterJ());
    dataAccessMap.put(FiCaSchedulerAppReservationsDataAccess.class,
            new FiCaSchedulerAppReservationsClusterJ());
    dataAccessMap.put(RMContainerDataAccess.class, new RMContainerClusterJ());
    dataAccessMap.put(ContainerDataAccess.class, new ContainerClusterJ());
    dataAccessMap.put(AppSchedulingInfoDataAccess.class,
            new AppSchedulingInfoClusterJ());
    dataAccessMap.put(AppSchedulingInfoBlacklistDataAccess.class,
            new AppSchedulingInfoBlacklistClusterJ());
    dataAccessMap
            .put(ResourceRequestDataAccess.class, new ResourceRequestClusterJ());
    dataAccessMap.put(BlockInfoDataAccess.class, new BlockInfoClusterj());
    dataAccessMap.put(PendingBlockDataAccess.class, new PendingBlockClusterj());
    dataAccessMap.put(ReplicaUnderConstructionDataAccess.class,
            new ReplicaUnderConstructionClusterj());
    dataAccessMap.put(INodeDataAccess.class, new INodeClusterj());
    dataAccessMap
            .put(INodeAttributesDataAccess.class, new INodeAttributesClusterj());
    dataAccessMap.put(LeaseDataAccess.class, new LeaseClusterj());
    dataAccessMap.put(LeasePathDataAccess.class, new LeasePathClusterj());
    dataAccessMap.put(OngoingSubTreeOpsDataAccess.class, new OnGoingSubTreeOpsClusterj());
    dataAccessMap
            .put(HdfsLeDescriptorDataAccess.class, new HdfsLeaderClusterj());
    dataAccessMap
            .put(YarnLeDescriptorDataAccess.class, new YarnLeaderClusterj());
    dataAccessMap.put(ReplicaDataAccess.class, new ReplicaClusterj());
    dataAccessMap
            .put(CorruptReplicaDataAccess.class, new CorruptReplicaClusterj());
    dataAccessMap
            .put(ExcessReplicaDataAccess.class, new ExcessReplicaClusterj());
    dataAccessMap
            .put(InvalidateBlockDataAccess.class, new InvalidatedBlockClusterj());
    dataAccessMap.put(UnderReplicatedBlockDataAccess.class,
            new UnderReplicatedBlockClusterj());
    dataAccessMap.put(VariableDataAccess.class, new VariableClusterj());
    dataAccessMap.put(StorageIdMapDataAccess.class, new StorageIdMapClusterj());
    dataAccessMap
            .put(EncodingStatusDataAccess.class, new EncodingStatusClusterj() {
            });
    dataAccessMap.put(BlockLookUpDataAccess.class, new BlockLookUpClusterj());
    dataAccessMap
            .put(FSSchedulerNodeDataAccess.class, new FSSchedulerNodeClusterJ());
    dataAccessMap.put(SafeBlocksDataAccess.class, new SafeBlocksClusterj());
    dataAccessMap.put(MisReplicatedRangeQueueDataAccess.class,
            new MisReplicatedRangeQueueClusterj());
    dataAccessMap.put(QuotaUpdateDataAccess.class, new QuotaUpdateClusterj());
    dataAccessMap.put(SecretMamagerKeysDataAccess.class,
            new SecretMamagerKeysClusterJ());
    dataAccessMap
            .put(AllocateResponseDataAccess.class, new AllocateResponseClusterJ());
    dataAccessMap
            .put(AllocatedContainersDataAccess.class, new AllocatedContainersClusterJ());
    dataAccessMap.
            put(CompletedContainersStatusDataAccess.class,
                    new CompletedContainersStatusClusterJ());
    dataAccessMap.put(PendingEventDataAccess.class, new PendingEventClusterJ());
    dataAccessMap
            .put(BlockChecksumDataAccess.class, new BlockChecksumClusterj());
    dataAccessMap
            .put(NextHeartbeatDataAccess.class, new NextHeartbeatClusterJ());
    dataAccessMap.put(RMLoadDataAccess.class, new RMLoadClusterJ());
    dataAccessMap.put(FullRMNodeDataAccess.class, new FullRMNodeClusterJ());
    dataAccessMap.put(MetadataLogDataAccess.class, new MetadataLogClusterj());
    dataAccessMap.put(AccessTimeLogDataAccess.class,
            new AccessTimeLogClusterj());
    dataAccessMap.put(SizeLogDataAccess.class, new SizeLogClusterj());
    dataAccessMap.put(EncodingJobsDataAccess.class, new EncodingJobsClusterj());
    dataAccessMap.put(RepairJobsDataAccess.class, new RepairJobsClusterj());
    dataAccessMap.
            put(LocalityLevelDataAccess.class, new LocalityLevelClusterJ());
    dataAccessMap.put(RunnableAppsDataAccess.class, new RunnableAppsClusterJ());
    dataAccessMap.
            put(PreemptionMapDataAccess.class, new PreemptionMapClusterJ());
    dataAccessMap.put(UserDataAccess.class, new UserClusterj());
    dataAccessMap.put(GroupDataAccess.class, new GroupClusterj());
    dataAccessMap.put(UserGroupDataAccess.class, new UserGroupClusterj());
    dataAccessMap.put(CSLeafQueuesPendingAppsDataAccess.class,
            new CSLeafQueuesPendingAppsClusterJ());
    // Quota Scheduling
    dataAccessMap.put(YarnProjectsQuotaDataAccess.class, new YarnProjectsQuotaClusterJ());
    dataAccessMap.put(YarnProjectsDailyCostDataAccess.class, new YarnProjectsDailyCostClusterJ());
    dataAccessMap.put(ContainersCheckPointsDataAccess.class, new ContainersCheckPointsClusterJ());
    
    dataAccessMap.put(YarnRunningPriceDataAccess.class, new YarnRunningPriceClusterJ());
    dataAccessMap.put(YarnHistoryPriceDataAccess.class, new YarnHistoryPriceClusterJ());
    dataAccessMap.put(YarnApplicationResourcesDataAccess.class, new YarnApplicationResourcesClusterJ());
  }

  @Override
  public StorageConnector getConnector() {
    return ClusterjConnector.getInstance();
  }

  @Override
  public EntityDataAccess getDataAccess(Class type) {
    return dataAccessMap.get(type);
  }
  }
