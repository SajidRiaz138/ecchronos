/*
 * Copyright 2024 Telefonaktiebolaget LM Ericsson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair.scheduler;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.core.impl.locks.RepairLockType;
import com.ericsson.bss.cassandra.ecchronos.core.impl.metrics.CassandraMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.TestUtils;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.incremental.IncrementalRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.vnode.VnodeRepairStatesImpl;
import com.ericsson.bss.cassandra.ecchronos.core.impl.table.TableRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.RepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduleManager;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledRepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairState;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairStateFactory;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairStateSnapshot;
import com.ericsson.bss.cassandra.ecchronos.core.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.state.VnodeRepairState;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableStorageStates;
import com.ericsson.bss.cassandra.ecchronos.data.repairhistory.RepairHistoryService;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType;
import com.google.common.collect.ImmutableSet;
import java.util.*;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.TimeUnit;

import static com.ericsson.bss.cassandra.ecchronos.core.impl.table.MockTableReferenceFactory.tableReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@RunWith (MockitoJUnitRunner.class)
public class TestRepairSchedulerImpl
{
    private static final TableReference TABLE_REFERENCE1 = tableReference("keyspace", "table1");
    private static final TableReference TABLE_REFERENCE2 = tableReference("keyspace", "table2");
    private static final VnodeRepairState VNODE_REPAIR_STATE = TestUtils.createVnodeRepairState(1, 2, ImmutableSet.of(), System.currentTimeMillis());


    @Mock
    private DistributedJmxProxyFactory jmxProxyFactory;

    @Mock
    private ScheduleManager scheduleManager;

    @Mock
    private TableRepairMetrics myTableRepairMetrics;

    @Mock
    private ReplicationState myReplicationState;

    @Mock
    private CassandraMetrics myCassandraMetrics;

    @Mock
    private Node mockNode;

    @Mock
    private RepairStateFactory myRepairStateFactory;

    @Mock
    private RepairState myRepairState;

    @Mock
    private RepairStateSnapshot myRepairStateSnapshot;

    @Mock
    private TableStorageStates myTableStorageStates;

    @Mock
    private RepairHistoryService myRepairHistory;

    private final UUID mockNodeID = UUID.randomUUID();

    @Before
    public void setup()
    {
        when(mockNode.getHostId()).thenReturn(mockNodeID);
        when(myRepairState.getSnapshot()).thenReturn(myRepairStateSnapshot);
        when(myRepairStateFactory.create(eq(mockNode), eq(TABLE_REFERENCE1), any(), any())).thenReturn(myRepairState);
        when(myRepairStateFactory.create(eq(mockNode), eq(TABLE_REFERENCE2), any(), any())).thenReturn(myRepairState);
        VnodeRepairStatesImpl vnodeRepairStates = VnodeRepairStatesImpl.newBuilder(Arrays.asList(VNODE_REPAIR_STATE)).build();
        when(myRepairStateSnapshot.getVnodeRepairStates()).thenReturn(vnodeRepairStates);
    }

    @Test
    public void testConfigureNewTable()
    {
        RepairSchedulerImpl repairSchedulerImpl = defaultRepairSchedulerImplBuilder().build();

        repairSchedulerImpl.putConfigurations(mockNode, TABLE_REFERENCE1,
                Collections.singleton(RepairConfiguration.DEFAULT));

        verify(scheduleManager, timeout(1000)).schedule(eq(mockNodeID), any(ScheduledJob.class));
        verify(scheduleManager, never()).deschedule(eq(mockNodeID), any(ScheduledJob.class));
        verify(myRepairStateFactory).create(eq(mockNode), eq(TABLE_REFERENCE1), eq(RepairConfiguration.DEFAULT), any());
        verify(myRepairState, atLeastOnce()).update();
        assertOneTableViewExist(repairSchedulerImpl, TABLE_REFERENCE1, RepairConfiguration.DEFAULT);

        repairSchedulerImpl.close();
        verify(scheduleManager).deschedule(eq(mockNodeID), any(ScheduledJob.class));

        verifyNoMoreInteractions(ignoreStubs(myTableRepairMetrics));
        verifyNoMoreInteractions(myRepairStateFactory);
        verifyNoMoreInteractions(scheduleManager);
    }

    @Test
    public void testConfigureTwoTables()
    {
        RepairSchedulerImpl repairSchedulerImpl = defaultRepairSchedulerImplBuilder().build();

        repairSchedulerImpl.putConfigurations(mockNode, TABLE_REFERENCE1, Collections.singleton(RepairConfiguration.DEFAULT));
        repairSchedulerImpl.putConfigurations(mockNode, TABLE_REFERENCE2, Collections.singleton(RepairConfiguration.DEFAULT));

        verify(scheduleManager, timeout(1000).times(2)).schedule(eq(mockNodeID), any(ScheduledJob.class));
        verify(scheduleManager, never()).deschedule(eq(mockNodeID), any(ScheduledJob.class));
        verify(myRepairStateFactory).create(eq(mockNode), eq(TABLE_REFERENCE1), eq(RepairConfiguration.DEFAULT), any());
        verify(myRepairStateFactory).create(eq(mockNode), eq(TABLE_REFERENCE2), eq(RepairConfiguration.DEFAULT), any());
        verify(myRepairState, atLeastOnce()).update();

        repairSchedulerImpl.close();
        verify(scheduleManager, times(1)).deschedule(eq(mockNodeID), any(ScheduledJob.class));

        verifyNoMoreInteractions(ignoreStubs(myTableRepairMetrics));
        verifyNoMoreInteractions(myRepairStateFactory);
        verifyNoMoreInteractions(scheduleManager);
    }

    @Test
    public void testRemoveTableConfiguration()
    {
        RepairSchedulerImpl repairSchedulerImpl = defaultRepairSchedulerImplBuilder().build();

        repairSchedulerImpl.putConfigurations(mockNode, TABLE_REFERENCE1, Collections.singleton(RepairConfiguration.DEFAULT));

        verify(scheduleManager, timeout(1000)).schedule(eq(mockNodeID), any(ScheduledJob.class));
        verify(scheduleManager, never()).deschedule(eq(mockNodeID), any(ScheduledJob.class));
        verify(myRepairStateFactory).create(eq(mockNode), eq(TABLE_REFERENCE1), eq(RepairConfiguration.DEFAULT), any());
        verify(myRepairState, atLeastOnce()).update();
        assertOneTableViewExist(repairSchedulerImpl, TABLE_REFERENCE1, RepairConfiguration.DEFAULT);

        repairSchedulerImpl.removeConfiguration(mockNode, TABLE_REFERENCE1);
        verify(scheduleManager, timeout(1000)).deschedule(eq(mockNodeID), any(ScheduledJob.class));
        assertThat(repairSchedulerImpl.getCurrentRepairJobs()).isEmpty();

        repairSchedulerImpl.close();
        verifyNoMoreInteractions(ignoreStubs(myTableRepairMetrics));
        verifyNoMoreInteractions(myRepairStateFactory);
        verifyNoMoreInteractions(scheduleManager);
    }

    @Test
    public void testUpdateTableConfiguration()
    {
        RepairSchedulerImpl repairSchedulerImpl = defaultRepairSchedulerImplBuilder().build();

        long expectedUpdatedRepairInterval = TimeUnit.DAYS.toMillis(1);

        RepairConfiguration updatedRepairConfiguration = RepairConfiguration.newBuilder()
                .withRepairInterval(expectedUpdatedRepairInterval, TimeUnit.MILLISECONDS)
                .build();

        repairSchedulerImpl.putConfigurations(mockNode, TABLE_REFERENCE1, Collections.singleton(RepairConfiguration.DEFAULT));

        verify(scheduleManager, timeout(1000)).schedule(eq(mockNodeID), any(ScheduledJob.class));
        verify(scheduleManager, never()).deschedule(eq(mockNodeID), any(ScheduledJob.class));
        verify(myRepairStateFactory).create(eq(mockNode), eq(TABLE_REFERENCE1), eq(RepairConfiguration.DEFAULT), any());
        verify(myRepairState, atLeastOnce()).update();
        assertOneTableViewExist(repairSchedulerImpl, TABLE_REFERENCE1, RepairConfiguration.DEFAULT);

        repairSchedulerImpl.putConfigurations(mockNode, TABLE_REFERENCE1,
                Collections.singleton(updatedRepairConfiguration));

        verify(scheduleManager, timeout(1000).times(2)).schedule(eq(mockNodeID), any(ScheduledJob.class));
        verify(scheduleManager, timeout(1000)).deschedule(eq(mockNodeID), any(ScheduledJob.class));
        verify(myRepairStateFactory).create(eq(mockNode), eq(TABLE_REFERENCE1), eq(updatedRepairConfiguration), any());
        verify(myRepairState, atLeastOnce()).update();
        assertOneTableViewExist(repairSchedulerImpl, TABLE_REFERENCE1, updatedRepairConfiguration);

        repairSchedulerImpl.close();
        verify(scheduleManager, times(2)).deschedule(eq(mockNodeID), any(ScheduledJob.class));
        assertThat(repairSchedulerImpl.getCurrentRepairJobs()).isEmpty();

        verifyNoMoreInteractions(ignoreStubs(myTableRepairMetrics));
        verifyNoMoreInteractions(myRepairStateFactory);
        verifyNoMoreInteractions(scheduleManager);
    }

    @Test
    public void testUpdateTableConfigurationToSame()
    {
        RepairSchedulerImpl repairSchedulerImpl = defaultRepairSchedulerImplBuilder().build();

        repairSchedulerImpl.putConfigurations(mockNode, TABLE_REFERENCE1, Collections.singleton(RepairConfiguration.DEFAULT));

        verify(scheduleManager, timeout(1000)).schedule(eq(mockNodeID), any(ScheduledJob.class));
        verify(scheduleManager, never()).deschedule(eq(mockNodeID), any(ScheduledJob.class));
        verify(myRepairStateFactory).create(eq(mockNode), eq(TABLE_REFERENCE1), eq(RepairConfiguration.DEFAULT), any());
        verify(myRepairState, atLeastOnce()).update();
        assertOneTableViewExist(repairSchedulerImpl, TABLE_REFERENCE1, RepairConfiguration.DEFAULT);

        repairSchedulerImpl.putConfigurations(mockNode, TABLE_REFERENCE1, Collections.singleton(RepairConfiguration.DEFAULT));

        assertOneTableViewExist(repairSchedulerImpl, TABLE_REFERENCE1, RepairConfiguration.DEFAULT);

        repairSchedulerImpl.close();
        verify(scheduleManager).deschedule(eq(mockNodeID), any(ScheduledJob.class));
        assertThat(repairSchedulerImpl.getCurrentRepairJobs()).isEmpty();

        verifyNoMoreInteractions(ignoreStubs(myTableRepairMetrics));
        verifyNoMoreInteractions(myRepairStateFactory);
        verifyNoMoreInteractions(scheduleManager);
    }

    @Test
    public void testConfigureTwoSchedulesForOneTable()
    {
        RepairSchedulerImpl repairSchedulerImpl = defaultRepairSchedulerImplBuilder().withReplicationState(myReplicationState).build();

        RepairConfiguration incrementalRepairConfiguration = RepairConfiguration.newBuilder().withRepairType(
                RepairType.INCREMENTAL).build();
        Set<RepairConfiguration> repairConfigurations = new HashSet<>();
        repairConfigurations.add(RepairConfiguration.DEFAULT);
        repairConfigurations.add(incrementalRepairConfiguration);
        repairSchedulerImpl.putConfigurations(mockNode, TABLE_REFERENCE1, repairConfigurations);

        verify(scheduleManager, timeout(1000)).schedule(eq(mockNodeID), any(TableRepairJob.class));
        verify(scheduleManager, timeout(1000)).schedule(eq(mockNodeID), any(IncrementalRepairJob.class));
        verify(scheduleManager, never()).deschedule(eq(mockNodeID), any(ScheduledJob.class));
        verify(myRepairStateFactory).create(eq(mockNode), eq(TABLE_REFERENCE1), eq(RepairConfiguration.DEFAULT),
                any());
        verify(myRepairState, atLeastOnce()).update();

        assertTableViewsExist(repairSchedulerImpl, TABLE_REFERENCE1, RepairConfiguration.DEFAULT,
                incrementalRepairConfiguration);

        repairSchedulerImpl.close();
        verify(scheduleManager, times(2)).deschedule(eq(mockNodeID), any(ScheduledJob.class));
        assertThat(repairSchedulerImpl.getCurrentRepairJobs()).isEmpty();

        verifyNoMoreInteractions(ignoreStubs(myTableRepairMetrics));
        verifyNoMoreInteractions(myRepairStateFactory);
        verifyNoMoreInteractions(scheduleManager);
    }

    @Test
    public void testScheduleChangesToIncremental()
    {
        RepairSchedulerImpl repairSchedulerImpl = defaultRepairSchedulerImplBuilder().withReplicationState(myReplicationState).build();
        repairSchedulerImpl.putConfigurations(mockNode, TABLE_REFERENCE1,
                Collections.singleton(RepairConfiguration.DEFAULT));

        verify(scheduleManager, timeout(1000)).schedule(eq(mockNodeID), any(TableRepairJob.class));
        verify(scheduleManager, never()).deschedule(eq(mockNodeID), any(ScheduledJob.class));
        verify(myRepairStateFactory).create(eq(mockNode), eq(TABLE_REFERENCE1), eq(RepairConfiguration.DEFAULT), any());
        verify(myRepairState, atLeastOnce()).update();

        assertTableViewsExist(repairSchedulerImpl, TABLE_REFERENCE1, RepairConfiguration.DEFAULT);

        RepairConfiguration incrementalRepairConfiguration = RepairConfiguration.newBuilder().withRepairType(
                RepairType.INCREMENTAL).build();
        repairSchedulerImpl.putConfigurations(mockNode, TABLE_REFERENCE1,
                Collections.singleton(incrementalRepairConfiguration));

        verify(scheduleManager, timeout(1000)).schedule(eq(mockNodeID), any(IncrementalRepairJob.class));
        verify(scheduleManager).deschedule(eq(mockNodeID), any(TableRepairJob.class));

        assertTableViewsExist(repairSchedulerImpl, TABLE_REFERENCE1, incrementalRepairConfiguration);

        repairSchedulerImpl.close();
        verify(scheduleManager).deschedule(eq(mockNodeID), any(IncrementalRepairJob.class));
        assertThat(repairSchedulerImpl.getCurrentRepairJobs()).isEmpty();

        verifyNoMoreInteractions(ignoreStubs(myTableRepairMetrics));
        verifyNoMoreInteractions(myRepairStateFactory);
        verifyNoMoreInteractions(scheduleManager);
    }

    private void assertOneTableViewExist(RepairScheduler repairScheduler, TableReference tableReference, RepairConfiguration repairConfiguration)
    {
        List<ScheduledRepairJobView> repairJobViews = repairScheduler.getCurrentRepairJobs();
        assertThat(repairJobViews).hasSize(1);

        ScheduledRepairJobView repairJobView = repairJobViews.get(0);
        assertThat(repairJobView.getTableReference()).isEqualTo(tableReference);
        assertThat(repairJobView.getRepairConfiguration()).isEqualTo(repairConfiguration);
    }

    private void assertTableViewsExist(RepairScheduler repairScheduler, TableReference tableReference, RepairConfiguration ...repairConfigurations)
    {
        List<ScheduledRepairJobView> repairJobViews = repairScheduler.getCurrentRepairJobs();

        assertThat(repairJobViews).hasSize(repairConfigurations.length);

        int matches = 0;
        for (RepairConfiguration repairConfiguration : repairConfigurations)
        {
            for (ScheduledRepairJobView repairJobView: repairJobViews)
            {
                assertThat(repairJobView.getTableReference()).isEqualTo(tableReference);
                if (repairJobView.getRepairConfiguration().equals(repairConfiguration))
                {
                    matches++;
                }
            }
        }
        assertThat(matches).isEqualTo(repairJobViews.size());
    }

    private RepairSchedulerImpl.Builder defaultRepairSchedulerImplBuilder()
    {
        return RepairSchedulerImpl.builder()
                .withJmxProxyFactory(jmxProxyFactory)
                .withTableRepairMetrics(myTableRepairMetrics)
                .withScheduleManager(scheduleManager)
                .withRepairStateFactory(myRepairStateFactory)
                .withTableStorageStates(myTableStorageStates)
                .withCassandraMetrics(myCassandraMetrics)
                .withRepairHistory(myRepairHistory)
                .withRepairLockType(RepairLockType.VNODE);
    }
}