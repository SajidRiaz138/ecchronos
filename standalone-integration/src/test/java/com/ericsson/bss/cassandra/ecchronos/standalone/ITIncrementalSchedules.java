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

package com.ericsson.bss.cassandra.ecchronos.standalone;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.ericsson.bss.cassandra.ecchronos.core.impl.locks.CASLockFactory;
import com.ericsson.bss.cassandra.ecchronos.core.impl.locks.RepairLockType;
import com.ericsson.bss.cassandra.ecchronos.core.impl.metadata.NodeResolverImpl;
import com.ericsson.bss.cassandra.ecchronos.core.impl.metrics.CassandraMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.scheduler.RepairSchedulerImpl;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.scheduler.ScheduleManagerImpl;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.state.HostStatesImpl;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.state.ReplicationStateImpl;
import com.ericsson.bss.cassandra.ecchronos.core.impl.table.TableReferenceFactoryImpl;
import com.ericsson.bss.cassandra.ecchronos.core.impl.utils.ConsistencyType;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledRepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReferenceFactory;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.fm.RepairFaultReporter;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType;
import java.util.UUID;
import net.jcip.annotations.NotThreadSafe;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;

@NotThreadSafe
public class ITIncrementalSchedules extends TestBase
{
    private static final Logger LOG = LoggerFactory.getLogger(ITIncrementalSchedules.class);
    private static final int DEFAULT_SCHEDULE_TIMEOUT_IN_SECONDS = 90;
    private static final int CASSANDRA_METRICS_UPDATE_IN_SECONDS = 5;
    private static RepairFaultReporter mockFaultReporter;
    private static TableRepairMetrics mockTableRepairMetrics;
    private static HostStatesImpl myHostStates;
    private static RepairSchedulerImpl myRepairSchedulerImpl;
    private static ScheduleManagerImpl myScheduleManagerImpl;
    private static CASLockFactory myLockFactory;
    private static RepairConfiguration myRepairConfiguration;
    private static TableReferenceFactory myTableReferenceFactory;
    private static CassandraMetrics myCassandraMetrics;
    protected static Metadata myMetadata;

    private final Set<TableReference> myRepairs = new HashSet<>();

    @Before
    public void init()
    {
        mockFaultReporter = mock(RepairFaultReporter.class);
        mockTableRepairMetrics = mock(TableRepairMetrics.class);
        myMetadata = mySession.getMetadata();

        myTableReferenceFactory = new TableReferenceFactoryImpl(mySession);

        myHostStates = HostStatesImpl.builder()
                .withRefreshIntervalInMs(1000)
                .withJmxProxyFactory(getJmxProxyFactory())
                .build();

        myLockFactory = CASLockFactory.builder()
                .withNativeConnectionProvider(getNativeConnectionProvider())
                .withHostStates(myHostStates)
                .withConsistencySerial(ConsistencyType.DEFAULT)
                .build();

        Set<UUID> nodeIds = getNativeConnectionProvider().getNodes().keySet();
        List<UUID> nodeIdList = new ArrayList<>(nodeIds);

        myScheduleManagerImpl = ScheduleManagerImpl.builder()
                .withLockFactory(myLockFactory)
                .withNodeIDList(nodeIdList)
                .withRunInterval(1, TimeUnit.SECONDS)
                .build();

        myCassandraMetrics = new CassandraMetrics(getJmxProxyFactory(),
                Duration.ofSeconds(CASSANDRA_METRICS_UPDATE_IN_SECONDS), Duration.ofMinutes(30));

        myRepairSchedulerImpl = RepairSchedulerImpl.builder()
                .withJmxProxyFactory(getJmxProxyFactory())
                .withTableRepairMetrics(mockTableRepairMetrics)
                .withFaultReporter(mockFaultReporter)
                .withScheduleManager(myScheduleManagerImpl)
                .withRepairLockType(RepairLockType.VNODE)
                .withCassandraMetrics(myCassandraMetrics)
                .withReplicationState(new ReplicationStateImpl(new NodeResolverImpl(mySession), mySession))
                .build();

        myRepairConfiguration = RepairConfiguration.newBuilder()
                .withRepairInterval(5, TimeUnit.SECONDS)
                .withRepairType(RepairType.INCREMENTAL)
                .build();
    }

    @After
    public void clean()
    {
        List<CompletionStage<AsyncResultSet>> stages = new ArrayList<>();
        for (TableReference tableReference : myRepairs)
        {
            Node node = getNodeFromDatacenterOne();
            myRepairSchedulerImpl.removeConfiguration(node, tableReference);

            stages.add(mySession.executeAsync(QueryBuilder.deleteFrom("system_distributed", "repair_history")
                    .whereColumn("keyspace_name")
                    .isEqualTo(literal(tableReference.getKeyspace()))
                    .whereColumn("columnfamily_name")
                    .isEqualTo(literal(tableReference.getTable()))
                    .build()));
        }
        for (CompletionStage<AsyncResultSet> stage : stages)
        {
            CompletableFutures.getUninterruptibly(stage);
        }
        myRepairs.clear();
        reset(mockTableRepairMetrics);
        reset(mockFaultReporter);
        closeConnections();
    }

    public static void closeConnections()
    {
        if (myHostStates != null)
        {
            myHostStates.close();
        }
        if (myRepairSchedulerImpl != null)
        {
            myRepairSchedulerImpl.close();
        }
        if (myScheduleManagerImpl != null)
        {
            myScheduleManagerImpl.close();
        }
        if (myLockFactory != null)
        {
            myLockFactory.close();
        }
    }

    @Test
    public void repairSingleTable() throws Exception
    {
        Node node = getNodeFromDatacenterOne();
        TableReference tableReference = myTableReferenceFactory.forTable(TEST_KEYSPACE, TEST_TABLE_ONE_NAME);
        insertSomeDataAndFlush(tableReference, mySession, node);
        long startTime = System.currentTimeMillis();

        // Wait for metrics to be updated, wait at least 3 times the update time for metrics (worst case scenario)
        await().pollInterval(1, TimeUnit.SECONDS)
                .atMost(CASSANDRA_METRICS_UPDATE_IN_SECONDS * 3, TimeUnit.SECONDS)
                .until(() ->
                {
                    double percentRepaired = myCassandraMetrics.getPercentRepaired(node.getHostId(), tableReference);
                    long maxRepairedAt = myCassandraMetrics.getMaxRepairedAt(node.getHostId(), tableReference);
                    LOG.info("Waiting for metrics to be updated, percentRepaired: {} maxRepairedAt: {}",
                            percentRepaired, maxRepairedAt);
                    return maxRepairedAt < startTime && percentRepaired < 100.0d;
                });

        // Create a schedule
        addSchedule(tableReference, node);
        await().pollInterval(1, TimeUnit.SECONDS)
                .atMost(DEFAULT_SCHEDULE_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS)
                .until(() -> getSchedule(tableReference).isPresent());

        await().pollInterval(1, TimeUnit.SECONDS)
                .atMost(DEFAULT_SCHEDULE_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS)
                .until(() ->
                {
                    double percentRepaired = myCassandraMetrics.getPercentRepaired(node.getHostId(), tableReference);
                    long maxRepairedAt = myCassandraMetrics.getMaxRepairedAt(node.getHostId(), tableReference);
                    LOG.info("Waiting for schedule to run, percentRepaired: {} maxRepairedAt: {}",
                            percentRepaired, maxRepairedAt);
                    return maxRepairedAt >= startTime && percentRepaired >= 100.0d;
                });
        verify(mockFaultReporter, never())
                .raise(any(RepairFaultReporter.FaultCode.class), anyMap());
        verify(mockTableRepairMetrics).repairSession(eq(tableReference),
                any(long.class), any(TimeUnit.class), eq(true));
        Optional<ScheduledRepairJobView> view = getSchedule(tableReference);
        assertThat(view).isPresent();
        assertThat(view.get().getStatus()).isEqualTo(ScheduledRepairJobView.Status.COMPLETED);
        assertThat(view.get().getCompletionTime()).isGreaterThanOrEqualTo(startTime);
        assertThat(view.get().getProgress()).isGreaterThanOrEqualTo(1.0d);
    }

    private void addSchedule(TableReference tableReference, Node node)
    {
        if (myRepairs.add(tableReference))
        {
            myRepairSchedulerImpl.putConfigurations(node, tableReference, Collections.singleton(myRepairConfiguration));
        }
    }

    private Optional<ScheduledRepairJobView> getSchedule(TableReference tableReference)
    {
        return myRepairSchedulerImpl.getCurrentRepairJobs()
                .stream()
                .filter(s -> s.getRepairConfiguration().equals(myRepairConfiguration))
                .filter(s -> s.getTableReference().equals(tableReference))
                .findFirst();
    }
}
