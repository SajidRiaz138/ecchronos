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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair;

import com.ericsson.bss.cassandra.ecchronos.connection.DistributedJmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.impl.refresh.NodeAddedAction;
import com.ericsson.bss.cassandra.ecchronos.core.impl.refresh.NodeRemovedAction;
import com.ericsson.bss.cassandra.ecchronos.core.metadata.Metadata;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.RepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.table.ReplicatedTableProvider;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReferenceFactory;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.HashSet;
import java.util.Map;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeStateListenerBase;
import com.datastax.oss.driver.api.core.metadata.schema.AggregateMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.FunctionMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.ViewMetadata;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.type.UserDefinedType;

import com.ericsson.bss.cassandra.ecchronos.data.sync.EccNodesSync;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A repair configuration provider that adds configuration to {@link RepairScheduler} based on whether the table
 * is replicated locally using the default repair configuration provided during construction of this object.
 */
@SuppressWarnings("PMD.GodClass")
public class DefaultRepairConfigurationProvider extends NodeStateListenerBase implements SchemaChangeListener
{
    private static final Logger LOG = LoggerFactory.getLogger(DefaultRepairConfigurationProvider.class);
    private static final Integer NO_OF_THREADS = 1;

    private CqlSession mySession;
    private List<Node> myNodes;
    private ReplicatedTableProvider myReplicatedTableProvider;
    private RepairScheduler myRepairScheduler;
    private Function<TableReference, Set<RepairConfiguration>> myRepairConfigurationFunction;
    private TableReferenceFactory myTableReferenceFactory;
    private final ExecutorService myService;
    private EccNodesSync myEccNodesSync;
    private DistributedJmxConnectionProvider myJmxConnectionProvider;
    private DistributedNativeConnectionProvider myAgentNativeConnectionProvider;

    /**
     * Default constructor.
     */
    public DefaultRepairConfigurationProvider()
    {
        myService = Executors.newFixedThreadPool(NO_OF_THREADS);
    }

    private DefaultRepairConfigurationProvider(final Builder builder)
    {
        mySession = builder.mySession;
        myNodes = builder.myNodesList;
        myReplicatedTableProvider = builder.myReplicatedTableProvider;
        myRepairScheduler = builder.myRepairScheduler;
        myRepairConfigurationFunction = builder.myRepairConfigurationFunction;
        myTableReferenceFactory = Preconditions.checkNotNull(builder.myTableReferenceFactory,
                "Table reference factory must be set");
        myEccNodesSync = builder.myEccNodesSync;
        myJmxConnectionProvider = builder.myJmxConnectionProvider;
        myAgentNativeConnectionProvider = builder.myAgentNativeConnectionProvider;

        setupConfiguration();
        myService = Executors.newFixedThreadPool(NO_OF_THREADS);
    }

    /**
     * From builder.
     *
     * @param builder A builder
     */
    public void fromBuilder(final Builder builder)
    {
        mySession = builder.mySession;
        myNodes = builder.myNodesList;
        myReplicatedTableProvider = builder.myReplicatedTableProvider;
        myRepairScheduler = builder.myRepairScheduler;
        myRepairConfigurationFunction = builder.myRepairConfigurationFunction;
        myTableReferenceFactory = Preconditions.checkNotNull(builder.myTableReferenceFactory,
                "Table reference factory must be set");
        myEccNodesSync = builder.myEccNodesSync;
        myJmxConnectionProvider = builder.myJmxConnectionProvider;
        myAgentNativeConnectionProvider = builder.myAgentNativeConnectionProvider;

        setupConfiguration();
    }

    /**
     * Called when keyspace is created.
     *
     * @param keyspace Keyspace metadata
     */
    @Override
    public void onKeyspaceCreated(final KeyspaceMetadata keyspace)
    {
        String keyspaceName = keyspace.getName().asInternal();
        for (Node node : myNodes)
        {
            if (myReplicatedTableProvider.accept(node, keyspaceName))
            {
                allTableOperation(keyspaceName, (tableReference, tableMetadata) -> updateConfiguration(node, tableReference, tableMetadata));
            }
            else
            {
                allTableOperation(keyspaceName, (tableReference, tableMetadata) -> myRepairScheduler.removeConfiguration(node, tableReference));
            }
        }
    }

    /**
     * Called when keyspace is updated.
     *
     * @param current Current keyspace metadata
     * @param previous Previous keyspace metadata
     */
    @Override
    public void onKeyspaceUpdated(final KeyspaceMetadata current,
            final KeyspaceMetadata previous)
    {
        onKeyspaceCreated(current);
    }

    /**
     * Called when keyspace is dropped.
     *
     * @param keyspace Keyspace metadata
     */
    @Override
    public void onKeyspaceDropped(final KeyspaceMetadata keyspace)
    {
        for (TableMetadata table : keyspace.getTables().values())
        {
            onTableDropped(table);
        }
    }

    /**
     * Called when table is created.
     *
     * @param table Table metadata
     */
    @Override
    public void onTableCreated(final TableMetadata table)
    {
        for (Node node : myNodes)
        {
            if (myReplicatedTableProvider.accept(node, table.getKeyspace().asInternal()))
            {
                TableReference tableReference = myTableReferenceFactory.forTable(table.getKeyspace().asInternal(),
                        table.getName().asInternal());
                updateConfiguration(node, tableReference, table);
            }
        }

    }

    /**
     * Called when table is dropped.
     *
     * @param table Table metadata
     */
    @Override
    public void onTableDropped(final TableMetadata table)
    {
        TableReference tableReference = myTableReferenceFactory.forTable(table);
        for (Node node : myNodes)
        {
            myRepairScheduler.removeConfiguration(node, tableReference);
        }
    }

    /**
     * Called when table is updated.
     *
     * @param current Current table metadata
     * @param previous Previous table metadata
     */
    @Override
    public void onTableUpdated(final TableMetadata current, final TableMetadata previous)
    {
        onTableCreated(current);
    }

    /**
     * Close.
     */
    @Override
    public void close()
    {
        if (mySession != null)
        {
            for (KeyspaceMetadata keyspaceMetadata : mySession.getMetadata().getKeyspaces().values())
            {
                allTableOperation(keyspaceMetadata.getName().asInternal(), (tableReference, tableMetadata) ->
                        myNodes.forEach(node -> myRepairScheduler.removeConfiguration(node, tableReference)));
            }
        }
    }

    private void allTableOperation(
            final String keyspaceName,
            final BiConsumer<TableReference, TableMetadata> consumer)
    {
        for (TableMetadata tableMetadata : Metadata.getKeyspace(mySession, keyspaceName).get().getTables().values())
        {
            String tableName = tableMetadata.getName().asInternal();
            TableReference tableReference = myTableReferenceFactory.forTable(keyspaceName, tableName);

            consumer.accept(tableReference, tableMetadata);
        }
    }

    private void updateConfiguration(
            final Node node,
            final TableReference tableReference,
            final TableMetadata table)
    {
        Set<RepairConfiguration> repairConfigurations = myRepairConfigurationFunction.apply(tableReference);
        Set<RepairConfiguration> enabledRepairConfigurations = new HashSet<>();
        for (RepairConfiguration repairConfiguration: repairConfigurations)
        {
            if (!RepairConfiguration.DISABLED.equals(repairConfiguration)
                    && !isTableIgnored(table, repairConfiguration.getIgnoreTWCSTables()))
            {
                enabledRepairConfigurations.add(repairConfiguration);
            }
        }
        myRepairScheduler.putConfigurations(node, tableReference, enabledRepairConfigurations);
    }

    private boolean isTableIgnored(final TableMetadata table, final boolean ignore)
    {
        Map<CqlIdentifier, Object> tableOptions = table.getOptions();
        if (tableOptions == null)
        {
            return false;
        }
        Map<String, String> compaction
                = (Map<String, String>) tableOptions.get(CqlIdentifier.fromInternal("compaction"));
        if (compaction == null)
        {
            return false;
        }
        return ignore
                && "org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy".equals(compaction.get("class"));
    }

    /**
     * Create Builder for DefaultRepairConfigurationProvider.
     * @return Builder the Builder instance for the class.
     */
    public static Builder newBuilder()
    {
        return new Builder();
    }

    /**
     * Called when user defined types are created.
     *
     * @param type User defined type
     */
    @Override
    public void onUserDefinedTypeCreated(final UserDefinedType type)
    {
        // NOOP
    }

    /**
     * Called when user defined types are dropped.
     *
     * @param type User defined type
     */
    @Override
    public void onUserDefinedTypeDropped(final UserDefinedType type)
    {
        // NOOP
    }

    /**
     * Called when user defined types are updated.
     *
     * @param current Current user defined type
     * @param previous previous user defined type
     */
    @Override
    public void onUserDefinedTypeUpdated(final UserDefinedType current, final UserDefinedType previous)
    {
        // NOOP
    }

    /**
     * Called when functions are created.
     *
     * @param function Function metadata
     */
    @Override
    public void onFunctionCreated(final FunctionMetadata function)
    {
        // NOOP
    }

    /**
     * Called when functions are dropped.
     *
     * @param function Function metadata
     */
    @Override
    public void onFunctionDropped(final FunctionMetadata function)
    {
        // NOOP
    }

    /**
     * Called when functions are updated.
     *
     * @param current Current function metadata
     * @param previous Previous function metadata
     */
    @Override
    public void onFunctionUpdated(final FunctionMetadata current, final FunctionMetadata previous)
    {
        // NOOP
    }

    /**
     * Called when aggregates are created.
     *
     * @param aggregate Aggregate metadata
     */
    @Override
    public void onAggregateCreated(final AggregateMetadata aggregate)
    {
        // NOOP
    }

    /**
     * Called when aggregates are dropped.
     *
     * @param aggregate Aggregate metadata
     */
    @Override
    public void onAggregateDropped(final AggregateMetadata aggregate)
    {
        // NOOP
    }

    /**
     * Called when aggregates are updated.
     *
     * @param current Current aggregate metadata
     * @param previous previous aggregate metadata
     */
    @Override
    public void onAggregateUpdated(final AggregateMetadata current, final AggregateMetadata previous)
    {
        // NOOP
    }

    /**
     * Called when views are created.
     *
     * @param view View metadata
     */
    @Override
    public void onViewCreated(final ViewMetadata view)
    {
        // NOOP
    }

    /**
     * Called when views are dropped.
     *
     * @param view View metadata
     */
    @Override
    public void onViewDropped(final ViewMetadata view)
    {
        // NOOP
    }

    /**
     * Called when views are updated.
     *
     * @param current Current view metadata
     * @param previous Previous view metadata
     */
    @Override
    public void onViewUpdated(final ViewMetadata current, final ViewMetadata previous)
    {
        // NOOP
    }

    /**
     * Called when the session is up and ready. Will invoke the listeners' onSessionReady methods.
     *
     * @param session The session
     */
    @Override
    public void onSessionReady(final Session session)
    {
        SchemaChangeListener.super.onSessionReady(session);
    }

    /**
     * Callback for when a node switches state to UP.
     *
     * @param node The node switching state to UP
     */
    @Override
    public void onUp(final Node node)
    {
        LOG.debug("{} switched state to UP.", node);
        setupConfiguration();
    }

    /**
     * Callback for when a node switches state to DOWN.
     *
     * @param node The node switching state to DOWN
     */
    @Override
    public void onDown(final Node node)
    {
        LOG.debug("{} switched state to DOWN.", node);
        setupConfiguration();
    }

    /**
     * Callback for when a new node is added to the cluster.
     * @param node
     */
    @Override
    public void onAdd(final Node node)
    {
        LOG.info("Node added {}", node.getHostId());
        NodeAddedAction callable = new NodeAddedAction(myEccNodesSync, myJmxConnectionProvider, myAgentNativeConnectionProvider, node);
        myService.submit(callable);
    }

    /**
     * callback for when a node is removed from the cluster.
     * @param node
     */
    @Override
    public void onRemove(final Node node)
    {
        LOG.info("Node removed {}", node.getHostId());
        NodeRemovedAction callable = new NodeRemovedAction(myEccNodesSync, myJmxConnectionProvider, myAgentNativeConnectionProvider, node);
        myService.submit(callable);
    }

    /**
     * This will go through all the configuration, given mySession is set, otherwise it will just silently
     * return.
     */
    private void setupConfiguration()
    {
        if (mySession == null)
        {
            LOG.debug("Session during setupConfiguration call was null.");
            return;
        }

        for (KeyspaceMetadata keyspaceMetadata : mySession.getMetadata().getKeyspaces().values())
        {
            String keyspaceName = keyspaceMetadata.getName().asInternal();
            for (Node node : myNodes)
            {
                if (myReplicatedTableProvider.accept(node, keyspaceName))
                {
                    allTableOperation(keyspaceName, (tableReference, tableMetadata) -> updateConfiguration(node, tableReference, tableMetadata));
                }
            }

        }
    }

    /**
     * Builder for DefaultRepairConfigurationProvider.
     */
    public static class Builder
    {
        private CqlSession mySession;
        private List<Node> myNodesList;
        private ReplicatedTableProvider myReplicatedTableProvider;
        private RepairScheduler myRepairScheduler;
        private Function<TableReference, Set<RepairConfiguration>> myRepairConfigurationFunction;
        private TableReferenceFactory myTableReferenceFactory;
        private EccNodesSync myEccNodesSync;
        private DistributedJmxConnectionProvider myJmxConnectionProvider;
        private DistributedNativeConnectionProvider myAgentNativeConnectionProvider;


        /**
         * Build with session.
         *
         * @param session The CQl session
         * @return Builder
         */
        public Builder withSession(final CqlSession session)
        {
            mySession = session;
            return this;
        }

        /**
         * Build with default repair configuration.
         *
         * @param defaultRepairConfiguration The default repair configuration
         * @return Builder
         */
        public Builder withDefaultRepairConfiguration(final RepairConfiguration defaultRepairConfiguration)
        {
            myRepairConfigurationFunction = (tableReference) -> Collections.singleton(defaultRepairConfiguration);
            return this;
        }

        /**
         * Build with repair configuration.
         *
         * @param defaultRepairConfiguration The default repair configuration
         * @return Builder
         */
        public Builder withRepairConfiguration(final Function<TableReference, Set<RepairConfiguration>>
                defaultRepairConfiguration)
        {
            myRepairConfigurationFunction = defaultRepairConfiguration;
            return this;
        }

        /**
         * Build with replicated table provider.
         *
         * @param replicatedTableProvider The replicated table provider
         * @return Builder
         */
        public Builder withReplicatedTableProvider(final ReplicatedTableProvider replicatedTableProvider)
        {
            myReplicatedTableProvider = replicatedTableProvider;
            return this;
        }

        /**
         * Build with table repair scheduler.
         *
         * @param repairScheduler The repair scheduler
         * @return Builder
         */
        public Builder withRepairScheduler(final RepairScheduler repairScheduler)
        {
            myRepairScheduler = repairScheduler;
            return this;
        }

        /**
         * Build with table reference factory.
         *
         * @param tableReferenceFactory The table reference factory
         * @return Builder
         */
        public Builder withTableReferenceFactory(final TableReferenceFactory tableReferenceFactory)
        {
            myTableReferenceFactory = tableReferenceFactory;
            return this;
        }

        /**
         * Build SchedulerManager with run interval.
         *
         * @param nodesList the interval to run a repair task
         * @return Builder with nodes list
         */
        public Builder withNodesList(final List<Node> nodesList)
        {
            myNodesList = nodesList;
            return this;
        }

        /**
         * Build with EccNodesSync.
         * @param eccNodesSync
         * @return Builder with EccNodesSync
         */
        public Builder withEccNodesSync(final EccNodesSync eccNodesSync)
        {
            myEccNodesSync = eccNodesSync;
            return this;
        }

        /**
         * Build with DistributedNativeConnectionProvider.
         * @param agentNativeConnectionProvider
         * @return
         */
        public Builder withDistributedNativeConnectionProvider(final DistributedNativeConnectionProvider agentNativeConnectionProvider)
        {
            myAgentNativeConnectionProvider = agentNativeConnectionProvider;
            return this;
        }

        /**
         * Build with DistributedJmxConnectionProvider.
         * @param jmxConnectionProvider
         * @return Builder with DistributedJmxConnectionProvider
         */
        public Builder withJmxConnectionProvider(final DistributedJmxConnectionProvider jmxConnectionProvider)
        {
            myJmxConnectionProvider = jmxConnectionProvider;
            return this;
        }

        /**
         * Build.
         *
         * @return DefaultRepairConfigurationProvider
         */
        public DefaultRepairConfigurationProvider build()
        {
            DefaultRepairConfigurationProvider configurationProvider = new DefaultRepairConfigurationProvider(this);
            return configurationProvider;
        }
    }
}
