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

import cassandracluster.AbstractCassandraCluster;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.ericsson.bss.cassandra.ecchronos.application.config.ConfigurationHelper;
import com.ericsson.bss.cassandra.ecchronos.application.config.security.Credentials;
import com.ericsson.bss.cassandra.ecchronos.application.config.security.JmxTLSConfig;
import com.ericsson.bss.cassandra.ecchronos.application.config.security.Security;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedJmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.impl.builders.DistributedNativeBuilder;
import com.ericsson.bss.cassandra.ecchronos.connection.impl.providers.DistributedJmxConnectionProviderImpl;
import com.ericsson.bss.cassandra.ecchronos.connection.impl.providers.DistributedNativeConnectionProviderImpl;
import com.ericsson.bss.cassandra.ecchronos.core.impl.jmx.DistributedJmxProxyFactoryImpl;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.data.sync.EccNodesSync;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.connection.ConnectionType;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import net.jcip.annotations.NotThreadSafe;
import org.junit.AfterClass;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.remote.JMXConnector;
import java.io.IOException;
import java.util.UUID;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.CassandraContainer;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@NotThreadSafe
abstract public class TestBase extends AbstractCassandraCluster
{
    private static final Logger LOG = LoggerFactory.getLogger(TestBase.class);
    private static final String JMX_FORMAT_URL = "service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi";
    private static final String SECURITY_FILE = "security.yml";
    protected static final String ECCHRONOS_KEYSPACE = "ecchronos";
    protected static final String TEST_TABLE_ONE_NAME = "test_table1";
    protected static final String TEST_TABLE_TWO_NAME = "test_table2";
    private static final String ON_DEMAND_REPAIR_STATUS_TABLE = "on_demand_repair_status";
    private static final int DEFAULT_INSERT_DATA_COUNT = 1000;

    private static final List<CassandraContainer<?>> nodes = new ArrayList<>();
    private static DistributedNativeConnectionProvider myNativeConnectionProvider;
    private static DistributedJmxConnectionProvider myJmxConnectionProvider;
    private static DistributedJmxProxyFactoryImpl myJmxProxyFactory;
    private static EccNodesSync myEccNodesSync;
    private static final AtomicReference<Security.JmxSecurity> jmxSecurity = new AtomicReference<>();

    private static String[] convertCredentials(final Supplier<Security.JmxSecurity> jmxSecurity)
    {
        Credentials credentials = jmxSecurity.get().getJmxCredentials();
        if (!credentials.isEnabled())
        {
            return null;
        }
        return new String[] {
                              credentials.getUsername(), credentials.getPassword()
        };
    }

    private static Map<String, String> convertTls(final Supplier<Security.JmxSecurity> jmxSecurity)
    {
        JmxTLSConfig tlsConfig = jmxSecurity.get().getJmxTlsConfig();
        if (!tlsConfig.isEnabled())
        {
            return new HashMap<>();
        }

        Map<String, String> config = new HashMap<>();
        if (tlsConfig.getProtocol() != null)
        {
            config.put("com.sun.management.jmxremote.ssl.enabled.protocols", tlsConfig.getProtocol());
        }
        if (tlsConfig.getCipherSuites() != null)
        {
            config.put("com.sun.management.jmxremote.ssl.enabled.cipher.suites", tlsConfig.getCipherSuites());
        }
        config.put("javax.net.ssl.keyStore", tlsConfig.getKeyStorePath());
        config.put("javax.net.ssl.keyStorePassword", tlsConfig.getKeyStorePassword());
        config.put("javax.net.ssl.trustStore", tlsConfig.getTrustStorePath());
        config.put("javax.net.ssl.trustStorePassword", tlsConfig.getTrustStorePassword());

        return config;
    }

    @BeforeClass
    public static void setUpCluster() throws Exception
    {
        Security security = ConfigurationHelper.DEFAULT_INSTANCE.getConfiguration(SECURITY_FILE, Security.class);
        jmxSecurity.set(security.getJmxSecurity());
        mySession.execute(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 2}",
                ECCHRONOS_KEYSPACE));
        String query = String.format("CREATE TABLE IF NOT EXISTS %s.nodes_sync(ecchronos_id TEXT, datacenter_name TEXT, node_id UUID, node_endpoint TEXT, node_status TEXT, last_connection TIMESTAMP, next_connection TIMESTAMP, PRIMARY KEY(ecchronos_id, datacenter_name, node_id)) WITH CLUSTERING ORDER BY(datacenter_name DESC, node_id DESC);", ECCHRONOS_KEYSPACE);
        mySession.execute(query);

        mySession.execute(String.format(
                "CREATE TABLE IF NOT EXISTS %s.lock (resource text, node uuid, metadata map<text,text>, PRIMARY KEY(resource)) WITH default_time_to_live = 600 AND gc_grace_seconds = 0",
                ECCHRONOS_KEYSPACE));
        mySession.execute(String.format(
                "CREATE TABLE IF NOT EXISTS %s.lock_priority (resource text, node uuid, priority int, PRIMARY KEY(resource, node)) WITH default_time_to_live = 600 AND gc_grace_seconds = 0",
                ECCHRONOS_KEYSPACE));
        mySession.execute(
                String.format("CREATE TYPE IF NOT EXISTS %s.token_range (start text, end text)", ECCHRONOS_KEYSPACE));
        mySession.execute(String.format(
                "CREATE TYPE IF NOT EXISTS %s.table_reference (id uuid, keyspace_name text, table_name text)",
                ECCHRONOS_KEYSPACE));
        mySession.execute(String.format(
                "CREATE TABLE IF NOT EXISTS %s.%s (host_id uuid, job_id uuid, table_reference frozen<table_reference>, token_map_hash int, repaired_tokens frozen<set<frozen<token_range>>>, status text, completed_time timestamp, repair_type text, PRIMARY KEY(host_id, job_id)) WITH default_time_to_live = 2592000 AND gc_grace_seconds = 0",
                ECCHRONOS_KEYSPACE, ON_DEMAND_REPAIR_STATUS_TABLE));
        mySession.execute(String.format(
                "CREATE TABLE IF NOT EXISTS %s.repair_history(table_id UUID, node_id UUID, repair_id timeuuid, job_id UUID, coordinator_id UUID, range_begin text, range_end text, participants set<uuid>, status text, started_at timestamp, finished_at timestamp, PRIMARY KEY((table_id,node_id), repair_id)) WITH CLUSTERING ORDER BY (repair_id DESC);",
                ECCHRONOS_KEYSPACE));
        mySession.execute(String.format("CREATE TABLE IF NOT EXISTS %s.%s (col1 UUID, col2 int, PRIMARY KEY(col1))", ECCHRONOS_KEYSPACE,
                TEST_TABLE_ONE_NAME));
        mySession.execute(
                String.format("CREATE TABLE IF NOT EXISTS %s.%s (col1 UUID, col2 int, PRIMARY KEY(col1))", ECCHRONOS_KEYSPACE,
                        TEST_TABLE_TWO_NAME));
        Map<UUID, Node> nodesList = mySession.getMetadata().getNodes();

        List<InetSocketAddress> contactPoints = new ArrayList<>();

        for (Node node : nodesList.values())
        {
            String hostname = node.getBroadcastRpcAddress().get().getHostName();
            int port = node.getBroadcastRpcAddress().get().getPort();
            contactPoints.add(new InetSocketAddress(hostname, port));
        }

        DistributedNativeBuilder distributedNativeBuilder = DistributedNativeConnectionProviderImpl.builder()
                .withInitialContactPoints(contactPoints)
                .withAgentType(ConnectionType.hostAware)
                .withHostAware(contactPoints);

        myNativeConnectionProvider =
                new DistributedNativeConnectionProviderImpl(mySession, nodesList, distributedNativeBuilder, ConnectionType.hostAware);
        myEccNodesSync = EccNodesSync.newBuilder()
                .withSession(mySession)
                .withNativeConnection(myNativeConnectionProvider)
                .withEcchronosID("ecchronos-test")
                .build();

        Supplier<String[]> credentials = () -> convertCredentials(jmxSecurity::get);
        @SuppressWarnings ("unchecked")
        Supplier<Map<String, String>> tls = mock(Supplier.class);
        when(tls.get()).thenReturn(Map.of());
        myJmxConnectionProvider = DistributedJmxConnectionProviderImpl.builder()
                .withCqlSession(mySession)
                .withCredentials(credentials)
                .withTLS(tls)
                .withNativeConnection(myNativeConnectionProvider)
                .withJolokiaEnabled(false)
                .withEccNodesSync(myEccNodesSync)
                .build();

        myJmxProxyFactory = DistributedJmxProxyFactoryImpl.builder()
                .withJmxConnectionProvider(myJmxConnectionProvider)
                .withEccNodesSync(new EccNodesSync.Builder()
                        .withConnectionDelayValue(10L)
                        .withConnectionDelayUnit(TimeUnit.SECONDS)
                        .withNativeConnection(getNativeConnectionProvider())
                        .withSession(mySession)
                        .withEcchronosID("Id")
                        .build())
                .withNodesMap(nodesList)
                .build();
    }

    @AfterClass
    public static void tearDownCluster()
    {
        AbstractCassandraCluster.tearDownCluster();
    }

    @AfterClass
    public static void cleanup() throws IOException
    {
        myJmxConnectionProvider.close();
        myNativeConnectionProvider.close();
    }

    protected static DistributedNativeConnectionProvider getNativeConnectionProvider()
    {
        return myNativeConnectionProvider;
    }

    protected static DistributedJmxConnectionProvider getJmxConnectionProvider()
    {
        return myJmxConnectionProvider;
    }

    protected static DistributedJmxProxyFactoryImpl getJmxProxyFactory()
    {
        return myJmxProxyFactory;
    }

    protected void insertSomeDataAndFlush(TableReference tableReference, CqlSession session, Node node)
                                                                                                        throws ReflectionException,
                                                                                                        MalformedObjectNameException,
                                                                                                        InstanceNotFoundException,
                                                                                                        MBeanException,
                                                                                                        IOException
    {
        for (int i = 0; i < DEFAULT_INSERT_DATA_COUNT; i++)
        {
            UUID randomUUID = UUID.randomUUID();
            SimpleStatement statement = QueryBuilder.insertInto(tableReference.getKeyspace(), tableReference.getTable())
                    .value("col1", literal(randomUUID))
                    .value("col2", literal(randomUUID.hashCode()))
                    .build();
            session.execute(statement);
        }
        forceFlush(tableReference, node);
    }

    private void forceFlush(TableReference tableReference, Node node)
                                                                      throws IOException,
                                                                      MalformedObjectNameException,
                                                                      ReflectionException,
                                                                      InstanceNotFoundException,
                                                                      MBeanException
    {
        try (JMXConnector jmxConnector = getJmxConnectionProvider().getJmxConnector(node.getHostId()))
        {
            String[] table = new String[] { tableReference.getTable() };
            boolean isConnected = jmxConnector.getConnectionId() != null;
            System.out.println(isConnected);
            jmxConnector.getMBeanServerConnection()
                    .invoke(new ObjectName("org.apache.cassandra.db:type=StorageService"),
                            "forceKeyspaceFlush",
                            new Object[] {
                                           tableReference.getKeyspace(), table
                            },
                            new String[] {
                                           String.class.getName(), String[].class.getName()
                            });
        }
    }
}
