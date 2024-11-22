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
package com.ericsson.bss.cassandra.ecchronos.connection.impl.providers;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.ericsson.bss.cassandra.ecchronos.connection.impl.builders.ContactEndPoint;
import com.ericsson.bss.cassandra.ecchronos.connection.impl.builders.DistributedNativeBuilder;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.connection.ConnectionType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.net.InetSocketAddress;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.Silent.class)
public class TestNodeInclusion
{
    @Mock
    private CqlSession mySessionMock;

    @Mock
    private Metadata myMetadataMock;

    private final Map<UUID, Node> myNodes = new HashMap<>();

    @Mock
    private Node mockNodeDC1Rack1;

    @Mock
    private Node mockNodeDC1Rack2;

    @Mock
    private Node mockNodeDC2Rack1;

    @Mock
    private Node mockNodeDC2Rack2;

    private final ContactEndPoint endPointNodeDC1Rack1 = new ContactEndPoint("127.0.0.1", 9042);

    private final ContactEndPoint endPointNodeDC1rack2 = new ContactEndPoint("127.0.0.2", 9042);

    private final ContactEndPoint endPointNodeDC2Rack1 = new ContactEndPoint("127.0.0.3", 9042);

    private final ContactEndPoint endPointNodeDC2Rack2 = new ContactEndPoint("127.0.0.4", 9042);

    private final List<InetSocketAddress> contactPoints = new ArrayList<>();

    @Before
    public void setup()
    {
        contactPoints.add(new InetSocketAddress("127.0.0.1", 9042));
        contactPoints.add(new InetSocketAddress("127.0.0.2", 9042));

        when(mockNodeDC1Rack1.getDatacenter()).thenReturn("datacenter1");
        when(mockNodeDC1Rack2.getDatacenter()).thenReturn("datacenter1");
        when(mockNodeDC2Rack1.getDatacenter()).thenReturn("datacenter2");
        when(mockNodeDC2Rack2.getDatacenter()).thenReturn("datacenter2");

        when(mockNodeDC1Rack1.getRack()).thenReturn("rack1");
        when(mockNodeDC1Rack2.getRack()).thenReturn("rack2");
        when(mockNodeDC2Rack1.getRack()).thenReturn("rack1");
        when(mockNodeDC2Rack2.getRack()).thenReturn("rack2");

        when(mockNodeDC1Rack1.getEndPoint()).thenReturn(endPointNodeDC1Rack1);
        when(mockNodeDC1Rack2.getEndPoint()).thenReturn(endPointNodeDC1rack2);
        when(mockNodeDC2Rack1.getEndPoint()).thenReturn(endPointNodeDC2Rack1);
        when(mockNodeDC2Rack2.getEndPoint()).thenReturn(endPointNodeDC2Rack2);

        when(mockNodeDC1Rack1.getState()).thenReturn(NodeState.UP);
        when(mockNodeDC1Rack2.getState()).thenReturn(NodeState.UP);
        when(mockNodeDC2Rack1.getState()).thenReturn(NodeState.UP);
        when(mockNodeDC2Rack2.getState()).thenReturn(NodeState.UP);

        myNodes.put(UUID.randomUUID(), mockNodeDC1Rack1);
        myNodes.put(UUID.randomUUID(), mockNodeDC1Rack2);
        myNodes.put(UUID.randomUUID(), mockNodeDC2Rack1);
        myNodes.put(UUID.randomUUID(), mockNodeDC2Rack2);

        when(myMetadataMock.getNodes()).thenReturn(myNodes);
        when(mySessionMock.getMetadata()).thenReturn(myMetadataMock);
    }
    
    @Test
    public void testValidNodesDatacenterAware()
    {
        List<String> datacentersInfo = new ArrayList<>();
        datacentersInfo.add("datacenter1");

        DistributedNativeBuilder provider = DistributedNativeConnectionProviderImpl.builder()
                .withInitialContactPoints(contactPoints)
                .withAgentType(ConnectionType.datacenterAware)
                .withDatacenterAware(datacentersInfo);

        assertTrue(provider.confirmNodeValid(mockNodeDC1Rack1));
        assertTrue(provider.confirmNodeValid(mockNodeDC1Rack2));
        assertFalse(provider.confirmNodeValid(mockNodeDC2Rack1));
        assertFalse(provider.confirmNodeValid(mockNodeDC2Rack2));
    }
    @Test
    public void testValidNodesRackAware()
    {
        List<Map<String, String>> rackList = new ArrayList<>();
        Map<String, String> rackInfo = new HashMap<>();
        rackInfo.put("datacenterName", "datacenter1");
        rackInfo.put("rackName", "rack1");
        rackList.add(rackInfo);

        DistributedNativeBuilder provider = DistributedNativeConnectionProviderImpl.builder()
                .withInitialContactPoints(contactPoints)
                .withAgentType(ConnectionType.rackAware)
                .withRackAware(rackList);


        assertTrue(provider.confirmNodeValid(mockNodeDC1Rack1));
        assertFalse(provider.confirmNodeValid(mockNodeDC1Rack2));
        assertFalse(provider.confirmNodeValid(mockNodeDC2Rack1));
        assertFalse(provider.confirmNodeValid(mockNodeDC2Rack2));

    }
    @Test
    public void testResolveHostAware()
    {
        List<InetSocketAddress> hostList = new ArrayList<>();
        hostList.add(new InetSocketAddress("127.0.0.1", 9042));
        hostList.add(new InetSocketAddress("127.0.0.2", 9042));
        hostList.add(new InetSocketAddress("127.0.0.3", 9042));
        DistributedNativeBuilder provider = DistributedNativeConnectionProviderImpl.builder()
                .withInitialContactPoints(contactPoints)
                .withAgentType(ConnectionType.hostAware)
                .withHostAware(hostList);

        assertTrue(provider.confirmNodeValid(mockNodeDC1Rack1));
        assertTrue(provider.confirmNodeValid(mockNodeDC1Rack2));
        assertTrue(provider.confirmNodeValid(mockNodeDC2Rack1));
        assertFalse(provider.confirmNodeValid(mockNodeDC2Rack2));

    }


}