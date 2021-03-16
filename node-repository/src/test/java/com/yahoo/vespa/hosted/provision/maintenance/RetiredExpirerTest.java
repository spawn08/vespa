// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespa.hosted.provision.maintenance;

import com.yahoo.component.Version;
import com.yahoo.config.provision.ApplicationId;
import com.yahoo.config.provision.ApplicationName;
import com.yahoo.config.provision.Capacity;
import com.yahoo.config.provision.ClusterResources;
import com.yahoo.config.provision.ClusterSpec;
import com.yahoo.config.provision.Deployer;
import com.yahoo.config.provision.InstanceName;
import com.yahoo.config.provision.NodeResources;
import com.yahoo.config.provision.NodeType;
import com.yahoo.config.provision.TenantName;
import com.yahoo.test.ManualClock;
import com.yahoo.vespa.applicationmodel.HostName;
import com.yahoo.vespa.hosted.provision.Node;
import com.yahoo.vespa.hosted.provision.NodeList;
import com.yahoo.vespa.hosted.provision.NodeRepository;
import com.yahoo.vespa.hosted.provision.node.Agent;
import com.yahoo.vespa.hosted.provision.node.IP;
import com.yahoo.vespa.hosted.provision.node.filter.NodeTypeFilter;
import com.yahoo.vespa.hosted.provision.provisioning.InfraDeployerImpl;
import com.yahoo.vespa.hosted.provision.provisioning.NodeRepositoryProvisioner;
import com.yahoo.vespa.hosted.provision.provisioning.ProvisioningTester;
import com.yahoo.vespa.hosted.provision.testutils.MockDeployer;
import com.yahoo.vespa.hosted.provision.testutils.MockDuperModel;
import com.yahoo.vespa.hosted.provision.testutils.MockNameResolver;
import com.yahoo.vespa.orchestrator.OrchestrationException;
import com.yahoo.vespa.orchestrator.Orchestrator;
import com.yahoo.vespa.service.duper.ConfigServerApplication;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author bratseth
 */
public class RetiredExpirerTest {

    private final NodeResources hostResources = new NodeResources(64, 128, 2000, 10);
    private final NodeResources nodeResources = new NodeResources(2, 8, 50, 1);

    private final ProvisioningTester tester = new ProvisioningTester.Builder().build();
    private final ManualClock clock = tester.clock();
    private final NodeRepository nodeRepository = tester.nodeRepository();
    private final NodeRepositoryProvisioner provisioner = tester.provisioner();
    private final Orchestrator orchestrator = mock(Orchestrator.class);

    private static final Duration RETIRED_EXPIRATION = Duration.ofHours(12);

    @Before
    public void setup() throws OrchestrationException {
        // By default, orchestrator should deny all request for suspension so we can test expiration
        doThrow(new RuntimeException()).when(orchestrator).acquirePermissionToRemove(any());
    }

    @Test
    public void ensure_retired_nodes_time_out() {
        tester.makeReadyNodes(7, nodeResources);
        tester.makeReadyHosts(4, hostResources);

        ApplicationId applicationId = ApplicationId.from(TenantName.from("foo"), ApplicationName.from("bar"), InstanceName.from("fuz"));

        // Allocate content cluster of sizes 7 -> 2 -> 3:
        // Should end up with 3 nodes in the cluster (one previously retired), and 4 retired
        ClusterSpec cluster = ClusterSpec.request(ClusterSpec.Type.content, ClusterSpec.Id.from("test")).vespaVersion("6.42").build();
        int wantedNodes;
        activate(applicationId, cluster, wantedNodes=7, 1);
        activate(applicationId, cluster, wantedNodes=2, 1);
        activate(applicationId, cluster, wantedNodes=3, 1);
        assertEquals(7, nodeRepository.nodes().list(Node.State.active).owner(applicationId).size());
        assertEquals(0, nodeRepository.nodes().list(Node.State.inactive).owner(applicationId).size());

        // Cause inactivation of retired nodes
        clock.advance(Duration.ofHours(30)); // Retire period spent
        MockDeployer deployer =
            new MockDeployer(provisioner,
                             clock,
                             Collections.singletonMap(applicationId, new MockDeployer.ApplicationContext(applicationId,
                                                                                                         cluster,
                                                                                                         Capacity.from(new ClusterResources(wantedNodes, 1, nodeResources)))));
        createRetiredExpirer(deployer).run();
        assertEquals(3, nodeRepository.nodes().list(Node.State.active).owner(applicationId).size());
        assertEquals(4, nodeRepository.nodes().list(Node.State.inactive).owner(applicationId).size());
        assertEquals(1, deployer.redeployments);

        // inactivated nodes are not retired
        for (Node node : nodeRepository.nodes().list(Node.State.inactive).owner(applicationId))
            assertFalse(node.allocation().get().membership().retired());
    }

    @Test
    public void ensure_early_inactivation() throws OrchestrationException {
        tester.makeReadyNodes(7, nodeResources);
        tester.makeReadyHosts(4, hostResources);

        ApplicationId applicationId = ApplicationId.from(TenantName.from("foo"), ApplicationName.from("bar"), InstanceName.from("fuz"));

        // Allocate content cluster of sizes 7 -> 2 -> 3:
        // Should end up with 3 nodes in the cluster (one previously retired), and 4 retired
        ClusterSpec cluster = ClusterSpec.request(ClusterSpec.Type.content, ClusterSpec.Id.from("test")).vespaVersion("6.42").build();
        int wantedNodes;
        activate(applicationId, cluster, wantedNodes=7, 1);
        activate(applicationId, cluster, wantedNodes=2, 1);
        activate(applicationId, cluster, wantedNodes=3, 1);
        assertEquals(7, nodeRepository.nodes().list(Node.State.active).owner(applicationId).size());
        assertEquals(0, nodeRepository.nodes().list(Node.State.inactive).owner(applicationId).size());

        // Cause inactivation of retired nodes
        MockDeployer deployer =
                new MockDeployer(provisioner,
                                 clock,
                                 Collections.singletonMap(
                                     applicationId,
                                     new MockDeployer.ApplicationContext(applicationId,
                                                                         cluster,
                                                                         Capacity.from(new ClusterResources(wantedNodes, 1, nodeResources)))));

        // Allow the 1st and 3rd retired nodes permission to inactivate
        doNothing()
                .doThrow(new OrchestrationException("Permission not granted 1"))
                .doNothing()
                .doThrow(new OrchestrationException("Permission not granted 2"))
                .when(orchestrator).acquirePermissionToRemove(any());

        RetiredExpirer retiredExpirer = createRetiredExpirer(deployer);
        retiredExpirer.run();
        assertEquals(5, nodeRepository.nodes().list(Node.State.active).owner(applicationId).size());
        assertEquals(2, nodeRepository.nodes().list(Node.State.inactive).owner(applicationId).size());
        assertEquals(1, deployer.redeployments);
        verify(orchestrator, times(4)).acquirePermissionToRemove(any());

        // Running it again has no effect
        retiredExpirer.run();
        assertEquals(5, nodeRepository.nodes().list(Node.State.active).owner(applicationId).size());
        assertEquals(2, nodeRepository.nodes().list(Node.State.inactive).owner(applicationId).size());
        assertEquals(1, deployer.redeployments);
        verify(orchestrator, times(6)).acquirePermissionToRemove(any());

        clock.advance(RETIRED_EXPIRATION.plusMinutes(1));
        retiredExpirer.run();
        assertEquals(3, nodeRepository.nodes().list(Node.State.active).owner(applicationId).size());
        assertEquals(4, nodeRepository.nodes().list(Node.State.inactive).owner(applicationId).size());
        assertEquals(2, deployer.redeployments);
        verify(orchestrator, times(6)).acquirePermissionToRemove(any());

        // inactivated nodes are not retired
        for (Node node : nodeRepository.nodes().list(Node.State.inactive).owner(applicationId))
            assertFalse(node.allocation().get().membership().retired());
    }

    @Test
    public void config_server_reprovisioning() throws OrchestrationException {
        NodeList configServers = tester.makeConfigServers(3, "default", Version.emptyVersion);
        var cfg1 = new HostName("cfg1");
        assertEquals(Set.of(cfg1.s(), "cfg2", "cfg3"), configServers.stream().map(Node::hostname).collect(Collectors.toSet()));

        var configServerApplication = new ConfigServerApplication();
        var duperModel = new MockDuperModel().support(configServerApplication);
        InfraDeployerImpl infraDeployer = new InfraDeployerImpl(tester.nodeRepository(), tester.provisioner(), duperModel);

        var deployer = mock(Deployer.class);
        when(deployer.deployFromLocalActive(eq(configServerApplication.getApplicationId())))
                .thenAnswer(invocation -> infraDeployer.getDeployment(configServerApplication.getApplicationId()));

        // Set wantToRetire on all 3 config servers
        List<Node> wantToRetireNodes = tester.nodeRepository().nodes()
                .retire(NodeTypeFilter.from(NodeType.config, null), Agent.operator, Instant.now());
        assertEquals(3, wantToRetireNodes.size());

        // Redeploy to retire all 3 config servers
        infraDeployer.activateAllSupportedInfraApplications(true);

        // Only 1 config server is allowed to retire at any given point in time
        List<Node> retiredNodes = tester.nodeRepository().nodes().list(() -> {}).stream()
                .filter(node -> node.allocation().map(allocation -> allocation.membership().retired()).orElse(false))
                .collect(Collectors.toList());
        assertEquals(1, retiredNodes.size());
        Node retiredNode = retiredNodes.get(0);
        String retiredNodeHostname = retiredNode.hostname();

        // Allow retiredNodeHostname to be removed
        doThrow(new OrchestrationException("denied")).when(orchestrator).acquirePermissionToRemove(any());
        doNothing().when(orchestrator).acquirePermissionToRemove(eq(new HostName(retiredNodeHostname)));

        // RetiredExpirer should remove cfg1 from application
        RetiredExpirer retiredExpirer = createRetiredExpirer(deployer);
        retiredExpirer.run();
        var activeConfigServerHostnames = new HashSet<>(Set.of("cfg1", "cfg2", "cfg3"));
        assertTrue(activeConfigServerHostnames.contains(retiredNodeHostname));
        activeConfigServerHostnames.remove(retiredNodeHostname);
        assertEquals(activeConfigServerHostnames, configServerHostnames(duperModel));
        assertEquals(1, tester.nodeRepository().nodes().list(Node.State.inactive).nodeType(NodeType.config).size());
        assertEquals(2, tester.nodeRepository().nodes().list(Node.State.active).nodeType(NodeType.config).size());

        // no changes while 1 cfg is inactive
        retiredExpirer.run();
        assertEquals(activeConfigServerHostnames, configServerHostnames(duperModel));
        assertEquals(1, tester.nodeRepository().nodes().list(Node.State.inactive).nodeType(NodeType.config).size());
        assertEquals(2, tester.nodeRepository().nodes().list(Node.State.active).nodeType(NodeType.config).size());

        // The node will eventually expire from inactive, and be removed by DynamicProvisioningMaintainer
        // (depending on its host), and these events should not affect the 2 active config servers.
        nodeRepository.nodes().deallocate(retiredNode, Agent.InactiveExpirer, "expired");
        retiredNode = tester.nodeRepository().nodes().list(Node.State.parked).nodeType(NodeType.config).asList().get(0);
        nodeRepository.nodes().removeRecursively(retiredNode, true);
        infraDeployer.activateAllSupportedInfraApplications(true);
        retiredExpirer.run();
        assertEquals(activeConfigServerHostnames, configServerHostnames(duperModel));
        assertEquals(2, tester.nodeRepository().nodes().list().nodeType(NodeType.config).size());
        assertEquals(2, tester.nodeRepository().nodes().list(Node.State.active).nodeType(NodeType.config).size());

        // Provision and ready new config server
        MockNameResolver nameResolver = (MockNameResolver)tester.nodeRepository().nameResolver();
        String ipv4 = "127.0.1.4";
        nameResolver.addRecord(retiredNodeHostname, ipv4);
        Node node = Node.create(retiredNodeHostname, new IP.Config(Set.of(ipv4), Set.of()), retiredNodeHostname,
                    tester.asFlavor("default", NodeType.config), NodeType.config).build();
        var nodes = List.of(node);
        nodes = nodeRepository.nodes().addNodes(nodes, Agent.system);
        nodes = nodeRepository.nodes().deallocate(nodes, Agent.system, getClass().getSimpleName());
        nodeRepository.nodes().setReady(nodes, Agent.system, getClass().getSimpleName());

        // no changes while replacement config server is ready
        retiredExpirer.run();
        assertEquals(activeConfigServerHostnames, configServerHostnames(duperModel));
        assertEquals(1, tester.nodeRepository().nodes().list(Node.State.ready).nodeType(NodeType.config).size());
        assertEquals(2, tester.nodeRepository().nodes().list(Node.State.active).nodeType(NodeType.config).size());

        // Activate replacement config server
        infraDeployer.activateAllSupportedInfraApplications(true);
        assertEquals(3, tester.nodeRepository().nodes().list(Node.State.active).nodeType(NodeType.config).size());

        // Another config server should now have retired
        retiredExpirer.run();
        assertEquals(3, tester.nodeRepository().nodes().list(Node.State.active).nodeType(NodeType.config).size());
        var retiredNodes2 = tester.nodeRepository().nodes().list(() -> {}).stream()
                .filter(n -> n.allocation().map(allocation -> allocation.membership().retired()).orElse(false))
                .collect(Collectors.toList());
        assertEquals(1, retiredNodes2.size());
        assertNotEquals(retiredNodeHostname, retiredNodes2.get(0));
    }

    private Set<String> configServerHostnames(MockDuperModel duperModel) {
        return duperModel.hostnamesOf(new ConfigServerApplication().getApplicationId()).stream()
                .map(com.yahoo.config.provision.HostName::value)
                .collect(Collectors.toSet());
    }

    private void activate(ApplicationId applicationId, ClusterSpec cluster, int nodes, int groups) {
        Capacity capacity = Capacity.from(new ClusterResources(nodes, groups, nodeResources));
        tester.activate(applicationId, tester.prepare(applicationId, cluster, capacity));
    }

    private RetiredExpirer createRetiredExpirer(Deployer deployer) {
        return new RetiredExpirer(nodeRepository,
                                  orchestrator,
                                  deployer,
                                  new TestMetric(),
                                  Duration.ofDays(30), /* Maintenance interval, use large value so it never runs by itself */
                                  RETIRED_EXPIRATION);
    }

}
