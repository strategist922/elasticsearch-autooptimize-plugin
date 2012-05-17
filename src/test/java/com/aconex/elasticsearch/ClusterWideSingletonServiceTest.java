package com.aconex.elasticsearch;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import junit.framework.TestCase;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test
public class ClusterWideSingletonServiceTest extends TestCase {

    @Mock
    private ClusterChangedEvent localNodeBecameMasterEvent;

    @Mock
    private ClusterState localNodeBecameMasterState;

    @Mock
    private DiscoveryNodes localNodeBecameMasterDiscoveryNode;

    @Mock
    private ClusterChangedEvent localNodeNoLongerMasterEvent;

    @Mock
    private ClusterState localNodeNoLongerMasterState;

    @Mock
    private DiscoveryNodes localNodeNoLongerMasterDiscoveryNode;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        when(localNodeBecameMasterEvent.state()).thenReturn(localNodeBecameMasterState);
        when(localNodeBecameMasterState.nodes()).thenReturn(localNodeBecameMasterDiscoveryNode);
        when(localNodeBecameMasterDiscoveryNode.localNodeMaster()).thenReturn(true);

        when(localNodeNoLongerMasterEvent.state()).thenReturn(localNodeNoLongerMasterState);
        when(localNodeNoLongerMasterState.nodes()).thenReturn(localNodeNoLongerMasterDiscoveryNode);
        when(localNodeNoLongerMasterDiscoveryNode.localNodeMaster()).thenReturn(false);
    }


    public void shouldNotBeLocalNodeMasterUntilClusterEventIsReceived() {
        DummyClusterWideSingletonService clusterWideSingletonService = new DummyClusterWideSingletonService(ImmutableSettings.settingsBuilder().build());

        assertThat(clusterWideSingletonService.isLocalNodeMaster(), is(false));
    }

    public void shouldBecomeLocalNodeMasterWhenClusterEventReceived() {
        DummyClusterWideSingletonService clusterWideSingletonService = new DummyClusterWideSingletonService(ImmutableSettings.settingsBuilder().build());
        clusterWideSingletonService.clusterChanged(localNodeBecameMasterEvent);

        assertThat(clusterWideSingletonService.isLocalNodeMaster(), is(true));
    }

    public void shouldNotifyDelegateMethodThatServiceIsNowMaster() {
        DummyClusterWideSingletonService clusterWideSingletonService = spy(new DummyClusterWideSingletonService(ImmutableSettings.settingsBuilder().build()));
        clusterWideSingletonService.clusterChanged(localNodeBecameMasterEvent);

        verify(clusterWideSingletonService).serviceIsNowOnMasterNode();
    }

    public void shouldNotifyDelegateMethodThatNodeIsNowNoLongerTheMasterNode() {
        DummyClusterWideSingletonService clusterWideSingletonService = spy(new DummyClusterWideSingletonService(ImmutableSettings.settingsBuilder().build()));
        clusterWideSingletonService.clusterChanged(localNodeBecameMasterEvent);
        clusterWideSingletonService.clusterChanged(localNodeNoLongerMasterEvent);

        verify(clusterWideSingletonService).serviceIsNoLongerOnMasterNode();
    }

    public void shouldNotNotifyDelegateMethodsIfClusterStateEventHasNotReallyChanged() {
        DummyClusterWideSingletonService clusterWideSingletonService = spy(new DummyClusterWideSingletonService(ImmutableSettings.settingsBuilder().build()));
        clusterWideSingletonService.clusterChanged(localNodeBecameMasterEvent);
        clusterWideSingletonService.clusterChanged(localNodeBecameMasterEvent);

        verify(clusterWideSingletonService, times(1)).serviceIsNowOnMasterNode();
    }
}
