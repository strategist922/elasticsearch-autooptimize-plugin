package com.aconex.elasticsearch;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Settings;

public abstract class
        ClusterWideSingletonService extends AbstractLifecycleComponent<ClusterWideSingletonService> implements ClusterStateListener {


    public boolean isLocalNodeMaster() {
        return localNodeMaster;
    }


    @Override
    public final void clusterChanged(ClusterChangedEvent event) {

        boolean newLocalNodeMasterState = event.state().nodes().localNodeMaster();

        if (newLocalNodeMasterState != localNodeMaster) {
            localNodeMaster = newLocalNodeMasterState;

            if (newLocalNodeMasterState) {
                serviceIsNowOnMasterNode();
            } else {
                serviceIsNoLongerOnMasterNode();
            }
        }
    }

    /**
     * Plugin delegate method to be notified when the service transitions from being on the Master to no longer being the master node
     */
    protected abstract void serviceIsNoLongerOnMasterNode();

    protected abstract void serviceIsNowOnMasterNode();


    protected ClusterWideSingletonService(Settings settings) {
        super(settings);
    }

    protected ClusterWideSingletonService(Settings settings, Class customClass) {
        super(settings, customClass);
    }

    protected ClusterWideSingletonService(Settings settings, Class loggerClass, Class componentClass) {
        super(settings, loggerClass, componentClass);
    }

    protected ClusterWideSingletonService(Settings settings, String prefixSettings) {
        super(settings, prefixSettings);
    }

    protected ClusterWideSingletonService(Settings settings, String prefixSettings, Class customClass) {
        super(settings, prefixSettings, customClass);
    }

    protected ClusterWideSingletonService(Settings settings, String prefixSettings, Class loggerClass, Class componentClass) {
        super(settings, prefixSettings, loggerClass, componentClass);
    }


    private volatile boolean localNodeMaster = false;




}
