package com.aconex.elasticsearch;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.settings.Settings;

public class DummyClusterWideSingletonService extends ClusterWideSingletonService {

    DummyClusterWideSingletonService(Settings settings) {
        super(settings);
    }

    @Override
    protected void serviceIsNoLongerOnMasterNode() {

    }

    @Override
    public void serviceIsNowOnMasterNode() {

    }

    @Override
    protected void doStart() throws ElasticSearchException {

    }

    @Override
    protected void doStop() throws ElasticSearchException {

    }

    @Override
    protected void doClose() throws ElasticSearchException {

    }
}
