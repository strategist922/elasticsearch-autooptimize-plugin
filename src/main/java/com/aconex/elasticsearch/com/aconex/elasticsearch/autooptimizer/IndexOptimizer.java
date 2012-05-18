package com.aconex.elasticsearch.com.aconex.elasticsearch.autooptimizer;

import org.elasticsearch.client.IndicesAdminClient;

public class IndexOptimizer {
    private final IndicesAdminClient indicesAdminClient;
    private final String indexName;

    public IndexOptimizer(IndicesAdminClient indicesAdminClient, String indexName) {
        this.indicesAdminClient = indicesAdminClient;
        this.indexName = indexName;
    }

    public void optimize() {
        indicesAdminClient.prepareOptimize(indexName).setWaitForMerge(true);
    }
}
