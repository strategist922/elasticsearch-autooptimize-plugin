package com.aconex.elasticsearch.com.aconex.elasticsearch.autooptimizer;

import org.elasticsearch.action.admin.indices.optimize.OptimizeResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;

public class IndexOptimizer {
    private final ESLogger log = ESLoggerFactory.getLogger(IndexOptimizer.class.getName());

    private final IndicesAdminClient indicesAdminClient;
    private final String indexName;
    private final int maxNumSegments = 1;

    public IndexOptimizer(IndicesAdminClient indicesAdminClient, String indexName) {
        this.indicesAdminClient = indicesAdminClient;
        this.indexName = indexName;
    }

    public void optimize() {
        try {
            OptimizeResponse optimizeResponse = indicesAdminClient.prepareOptimize(indexName).setMaxNumSegments(maxNumSegments).setWaitForMerge(true).execute().get();
            log.info("Optimize of '{}' completed: {} success, {} failures", indexName, optimizeResponse.successfulShards(), optimizeResponse.failedShards());
        } catch (Exception e) {
            log.error("Error during ...optimize", e);
        }
    }

}
