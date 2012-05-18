package com.aconex.elasticsearch.com.aconex.elasticsearch.autooptimizer;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.admin.indices.optimize.OptimizeRequestBuilder;
import org.elasticsearch.action.admin.indices.optimize.OptimizeResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test
public class IndexOptimizerTest {

    private static final String INDEX_NAME = "paul";
    @Mock
    private IndicesAdminClient indicesAdminClient;

    @Mock
    private OptimizeRequestBuilder optimizeRequestBuilder;

    @Mock
    private OptimizeResponse optimizeResponse;

    @Mock
    private ListenableActionFuture<OptimizeResponse> optimizeResponses;

    @BeforeMethod
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);

        when(indicesAdminClient.prepareOptimize(anyString())).thenReturn(optimizeRequestBuilder);

        when(optimizeRequestBuilder.setWaitForMerge(true)).thenReturn(optimizeRequestBuilder);
        when(optimizeRequestBuilder.setMaxNumSegments(anyInt())).thenReturn(optimizeRequestBuilder);
        when(optimizeRequestBuilder.execute()).thenReturn(optimizeResponses);

        when(optimizeResponses.get()).thenReturn(optimizeResponse);
    }

    public void shouldWaitForMerge() {
        IndexOptimizer indexOptimizer = new IndexOptimizer(indicesAdminClient, INDEX_NAME);
        indexOptimizer.optimize();

        verify(optimizeRequestBuilder).setWaitForMerge(true);
    }

    public void shouldExecuteOptimizeAndWaitForResponse() throws Exception {
        IndexOptimizer indexOptimizer = new IndexOptimizer(indicesAdminClient, INDEX_NAME);
        indexOptimizer.optimize();

        verify(optimizeResponses).get();
    }

    public void shouldDefaultToOptimizeDownToOneSegmentMax() {
        IndexOptimizer indexOptimizer = new IndexOptimizer(indicesAdminClient, INDEX_NAME);
        indexOptimizer.optimize();

        verify(optimizeRequestBuilder).setMaxNumSegments(1);
    }
}
