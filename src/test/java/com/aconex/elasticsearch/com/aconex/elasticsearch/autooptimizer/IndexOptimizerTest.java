package com.aconex.elasticsearch.com.aconex.elasticsearch.autooptimizer;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.elasticsearch.action.admin.indices.optimize.OptimizeRequestBuilder;
import org.elasticsearch.client.IndicesAdminClient;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test
public class IndexOptimizerTest {

    @Mock
    private IndicesAdminClient indicesAdminClient;

    @Mock
    private OptimizeRequestBuilder optimizeRequestBuilder;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);

        when(indicesAdminClient.prepareOptimize(anyString())).thenReturn(optimizeRequestBuilder);
    }

    public void shouldWaitForMerge() {

        IndexOptimizer indexOptimizer = new IndexOptimizer(indicesAdminClient, "paul");
        indexOptimizer.optimize();

        verify(optimizeRequestBuilder).setWaitForMerge(true);

    }


}
