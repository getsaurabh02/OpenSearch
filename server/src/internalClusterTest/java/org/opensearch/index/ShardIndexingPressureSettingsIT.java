/*
 * Copyright OpenSearch Contributors.
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.index;

import org.apache.lucene.util.RamUsageEstimator;
import org.opensearch.action.ActionFuture;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.admin.indices.stats.CommonStatsFlags;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.action.bulk.BulkItemRequest;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.bulk.BulkShardRequest;
import org.opensearch.action.bulk.TransportShardBulkAction;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.UUIDs;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchRejectedExecutionException;
import org.opensearch.index.shard.ShardId;
import org.opensearch.indices.IndicesService;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 2, numClientNodes = 1,
    transportClientRatio = 0.0D)
public class ShardIndexingPressureSettingsIT extends OpenSearchIntegTestCase {

    public static final String INDEX_NAME = "test_index";

    private static final Settings unboundedWriteQueue = Settings.builder().put("thread_pool.write.queue_size", -1).build();

    public static final Settings settings = Settings.builder()
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED.getKey(), false)
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENFORCED.getKey(), false)
            .build();

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(unboundedWriteQueue)
                .put(settings)
                .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class, InternalSettingsPlugin.class);
    }

    @Override
    protected int numberOfReplicas() {
        return 1;
    }

    @Override
    protected int numberOfShards() {
        return 1;
    }

    public void testNodeAttributeSetForShardIndexingPressure() throws Exception {
        assertAcked(prepareCreate(INDEX_NAME, Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)));

        ensureGreen(INDEX_NAME);

        for (DiscoveryNode nodes : client().admin().cluster().prepareState().get().getState().nodes()) {
            assertEquals("true", nodes.getAttributes().get("shard_indexing_pressure_enabled"));
        }
    }

    public void testShardIndexingPressureFeatureEnabledDisabledSetting() {
        final CommonStatsFlags statsFlag = new CommonStatsFlags();
        statsFlag.includeAllShardIndexingPressureTrackers(true);

        assertAcked(prepareCreate(INDEX_NAME, Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)));

        ensureGreen(INDEX_NAME);
        Tuple<String, String> primaryReplicaNodeNames = getPrimaryReplicaNodeNames(INDEX_NAME);
        String primaryName = primaryReplicaNodeNames.v1();
        String replicaName = primaryReplicaNodeNames.v2();
        String coordinatingOnlyNode = getCoordinatingOnlyNode();

        final BulkRequest bulkRequest = new BulkRequest();
        int totalRequestSize = 0;
        for (int i = 0; i < 80; ++i) {
            IndexRequest request = new IndexRequest(INDEX_NAME).id(UUIDs.base64UUID())
                    .source(Collections.singletonMap("key", randomAlphaOfLength(50)));
            totalRequestSize += request.ramBytesUsed();
            assertTrue(request.ramBytesUsed() > request.source().length());
            bulkRequest.add(request);
        }

        final long bulkShardRequestSize = totalRequestSize + (RamUsageEstimator.shallowSizeOfInstance(BulkItemRequest.class) * 80)
                + RamUsageEstimator.shallowSizeOfInstance(BulkShardRequest.class);

        ActionFuture<BulkResponse> successFuture = client(coordinatingOnlyNode).bulk(bulkRequest);
        successFuture.actionGet();

        IndexService indexService = internalCluster().getInstance(IndicesService.class, primaryName).iterator().next();
        Index index = indexService.getIndexSettings().getIndex();
        ShardId shardId = new ShardId(index, 0);

        IndexingPressureService primaryShardTracker = internalCluster()
            .getInstance(IndexingPressureService.class, primaryName);
        IndexingPressureService replicaShardTracker = internalCluster()
            .getInstance(IndexingPressureService.class, replicaName);
        IndexingPressureService coordinatingShardTracker = internalCluster()
            .getInstance(IndexingPressureService.class, coordinatingOnlyNode);

        // Assert no tracking at shard level for indexing pressure since default setting is disabled
        assertNull(primaryShardTracker.shardStats(statsFlag).getIndexingPressureShardStats(shardId));
        assertEquals(bulkRequest.ramBytesUsed(), coordinatingShardTracker.nodeStats().getTotalCoordinatingBytes());
        assertEquals(bulkShardRequestSize, primaryShardTracker.nodeStats().getTotalPrimaryBytes());
        assertEquals(bulkShardRequestSize, replicaShardTracker.nodeStats().getTotalReplicaBytes());

        // Enable the setting for shard indexing pressure as true
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(Settings.builder()
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED.getKey(), true));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        // Send a second request
        successFuture = client(coordinatingOnlyNode).bulk(bulkRequest);
        successFuture.actionGet();

        assertEquals(bulkShardRequestSize, coordinatingShardTracker.shardStats(statsFlag)
            .getIndexingPressureShardStats(shardId).getTotalCombinedCoordinatingAndPrimaryBytes());
        assertEquals(bulkShardRequestSize, coordinatingShardTracker.shardStats(statsFlag)
            .getIndexingPressureShardStats(shardId).getTotalCoordinatingBytes());
        assertEquals(bulkShardRequestSize, primaryShardTracker.shardStats(statsFlag)
            .getIndexingPressureShardStats(shardId).getTotalCombinedCoordinatingAndPrimaryBytes());
        assertEquals(bulkShardRequestSize, primaryShardTracker.shardStats(statsFlag)
            .getIndexingPressureShardStats(shardId).getTotalPrimaryBytes());
        assertEquals(bulkShardRequestSize, replicaShardTracker.shardStats(statsFlag)
            .getIndexingPressureShardStats(shardId).getTotalReplicaBytes());

        assertEquals(bulkRequest.ramBytesUsed() + bulkShardRequestSize, coordinatingShardTracker.nodeStats()
            .getTotalCoordinatingBytes());
        assertEquals(2 * bulkShardRequestSize, primaryShardTracker.nodeStats().getTotalPrimaryBytes());
        assertEquals(2 * bulkShardRequestSize, replicaShardTracker.nodeStats().getTotalReplicaBytes());

        // Disable the setting again for shard indexing pressure as true
        updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(Settings.builder()
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED.getKey(), false));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        // Send a third request which should not be tracked since feature is disabled again
        successFuture = client(coordinatingOnlyNode).bulk(bulkRequest);
        successFuture.actionGet();

        assertEquals(bulkShardRequestSize, coordinatingShardTracker.shardStats(statsFlag)
            .getIndexingPressureShardStats(shardId).getTotalCombinedCoordinatingAndPrimaryBytes());
        assertEquals(bulkShardRequestSize, coordinatingShardTracker.shardStats(statsFlag)
            .getIndexingPressureShardStats(shardId).getTotalCoordinatingBytes());
        assertEquals(bulkShardRequestSize, primaryShardTracker.shardStats(statsFlag)
            .getIndexingPressureShardStats(shardId).getTotalCombinedCoordinatingAndPrimaryBytes());
        assertEquals(bulkShardRequestSize, primaryShardTracker.shardStats(statsFlag)
            .getIndexingPressureShardStats(shardId).getTotalPrimaryBytes());
        assertEquals(bulkShardRequestSize, replicaShardTracker.shardStats(statsFlag)
            .getIndexingPressureShardStats(shardId).getTotalReplicaBytes());

        assertEquals(2 * bulkRequest.ramBytesUsed() + bulkShardRequestSize, coordinatingShardTracker.nodeStats()
            .getTotalCoordinatingBytes());
        assertEquals(3 * bulkShardRequestSize, primaryShardTracker.nodeStats()
            .getTotalPrimaryBytes());
        assertEquals(3 * bulkShardRequestSize, replicaShardTracker.nodeStats()
            .getTotalReplicaBytes());
    }

    public void testShardIndexingPressureNodeLimitUpdateSetting() throws Exception {
        final CommonStatsFlags statsFlag = new CommonStatsFlags();
        statsFlag.includeAllShardIndexingPressureTrackers(true);

        final BulkRequest bulkRequest = new BulkRequest();
        int totalRequestSize = 0;
        for (int i = 0; i < 80; ++i) {
            IndexRequest request = new IndexRequest(INDEX_NAME).id(UUIDs.base64UUID())
                    .source(Collections.singletonMap("key", randomAlphaOfLength(50)));
            totalRequestSize += request.ramBytesUsed();
            assertTrue(request.ramBytesUsed() > request.source().length());
            bulkRequest.add(request);
        }
        final long  bulkShardRequestSize = totalRequestSize + (RamUsageEstimator.shallowSizeOfInstance(BulkItemRequest.class) * 80)
                + RamUsageEstimator.shallowSizeOfInstance(BulkShardRequest.class);

        // Set the Node limit threshold above the request-size; for no rejection
        restartCluster(Settings.builder().put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED.getKey(), true)
                .put(IndexingPressure.MAX_INDEXING_BYTES.getKey(), (long) (bulkShardRequestSize * 1.5) + "B").build());

        assertAcked(prepareCreate(INDEX_NAME, Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)));
        ensureGreen(INDEX_NAME);

        Tuple<String, String> primaryReplicaNodeNames = getPrimaryReplicaNodeNames(INDEX_NAME);
        String primaryName = primaryReplicaNodeNames.v1();
        String replicaName = primaryReplicaNodeNames.v2();
        String coordinatingOnlyNode = getCoordinatingOnlyNode();

        ActionFuture<BulkResponse> successFuture = client(coordinatingOnlyNode).bulk(bulkRequest);
        successFuture.actionGet();

        IndexService indexService = internalCluster().getInstance(IndicesService.class, primaryName).iterator().next();
        Index index = indexService.getIndexSettings().getIndex();
        ShardId shardId= new ShardId(index, 0);


        IndexingPressureService primaryShardTracker = internalCluster()
            .getInstance(IndexingPressureService.class, primaryName);
        IndexingPressureService replicaShardTracker = internalCluster()
            .getInstance(IndexingPressureService.class, replicaName);
        IndexingPressureService coordinatingShardTracker = internalCluster()
            .getInstance(IndexingPressureService.class, coordinatingOnlyNode);

        // Tracking done with no rejections
        assertEquals(0, coordinatingShardTracker.shardStats(statsFlag)
            .getIndexingPressureShardStats(shardId).getCoordinatingRejections());
        assertEquals(bulkShardRequestSize, coordinatingShardTracker.shardStats(statsFlag)
            .getIndexingPressureShardStats(shardId).getTotalCoordinatingBytes());
        assertEquals(bulkShardRequestSize, primaryShardTracker.shardStats(statsFlag)
            .getIndexingPressureShardStats(shardId).getTotalPrimaryBytes());
        assertEquals(bulkShardRequestSize, replicaShardTracker.shardStats(statsFlag)
            .getIndexingPressureShardStats(shardId).getTotalReplicaBytes());

        // Update the indexing byte setting to a lower value
        restartCluster(Settings.builder().put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED.getKey(), true)
                .put(IndexingPressure.MAX_INDEXING_BYTES.getKey(),
                (long) (bulkShardRequestSize * 0.5) + "B").build());

        // Any node receiving the request will end up rejecting request due to node level limit breached
        expectThrows(OpenSearchRejectedExecutionException.class, () -> {
            if (randomBoolean()) {
                client(coordinatingOnlyNode).bulk(bulkRequest).actionGet();
            } else if (randomBoolean()) {
                client(primaryName).bulk(bulkRequest).actionGet();
            } else {
                client(replicaName).bulk(bulkRequest).actionGet();
            }
        });
    }

    public void testShardIndexingPressureEnforcedEnabledDisabledSetting() throws Exception {
        final CommonStatsFlags statsFlag = new CommonStatsFlags();
        statsFlag.includeAllShardIndexingPressureTrackers(true);

        final BulkRequest bulkRequest = new BulkRequest();
        int totalRequestSize = 0;
        for (int i = 0; i < 80; ++i) {
            IndexRequest request = new IndexRequest(INDEX_NAME).id(UUIDs.base64UUID())
                    .source(Collections.singletonMap("key", randomAlphaOfLength(50)));
            totalRequestSize += request.ramBytesUsed();
            assertTrue(request.ramBytesUsed() > request.source().length());
            bulkRequest.add(request);
        }
        final long  bulkShardRequestSize = totalRequestSize + (RamUsageEstimator.shallowSizeOfInstance(BulkItemRequest.class) * 80)
                + RamUsageEstimator.shallowSizeOfInstance(BulkShardRequest.class);

        Settings settings = Settings.builder()
                .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED.getKey(), true)
                .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENFORCED.getKey(), true)
                .put(IndexingPressure.MAX_INDEXING_BYTES.getKey(), (long) (bulkShardRequestSize * 3.8) + "B")
                .put(ShardIndexingPressureMemoryManager.MAX_OUTSTANDING_REQUESTS.getKey(), 1)
                .put(ShardIndexingPressureMemoryManager.SUCCESSFUL_REQUEST_ELAPSED_TIMEOUT.getKey(), 10)
                .build();
        restartCluster(settings);

        assertAcked(prepareCreate(INDEX_NAME, Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)));
        ensureGreen(INDEX_NAME);

        Tuple<String, String> primaryReplicaNodeNames = getPrimaryReplicaNodeNames(INDEX_NAME);
        String primaryName = primaryReplicaNodeNames.v1();
        String coordinatingOnlyNode = getCoordinatingOnlyNode();

        IndexService indexService = internalCluster().getInstance(IndicesService.class, primaryName).iterator().next();
        Index index = indexService.getIndexSettings().getIndex();
        ShardId shardId= new ShardId(index, 0);

        IndexingPressureService coordinatingShardTracker = internalCluster()
            .getInstance(IndexingPressureService.class, coordinatingOnlyNode);

        // Send first request which gets successful to set the last successful time-stamp
        ActionFuture<BulkResponse> successFuture = client(coordinatingOnlyNode).bulk(bulkRequest);
        successFuture.actionGet();

        // Send couple of more requests which remains outstanding
        successFuture = client(coordinatingOnlyNode).bulk(bulkRequest);
        ActionFuture<BulkResponse> secondSuccessFuture = client(coordinatingOnlyNode).bulk(bulkRequest);
        // Delay to breach the success time stamp threshold
        Thread.sleep(25);

        // This request breaches the threshold and hence will be rejected
        expectThrows(OpenSearchRejectedExecutionException.class, () -> client(coordinatingOnlyNode).bulk(bulkRequest).actionGet());
        assertEquals(1, coordinatingShardTracker.shardStats(statsFlag)
            .getIndexingPressureShardStats(shardId).getCoordinatingRejections());
        assertEquals(1, coordinatingShardTracker.shardStats(statsFlag)
            .getIndexingPressureShardStats(shardId).getCoordinatingLastSuccessfulRequestLimitsBreachedRejections());

        successFuture.actionGet();
        secondSuccessFuture.actionGet();

        // Disable the enforced mode by setting it to false
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(Settings.builder()
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENFORCED.getKey(), false));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        // Repeat the previous set of requests; but no actual rejection this time
        successFuture = client(coordinatingOnlyNode).bulk(bulkRequest);
        successFuture.actionGet();
        successFuture = client(coordinatingOnlyNode).bulk(bulkRequest);
        secondSuccessFuture = client(coordinatingOnlyNode).bulk(bulkRequest);
        Thread.sleep(11);
        ActionFuture<BulkResponse> thirdSuccessFuture = client(coordinatingOnlyNode).bulk(bulkRequest);

        // No new actual rejection
        assertEquals(1, coordinatingShardTracker.shardStats(statsFlag)
            .getIndexingPressureShardStats(shardId).getCoordinatingRejections());
        // Shadow mode rejection count breakup still updated
        assertEquals(2, coordinatingShardTracker.shardStats(statsFlag)
            .getIndexingPressureShardStats(shardId).getCoordinatingLastSuccessfulRequestLimitsBreachedRejections());

        successFuture.actionGet();
        secondSuccessFuture.actionGet();
        thirdSuccessFuture.actionGet();

        // Enable the enforced mode again by setting it to true
        updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(Settings.builder()
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENFORCED.getKey(), true));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        // Repeat the previous set of requests; an actual rejection this time
        successFuture = client(coordinatingOnlyNode).bulk(bulkRequest);
        successFuture.actionGet();
        client(coordinatingOnlyNode).bulk(bulkRequest);
        client(coordinatingOnlyNode).bulk(bulkRequest);
        Thread.sleep(11);
        expectThrows(OpenSearchRejectedExecutionException.class, () -> client(coordinatingOnlyNode).bulk(bulkRequest).actionGet());

        // new rejection added to the actual rejection count
        assertEquals(2, coordinatingShardTracker.shardStats(statsFlag)
            .getIndexingPressureShardStats(shardId).getCoordinatingRejections());
        assertEquals(3, coordinatingShardTracker.shardStats(statsFlag)
            .getIndexingPressureShardStats(shardId).getCoordinatingLastSuccessfulRequestLimitsBreachedRejections());
    }

    public void testShardIndexingPressureEnforcedEnabledNoOpIfFeatureDisabled() throws Exception {
        final CommonStatsFlags statsFlag = new CommonStatsFlags();
        statsFlag.includeAllShardIndexingPressureTrackers(true);

        final BulkRequest bulkRequest = new BulkRequest();
        int totalRequestSize = 0;
        for (int i = 0; i < 80; ++i) {
            IndexRequest request = new IndexRequest(INDEX_NAME).id(UUIDs.base64UUID())
                    .source(Collections.singletonMap("key", randomAlphaOfLength(50)));
            totalRequestSize += request.ramBytesUsed();
            assertTrue(request.ramBytesUsed() > request.source().length());
            bulkRequest.add(request);
        }

        Settings settings = Settings.builder()
                .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED.getKey(), false)
                .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENFORCED.getKey(), true)
                .build();
        restartCluster(settings);

        assertAcked(prepareCreate(INDEX_NAME, Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)));
        ensureGreen(INDEX_NAME);

        Tuple<String, String> primaryReplicaNodeNames = getPrimaryReplicaNodeNames(INDEX_NAME);
        String primaryName = primaryReplicaNodeNames.v1();
        String coordinatingOnlyNode = getCoordinatingOnlyNode();

        IndexService indexService = internalCluster().getInstance(IndicesService.class, primaryName).iterator().next();
        Index index = indexService.getIndexSettings().getIndex();
        ShardId shardId= new ShardId(index, 0);

        // Send first request
        ActionFuture<BulkResponse> successFuture = client(coordinatingOnlyNode).bulk(bulkRequest);
        successFuture.actionGet();

        // Verify No actual tracking being done

        IndexingPressureService primaryShardTracker = internalCluster().getInstance(IndexingPressureService.class, primaryName);
        assertNull(primaryShardTracker.shardStats(statsFlag).getIndexingPressureShardStats(shardId));
    }

    public void testShardIndexingPressureVerifyShardMinLimitSettingUpdate() throws Exception {
        final CommonStatsFlags statsFlag = new CommonStatsFlags();
        statsFlag.includeAllShardIndexingPressureTrackers(true);

        final BulkRequest bulkRequest = new BulkRequest();
        int totalRequestSize = 0;
        for (int i = 0; i < 80; ++i) {
            IndexRequest request = new IndexRequest(INDEX_NAME).id(UUIDs.base64UUID())
                    .source(Collections.singletonMap("key", randomAlphaOfLength(50)));
            totalRequestSize += request.ramBytesUsed();
            assertTrue(request.ramBytesUsed() > request.source().length());
            bulkRequest.add(request);
        }

        // Set the Node limit threshold above the request-size; for no rejection
        long initialNodeLimit = 1000000;
        restartCluster(Settings.builder().put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED.getKey(), true)
                .put(IndexingPressure.MAX_INDEXING_BYTES.getKey(), (long) initialNodeLimit + "B").build());

        assertAcked(prepareCreate(INDEX_NAME, Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)));
        ensureGreen(INDEX_NAME);

        Tuple<String, String> primaryReplicaNodeNames = getPrimaryReplicaNodeNames(INDEX_NAME);
        String primaryName = primaryReplicaNodeNames.v1();
        String replicaName = primaryReplicaNodeNames.v2();
        String coordinatingOnlyNode = getCoordinatingOnlyNode();

        ActionFuture<BulkResponse> successFuture = client(coordinatingOnlyNode).bulk(bulkRequest);
        successFuture.actionGet();

        IndexService indexService = internalCluster().getInstance(IndicesService.class, primaryName).iterator().next();
        Index index = indexService.getIndexSettings().getIndex();
        ShardId shardId= new ShardId(index, 0);

        IndexingPressureService primaryShardTracker = internalCluster()
            .getInstance(IndexingPressureService.class, primaryName);
        IndexingPressureService replicaShardTracker = internalCluster()
            .getInstance(IndexingPressureService.class, replicaName);
        IndexingPressureService coordinatingShardTracker = internalCluster()
            .getInstance(IndexingPressureService.class, coordinatingOnlyNode);

        // Verify initial shard limits
        assertEquals(1000, coordinatingShardTracker.shardStats(statsFlag)
            .getIndexingPressureShardStats(shardId).getCurrentPrimaryAndCoordinatingLimits());
        assertEquals(1000, primaryShardTracker.shardStats(statsFlag)
            .getIndexingPressureShardStats(shardId).getCurrentPrimaryAndCoordinatingLimits());
        assertEquals(1500, replicaShardTracker.shardStats(statsFlag)
            .getIndexingPressureShardStats(shardId).getCurrentReplicaLimits());

        // New Increment factor
        double incrementFactor = 0.01d;
        // Update the setting for initial shard limit
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(Settings.builder()
            .put(ShardIndexingPressureSettings.SHARD_MIN_LIMIT.getKey(), incrementFactor));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        // Create a new index to send new request
        assertAcked(prepareCreate(INDEX_NAME + "new", Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)));
        ensureGreen(INDEX_NAME + "new");

        primaryReplicaNodeNames = getPrimaryReplicaNodeNames(INDEX_NAME + "new");
        primaryName = primaryReplicaNodeNames.v1();
        replicaName = primaryReplicaNodeNames.v2();
        coordinatingOnlyNode = getCoordinatingOnlyNode();

        successFuture = client(coordinatingOnlyNode).bulk(bulkRequest);
        successFuture.actionGet();

        indexService = internalCluster().getInstance(IndicesService.class, primaryName).iterator().next();
        index = indexService.getIndexSettings().getIndex();
        shardId= new ShardId(index, 0);

        primaryShardTracker = internalCluster().getInstance(IndexingPressureService.class, primaryName);
        replicaShardTracker = internalCluster().getInstance(IndexingPressureService.class, replicaName);
        coordinatingShardTracker = internalCluster().getInstance(IndexingPressureService.class, coordinatingOnlyNode);

        // Verify updated initial shard limits
        assertEquals(10000, coordinatingShardTracker.shardStats(statsFlag)
            .getIndexingPressureShardStats(shardId).getCurrentPrimaryAndCoordinatingLimits());
        assertEquals(10000, primaryShardTracker.shardStats(statsFlag)
            .getIndexingPressureShardStats(shardId).getCurrentPrimaryAndCoordinatingLimits());
        assertEquals(15000, replicaShardTracker.shardStats(statsFlag)
            .getIndexingPressureShardStats(shardId).getCurrentReplicaLimits());
    }

    public void testShardIndexingPressureLastSuccessfulSettingsUpdate() throws Exception {
        final CommonStatsFlags statsFlag = new CommonStatsFlags();
        statsFlag.includeAllShardIndexingPressureTrackers(true);

        final BulkRequest bulkRequest = new BulkRequest();
        int totalRequestSize = 0;
        for (int i = 0; i < 80; ++i) {
            IndexRequest request = new IndexRequest(INDEX_NAME).id(UUIDs.base64UUID())
                    .source(Collections.singletonMap("key", randomAlphaOfLength(50)));
            totalRequestSize += request.ramBytesUsed();
            assertTrue(request.ramBytesUsed() > request.source().length());
            bulkRequest.add(request);
        }
        final long  bulkShardRequestSize = totalRequestSize + (RamUsageEstimator.shallowSizeOfInstance(BulkItemRequest.class) * 80)
                + RamUsageEstimator.shallowSizeOfInstance(BulkShardRequest.class);

        boolean randomBoolean = randomBoolean();

        Settings settings = Settings.builder()
                .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED.getKey(), true)
                .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENFORCED.getKey(), true)
                .put(IndexingPressure.MAX_INDEXING_BYTES.getKey(), (long) (bulkShardRequestSize * 3.8) + "B")
                .put(ShardIndexingPressureMemoryManager.MAX_OUTSTANDING_REQUESTS.getKey(), 1)
                .put(ShardIndexingPressureMemoryManager.SUCCESSFUL_REQUEST_ELAPSED_TIMEOUT.getKey(), 10)
                .build();
        restartCluster(settings);

        assertAcked(prepareCreate(INDEX_NAME, Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)));
        ensureGreen(INDEX_NAME);

        Tuple<String, String> primaryReplicaNodeNames = getPrimaryReplicaNodeNames(INDEX_NAME);
        String primaryName = primaryReplicaNodeNames.v1();
        String coordinatingOnlyNode = getCoordinatingOnlyNode();

        IndexService indexService = internalCluster().getInstance(IndicesService.class, primaryName).iterator().next();
        Index index = indexService.getIndexSettings().getIndex();
        ShardId shardId= new ShardId(index, 0);

        IndexingPressureService primaryShardTracker = internalCluster()
            .getInstance(IndexingPressureService.class, primaryName);
        IndexingPressureService coordinatingShardTracker = internalCluster()
            .getInstance(IndexingPressureService.class, coordinatingOnlyNode);

        // Send first request which gets successful to set the last successful time-stamp
        if (randomBoolean) {
            ActionFuture<BulkResponse> successFuture = client(coordinatingOnlyNode).bulk(bulkRequest);
            successFuture.actionGet();
        } else {
            ActionFuture<BulkResponse> successFuture = client(primaryName).bulk(bulkRequest);
            successFuture.actionGet();
        }

        // Send couple of more requests which remains outstanding to increase time-stamp value
        ActionFuture<BulkResponse> successFuture;
        ActionFuture<BulkResponse> secondSuccessFuture;
        if (randomBoolean) {
            successFuture = client(coordinatingOnlyNode).bulk(bulkRequest);
            secondSuccessFuture = client(coordinatingOnlyNode).bulk(bulkRequest);
        } else {
            successFuture = client(primaryName).bulk(bulkRequest);
            secondSuccessFuture = client(primaryName).bulk(bulkRequest);
        }

        // Delay to breach the success time stamp threshold
        Thread.sleep(25);

        // This request breaches the threshold and hence will be rejected
        if (randomBoolean) {
            expectThrows(OpenSearchRejectedExecutionException.class, () -> client(coordinatingOnlyNode).bulk(bulkRequest).actionGet());
            assertEquals(1, coordinatingShardTracker.shardStats(statsFlag)
                .getIndexingPressureShardStats(shardId).getCoordinatingRejections());
            assertEquals(1, coordinatingShardTracker.shardStats(statsFlag)
                .getIndexingPressureShardStats(shardId).getCoordinatingLastSuccessfulRequestLimitsBreachedRejections());
        } else {
            expectThrows(OpenSearchRejectedExecutionException.class, () -> client(primaryName).bulk(bulkRequest).actionGet());
            assertEquals(1, primaryShardTracker.shardStats(statsFlag)
                .getIndexingPressureShardStats(shardId).getCoordinatingRejections());
            assertEquals(1, primaryShardTracker.shardStats(statsFlag)
                .getIndexingPressureShardStats(shardId).getCoordinatingLastSuccessfulRequestLimitsBreachedRejections());
        }

        successFuture.actionGet();
        secondSuccessFuture.actionGet();

        // Update the outstanding threshold setting to see no rejections
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(Settings.builder()
            .put(ShardIndexingPressureMemoryManager.MAX_OUTSTANDING_REQUESTS.getKey(), 10));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        // Verify no rejection with similar request pattern
        if (randomBoolean) {
            successFuture = client(coordinatingOnlyNode).bulk(bulkRequest);
            secondSuccessFuture = client(coordinatingOnlyNode).bulk(bulkRequest);
        } else {
            successFuture = client(primaryName).bulk(bulkRequest);
            secondSuccessFuture = client(primaryName).bulk(bulkRequest);
        }
        Thread.sleep(25);
        if (randomBoolean) {
            client(coordinatingOnlyNode).bulk(bulkRequest).actionGet();
            assertEquals(1, coordinatingShardTracker.shardStats(statsFlag)
                .getIndexingPressureShardStats(shardId).getCoordinatingRejections());
            assertEquals(1, coordinatingShardTracker.shardStats(statsFlag)
                .getIndexingPressureShardStats(shardId).getCoordinatingLastSuccessfulRequestLimitsBreachedRejections());
        } else {
            client(primaryName).bulk(bulkRequest).actionGet();
            assertEquals(1, primaryShardTracker.shardStats(statsFlag)
                .getIndexingPressureShardStats(shardId).getCoordinatingRejections());
            assertEquals(1, primaryShardTracker.shardStats(statsFlag)
                .getIndexingPressureShardStats(shardId).getCoordinatingLastSuccessfulRequestLimitsBreachedRejections());
        }

        successFuture.actionGet();
        secondSuccessFuture.actionGet();
    }

    public void testShardIndexingPressureRequestSizeWindowSettingUpdate() throws Exception {
        final CommonStatsFlags statsFlag = new CommonStatsFlags();
        statsFlag.includeAllShardIndexingPressureTrackers(true);

        final BulkRequest bulkRequest = new BulkRequest();
        int totalRequestSize = 0;
        for (int i = 0; i < 80; ++i) {
            IndexRequest request = new IndexRequest(INDEX_NAME).id(UUIDs.base64UUID())
                .source(Collections.singletonMap("key", randomAlphaOfLength(50)));
            totalRequestSize += request.ramBytesUsed();
            assertTrue(request.ramBytesUsed() > request.source().length());
            bulkRequest.add(request);
        }
        final long  bulkShardRequestSize = totalRequestSize + (RamUsageEstimator.shallowSizeOfInstance(BulkItemRequest.class) * 80)
            + RamUsageEstimator.shallowSizeOfInstance(BulkShardRequest.class);

        Settings settings = Settings.builder()
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED.getKey(), true)
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENFORCED.getKey(), true)
            .put(IndexingPressure.MAX_INDEXING_BYTES.getKey(), (long) (bulkShardRequestSize * 1.2) + "B")
            .put(ShardIndexingPressureSettings.REQUEST_SIZE_WINDOW.getKey(), 1)
            .put(ShardIndexingPressureMemoryManager.THROUGHPUT_DEGRADATION_LIMITS.getKey(), 1)
            .build();
        restartCluster(settings);

        assertAcked(prepareCreate(INDEX_NAME, Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)));
        ensureGreen(INDEX_NAME);

        Tuple<String, String> primaryReplicaNodeNames = getPrimaryReplicaNodeNames(INDEX_NAME);
        String primaryName = primaryReplicaNodeNames.v1();
        String replicaName = primaryReplicaNodeNames.v2();
        String coordinatingOnlyNode = getCoordinatingOnlyNode();

        IndexService indexService = internalCluster().getInstance(IndicesService.class, primaryName).iterator().next();
        Index index = indexService.getIndexSettings().getIndex();
        ShardId shardId= new ShardId(index, 0);

        boolean randomBoolean = true;

        // Send first request which gets successful
        ActionFuture<BulkResponse> successFuture;
        if (randomBoolean) {
            successFuture = client(coordinatingOnlyNode).bulk(bulkRequest);
        } else {
            successFuture = client(primaryName).bulk(bulkRequest);
        }
        successFuture.actionGet();

        final CountDownLatch replicationSendPointReached = new CountDownLatch(1);
        final CountDownLatch latchBlockingReplicationSend = new CountDownLatch(1);

        TransportService primaryService = internalCluster().getInstance(TransportService.class, primaryName);
        final MockTransportService primaryTransportService = (MockTransportService) primaryService;
        TransportService replicaService = internalCluster().getInstance(TransportService.class, replicaName);
        final MockTransportService replicaTransportService = (MockTransportService) replicaService;

        primaryTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(TransportShardBulkAction.ACTION_NAME + "[r]")) {
                try {
                    replicationSendPointReached.countDown();
                    latchBlockingReplicationSend.await();
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
            }
            connection.sendRequest(requestId, action, request, options);
        });

        final ThreadPool replicaThreadPool = replicaTransportService.getThreadPool();
        final Releasable replicaRelease = blockReplicas(replicaThreadPool);
        // Send one more requests which remains outstanding for delayed time
        if (randomBoolean) {
            successFuture = client(coordinatingOnlyNode).bulk(bulkRequest);
        } else {
            successFuture = client(primaryName).bulk(bulkRequest);
        }
        // Delay to breach the success time stamp threshold
        Thread.sleep(3000);
        latchBlockingReplicationSend.countDown();
        replicaRelease.close();
        successFuture.actionGet();

        // This request breaches the threshold and hence will be rejected
        if (randomBoolean) {
            expectThrows(OpenSearchRejectedExecutionException.class, () -> client(coordinatingOnlyNode).bulk(bulkRequest).actionGet());
        } else {
            expectThrows(OpenSearchRejectedExecutionException.class, () -> client(primaryName).bulk(bulkRequest).actionGet());
        }

        // Update the outstanding threshold setting to see no rejections
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(Settings.builder()
            .put(ShardIndexingPressureSettings.REQUEST_SIZE_WINDOW.getKey(), 10));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        IndexingPressureService primaryShardTracker = internalCluster()
            .getInstance(IndexingPressureService.class, primaryName);
        IndexingPressureService coordinatingShardTracker = internalCluster()
            .getInstance(IndexingPressureService.class, coordinatingOnlyNode);

        // Verify no rejection with similar request pattern
        if (randomBoolean) {
            successFuture = client(coordinatingOnlyNode).bulk(bulkRequest);
            successFuture.actionGet();
            successFuture = client(coordinatingOnlyNode).bulk(bulkRequest);
            Thread.sleep(10);
            successFuture.actionGet();
            client(coordinatingOnlyNode).bulk(bulkRequest).actionGet();
            assertEquals(1, coordinatingShardTracker.shardStats(statsFlag)
                .getIndexingPressureShardStats(shardId).getCoordinatingRejections());
            assertEquals(1, coordinatingShardTracker.shardStats(statsFlag)
                .getIndexingPressureShardStats(shardId).getCoordinatingThroughputDegradationLimitsBreachedRejections());
        } else {
            successFuture = client(primaryName).bulk(bulkRequest);
            successFuture.actionGet();
            successFuture = client(primaryName).bulk(bulkRequest);
            Thread.sleep(10);
            successFuture.actionGet();
            client(primaryName).bulk(bulkRequest).actionGet();
            assertEquals(1, primaryShardTracker.shardStats(statsFlag)
                .getIndexingPressureShardStats(shardId).getPrimaryRejections());
            assertEquals(1, primaryShardTracker.shardStats(statsFlag)
                .getIndexingPressureShardStats(shardId).getPrimaryThroughputDegradationLimitsBreachedRejections());
        }
    }

    private Tuple<String, String> getPrimaryReplicaNodeNames(String indexName) {
        IndicesStatsResponse response = client().admin().indices().prepareStats(indexName).get();
        String primaryId = Stream.of(response.getShards())
                .map(ShardStats::getShardRouting)
                .filter(ShardRouting::primary)
                .findAny()
                .get()
                .currentNodeId();
        String replicaId = Stream.of(response.getShards())
                .map(ShardStats::getShardRouting)
                .filter(sr -> sr.primary() == false)
                .findAny()
                .get()
                .currentNodeId();
        DiscoveryNodes nodes = client().admin().cluster().prepareState().get().getState().nodes();
        String primaryName = nodes.get(primaryId).getName();
        String replicaName = nodes.get(replicaId).getName();
        return new Tuple<>(primaryName, replicaName);
    }

    private String getCoordinatingOnlyNode() {
        return client().admin().cluster().prepareState().get().getState().nodes().getCoordinatingOnlyNodes().iterator().next()
                .value.getName();
    }

    private void restartCluster(Settings settings) throws Exception {
        internalCluster().fullRestart(new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) {
                return Settings.builder().put(unboundedWriteQueue).put(settings).build();
            }
        });
    }

    private Releasable blockReplicas(ThreadPool threadPool) {
        final CountDownLatch blockReplication = new CountDownLatch(1);
        final int threads = threadPool.info(ThreadPool.Names.WRITE).getMax();
        final CountDownLatch pointReached = new CountDownLatch(threads);
        for (int i = 0; i< threads; ++i) {
            threadPool.executor(ThreadPool.Names.WRITE).execute(() -> {
                try {
                    pointReached.countDown();
                    blockReplication.await();
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
            });
        }

        return () -> {
            if (blockReplication.getCount() > 0) {
                blockReplication.countDown();
            }
        };
    }
}
