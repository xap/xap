package com.gigaspaces.internal.server.space.repartitioning;

import com.gigaspaces.internal.cluster.ClusterTopology;
import com.gigaspaces.internal.cluster.ClusterTopology;
import com.gigaspaces.internal.remoting.routing.partitioned.PartitionedClusterUtils;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.query.aggregators.SpaceEntriesAggregator;
import com.gigaspaces.query.aggregators.SpaceEntriesAggregatorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class DeleteChunksProducer extends SpaceEntriesAggregator<DeleteChunksResponseInfo> {

    public static Logger logger = LoggerFactory.getLogger("org.openspaces.admin.internal.pu.scale_horizontal.ScaleManager");

    private ClusterTopology newMap;
    private Map<String, List<Object>> batchMap;
    private BlockingQueue<Batch> queue;
    private int batchSize;

    DeleteChunksProducer(ClusterTopology newMap, BlockingQueue<Batch> queue, int batchSize) {
        this.newMap = newMap;
        this.batchSize = batchSize;
        this.queue = queue;
        this.batchMap = new HashMap<>();
    }

    @Override
    public String getDefaultAlias() {
        return CopyChunksRequestInfo.class.getName();
    }

    @Override
    public void aggregate(SpaceEntriesAggregatorContext context) {
        SpaceTypeDescriptor typeDescriptor = context.getTypeDescriptor();
        if(typeDescriptor.isBroadcast())
            return;
        Object routingValue = context.getPathValue(typeDescriptor.getRoutingPropertyName());
        int newPartitionId = PartitionedClusterUtils.getPartitionId(routingValue, newMap) + 1;
        if (newPartitionId != context.getPartitionId() + 1) {
            Object idValue = context.getPathValue(typeDescriptor.getIdPropertyName());
            String type = typeDescriptor.getTypeName();

            if (batchMap.containsKey(type)) {
                List<Object> ids = batchMap.get(type);
                ids.add(idValue);
                if (ids.size() == batchSize) {
                    try {
                        queue.put(new DeleteBatch(type, ids));
                        batchMap.remove(type);
                    } catch (Exception e) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Exception in aggregator while trying to put batch in queue");
                            e.printStackTrace();
                        }
                        throw new RuntimeException(e);
                    }
                }

            } else {
                ArrayList<Object> ids = new ArrayList<>(batchSize);
                ids.add(idValue);
                batchMap.put(type, ids);
            }
        }
    }


    @Override
    public DeleteChunksResponseInfo getIntermediateResult() {
        for (Map.Entry<String, List<Object>> entry : batchMap.entrySet()) {
            try {
                queue.put(new DeleteBatch(entry.getKey(), entry.getValue()));
            } catch (Exception e) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Exception in aggregator while trying to put batch in queue");
                    e.printStackTrace();
                }
                throw new RuntimeException(e);
            }
        }
        return null;
    }

    @Override
    public void aggregateIntermediateResult(DeleteChunksResponseInfo partitionResult) {

    }
}
