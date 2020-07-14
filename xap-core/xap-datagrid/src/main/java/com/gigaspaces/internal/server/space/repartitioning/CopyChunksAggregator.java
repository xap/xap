package com.gigaspaces.internal.server.space.repartitioning;

import com.gigaspaces.internal.cluster.PartitionToChunksMap;
import com.gigaspaces.internal.remoting.routing.partitioned.PartitionedClusterUtils;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.query.aggregators.SpaceEntriesAggregator;
import com.gigaspaces.query.aggregators.SpaceEntriesAggregatorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class CopyChunksAggregator extends SpaceEntriesAggregator<CopyChunksResponseInfo> {

    public static Logger logger = LoggerFactory.getLogger("org.openspaces.admin.internal.pu.scale_horizontal.ScaleManager");

    private PartitionToChunksMap newMap;
    private Map<Integer, List<IEntryPacket>> batchMap;
    private BlockingQueue<Batch> queue;
    private int batchSize;

    CopyChunksAggregator(PartitionToChunksMap newMap, BlockingQueue<Batch> queue, int batchSize) {
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
        Object routingValue = context.getPathValue(context.getTypeDescriptor().getRoutingPropertyName());
        int newPartitionId = PartitionedClusterUtils.getPartitionId(routingValue, newMap) + 1;
        if (newPartitionId != context.getPartitionId() + 1) {
            if (batchMap.containsKey(newPartitionId)) {
                List<IEntryPacket> entries = batchMap.get(newPartitionId);
                entries.add((IEntryPacket) context.getRawEntry());
                if (entries.size() == batchSize) {
                    try {
                        queue.put(new Batch(newPartitionId, entries));
                        batchMap.remove(newPartitionId);
                    } catch (Exception e) {
                        if(logger.isDebugEnabled()){
                            logger.debug("Exception in aggregator while trying to put batch in queue");
                            e.printStackTrace();
                        }
                        throw new RuntimeException(e);
                    }
                }

            } else {
                ArrayList<IEntryPacket> entries = new ArrayList<>(batchSize);
                entries.add((IEntryPacket) context.getRawEntry());
                batchMap.put(newPartitionId, entries);
            }
        }
    }


    @Override
    public CopyChunksResponseInfo getIntermediateResult() {
        for (Map.Entry<Integer, List<IEntryPacket>> entry : batchMap.entrySet()) {
            if (entry.getValue().size() != 0) {
                queue.add(new Batch(entry.getKey(), entry.getValue()));
            }
        }
        return null;
    }

    @Override
    public void aggregateIntermediateResult(CopyChunksResponseInfo partitionResult) {

    }
}
