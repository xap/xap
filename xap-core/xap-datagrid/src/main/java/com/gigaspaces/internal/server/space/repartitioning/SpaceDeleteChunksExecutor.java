package com.gigaspaces.internal.server.space.repartitioning;

import com.gigaspaces.internal.client.QueryResultTypeInternal;
import com.gigaspaces.internal.client.spaceproxy.SpaceProxyImpl;
import com.gigaspaces.internal.io.BootIOUtils;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.server.space.executors.SpaceActionExecutor;
import com.gigaspaces.internal.space.requests.SpaceRequestInfo;
import com.gigaspaces.internal.space.responses.SpaceResponseInfo;
import com.gigaspaces.internal.transport.EmptyQueryPacket;
import com.j_spaces.core.client.Modifiers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.*;

public class SpaceDeleteChunksExecutor extends SpaceActionExecutor {

    public static Logger logger = LoggerFactory.getLogger("org.openspaces.admin.internal.pu.scale_horizontal.ScaleManager");

    @Override
    public SpaceResponseInfo execute(SpaceImpl space, SpaceRequestInfo requestInfo) {
        int queueSize = 10000;
        int batchSize = 1000;
        int threadCount = 10;

        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        BlockingQueue<Batch> batchQueue = new ArrayBlockingQueue<>(queueSize);
        DeleteChunksRequestInfo info = (DeleteChunksRequestInfo) requestInfo;
        DeleteChunksResponseInfo responseInfo = new DeleteChunksResponseInfo(space.getPartitionIdOneBased());
        try {
            for (int i = 0; i < threadCount; i++) {
                SpaceProxyImpl proxy = space.getServiceProxy();
                proxy.setQuiesceToken(info.getToken());
                executorService.submit(new DeleteChunksConsumer(proxy, batchQueue, responseInfo));
            }
            DeleteChunksProducer aggregator = new DeleteChunksProducer(info.getNewMap(), batchQueue, batchSize);
            EmptyQueryPacket queryPacket = new EmptyQueryPacket();
            queryPacket.setQueryResultType(QueryResultTypeInternal.NOT_SET);
            space.getEngine().aggregate(queryPacket, Collections.singletonList(aggregator), Modifiers.NONE, requestInfo.getSpaceContext());
            aggregator.getIntermediateResult();
            for (int i = 0; i < threadCount; i++) {
                batchQueue.put(Batch.EMPTY_BATCH);
            }
            boolean isEmpty = BootIOUtils.waitFor(batchQueue::isEmpty, 10 * 60 * 1000, 5000);
            if (!isEmpty) {
                throw new IOException("Failed while waiting for queue to be empty");
            }
            executorService.shutdown();
            executorService.awaitTermination(5, TimeUnit.MINUTES);
        } catch (Exception e) {
            throw new RuntimeException("Copy chunks executor failed", e);
        }
        return responseInfo;
    }
}
