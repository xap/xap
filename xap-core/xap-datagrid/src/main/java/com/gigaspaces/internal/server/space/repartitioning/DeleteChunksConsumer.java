/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gigaspaces.internal.server.space.repartitioning;

import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.query.IdsQuery;
import com.j_spaces.core.client.Modifiers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class DeleteChunksConsumer implements Runnable {

    public static Logger logger = LoggerFactory.getLogger("org.openspaces.admin.internal.pu.scale_horizontal.ScaleManager");

    private ISpaceProxy space;
    private BlockingQueue<Batch> batchQueue;
    private DeleteChunksResponseInfo responseInfo;

    DeleteChunksConsumer(ISpaceProxy space, BlockingQueue<Batch> batchQueue, DeleteChunksResponseInfo responseInfo) {
        this.space = space;
        this.batchQueue = batchQueue;
        this.responseInfo = responseInfo;
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            DeleteBatch deleteBatch;
            try {
                Batch batch = batchQueue.poll(5, TimeUnit.SECONDS);
                if (batch == Batch.EMPTY_BATCH) {
                    return;
                }
                if (batch != null) {
                    deleteBatch = ((DeleteBatch) batch);
                    space.clear(new IdsQuery<>(deleteBatch.getType(), deleteBatch.getIds().toArray()), null, Modifiers.BACKUP_ONLY);
                    responseInfo.getDeleted().addAndGet(deleteBatch.getIds().size());
                }
            } catch (InterruptedException e) {
                responseInfo.setException(new IOException("Delete chunks consumer thread was interrupted", e));
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Consumer thread caught exception");
                    e.printStackTrace();
                }
                responseInfo.setException(new IOException("Caught exception while trying to Delete from partition " +
                        responseInfo.getPartitionId(), e));
                return;
            }
        }
    }
}
