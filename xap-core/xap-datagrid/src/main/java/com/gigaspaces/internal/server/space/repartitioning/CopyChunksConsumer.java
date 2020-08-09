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
import com.j_spaces.core.client.Modifiers;
import net.jini.core.lease.Lease;
import net.jini.core.transaction.TransactionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.rmi.RemoteException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class CopyChunksConsumer implements Runnable {

    public static Logger logger = LoggerFactory.getLogger("org.openspaces.admin.internal.pu.scale_horizontal.ScaleManager");

    private Map<Integer, ISpaceProxy> proxyMap;
    private BlockingQueue<Batch> batchQueue;
    private CopyChunksResponseInfo responseInfo;

    CopyChunksConsumer(Map<Integer, ISpaceProxy> proxyMap, BlockingQueue<Batch> batchQueue, CopyChunksResponseInfo responseInfo) {
        this.proxyMap = proxyMap;
        this.batchQueue = batchQueue;
        this.responseInfo = responseInfo;
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            WriteBatch writeBatch = null;
            try {
                Batch batch = batchQueue.poll(5, TimeUnit.SECONDS);
                if (batch == Batch.EMPTY_BATCH) {
                    return;
                }
                if (batch != null) {
                    writeBatch = ((WriteBatch) batch);
                    ISpaceProxy spaceProxy = proxyMap.get(writeBatch.getPartitionId());
                    spaceProxy.writeMultiple(writeBatch.getEntries().toArray(), null, Lease.FOREVER, Modifiers.BACKUP_ONLY);
                    responseInfo.getMovedToPartition().get((short) writeBatch.getPartitionId()).addAndGet(writeBatch.getEntries().size());

                }
            } catch (InterruptedException e) {
                responseInfo.setException(new IOException("Copy chunks consumer thread was interrupted", e));
                Thread.currentThread().interrupt();
                return;
            } catch (RemoteException | TransactionException e) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Consumer thread caught exception");
                    e.printStackTrace();
                }
                responseInfo.setException(new IOException("Caught exception while trying to write to partition " +
                        writeBatch.getPartitionId(), e));
                return;
            }
        }
    }
}
