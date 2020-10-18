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
package com.gigaspaces.internal.server.space.iterator;

import java.util.UUID;

public class ServerIteratorRequestInfo {
    private final UUID uuid;
    private final int batchSize;
    private final int requestedBatchNumber;
    private final long maxInactiveDuration;

    public ServerIteratorRequestInfo(UUID uuid, int batchSize, int requestedBatchNumber, long maxInactiveDuration) {
        this.uuid = uuid;
        this.batchSize = batchSize;
        this.requestedBatchNumber = requestedBatchNumber;
        this.maxInactiveDuration = maxInactiveDuration;
    }

    public UUID getUuid() {
        return uuid;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getRequestedBatchNumber() {
        return requestedBatchNumber;
    }

    public long getMaxInactiveDuration() {
        return maxInactiveDuration;
    }

    public boolean isFirstTime() {
        return requestedBatchNumber == 0;
    }
}
