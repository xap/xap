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

package com.j_spaces.core.filters;

import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Holds runtime statistics counters.
 *
 * @author moran
 * @since 9.1.0
 */
@com.gigaspaces.api.InternalApi
public class RuntimeStatisticsHolder implements SmartExternalizable {

    private static final long serialVersionUID = 1;

    private long objectCount;
    private long notifyTemplateCount;
    private long activeConnectionCount;
    private long activeTransactionCount;

    /**
     * Externalizable default constructor
     */
    public RuntimeStatisticsHolder() {
    }

    ;

    public RuntimeStatisticsHolder(long numOfEntries, long numOfTemplates, long numOfConnections, long numOfTransactions) {
        this.objectCount = numOfEntries;
        this.notifyTemplateCount = numOfTemplates;
        this.activeConnectionCount = numOfConnections;
        this.activeTransactionCount = numOfTransactions;
    }

    /**
     * @return count of all the objects in this Space instance.
     * @since 9.1.0
     */
    public long getObjectCount() {
        return objectCount;
    }

    /**
     * @return count of all the notify templates this Space instance.
     * @since 9.1.0
     */
    public long getNotifyTemplateCount() {
        return notifyTemplateCount;
    }

    /**
     * @return count of all the active connections to this Space instance.
     * @since 9.1.0
     */
    public long getActiveConnectionCount() {
        return activeConnectionCount;
    }

    /**
     * @return count of all the active transactions (of all types) in this Space instance.
     * @since 9.1.0
     */
    public long getActiveTransactionCount() {
        return activeTransactionCount;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(objectCount);
        out.writeLong(notifyTemplateCount);
        out.writeLong(activeConnectionCount);
        out.writeLong(activeTransactionCount);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        objectCount = in.readLong();
        notifyTemplateCount = in.readLong();
        activeConnectionCount = in.readLong();
        activeTransactionCount = in.readLong();
    }
}
