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
package com.gigaspaces.internal.server.space.tiered_storage;

import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.j_spaces.core.cache.context.TemplateMatchTier;

import java.time.Duration;

public class TimePredicate implements CachePredicate {
    private final String typeName;
    private final String timeColumn;
    private final Duration period;
    private final boolean isTransient;

    public TimePredicate(String typeName, String timeColumn, Duration period, boolean isTransient) {
        this.typeName = typeName;
        this.timeColumn = timeColumn;
        this.period = period;
        this.isTransient = isTransient;
    }

    public String getTypeName() {
        return typeName;
    }

    public Duration getPeriod() {
        return period;
    }

    public String getTimeColumn() {
        return timeColumn;
    }

    //eviction from hot tier
    public static long getExpirationTime(long currentTime){
        //TODO - tiered storage
        return 0;
    }

    //For tests
    public TemplateMatchTier evaluate(ITemplatePacket packet) {
        return TemplateMatchTier.MATCH_COLD; //TODO - tiered storage
    }

    @Override
    public boolean evaluate(IEntryData entryData) {
        return false; //TODO - tiered storage
    }

    @Override
    public boolean isTransient() {
        return isTransient;
    }

    @Override
    public TemplateMatchTier evaluate(ITemplateHolder template) {
        return TemplateMatchTier.MATCH_COLD; //TODO - tiered storage
    }

    @Override
    public String toString() {
        return "TimePredicate{" +
                "typeName='" + typeName + '\'' +
                ", timeColumn='" + timeColumn + '\'' +
                ", period=" + period +
                '}';
    }
}
