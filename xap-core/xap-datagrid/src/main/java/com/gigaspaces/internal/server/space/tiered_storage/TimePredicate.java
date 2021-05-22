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
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.metadata.SpaceMetadataException;
import com.j_spaces.core.cache.context.TemplateMatchTier;
import com.j_spaces.jdbc.builder.range.SegmentRange;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;

public class TimePredicate implements CachePredicate, InternalCachePredicate {
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
    public long getExpirationTime(Object value, long gracePeriod) {
        if (value == null){
            throw new SpaceMetadataException("property of type [" + timeColumn + "] which set as time rule cannot be null.");
        }
        long originalValueInLong = Long.class.equals(value.getClass())? (long)value : SqliteUtils.convertTimeTypeToInstant(value).toEpochMilli();
        return originalValueInLong + period.toMillis() + gracePeriod;
    }

    //For tests
    @Override
    public TemplateMatchTier evaluate(ITemplatePacket packet) {
        String columnTimeType = packet.getTypeDescriptor().getFixedProperty(getTimeColumn()).getType().getName();
        return SqliteUtils.getTemplateMatchTier(getTimeRuleAsRange(), packet, columnTimeType);
    }

    @Override
    public boolean evaluate(IEntryData entryData)  {
        Object value = entryData.getFixedPropertyValue(entryData.getSpaceTypeDescriptor().getFixedPropertyPosition(timeColumn));
        value = SqliteUtils.convertTimeTypeToInstant(value);
        return getTimeRuleAsRange().getPredicate().execute(value);
    }

    @Override
    public boolean isTransient() {
        return isTransient;
    }

    @Override
    public TemplateMatchTier evaluate(ITemplateHolder template) {
       String timeType = template.getServerTypeDesc().getTypeDesc().getFixedProperty(timeColumn).getTypeName();
       TemplateMatchTier templateMatchTier = SqliteUtils.getTemplateMatchTier(getTimeRuleAsRange(), template, timeType);
       return SqliteUtils.evaluateByMatchTier(template, templateMatchTier);
    }

    @Override
    public boolean isTimeRule(){
        return true;
    }

    public SegmentRange getTimeRuleAsRange(){
         Instant timeRule = Instant.now().minus(period);
         return new SegmentRange(timeColumn, timeRule, true, null, false);
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
