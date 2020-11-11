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

package com.gigaspaces.internal.transport;

import com.gigaspaces.internal.client.QueryResultTypeInternal;

import java.util.Map;

/**
 * Extends IEntryPacket and provides an interface for templates.
 *
 * @author Assaf R
 * @since 6.5
 */
public interface ITemplatePacket extends ITransportPacket, IEntryPacket {
    QueryResultTypeInternal getQueryResultType();

    boolean isReturnOnlyUids();

    void setReturnOnlyUIDs(boolean returnOnlyUIDs);

    boolean supportExtendedMatching();

    short[] getExtendedMatchCodes();

    Object[] getRangeValues();

    boolean[] getRangeValuesInclusion();

    ITemplatePacket clone();

    void setDynamicProperties(Map<String, Object> dynamicProperties);

    void validate();

    void setProjectionTemplate(AbstractProjectionTemplate projectionTemplate);

    AbstractProjectionTemplate getProjectionTemplate();

    boolean isIdQuery();

    boolean isIdsQuery();

    boolean isTemplateQuery();

    boolean isAllIndexValuesSqlQuery();

    default Object getTemplateRoutingValue() {
        return isBroadcast() ? 0 : getRoutingFieldValue();
    }

    default boolean isBroadcast(){
        return getTypeDescriptor() != null && getTypeDescriptor().isBroadcast();
    }

}
