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
import com.gigaspaces.internal.metadata.EntryType;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.query.ICustomQuery;

/**
 * This class represents a packet of information transmitted between a J-Space client and its
 * J-Space server .
 *
 * TemplatePacket contains additional information that describes the template All the fields in this
 * class are public, in order to increase access and modification speed (no need to call a method).
 *
 * @author Guy Korland
 * @version 1.0
 * @since 6.0
 **/
@com.gigaspaces.api.InternalApi
public class TemplatePacket extends EntryPacket implements ITemplatePacket {
    private static final long serialVersionUID = 1L;

    /**
     * Default constructor required by {@link java.io.Externalizable}.
     */
    public TemplatePacket() {
    }

    public TemplatePacket(ITypeDesc typeDesc, EntryType entryType, Object[] fixedProperties, ICustomQuery customQuery,
                          String uid, int version, long timeToLive, boolean isTransient) {
        super(typeDesc, entryType, fixedProperties, null, uid, version, timeToLive, isTransient);
        setCustomQuery(customQuery);
    }

    public TemplatePacket(ITypeDesc typeDesc) {
        super(typeDesc, new Object[typeDesc.getNumOfFixedProperties()]);
    }

    public QueryResultTypeInternal getQueryResultType() {
        return QueryResultTypeInternal.fromEntryType(_entryType);
    }

    public boolean supportExtendedMatching() {
        return false;
    }

    public short[] getExtendedMatchCodes() {
        return null;
    }

    public Object[] getRangeValues() {
        return null;
    }

    public boolean[] getRangeValuesInclusion() {
        return null;
    }

    @Override
    public AbstractProjectionTemplate getProjectionTemplate() {
        return null;
    }

    @Override
    public void setProjectionTemplate(AbstractProjectionTemplate projectionTemplate) {
    }

    @Override
    public boolean isIdQuery() {
        return false;
    }

    @Override
    public boolean isIdsQuery() {
        return false;
    }

    @Override
    public boolean isTemplateQuery() {
        return true;
    }

    @Override
    public boolean isAllIndexValuesSqlQuery() {
        return false;
    }

    @Override
    public TemplatePacket clone() {
        return (TemplatePacket) super.clone();
    }

    @Override
    public void validate() {
        validateStorageType();
    }

}