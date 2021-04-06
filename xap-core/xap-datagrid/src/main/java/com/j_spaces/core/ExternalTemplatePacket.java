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

package com.j_spaces.core;

import com.gigaspaces.internal.client.QueryResultTypeInternal;
import com.gigaspaces.internal.collections.CollectionsFactory;
import com.gigaspaces.internal.collections.ObjectShortMap;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.metadata.EntryType;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.metadata.converter.ConversionException;
import com.gigaspaces.internal.transport.AbstractProjectionTemplate;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.j_spaces.core.client.ExternalEntry;
import com.j_spaces.core.client.TemplateMatchCodes;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by IntelliJ IDEA. User: assafr Date: 05/08/2008 Time: 18:59:02 To change this template
 * use File | Settings | File Templates.
 */
@com.gigaspaces.api.InternalApi
public class ExternalTemplatePacket extends ExternalEntryPacket implements ITemplatePacket {
    private static final long serialVersionUID = 1L;

    protected short[] _extendedMatchCodes;
    protected Object[] _rangeValues;
    protected boolean[] _rangeValuesInclusion;

    public ExternalTemplatePacket() {
    }

    public ExternalTemplatePacket(ITypeDesc typeDesc, EntryType entryType, Object[] values,
                                  String uid, int version, long timeToLive, boolean isTransient, ExternalEntry ee) {
        super(typeDesc, entryType, values, uid, version, timeToLive, isTransient);
        if (ee != null) {
            final Class<? extends ExternalEntry> realClz = ee.getClass();
            _implClassName = realClz.equals(ExternalEntry.class) ? null : realClz.getName();
            _entryType = ee._returnTrueType ? entryType : EntryType.EXTERNAL_ENTRY;
            _returnOnlyUIDs = ee.m_ReturnOnlyUids;
            _extendedMatchCodes = getOrderedExtMatchCodes(ee);
            _rangeValues = getOrderedExtRangeValues(ee);
            _rangeValuesInclusion = getOrderedExtRangeValuesInclusion(ee);
        }
    }

    public QueryResultTypeInternal getQueryResultType() {
        return QueryResultTypeInternal.fromEntryType(_entryType);
    }

    @Override
    public Object getRoutingFieldValue() {
        int pos = _typeDesc.getRoutingPropertyId();
        if (pos != -1 && _extendedMatchCodes != null && _extendedMatchCodes[pos] != TemplateMatchCodes.EQ)
            return null;

        return super.getRoutingFieldValue();
    }

    public boolean supportExtendedMatching() {
        return _extendedMatchCodes != null || _rangeValues != null || _rangeValuesInclusion != null;
    }

    public short[] getExtendedMatchCodes() {
        return _extendedMatchCodes;
    }

    public Object[] getRangeValues() {
        return _rangeValues;
    }

    public boolean[] getRangeValuesInclusion() {
        return _rangeValuesInclusion;
    }

    private short[] getOrderedExtMatchCodes(ExternalEntry ee) {
        final short[] extendedMatchCodes = ee.m_ExtendedMatchCodes;
        if (extendedMatchCodes == null)
            return null;

        final String[] fieldsNames = ee.getFieldsNames();
        if (fieldsNames == null) {
            if (_typeDesc.getNumOfFixedProperties() != extendedMatchCodes.length)
                throw new ConversionException(new IllegalArgumentException("Original fields count does not match the size of extendedMatchCode"));
            return extendedMatchCodes;
        }

        ObjectShortMap<String> fieldsNamesToCodes = CollectionsFactory.getInstance().createObjectShortMap();
        for (int i = 0; i < fieldsNames.length; i++)
            fieldsNamesToCodes.put(fieldsNames[i], extendedMatchCodes[i]);

        short[] newMatchCodes = new short[_typeDesc.getNumOfFixedProperties()];
        for (int i = 0; i < newMatchCodes.length; i++) {
            String fieldName = _typeDesc.getFixedProperty(i).getName();
            if (fieldsNamesToCodes.containsKey(fieldName))
                newMatchCodes[i] = fieldsNamesToCodes.get(fieldName);
        }

        return newMatchCodes;
    }

    private Object[] getOrderedExtRangeValues(ExternalEntry ee) {
        Object[] rangeValues = ee.m_RangeValues;
        if (rangeValues == null)
            return null;

        Map<String, Object> fieldNamesToRange = new HashMap<String, Object>();
        for (int i = 0; i < ee.m_FieldsNames.length; i++)
            fieldNamesToRange.put(ee.m_FieldsNames[i], rangeValues[i]);

        Object[] newRangeValue = new Object[_typeDesc.getNumOfFixedProperties()];
        for (int i = 0; i < newRangeValue.length; i++)
            newRangeValue[i] = fieldNamesToRange.get(_typeDesc.getFixedProperty(i).getName());

        return newRangeValue;
    }

    private boolean[] getOrderedExtRangeValuesInclusion(ExternalEntry ee) {
        boolean[] rangeValues = ee.m_RangeValuesInclusion;
        if (rangeValues == null)
            return null;

        Map<String, Boolean> fieldNamesToInclusions = new HashMap<String, Boolean>();
        for (int i = 0; i < ee.m_FieldsNames.length; i++)
            fieldNamesToInclusions.put(ee.m_FieldsNames[i], rangeValues[i]);

        boolean[] newRangeValue = new boolean[_typeDesc.getNumOfFixedProperties()];
        for (int i = 0; i < newRangeValue.length; i++) {
            final Boolean include = fieldNamesToInclusions.get(_typeDesc.getFixedProperty(i).getName());
            newRangeValue[i] = include != null ? include : false;
        }

        return newRangeValue;
    }

    @Override
    public ExternalTemplatePacket clone() {
        ExternalTemplatePacket clonedExternalTemplatePacket = (ExternalTemplatePacket) super.clone();
        System.out.println("!!! CLONE:" + clonedExternalTemplatePacket );
        return clonedExternalTemplatePacket;
    }

    @Override
    public void writeToSwap(ObjectOutput out) throws IOException {
        super.writeToSwap(out);

        serializePacket(out);
    }

    @Override
    public void readFromSwap(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readFromSwap(in);

        deserializePacket(in);
    }

    @Override
    protected void writeExternal(ObjectOutput out, PlatformLogicalVersion version)
            throws IOException {
        super.writeExternal(out, version);

        serializePacket(out);
    }

    private final void serializePacket(ObjectOutput out) throws IOException {
        IOUtils.writeShortArray(out, _extendedMatchCodes);
        IOUtils.writeObjectArray(out, _rangeValues);
        IOUtils.writeBooleanArray(out, _rangeValuesInclusion);
    }

    @Override
    protected void readExternal(ObjectInput in, PlatformLogicalVersion version)
            throws IOException, ClassNotFoundException {
        super.readExternal(in, version);

        deserializePacket(in);
    }

    private final void deserializePacket(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _extendedMatchCodes = IOUtils.readShortArray(in);
        _rangeValues = IOUtils.readObjectArray(in);
        _rangeValuesInclusion = IOUtils.readBooleanArray(in);
    }

    @Override
    public void validate() {
        validateStorageType();
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
        return false;
    }

    @Override
    public boolean isAllIndexValuesSqlQuery() {
        return false;
    }

}
