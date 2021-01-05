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

package com.gigaspaces.internal.server.storage;

import com.gigaspaces.client.protective.ProtectiveMode;
import com.gigaspaces.client.protective.ProtectiveModeException;
import com.gigaspaces.internal.metadata.EntryTypeDesc;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.query.ICustomQuery;
import com.gigaspaces.internal.query.IQueryIndexScanner;
import com.gigaspaces.internal.query.RegexCache;
import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.internal.transport.ITransportPacket;
import com.gigaspaces.metadata.StorageType;
import com.gigaspaces.server.ServerEntry;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.client.SQLQuery;
import com.j_spaces.core.client.TemplateMatchCodes;
import com.j_spaces.sadapter.datasource.DefaultSQLQueryBuilder;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Niv Ingberg
 * @since 8.0.4
 */
@com.gigaspaces.api.InternalApi
public class TemplateEntryData implements IEntryData {
    private final EntryTypeDesc _entryTypeDesc;
    private Object[] _fieldsValues;
    private Map<String, Object> _dynamicProperties;
    private ICustomQuery _customQuery;
    private int _versionID;            //??? why we need it in template ???
    private long _expirationTime;
    private boolean checkedAllNullFields;
    private boolean isAllNullFields;

    private final short[] _extendedMatchCodes;
    private final Object[] _rangeValues;
    private final boolean[] _rangeValuesInclusion;

    private final boolean _isIdQuery;

    public TemplateEntryData(ITypeDesc typeDesc, ITransportPacket packet, long expirationTime, boolean fromReplication) {
        this._entryTypeDesc = typeDesc.getEntryTypeDesc(packet.getEntryType());
        this._fieldsValues = packet.getFieldValues();
        this._dynamicProperties = typeDesc.supportsDynamicProperties() ? new HashMap<String, Object>() : null;
        this._customQuery = packet.getCustomQuery();
        this._versionID = packet.getVersion();
        this._expirationTime = expirationTime;

        ITemplatePacket templatePacket = packet instanceof ITemplatePacket ? (ITemplatePacket) packet : null;
        if (templatePacket != null && templatePacket.supportExtendedMatching()) {
            _extendedMatchCodes = templatePacket.getExtendedMatchCodes();
            _rangeValues = templatePacket.getRangeValues();
            _rangeValuesInclusion = templatePacket.getRangeValuesInclusion();
        } else {
            _extendedMatchCodes = null;
            _rangeValues = null;
            _rangeValuesInclusion = null;
        }

        this._isIdQuery = templatePacket != null && templatePacket.isIdQuery();

        final boolean isTemplateQuery = templatePacket != null && templatePacket.isTemplateQuery();
        if (ProtectiveMode.isPrimitiveWithoutNullValueProtectionEnabled() && isTemplateQuery &&
                typeDesc.getPrimitivePropertiesWithoutNullValues() != null && !fromReplication)
            throw new ProtectiveModeException("Operation is rejected - template matching on type " + typeDesc.getTypeName() +
                    " is illegal because it has primitive properties without null value: " + typeDesc.getPrimitivePropertiesWithoutNullValues() +
                    ". When using template matching to query the space, properties with primitive types require a " +
                    "'null value' definition to avoid ambiguity (should the property be matched or not). " +
                    "Select one of the following resolutions: a) Use non-primitive types instead " +
                    "b) define a null value for those properties " +
                    "c) use an alternate query mechanism (e.g. SQLQuery). " +
                    "(you can disable this protection, though it is not recommended, by setting the following system property: " + ProtectiveMode.PRIMITIVE_WITHOUT_NULL_VALUE + "=false)");

        this.checkedAllNullFields = false;
    }

    public TemplateEntryData() {
        _entryTypeDesc = null;
        _extendedMatchCodes = null;
        _rangeValues = null;
        _rangeValuesInclusion = null;
        _isIdQuery = false;
    }

    public boolean isExpired() {
        if (this._expirationTime == 0 || this._expirationTime == Long.MAX_VALUE)
            return false; //avoid getting time in vain
        return isExpired(SystemTime.timeMillis());
    }

    public boolean isExpired(long limit) {
        return this._expirationTime != 0 && this._expirationTime <= limit;
    }

    @Override
    public EntryDataType getEntryDataType() {
        return EntryDataType.FLAT;
    }

    @Override
    public Map<String, Object> getDynamicProperties() {
        return _dynamicProperties;
    }

    //why we need it
    @Override
    public int getVersion() {
        return _versionID;
    }

    public void setVersion(int version) {
        this._versionID = version;
    }

    @Override
    public long getExpirationTime() {
        return _expirationTime;
    }

    public void setExpirationTime(long expirationTime) {
        this._expirationTime = expirationTime;
    }

    @Override
    public Object getFixedPropertyValue(int index) {
        return _fieldsValues[index];
    }

    @Override
    public void setFixedPropertyValue(int index, Object value) {
        _fieldsValues[index] = value;
    }

    @Override
    public void setFixedPropertyValues(Object[] values) {
        if (values.length != _fieldsValues.length) {
            throw new IllegalArgumentException("Cannot substitute fixed property values with array of different size!");
        }
        for (int i = 0; i < values.length; i++) {
            _fieldsValues[i] = values[i];
        }
    }

    @Override
    public Object[] getFixedPropertiesValues() {
        return _fieldsValues;
    }

    public void updateData(IEntryData entryData) {
        _fieldsValues = entryData.getFixedPropertiesValues();
        _dynamicProperties = entryData.getDynamicProperties();
    }

    @Override
    public Object getPropertyValue(String name) {
        int pos = _entryTypeDesc.getTypeDesc().getFixedPropertyPosition(name);
        if (pos != -1)
            return getFixedPropertyValue(pos);

        if (_dynamicProperties == null)
            throw new IllegalArgumentException("Unknown property name '" + name + "'");
        return _dynamicProperties.get(name);
    }

    @Override
    public EntryTypeDesc getEntryTypeDesc() {
        return _entryTypeDesc;
    }

    @Override
    public void setDynamicProperties(Map<String, Object> dynamicProperties) {
        this._dynamicProperties = dynamicProperties;
    }

    public ICustomQuery getCustomQuery() {
        return _customQuery;
    }

    public void setCustomQuery(ICustomQuery customQuery) {
        this._customQuery = customQuery;
    }

    public List<IQueryIndexScanner> getCustomIndexes() {
        return (_customQuery == null) ? null : _customQuery.getCustomIndexes();
    }

    public Object getRangeValue(int index) {
        return _rangeValues == null ? null : _rangeValues[index];
    }

    public boolean getRangeInclusion(int index) {
        return _rangeValuesInclusion == null ? true : _rangeValuesInclusion[index];
    }

    public boolean match(Context cacheContext, CacheManager cacheManager, ServerEntry entry, int skipAlreadyMatchedFixedPropertyIndex, String skipAlreadyMatchedIndexPath, RegexCache regexCache) {
        if (!isNullTemplate()) {
            entry = cacheContext.getViewEntryData((IEntryData) entry);
        }
        int[] positions = getEntryTypeDesc().getTypeDesc().getPositionsForScanning();
        boolean result = _extendedMatchCodes == null
                ? matchBasic(entry, skipAlreadyMatchedFixedPropertyIndex, positions)
                : matchExtended(entry, skipAlreadyMatchedFixedPropertyIndex, regexCache, positions);

        if (result && _customQuery != null)
            result = _customQuery.matches(cacheManager, entry, skipAlreadyMatchedIndexPath);
        return result;
    }

    private boolean isNullTemplate() {
        if(!this.checkedAllNullFields){
            //initializing isAllNullFields
            this.isAllNullFields = true;
            if(_fieldsValues != null) {
                for (Object fieldsValue : _fieldsValues) {
                    if (fieldsValue != null) {
                        this.isAllNullFields =  false;
                        break;
                    }
                }
            }
            this.isAllNullFields = isAllNullFields && (_customQuery == null && _extendedMatchCodes == null);
            this.checkedAllNullFields = true;
        }
        return this.isAllNullFields;
    }

    private boolean matchBasic(ServerEntry entry, int skipIndex, int[] positionsForScanning) {
        // if the template has no fields, there is a match
        if (_fieldsValues == null || _fieldsValues.length == 0)
            return true;

        // first try quick-reject
        if (quickReject(entry, positionsForScanning))
            return false;

        // compare every template field (besides the skipIndex, if skipIndex != -1)
        for(int i : positionsForScanning){
            Object templateValue = getFixedPropertyValue(i);
            if (i == skipIndex || templateValue == null)
                continue;
            Object entryValue = entry.getFixedPropertyValue(i);
            if (entryValue == null || !templateValue.equals(entryValue))
                return false;
        }

        // every non-null template field equals corresponding entry field
        return true;
    }

    /**
     * Hashcode based quick-reject. If this method returns true, the match is rejected. If it
     * returns false, a full match will be performed by the engine.
     */
    private boolean quickReject(ServerEntry entry, int[] positionsForScanning) {
        for(int i : positionsForScanning){
            Object templateFieldValue = _fieldsValues[i];
            if (templateFieldValue == null)
                continue;
            Object entryFieldValue = entry.getFixedPropertyValue(i);
            if (entryFieldValue == null)
                return true; // rejected
            if (templateFieldValue == entryFieldValue)
                continue;
            if (templateFieldValue.hashCode() != entryFieldValue.hashCode())
                return true; // rejected
        }

        return false;
    }

    private boolean matchExtended(ServerEntry entry, int skipIndex, RegexCache regexCache, int[] templateIndexes) {
        for(int i : templateIndexes){
            if (i == skipIndex)
                continue;

            short matchCode = _extendedMatchCodes[i];

            if(matchCode != TemplateMatchCodes.IS_NULL && matchCode != TemplateMatchCodes.NOT_NULL && _fieldsValues[i] == null){
                continue;
            }

            Object entryValue = entry.getFixedPropertyValue(i);
            if (matchCode == TemplateMatchCodes.IS_NULL) {
                if (entryValue == null)
                    continue;

                return false;
            }

            if (matchCode == TemplateMatchCodes.NOT_NULL) {
                if (entryValue != null)
                    continue;

                return false;
            }

            Object templateValue = _fieldsValues[i];
            if (templateValue == null)
                continue;
            if (entryValue == null)
                return false; //TBD- null terminology

            Object rangeValue = (_rangeValues != null ? _rangeValues[i] : null);
            if (!matchExtendedProperty(entryValue, templateValue, matchCode, this.getRangeInclusion(i), rangeValue, regexCache))
                return false;
        }

        // every non-null template field equals corresponding entry field
        return true;
    }

    /**
     * single field extended match
     */
    private static boolean matchExtendedProperty(Object entryValue, Object templateValue, short matchCode, boolean includeRange, Object rangeValue, RegexCache regexCache) {
        if (templateValue == null)
            return true; //accepted
        if (entryValue == null)
            return false;  // rejected

        //check for regular expression match
        if (matchCode == TemplateMatchCodes.REGEX) {
            Pattern p = regexCache.getPattern((String) templateValue);
            Matcher m = p.matcher((String) entryValue);
            return m.matches();
        }

        //what kind of match we need on the template ?
        int compareResult = 0;

        if (matchCode != TemplateMatchCodes.NE && matchCode != TemplateMatchCodes.EQ) {
            Comparable eobj = castToComparable(entryValue);
            Comparable tobj = castToComparable(templateValue);

            try {
                compareResult = eobj.compareTo(tobj);
            } catch (ClassCastException cce) {
                /*
                 * ClassCastException if the specified object's type
				 * prevents it from being compared to this Object.
				 */
                if (!eobj.getClass().isInstance(tobj))
                    return false;

                // other unexpected error (probably from within implementation of compareTo)
                throw cce;
            }
        }

        switch (matchCode) {
            case TemplateMatchCodes.NE:
                return (!templateValue.equals(entryValue));

            case TemplateMatchCodes.EQ:
                if (quickRejectField(entryValue, templateValue))
                    return false;
                return (templateValue.equals(entryValue));

            case TemplateMatchCodes.LT:
                if (rangeValue == null)
                    return compareResult < 0;
                if (!(compareResult < 0))
                    return false;
                return fieldExtendedMatchLimitValue(entryValue, rangeValue, matchCode, includeRange);

            case TemplateMatchCodes.LE:
                if (rangeValue == null)
                    return compareResult <= 0;
                if (!(compareResult <= 0))
                    return false;
                return fieldExtendedMatchLimitValue(entryValue, rangeValue, matchCode, includeRange);

            case TemplateMatchCodes.GE:
                if (rangeValue == null)
                    return compareResult >= 0;
                if (!(compareResult >= 0))
                    return false;
                return fieldExtendedMatchLimitValue(entryValue, rangeValue, matchCode, includeRange);

            case TemplateMatchCodes.GT:
                if (rangeValue == null)
                    return compareResult > 0;
                if (!(compareResult > 0))
                    return false;
                return fieldExtendedMatchLimitValue(entryValue, rangeValue, matchCode, includeRange);
        }//switch
        return false;  // rejected
    }

    /**
     * Cast the object to Comparable otherwise throws an IllegalArgumentException exception
     */
    private static Comparable<?> castToComparable(Object obj) {
        try {
            //NOTE- a check for Comparable interface implementation is be done in the proxy
            return (Comparable<?>) obj;
        } catch (ClassCastException cce) {
            throw new IllegalArgumentException("Type " + obj.getClass() +
                    " doesn't implement Comparable, Serialization mode might be different than " + StorageType.OBJECT + ".", cce);
        }
    }

    /**
     * check the limit of a range condition in extended matching Note- currently we support only
     * inclusive range at the "TO" endpoint
     */
    private static boolean fieldExtendedMatchLimitValue(Object entryValue, Object rangeValue, short matchCode, boolean includeRange) {
        //what kind of match we need on the template ?
        //NOTE- a check for Comparable interface implementation should be done in the proxy
        int compareResult = ((Comparable) entryValue).compareTo(rangeValue);

        switch (matchCode) {
            case TemplateMatchCodes.LE:
            case TemplateMatchCodes.LT:
                return (includeRange ? compareResult >= 0 : compareResult > 0);
            case TemplateMatchCodes.GE:
            case TemplateMatchCodes.GT:
                return (includeRange ? compareResult <= 0 : compareResult < 0);
        }
        return false;  // rejected
    }

    /**
     * Hashcode based quick-reject per field.
     *
     * If this method returns true, the match is rejected. If it returns false, a full match will be
     * performed by the engine.
     */
    private static boolean quickRejectField(Object entryValue, Object templateValue) {
        if (templateValue == null)
            return false;
        if (entryValue == null)
            return true; // rejected

        return (templateValue.hashCode() != entryValue.hashCode());
    }

    public short[] getExtendedMatchCodes() {
        return _extendedMatchCodes;
    }

    public SQLQuery<?> toSQLQuery(ITypeDesc typeDesc) {
        if (_fieldsValues == null || _fieldsValues.length == 0)
            return new SQLQuery<Object>(typeDesc.getTypeName(), "");

        List<Object> preparedValues = new LinkedList<Object>();
        StringBuilder wherePart = new StringBuilder();
        for (int i = 0; i < _fieldsValues.length; i++) {
            short matchCode = _extendedMatchCodes != null ? _extendedMatchCodes[i] : TemplateMatchCodes.EQ;

            Object templateFieldValue = _fieldsValues[i];
            if (templateFieldValue != null) {
                if (wherePart.length() > 0)
                    wherePart.append(DefaultSQLQueryBuilder.AND);

                wherePart.append(typeDesc.getFixedProperty(i).getName());
                wherePart.append(DefaultSQLQueryBuilder.mapCodeToSign(matchCode));
                wherePart.append(DefaultSQLQueryBuilder.BIND_PARAMETER);

                // Add the field values to the prepared values
                Object value = DefaultSQLQueryBuilder.convertToSQLFormat(templateFieldValue, matchCode);
                preparedValues.add(value);
            } else // field is null, check extended match-code
            {
                if (matchCode == TemplateMatchCodes.IS_NULL ||
                        matchCode == TemplateMatchCodes.NOT_NULL) {
                    if (wherePart.length() > 0)
                        wherePart.append(DefaultSQLQueryBuilder.AND);

                    wherePart.append(typeDesc.getFixedProperty(i).getName());
                    wherePart.append(DefaultSQLQueryBuilder.mapCodeToSign(matchCode));
                }
            }
        }

        //build query with range (e.g. id > 13 and id < 43)
        if (_rangeValues != null) {
            for (int i = 0; i < _rangeValues.length; i++) {
                if (_rangeValues[i] != null) {
                    if (wherePart.length() > 0)
                        wherePart.append(DefaultSQLQueryBuilder.AND);

                    wherePart.append(typeDesc.getFixedProperty(i).getName());

                    if (_extendedMatchCodes != null)
                        switch (_extendedMatchCodes[i]) {
                            case TemplateMatchCodes.GE:
                            case TemplateMatchCodes.GT:
                                if (getRangeInclusion(i))
                                    wherePart.append(DefaultSQLQueryBuilder.mapCodeToSign(TemplateMatchCodes.LE));
                                else
                                    wherePart.append(DefaultSQLQueryBuilder.mapCodeToSign(TemplateMatchCodes.LT));
                                break;
                            case TemplateMatchCodes.LE:
                            case TemplateMatchCodes.LT:
                                if (getRangeInclusion(i))
                                    wherePart.append(DefaultSQLQueryBuilder.mapCodeToSign(TemplateMatchCodes.GE));
                                else
                                    wherePart.append(DefaultSQLQueryBuilder.mapCodeToSign(TemplateMatchCodes.GT));
                                break;
                        }

                    wherePart.append(DefaultSQLQueryBuilder.BIND_PARAMETER);

                    // Add the range  values to the prepared values
                    preparedValues.add(_rangeValues[i]);
                }
            }
        }

        // check if there is a custom query and add it 
        if (_customQuery != null) {
            SQLQuery customQuery = _customQuery.toSQLQuery(typeDesc);
            String sqlString = customQuery.getQuery();
            if (sqlString != null
                    && sqlString.length() > 0) {
                if (wherePart.length() > 0)
                    wherePart.append(DefaultSQLQueryBuilder.AND);

                wherePart.append(sqlString);

                if (customQuery.getParameters() != null) {
                    for (int i = 0; i < customQuery.getParameters().length; i++) {
                        preparedValues.add(customQuery.getParameters()[i]);
                    }
                }
            }
        }

        return new SQLQuery<Object>(typeDesc.getTypeName(), wherePart.toString(), preparedValues.toArray());
    }

    public boolean isAssignableFrom(IServerTypeDesc serverTypeDesc) {
        final String typeName = _entryTypeDesc.getTypeDesc().getTypeName();
        //
        for (IServerTypeDesc typeDesc : serverTypeDesc.getSuperTypes())
            if (typeDesc.getTypeName().equals(typeName))
                return true;

        return false;
    }

    public boolean isIdQuery() {
        return _isIdQuery;
    }

}
