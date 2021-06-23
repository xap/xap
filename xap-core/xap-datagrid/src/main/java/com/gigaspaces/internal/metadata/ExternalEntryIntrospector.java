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

package com.gigaspaces.internal.metadata;

import com.gigaspaces.internal.client.StorageTypeDeserialization;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.metadata.converter.ConversionException;
import com.gigaspaces.internal.reflection.IConstructor;
import com.gigaspaces.internal.reflection.ReflectionUtil;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.j_spaces.core.IGSEntry;
import com.j_spaces.core.client.ExternalEntry;
import com.j_spaces.core.client.ExternalEntryUtils;

import net.jini.space.InternalSpaceException;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Niv Ingberg
 * @since 7.0
 */
@com.gigaspaces.api.InternalApi
public class ExternalEntryIntrospector<T extends ExternalEntry> extends AbstractTypeIntrospector<T> {
    // serialVersionUID should never be changed.
    private static final long serialVersionUID = 1L;
    public static final byte EXTERNALIZABLE_CODE = 4;

    private static final IConstructor<ExternalEntry> DEFAULT_CTOR = new ExternalEntryFactory();

    private Class<T> _implClass;
    private IConstructor<T> _constructor;
    private PropertyInfo[] _properties;

    /**
     * Default constructor for Externalizable.
     */
    public ExternalEntryIntrospector() {
    }

    public ExternalEntryIntrospector(ITypeDesc typeDesc, Class<T> implClass) {
        super(typeDesc);
        this._implClass = implClass != null ? implClass : (Class<T>) ExternalEntry.class;
        this._constructor = ExternalEntry.class.equals(_implClass) ? (IConstructor<T>) DEFAULT_CTOR : ReflectionUtil.createCtor(_implClass);
        this._properties = typeDesc.getProperties();
    }

    public boolean hasUID(T target) {
        return getUID(target) != null;
    }

    public String getUID(T target, boolean isTemplate, boolean ignoreAutoGenerateUid) {
        return target.getUID();
    }

    public boolean setUID(T target, String uid) {
        target.setUID(uid);
        return true;
    }

    public boolean hasVersionProperty(T target) {
        return true;
    }

    public int getVersion(T target) {
        return target.getVersion();
    }

    public boolean setVersion(T target, int version) {
        target.setVersion(version);
        return true;
    }

    public boolean hasTransientProperty(T target) {
        return true;
    }

    public boolean isTransient(T target) {
        if (target == null)
            return false;
        return target.isTransient();
    }

    public boolean setTransient(T target, boolean isTransient) {
        target.setTransient(isTransient);
        return true;
    }

    public boolean hasTimeToLiveProperty(T target) {
        return true;
    }

    public long getTimeToLive(T target) {
        return target.getTimeToLive();
    }

    public boolean setTimeToLive(T target, long ttl) {
        target.setTimeToLive(ttl);
        return true;
    }

    public void setEntryInfo(T target, String uid, int version, long timeToLive) {
        setUID(target, uid);
        setVersion(target, version);
        setTimeToLive(target, timeToLive);
    }

    @Override
    public T newInstance() {
        return _constructor.newInstance();
    }

    @Override
    public T[] newArray(int length) {
        return _constructor.newArray(length);
    }

    @Override
    protected Object[] processDocumentObjectInterop(Object[] values, EntryType entryType, boolean cloneOnChange) {
        return values;
    }

    public Class<T> getType() {
        return _implClass;
    }

    public boolean hasDynamicProperties() {
        return false;
    }

    public Map<String, Object> getDynamicProperties(T target) {
        return null;
    }

    public void setDynamicProperties(T target, Map<String, Object> dynamicProperties) {
    }

    public Object getValue(T target, int index) {
        return target.getFieldValue(index);
    }

    public void setValue(T target, Object value, int index) {
        target.setFieldValue(index, value);
    }

    protected Object getDynamicProperty(T target, String name) {
        throw new UnsupportedOperationException("This operation is not supported for ExternalEntry types");
    }

    public void setDynamicProperty(T target, String name, Object value) {
        throw new UnsupportedOperationException("This operation is not supported for ExternalEntry types");
    }

    @Override
    public void unsetDynamicProperty(T target, String name) {
        throw new UnsupportedOperationException("This operation is not supported for ExternalEntry types");
    }

    public Object[] getValues(T target) {
        final Object[] values = target.getFieldsValues();
        if (values == null) {
            return new Object[_properties.length];
        }

        String[] fieldsNames = target.getFieldsNames();
        if (fieldsNames == null)
            return values;

        Map<String, Object> fieldNamesToValues = new HashMap<String, Object>();
        for (int i = 0; i < fieldsNames.length; i++) {
            fieldNamesToValues.put(fieldsNames[i], values[i]);
        }

        Object[] results = new Object[_properties.length];

        for (int i = 0; i < _properties.length; i++) {
            final String fieldName = _properties[i].getName();
            results[i] = fieldNamesToValues.get(fieldName);
        }

        return results;
    }

    public void setValues(T target, Object[] values) {
        target.setFieldsValues(values);
    }

    @Override
    public T toObject(IEntryPacket packet, StorageTypeDeserialization storageTypeDeserialization) {
        try {
            final T res = super.toObject(packet, storageTypeDeserialization);

            res.setClassName(packet.getTypeName());
            ExternalEntryUtils.updateTypeDescription(res, packet.getTypeDescriptor());
            res.setFifo(packet.isFifo());

            return res;
        } catch (Exception e) {
            throw new ConversionException(e);
        }
    }

    @Override
    public T toObject(IGSEntry entry, ITypeDesc typeDesc) {
        T result = super.toObject(entry, typeDesc);
        ExternalEntryUtils.updateTypeDescription(result, typeDesc);
        return result;
    }

    public static String getUid(ITypeDesc typeDesc, Object[] fieldsValues) {
        // id can be null in case of ExternalEntry that was created from SQL Query.
        if (typeDesc.getIdPropertyName() == null || !typeDesc.isAutoGenerateId())
            return null;
        // check the case when the uid is one of the fields
        // in this case update the entry uid with its value
        // and remove it from the values - so the matching will be done correctly
        int propertyId = typeDesc.getIdentifierPropertyId();
        if (fieldsValues[propertyId] != null) {
            String uid = (String) fieldsValues[propertyId];
            fieldsValues[propertyId] = null;
            return uid;
        }

        return null;
    }

    @Override
    public boolean supportsNestedOperations() {
        return false;
    }

    @Override
    public Class<?> getPathType(String path) {
        throw new InternalSpaceException("This operation is currently not supported for VirtualEntry.");
    }

    @Override
    public byte getExternalizableCode() {
        return EXTERNALIZABLE_CODE;
    }

    @Override
    public void readExternal(ObjectInput in, PlatformLogicalVersion version)
            throws IOException, ClassNotFoundException {
        super.readExternal(in, version);

        _implClass = IOUtils.readObject(in);
        int length = in.readInt();
        if (length >= 0) {
            _properties = new PropertyInfo[length];
            for (int i = 0; i < length; i++)
                _properties[i] = IOUtils.readObject(in);
        }
    }

    /**
     * NOTE: if you change this method, you need to make this class ISwapExternalizable
     */
    @Override
    public void writeExternal(ObjectOutput out, PlatformLogicalVersion version)
            throws IOException {
        super.writeExternal(out, version);

        IOUtils.writeObject(out, _implClass);

        int length = _properties == null ? -1 : _properties.length;
        out.writeInt(length);
        for (int i = 0; i < length; i++)
            IOUtils.writeObject(out, _properties[i]);
    }

    private static class ExternalEntryFactory implements IConstructor<ExternalEntry> {
        static final long serialVersionUID = 5121268238826294570L;
        @Override
        public ExternalEntry newInstance() {
            return new ExternalEntry();
        }

        @Override
        public ExternalEntry[] newArray(int length) {
            return new ExternalEntry[length];
        }
    }
}
