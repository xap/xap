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

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.metadata.converter.ConversionException;
import com.gigaspaces.internal.reflection.IConstructor;
import com.gigaspaces.internal.reflection.IField;
import com.gigaspaces.internal.reflection.IGetterMethod;
import com.gigaspaces.internal.reflection.IProperties;
import com.gigaspaces.internal.reflection.ISetterMethod;
import com.gigaspaces.internal.reflection.ReflectionUtil;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.j_spaces.core.client.EntryInfo;
import com.j_spaces.core.client.version.space.SpaceVersionTable;
import com.j_spaces.kernel.ClassLoaderHelper;

import net.jini.core.entry.Entry;
import net.jini.core.lease.Lease;
import net.jini.space.InternalSpaceException;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

/**
 * Entry introspector for all the Entry introspectors implementations.
 *
 * @author Niv Ingberg
 * @since 7.0
 */
@com.gigaspaces.api.InternalApi
public class EntryIntrospector<T extends Entry> extends AbstractTypeIntrospector<T> {
    // serialVersionUID should never be changed.
    private static final long serialVersionUID = 1L;

    private static final SpaceVersionTable _versionTable = SpaceVersionTable.getInstance();
    public static final byte EXTERNALIZABLE_CODE = 2;

    private boolean _isVersioned = false;

    // Reflection data is gathered on deserialization 
    private transient Class<T> _class;
    private transient IField<T, Object>[] _fields;
    private transient IGetterMethod<T> _uidGetter;
    private transient ISetterMethod<T> _uidSetter;
    private transient IGetterMethod<T> _infoGetter;
    private transient ISetterMethod<T> _infoSetter;
    private transient IConstructor<T> _ctor;
    private transient IProperties<T> _properties;

    /**
     * Default constructor for Externalizable.
     */
    public EntryIntrospector() {
    }

    public EntryIntrospector(ITypeDesc typeDesc) {
        super(typeDesc);
        this._isVersioned = typeDesc.supportsOptimisticLocking();
        init((Class<T>) typeDesc.getObjectClass());
    }

    protected void init(Class<T> clz) {
        _class = clz;
        _ctor = ReflectionUtil.createCtor(clz);
        List<IField> fieldsList = ReflectionUtil.getCanonicalSortedFields(clz);
        _fields = fieldsList.toArray(new IField[fieldsList.size()]);

        //prepares a fields list for the Reflection Util.
        Field[] fields = new Field[_fields.length];
        for (int i = 0; i < fields.length; ++i)
            fields[i] = (Field) _fields[i].getMember();

        _properties = ReflectionUtil.createFieldProperties(clz, fields);
        initMethods(clz);
    }

    private void initMethods(Class<?> cls) {
        try {
            Method method = cls.getMethod("__setEntryUID", String.class);
            _uidSetter = ReflectionUtil.createSetterMethod(method);
        } catch (NoSuchMethodException ex) {
        }

        try {
            Method method = cls.getMethod("__getEntryUID");
            _uidGetter = ReflectionUtil.createGetterMethod(method);
            if (_uidSetter == null)
                throw new IllegalArgumentException("__getEntryUID was found but no __setEntryUID was found in: " + cls.getName());
        } catch (NoSuchMethodException ex) {
            if (_uidSetter != null)
                throw new IllegalArgumentException("__setEntryUID was found but no __getEntryUID was found in: " + cls.getName(), ex);
        }

        try {
            Method method = cls.getMethod("__setEntryInfo", EntryInfo.class);
            _infoSetter = ReflectionUtil.createSetterMethod(method);
        } catch (NoSuchMethodException ex) {
        }

        try {
            Method method = cls.getMethod("__getEntryInfo");
            _infoGetter = ReflectionUtil.createGetterMethod(method);
            if (_infoSetter == null)
                throw new IllegalArgumentException("__getEntryInfo was found but no __setEntryInfo was found in: " + cls.getName());
        } catch (NoSuchMethodException ex) {
            if (_infoSetter != null)
                throw new IllegalArgumentException("__setEntryInfo was found but no __getEntryInfo was found in: " + cls.getName(), ex);
        }
    }

    public Class<T> getType() {
        return _class;
    }

    public T newInstance() {
        return _ctor.newInstance();
    }

    @Override
    public T[] newArray(int length) {
        return _ctor.newArray(length);
    }

    @Override
    protected Object[] processDocumentObjectInterop(Object[] values, EntryType entryType, boolean cloneOnChange) {
        return entryType.isVirtual() ? fromDocumentIfNeeded(values, cloneOnChange) : values;
    }

    public String getUID(T target, boolean isTemplate, boolean ignoreAutoGenerateUid) {
        try {
            if (_uidGetter != null)
                return (String) _uidGetter.get(target);
            EntryInfo entryInfo = getEntryInfo(target);
            return entryInfo != null ? entryInfo.m_UID : null;
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

    public boolean setUID(T target, String uid) {
        if (_uidSetter != null) {
            try {
                _uidSetter.set(target, uid);
                return true;
            } catch (Exception e) {
                throw new IllegalArgumentException(e);
            }
        }

        if (_infoGetter != null && _infoSetter != null) {
            try {
                EntryInfo info = (EntryInfo) _infoGetter.get(target);
                if (info != null)
                    info.m_UID = uid;
                else
                    _infoSetter.set(target, new EntryInfo(uid, 0));

                return true;
            } catch (Exception e) {
                throw new IllegalArgumentException(e);
            }
        }

        if (_isVersioned)
            _versionTable.setEntryVersion(target, new EntryInfo(uid, getVersion(target)));

        return false;
    }

    public int getVersion(T target) {
        try {
            EntryInfo entryInfo = getEntryInfo(target);
            return entryInfo != null ? entryInfo.m_VersionID : 0;
        } catch (Exception e) {
            throw new ConversionException(e);
        }
    }

    public boolean setVersion(T target, int version) {
        if (_infoGetter != null) {
            try {
                final EntryInfo entryInfo = (EntryInfo) _infoGetter.get(target);
                if (entryInfo == null) {
                    EntryInfo newEntryInfo = new EntryInfo(null, version);
                    _infoSetter.set(target, newEntryInfo);
                } else {
                    entryInfo.m_VersionID = version;
                }
                return true;
            } catch (Exception e) {
                throw new ConversionException(e);
            }
        }

        if (_isVersioned)
            _versionTable.setEntryVersion(target, new EntryInfo(getUID(target), version));

        return false;
    }

    public boolean isTransient(T target) {
        return false;
    }

    public boolean setTransient(T target, boolean isTransient) {
        return false;
    }

    public boolean hasDynamicProperties() {
        return false;
    }

    public Map<String, Object> getDynamicProperties(T target) {
        return null;
    }

    public void setDynamicProperties(T target, Map<String, Object> dynamicProperties) {
    }

    public Object[] getValues(T target) {
        return _properties.getValues(target);
    }

    public void setValues(T target, Object[] values) {
        _properties.setValues(target, values);
    }

    public Object getValue(T target, int index) {
        try {
            return _fields[index].get(target);
        } catch (Exception e) {
            throw new ConversionException(e);
        }
    }

    public void setValue(T target, Object value, int index) {
        try {
            _fields[index].set(target, value);
        } catch (Exception e) {
            throw new ConversionException(e);
        }
    }

    @Override
    protected Object getDynamicProperty(T target, String name) {
        throw new UnsupportedOperationException("This operation is not supported for " + Entry.class + " types");
    }

    @Override
    public void setDynamicProperty(T target, String name, Object value) {
        throw new UnsupportedOperationException("This operation is not supported for " + Entry.class + " types");
    }

    @Override
    public void unsetDynamicProperty(T target, String name) {
        throw new UnsupportedOperationException("This operation is not supported for " + Entry.class + " types");
    }

    public long getTimeToLive(T target) {
        EntryInfo entryInfo = getEntryInfo(target);
        return entryInfo != null ? entryInfo.m_TimeToLive : Lease.FOREVER;
    }

    public boolean setTimeToLive(T target, long ttl) {
        EntryInfo info = getEntryInfo(target);
        if (info != null) {
            info.m_TimeToLive = ttl;
            return true;
        }

        return false;
    }

    protected EntryInfo getEntryInfo(T target) {
        try {
            if (_infoGetter != null)
                return (EntryInfo) _infoGetter.get(target);
            if (_isVersioned)
                return _versionTable.getEntryVersion(target);
            return null;
        } catch (Exception ex) {
            throw new InternalSpaceException("Fail to invoke __getEntryInfo()", ex);
        }
    }

    public void setEntryInfo(T target, String uid, int version, long ttl) {
        if (_infoSetter != null) {
            try {
                _infoSetter.set(target, new EntryInfo(uid, version, ttl));
                return;
            } catch (Exception ex) {
                throw new InternalSpaceException("Fail to invoke __setEntryInfo() UID: " +
                        uid + " for Entry: " + target.getClass(), ex);
            }
        }

        setUID(target, uid);

        if (_isVersioned)
            _versionTable.setEntryVersion(target, new EntryInfo(uid, version, ttl));
    }

    public boolean hasTimeToLiveProperty(T target) {
        return getEntryInfo(target) != null;
    }

    public boolean hasTransientProperty(T target) {
        return false;
    }

    public boolean hasUID(T target) {
        boolean hasUIDProperty = _uidSetter != null || (_infoGetter != null && _infoSetter != null);
        return hasUIDProperty && getUID(target) != null;
    }

    public boolean hasVersionProperty(T target) {
        return _infoGetter != null;
    }

    @Override
    public boolean supportsNestedOperations() {
        return false;
    }

    @Override
    public Class<?> getPathType(String path) {
        throw new InternalSpaceException("This operation is currently not supported for Entry.");
    }


    @Override
    public byte getExternalizableCode() {
        return EXTERNALIZABLE_CODE;
    }

    @Override
    public void readExternal(ObjectInput in, PlatformLogicalVersion version)
            throws IOException, ClassNotFoundException {
        super.readExternal(in, version);

        final String className = IOUtils.readString(in);
        Class<T> cls = ClassLoaderHelper.loadClass(className);
        init(cls);
    }

    /**
     * NOTE: if you change this method, you need to make this class ISwapExternalizable
     */
    @Override
    public void writeExternal(ObjectOutput out, PlatformLogicalVersion version)
            throws IOException {
        super.writeExternal(out, version);
        IOUtils.writeString(out, _class.getName());
    }
}
