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

package com.gigaspaces.internal.server.space.metadata;

import com.gigaspaces.internal.metadata.EntryIntrospector;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.metadata.InactiveTypeDesc;
import com.gigaspaces.metrics.LongCounter;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

@com.gigaspaces.api.InternalApi
public class ServerTypeDesc implements IServerTypeDesc {
    private static final AtomicInteger  _codesGen = new AtomicInteger(-1);
    private static final ConcurrentMap<Short,IServerTypeDesc> _codesRepo = new ConcurrentHashMap<Short, IServerTypeDesc>();

    private final int _typeId;
    private final String _typeName;
    private final boolean _isRootType;

    private final IServerTypeDesc[] _superTypes;
    private final short _serverTypeDescCode;

    private ITypeDesc _typeDesc;
    private boolean _inactive;

    private IServerTypeDesc[] _subTypes;
    private IServerTypeDesc[] _assignableTypes;

    private volatile boolean _maybeOutdated;
    private LongCounter _readCounter;
    private LongCounter _ramReadCounter;

    public ServerTypeDesc(int typeId, String typeName) {
        this(typeId, typeName, null, null);
    }

    public ServerTypeDesc(int typeId, String typeName, ITypeDesc typeDesc, IServerTypeDesc superType) {
        this(typeId, typeName, typeDesc,  superType, null);
    }

    private ServerTypeDesc(int typeId, String typeName, ITypeDesc typeDesc, IServerTypeDesc superType,Short code) {
        this._typeId = typeId;
        this._typeName = typeName;
        this._isRootType = typeName.equals(ROOT_TYPE_NAME);
        this._superTypes = initSuperTypes(superType);
        this._readCounter = new LongCounter();
        this._ramReadCounter = new LongCounter();
        if (typeDesc == null)
            typeDesc = createInactiveTypeDesc(typeName, _superTypes);
        setTypeDesc(typeDesc);

        this._subTypes = new ServerTypeDesc[0];
        this._assignableTypes = new ServerTypeDesc[]{this};

        if (superType != null)
            superType.addSubType(this);
        if (code == null)
        {
            Integer c = _codesGen.incrementAndGet();
            if(c > Short.MAX_VALUE){
                throw new IllegalStateException("type map key has reached Short.MAX_VALUE, cannot create more ServerTypeDec instances");
            }
            code = c.shortValue();
            _codesRepo.put(code,this);
        }
        _serverTypeDescCode = code;

    }

    public static IServerTypeDesc getByServerTypeDescCode(short code)
    {
        return _codesRepo.get(code);
    }

    @Override
    public String toString() {
        return "ServerTypeDesc(" + _typeId + ", " + _typeName + ")";
    }

    public int getTypeId() {
        return _typeId;
    }

    public String getTypeName() {
        return _typeName;
    }

    public boolean isRootType() {
        return _isRootType;
    }

    public ITypeDesc getTypeDesc() {
        return _typeDesc;
    }

    public void setTypeDesc(ITypeDesc typeDesc) {
        if (typeDesc == null)
            throw new IllegalArgumentException("Argument cannot be null - 'typeDesc'");

        this._typeDesc = typeDesc;
        this._inactive = typeDesc.isInactive();
    }

    public boolean isActive() {
        return !_inactive;
    }

    public boolean isInactive() {
        return _inactive;
    }

    public void inactivateType() {
        _inactive = true;
    }

    public boolean isFifoSupported() {
        return _typeDesc.isFifoSupported();
    }

    public IServerTypeDesc[] getSuperTypes() {
        return _superTypes;
    }

    public IServerTypeDesc[] getAssignableTypes() {
        return _assignableTypes;
    }

    @Override
    public boolean hasSubTypes() {
        return _subTypes.length != 0;
    }


    public IServerTypeDesc createCopy(IServerTypeDesc superType) {
        // Create a copy of this type with the new super type:
        ServerTypeDesc copy = new ServerTypeDesc(this._typeId, this._typeName, this._typeDesc, superType, this._serverTypeDescCode);
        copy._inactive = this._inactive;
        copy._readCounter = this._readCounter;
        copy._ramReadCounter = this._ramReadCounter;
        IServerTypeDesc oldServerTypeDesc = _codesRepo.put(this._serverTypeDescCode, copy);
        if(oldServerTypeDesc != null){
            oldServerTypeDesc.setMaybeOutdated();
        }
        // Create a copy of the direct sub types recursively:
        for (int i = 0; i < this._subTypes.length; i++)
            this._subTypes[i].createCopy(copy);

        return copy;
    }

    private IServerTypeDesc[] initSuperTypes(IServerTypeDesc superType) {
        if (superType == null)
            return new IServerTypeDesc[]{this};

        IServerTypeDesc[] superSuperTypes = superType.getSuperTypes();
        return prependItemToArray(superSuperTypes, new IServerTypeDesc[superSuperTypes.length + 1], this);
    }

    private static ITypeDesc createInactiveTypeDesc(String typeName, IServerTypeDesc[] superTypes) {
        String[] superTypesNames = new String[superTypes.length];
        for (int i = 0; i < superTypesNames.length; i++)
            superTypesNames[i] = superTypes[i].getTypeName();

        return new InactiveTypeDesc(typeName, superTypesNames);
    }

    public void addSubType(IServerTypeDesc subType) {
        if (subType.getSuperTypes()[1] == this)
            this._subTypes = appendItemToArray(_subTypes, subType);

        this._assignableTypes = appendItemToArray(_assignableTypes, subType);

        if (this._superTypes.length > 1)
            this._superTypes[1].addSubType(subType);
    }

    @Override
    public void removeSubType(IServerTypeDesc subType) {
        if (subType.getSuperTypes()[1] == this)
            this._subTypes = removeItemFromArray(_subTypes, subType);
        this._assignableTypes = removeItemFromArray(_assignableTypes, subType);

        if (this._superTypes.length > 1)
            this._superTypes[1].removeSubType(subType);
    }

    private static <T> T[] prependItemToArray(T[] source, T[] target, T newFirstItem) {
        target[0] = newFirstItem;
        System.arraycopy(source, 0, target, 1, source.length);
        return target;
    }

    private static IServerTypeDesc[] appendItemToArray(IServerTypeDesc[] source, IServerTypeDesc newLastItem) {
        IServerTypeDesc[] target = new IServerTypeDesc[source.length + 1];
        target[source.length] = newLastItem;
        System.arraycopy(source, 0, target, 0, source.length);
        return target;
    }

    private static <T> int indexOf(T[] array, T item) {
        for (int i = 0; i < array.length; i++) {
            if (array[i].equals(item)) {
                return i;
            }
        }
        return -1;
    }

    private static IServerTypeDesc[] removeItemFromArray(IServerTypeDesc[] source, IServerTypeDesc old) {
        int oldPos = indexOf(source, old);
        if (oldPos == -1)
            return source;

        IServerTypeDesc[] target = new IServerTypeDesc[source.length - 1];
        int pos = 0;
        for (int i = 0; i < source.length; i++) {
            if (i != oldPos) {
                target[pos++] = source[i];
            }
        }
        return target;
    }

    public static boolean isEntry(IServerTypeDesc typeDesc) {
        return typeDesc.getTypeDesc().getIntrospector(null) instanceof EntryIntrospector;
    }

    @Override
    public short getServerTypeDescCode()
    {
        return _serverTypeDescCode;
    }

    @Override
    public boolean isMaybeOutdated() {
        return _maybeOutdated;
    }

    @Override
    public void setMaybeOutdated() {
        this._maybeOutdated = true;
    }

    @Override
    public LongCounter getReadCounter() {
        return _readCounter;
    }
    @Override
    public LongCounter getRAMReadCounter() {
        return _ramReadCounter;
    }
}
