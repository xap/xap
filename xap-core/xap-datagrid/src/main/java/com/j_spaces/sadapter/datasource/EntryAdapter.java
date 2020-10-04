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

package com.j_spaces.sadapter.datasource;

import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.transport.EntryPacketFactory;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.transport.TemplatePacketFactory;
import com.j_spaces.core.UnknownTypeException;
import com.j_spaces.map.MapEntryFactory;
import com.j_spaces.map.SpaceMapEntry;
import com.j_spaces.sadapter.cache.CacheEntry;

import net.jini.core.entry.UnusableEntryException;

import java.rmi.RemoteException;
import java.util.Map;

/**
 * Wraps space EntryHolder and supplies seamless conversion
 *
 * to used defined types
 *
 * @author anna
 * @since 6.0
 */
@com.gigaspaces.api.InternalApi
public class EntryAdapter extends CacheEntry {
    private static final long serialVersionUID = 1123908388737972298L;

    /**
     * Converts the external entry to user defined objects
     */
    protected IDataConverter<IEntryPacket> _converter;

    /**
     * @param converter
     */
    public EntryAdapter(IDataConverter<IEntryPacket> converter) {
        super();
        _converter = converter;
    }

    /**
     *
     * @param entryHolder
     * @param typeDesc
     * @param converter
     */
    public EntryAdapter(IEntryHolder entryHolder, ITypeDesc typeDesc, IDataConverter<IEntryPacket> converter) {
        this(entryHolder, typeDesc);

        _converter = converter;
    }

    /**
     * @param entryHolder
     * @param typeDesc
     */
    public EntryAdapter(IEntryHolder entryHolder, ITypeDesc typeDesc) {
        super(entryHolder, typeDesc);
    }

    /**
     * @return the converter
     */
    public IDataConverter<IEntryPacket> getConverter() {
        return _converter;
    }

    /**
     * @param converter the converter to set
     */
    public void setConverter(IDataConverter<IEntryPacket> converter) {
        _converter = converter;
    }

    /**
     * @return the user defined representation of the underlying entry
     */
    public Object toObject() {
        if (getMapEntry() != null) {
            return getMapEntry();
        }

        if (_converter != null) {
            IEntryPacket packet = toEntryPacket();
            return _converter.toObject(packet);
        }

        return this;

    }

    /**
     * @return {@link SpaceDocument} instance representation of this entry.
     */
    public SpaceDocument toDocument() {
        // TODO DATASOURCE: document conversion optimization (perhaps skip to entry packet conversion)
        return _converter.toDocument(toEntryPacket());
    }

    /**
     * @return {@link SpaceDocument} instance representation of this entry with the provided type
     * name.
     */
    public SpaceDocument toDocument(String typeName) {
        final SpaceDocument document = toDocument();
        document.setTypeName(typeName);
        return document;
    }

    /**
     * @return the converted IEntryHolder to EnteryPacket
     */
    public IEntryPacket toEntryPacket() {
        if (hasDummyTTE())
            return TemplatePacketFactory.createEmptyPacket(_typeDesc);

        return EntryPacketFactory.createFullPacket(null, _entryHolder, null, _entryHolder.getUidToOperateBy());
    }

    /**
     * Converts given object to IGSEntry
     */
    public IEntryPacket toEntry(Object object)
            throws RemoteException, UnusableEntryException, UnknownTypeException {
        if (object == null)
            return null;

        if (object instanceof SpaceMapEntry)
            return _converter.toInternal(object);

        if (object instanceof Map.Entry) {
            Map.Entry entry = (Map.Entry) object;
            SpaceMapEntry envelope = MapEntryFactory.create(entry.getKey(), entry.getValue());
            return _converter.toInternal(envelope);
        }

        if (_converter != null)
            return _converter.toInternal(object);

        return null;
    }

    /**
     * Convert key value to entry
     *
     * @return IEntryPacket
     */
    public IEntryPacket toEntry(Object key, Object value)
            throws RemoteException, UnusableEntryException, UnknownTypeException {
        if (key == null || value == null)
            return null;

        return _converter.toInternal(MapEntryFactory.create(key, value));
    }

    public ITypeDesc getTypeDescriptor() {
        return _typeDesc;
    }

}
