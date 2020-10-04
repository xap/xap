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

//

/**
 * an object used to Externalize BlobStoreEntryHolder
 */
package com.j_spaces.core.cache.blobStore;

import com.gigaspaces.client.storage_adapters.BinaryWrapper;
import com.gigaspaces.internal.io.CompressedMarshObject;
import com.gigaspaces.internal.io.IOArrayException;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.io.MarshObject;
import com.gigaspaces.internal.metadata.*;
import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.space.metadata.ServerTypeDesc;
import com.gigaspaces.internal.server.storage.EntryDataType;
import com.gigaspaces.internal.server.storage.FlatEntryData;
import com.gigaspaces.internal.server.storage.ITransactionalEntryData;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.cache.TypeData;
import com.j_spaces.core.cache.blobStore.sadapter.BlobStoreStorageAdapterClassInfo;
import com.j_spaces.core.cache.blobStore.sadapter.IBlobStoreStorageAdapter;

import java.io.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;


@com.gigaspaces.api.InternalApi
public class BlobStoreEntryLayout implements Externalizable {
    private static final long serialVersionUID = -5072883352415904068L;
    private transient boolean _recoverable;
    private transient boolean _onlyIndexesPart;


    private String _m_Uid;

    //++++++++++++++++++ Embedded sync list stuff
    private long _generationId;
    private long _sequenceId;
    private boolean _phantom;
    private boolean _partOfMultipleUidsInfo;

    //+++++++++++++++++++ BlobStoreEntryHolder stuff
    private short _blobStoreVersion;

    private String _typeName;
    private byte _entryTypeCode;

    //+++++++++++++++++++ EntryHolder + AbstractSpaceItem stuff
    private long _scn;
    private boolean _transient;
    private int _order;

    //++++++++++++++++++ AbstractEntryData + FlatEntryData stuff 
    private int _versionID;
    private long _expirationTime;
    private Object[] _fieldsValues;
    private Map<String, Object> _dynamicProperties;
    private static final Map<Class<?>, Serializer> types = initSerializers();

    //++++++++++++++++++++++  temp field not serialized +++++++++++++++++++++++++++
    private transient long _dynamicIndexesRelatedIndicators;
    private transient int _dynamicIndexesRelatedLength;

    public BlobStoreEntryLayout(BlobStoreEntryHolder eh, boolean recoverable) {
        if (eh.getEntryData().getEntryDataType() != EntryDataType.FLAT)
            throw new UnsupportedOperationException("off-heap supported only fpr flat entry-data");
        _recoverable = recoverable;

        //keep uid for recovery sake
        _m_Uid = eh.getUID();

        _blobStoreVersion = eh.getBlobStoreVersion();

        _typeName = eh.getTypeName();
        _entryTypeCode = eh.getEntryTypeCode();

        //+++++++++++++++++++ EntryHolder + AbstractSpaceItem stuff
        _scn = eh.getSCN();
        _transient = eh.isTransient();
        _order = eh.getOrder();

        //++++++++++++++++++ AbstractEntryData + FlatEntryData stuff
        _versionID = eh.getEntryData().getVersion();
        _expirationTime = eh.getExpirationTime();
        _fieldsValues = eh.getEntryData().getFixedPropertiesValues();
        _dynamicProperties = eh.getEntryData().getDynamicProperties();

        //++++++++++++++++++ Embedded sync list stuff
        if (eh.getEmbeddedSyncOpInfo() != null) {
            _generationId = eh.getEmbeddedSyncOpInfo().getGenerationId();
            _sequenceId = eh.getEmbeddedSyncOpInfo().getSequenceId();
            _phantom = eh.getEmbeddedSyncOpInfo().isPhantom();
            _partOfMultipleUidsInfo = eh.getEmbeddedSyncOpInfo().isPartOfMultipleUidsInfo();
        }
    }

    public BlobStoreEntryLayout() {
    }


    BlobStoreEntryHolder buildBlobStoreEntryHolder(CacheManager cacheManager, BlobStoreRefEntryCacheInfo eci) {
        if (_phantom)
            throw new UnsupportedOperationException();


        IServerTypeDesc typeDesc = cacheManager.getEngine().getTypeManager().getServerTypeDesc(_typeName);
        EntryTypeDesc entryTypeDesc = typeDesc.getTypeDesc().getEntryTypeDesc(EntryType.fromByte(_entryTypeCode));

        ITransactionalEntryData entryData = new FlatEntryData(_fieldsValues, _dynamicProperties,
                entryTypeDesc, _versionID, _expirationTime, null /*createXtnEntryInfo*/);


        BlobStoreEntryHolder entry = new BlobStoreEntryHolder(typeDesc, eci.getUID(), _scn, _transient, entryData, _onlyIndexesPart);
        if (_order > 0)
            entry.setOrder(_order);

        //set the specific blobStore fields
        entry.setBlobStoreResidentPart(eci);
        entry.setBlobStoreVersion(_blobStoreVersion);
        //set embedded sync list info
        if (_generationId != 0) {
            entry.setEmbeddedSyncOpInfo(_generationId, _sequenceId, _phantom, _partOfMultipleUidsInfo);
        }
        return entry;
    }

    public BlobStoreEntryHolder buildBlobStoreEntryHolder(CacheManager cacheManager) {

        IServerTypeDesc typeDesc = cacheManager.getEngine().getTypeManager().getServerTypeDesc(_typeName);
        EntryTypeDesc entryTypeDesc = typeDesc.getTypeDesc().getEntryTypeDesc(EntryType.fromByte(_entryTypeCode));

        ITransactionalEntryData entryData = new FlatEntryData(_fieldsValues, _dynamicProperties,
                entryTypeDesc, _versionID, _expirationTime, null /*createXtnEntryInfo*/);


        BlobStoreEntryHolder entry = new BlobStoreEntryHolder(typeDesc, _m_Uid, _scn, _transient, entryData, _onlyIndexesPart);
        if (_order > 0)
            entry.setOrder(_order);

        //set the specific blobStore fields
        entry.setBlobStoreVersion(_blobStoreVersion);
        //set embedded sync list info
        if (_generationId != 0) {
            entry.setEmbeddedSyncOpInfo(_generationId, _sequenceId, _phantom, _partOfMultipleUidsInfo);
        }
        return entry;
    }

    public short getBlobStoreVersion() {
        return _blobStoreVersion;
    }

    private void writeObjectArray(ObjectOutput out, Object[] array, IServerTypeDesc typeDesc, boolean isIndexesPart, boolean[] fixed_indexes)
            throws IOException {
        if (array == null)
            out.writeInt(-1);
        else {
            int length = array.length;
            out.writeInt(length);
            int i = 0;
            int noNullIndicators;
            try {
                for (; i < length; i++) {
                    if (i % 7 == 0)  //set the non null indicators
                    {
                        noNullIndicators = 0;
                        int lim = Math.min(length, (i + 7));
                        for (int j = i; j < lim; j++) {
                            if (!_phantom && array[j] != null && fixed_indexes[j] == isIndexesPart)
                                noNullIndicators |= (1 << (j % 7));
                        }
                        out.writeByte(noNullIndicators);
                    }
                    if (fixed_indexes[i] == isIndexesPart)
                        writeObjectToStream(array[i], out, typeDesc, i);
                }
            } catch (IOException e) {
                throw new IOArrayException(i, "Failed to serialize item #" + i, e);
            }
        }
    }

    private void writeObjectToStream(Object obj, ObjectOutput out, IServerTypeDesc typeDesc, int arrayPosition) throws IOException {
        if (_phantom || obj == null)
            return;
        Serializer serializer = getPropertySerializer(typeDesc, arrayPosition);
        if (serializer != null)
            serializer.write(obj, out);
        else
            out.writeObject(obj);
    }

    private static Serializer getPropertySerializer(IServerTypeDesc typeDesc, int propertyPos) {
        if (typeDesc.getTypeDesc().getObjectType() == EntryType.EXTERNAL_ENTRY)
            return null;
        PropertyInfo property = typeDesc.getTypeDesc().getFixedProperty(propertyPos);
        Class clazz = property.getStorageAdapter() != null ? property.getStorageAdapter().getStorageClass() : property.getType();
        return clazz != null ? types.get(clazz) : null;
    }

    public void setUid(String _m_Uid) {
        this._m_Uid = _m_Uid;
    }

    public void setOnlyIndexesPart(boolean _onlyIndexesPart) {
        this._onlyIndexesPart = _onlyIndexesPart;
    }

    public void setBlobStoreVersion(short version){
        _blobStoreVersion = version;
    }

    private Object[] readObjectArray(ObjectInput in, IServerTypeDesc typeDesc, Object[] current)
            throws IOException, ClassNotFoundException {
        final int length = in.readInt();
        if (length < 0)
            return current;

        Object[] array = current == null ? new Object[length] : current;
        int i = 0;
        try {
            int noNullIndicators = 0;
            for (; i < length; i++) {
                if (i % 7 == 0)  //get the non null indicators
                    noNullIndicators = in.readByte();
                if ((noNullIndicators & (1 << (i % 7))) != 0)
                    array[i] = readObjectFromStream(in, typeDesc, i);
            }
        } catch (IOException e) {
            throw new IOArrayException(i, "Failed to deserialize item #" + i, e);
        }

        return array;
    }

    private Object readObjectFromStream(ObjectInput in, IServerTypeDesc typeDesc, int arrayPosition) throws IOException, ClassNotFoundException {
        Serializer serializer = getPropertySerializer(typeDesc, arrayPosition);
        if (serializer != null)
            return serializer.read(in);
        else
            return in.readObject();
    }

    private void writeMapStringObject(ObjectOutput out,
                                      Map<String, Object> map, boolean isIndexesPart, HashSet<String> indexesIndicators) throws IOException {
        if (map == null || _phantom) {
            out.writeInt(-1);
            return;
        }
        int length = 0;
        int overall = 0;
        if (isIndexesPart) {
            _dynamicIndexesRelatedIndicators = 0;
            for (Entry<String, Object> entry : map.entrySet()) {
                if (indexesIndicators.contains(entry.getKey())) {
                    if (overall < 63)
                        _dynamicIndexesRelatedIndicators |= (1L << overall);
                    length++;
                }
                overall++;
            }
            _dynamicIndexesRelatedLength = length;
        } else
            length = map.size() - _dynamicIndexesRelatedLength;

        out.writeInt(length);
        if (length == 0)
            return;
        overall = 0;
        for (Entry<String, Object> entry : map.entrySet()) {
            if (overall < 63) {
                if (((_dynamicIndexesRelatedIndicators & (1L << overall)) != 0L) == isIndexesPart) {
                    out.writeUTF(entry.getKey());
                    out.writeObject(entry.getValue());
                }
            } else {
                if (indexesIndicators.contains(entry.getKey()) == isIndexesPart) {
                    out.writeUTF(entry.getKey());
                    out.writeObject(entry.getValue());
                }
            }
            overall++;
        }
    }

    private Map<String, Object> readMapStringObject(ObjectInput in, Map<String, Object> current)
            throws IOException, ClassNotFoundException {
        Map<String, Object> map = current;

        int length = in.readInt();
        if (length >= 0) {
            if (map == null)
                map = new HashMap<String, Object>(length);
            for (int i = 0; i < length; i++) {
                String key = in.readUTF();
                Object value = in.readObject();
                map.put(key, value);
            }
        }

        return map;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }

    public void writeExternal(ObjectOutput out, CacheManager cacheManager) throws IOException {
        if(cacheManager.isPersistentBlobStore()){
            writeExternalPersistentBlobStore(out, cacheManager);
        } else {
            writeExternalOffHeap(cacheManager, out, false);
        }
    }

    public byte[] getIndexValuesBytes(CacheManager cacheManager) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(byteArrayOutputStream);

        writeExternalOffHeap(cacheManager, out, true);

        out.close();
        return byteArrayOutputStream.toByteArray();
    }

    private void writeExternalPersistentBlobStore(ObjectOutput out, CacheManager cacheManager) throws IOException {
        byte flags = buildFlags();
        out.writeByte(flags);
        if ((flags & FLAG_RECOVERABLE) == FLAG_RECOVERABLE) {
            //platform version tokens
            //platform version fields- we dont store the platform version as object to avoid
            //class info in each stream
            PlatformLogicalVersion version = PlatformLogicalVersion.getLogicalVersion();
            version.write(out);
        }

        out.writeUTF(_m_Uid);

        byte embeddedSyncInfoFlags = buildEmbeddedSyncInfoFlags();
        out.writeByte(embeddedSyncInfoFlags);
        if ((embeddedSyncInfoFlags & FLAG_CONTAINS_EMBEDDED_SYNC_INFO) == FLAG_CONTAINS_EMBEDDED_SYNC_INFO) {
            out.writeLong(_generationId);
            out.writeLong(_sequenceId);
        }
        out.writeShort(_blobStoreVersion);
        out.writeUTF(_typeName);
        out.writeByte(_entryTypeCode);
        out.writeLong(_scn);
        if ((flags & FLAG_ORDER) == FLAG_ORDER) {
            out.writeInt(_order);
        }

        if ((flags & FLAG_VERSIONID) == FLAG_VERSIONID) {
            out.writeInt(_versionID);
        }

        if ((flags & FLAG_EXPIRATION) == FLAG_EXPIRATION) {
            out.writeLong(_expirationTime);
        }

        boolean[] fixed_indices = null;
        HashSet<String> dynamic_indices = null;
        short indexesStoredVersion = (short) 0;


        IServerTypeDesc typeDesc = cacheManager.getEngine().getTypeManager().getServerTypeDesc(_typeName);
        BlobStoreStorageAdapterClassInfo ci = ((IBlobStoreStorageAdapter) cacheManager.getStorageAdapter()).getBlobStoreStorageAdapterClassInfo(_typeName);
        if (ci != null) {
            fixed_indices = ci.getIndexesRelatedFixedProperties();
            dynamic_indices = ci.getIndexesRelatedDynamicProperties();
            indexesStoredVersion = ci.getStoredVersion();
        } else {//can happen when a typw was added and the SA intoroduceType call didnt occur yet
            TypeData typeData = cacheManager.getTypeData(typeDesc);
            fixed_indices = typeData.getIndexesRelatedFixedProperties();
            dynamic_indices = typeData.getIndexesRelatedDynamicProperties();
            //note- version is set to 0 which is first one
        }
        out.writeShort(indexesStoredVersion);

        //indexes-related
        writeObjectArray(out, _fieldsValues, typeDesc, true /*indexes*/, fixed_indices);

        if ((flags & FLAG_DYNAMIC_PROPERTIES) == FLAG_DYNAMIC_PROPERTIES) {
            writeMapStringObject(out, _dynamicProperties, true /*indexes*/, dynamic_indices);
        }
        //non-indexes-related
        writeObjectArray(out, _fieldsValues, typeDesc, false /*indexes*/, fixed_indices);

        if ((flags & FLAG_DYNAMIC_PROPERTIES) == FLAG_DYNAMIC_PROPERTIES) {
            writeMapStringObject(out, _dynamicProperties, false /*indexes*/, dynamic_indices);
        }
    }

    private void writeExternalOffHeap(CacheManager cacheManager, ObjectOutput out, boolean onlyIndexesPart) throws IOException {
        byte flags = buildFlags();
        out.writeByte(flags);

        byte embeddedSyncInfoFlags = buildEmbeddedSyncInfoFlags();
        out.writeByte(embeddedSyncInfoFlags);
        if ((embeddedSyncInfoFlags & FLAG_CONTAINS_EMBEDDED_SYNC_INFO) == FLAG_CONTAINS_EMBEDDED_SYNC_INFO) {
            IOUtils.writeLong(out,_generationId);
            IOUtils.writeLong(out,_sequenceId);
        }
        IOUtils.writeShort(out,_blobStoreVersion);
        out.writeByte(_entryTypeCode);
        IOUtils.writeLong(out,_scn);
        if ((flags & FLAG_ORDER) == FLAG_ORDER) {
            IOUtils.writeInt(out, _order);
        }

        if ((flags & FLAG_VERSIONID) == FLAG_VERSIONID) {
            IOUtils.writeInt(out,_versionID);
        }

        if ((flags & FLAG_EXPIRATION) == FLAG_EXPIRATION) {
            IOUtils.writeLong(out, _expirationTime);
        }

        boolean[] fixed_indices = null;
        HashSet<String> dynamic_indices = null;
        short indexesStoredVersion = (short) 0;


        IServerTypeDesc typeDesc = cacheManager.getEngine().getTypeManager().getServerTypeDesc(_typeName);
        BlobStoreStorageAdapterClassInfo ci = ((IBlobStoreStorageAdapter) cacheManager.getStorageAdapter()).getBlobStoreStorageAdapterClassInfo(_typeName);
        if (ci != null) {
            fixed_indices = ci.getIndexesRelatedFixedProperties();
            dynamic_indices = ci.getIndexesRelatedDynamicProperties();
            indexesStoredVersion = ci.getStoredVersion();
        } else {//can happen when a typw was added and the SA intoroduceType call didnt occur yet
            TypeData typeData = cacheManager.getTypeData(typeDesc);
            fixed_indices = typeData.getIndexesRelatedFixedProperties();
            dynamic_indices = typeData.getIndexesRelatedDynamicProperties();
            //note- version is set to 0 which is first one
        }
        IOUtils.writeShort(out, indexesStoredVersion);

        //indexes-related
        writeObjectArray(out, _fieldsValues, typeDesc, true /*indexes*/, fixed_indices);

        if ((flags & FLAG_DYNAMIC_PROPERTIES) == FLAG_DYNAMIC_PROPERTIES) {
            writeMapStringObject(out, _dynamicProperties, true /*indexes*/, dynamic_indices);
        }

        if(!onlyIndexesPart){
            writeObjectArray(out, _fieldsValues, typeDesc, false, fixed_indices);
            if ((flags & FLAG_DYNAMIC_PROPERTIES) == FLAG_DYNAMIC_PROPERTIES) {
                writeMapStringObject(out, _dynamicProperties, false /*indexes*/, dynamic_indices);
            }
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        throw new UnsupportedOperationException();
    }

    public void readExternal(ObjectInput in, CacheManager cacheManager, boolean fromInitialLoad, boolean onlyIndexedPart, IBlobStoreOffHeapInfo offHeapInfo) throws IOException, ClassNotFoundException {
        if(cacheManager.isPersistentBlobStore()){
            readExternalPersistentBlobStore(in, cacheManager, fromInitialLoad, onlyIndexedPart);
        } else {
            readExternalOffHeap(cacheManager, offHeapInfo.getTypeName(), in,false);
        }
    }

    public void readIndexValuesBytes(CacheManager cacheManager, short serverTypeDescCode, byte[] bytes) throws IOException, ClassNotFoundException {
        ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes));

        readExternalOffHeap(cacheManager, ServerTypeDesc.getByServerTypeDescCode(serverTypeDescCode).getTypeName(), in, true);
        in.close();
    }

    private void readExternalPersistentBlobStore(ObjectInput in, CacheManager cacheManager, boolean fromInitialLoad, boolean onlyIndexedPart) throws IOException, ClassNotFoundException {
        PlatformLogicalVersion version = PlatformLogicalVersion.getLogicalVersion();

        byte flags = in.readByte();

        if ((flags & FLAG_RECOVERABLE) == FLAG_RECOVERABLE) {
            _recoverable = true;
            //construct a logical version of the stored data- will be used when format changes
            version = new PlatformLogicalVersion();
            version.read(in);
        }
        _m_Uid = in.readUTF();

        if (version.greaterOrEquals(PlatformLogicalVersion.v11_0_0)) {
            byte embeddedSyncInfoFlags = in.readByte();
            if ((embeddedSyncInfoFlags & FLAG_CONTAINS_EMBEDDED_SYNC_INFO) == FLAG_CONTAINS_EMBEDDED_SYNC_INFO) {
                _generationId = in.readLong();
                _sequenceId = in.readLong();
                _phantom = (embeddedSyncInfoFlags & FLAG_PHANTOM_OP) == FLAG_PHANTOM_OP;
                _partOfMultipleUidsInfo = (embeddedSyncInfoFlags & FLAG_PART_OF_MULTIPLE_OP) == FLAG_PART_OF_MULTIPLE_OP;
            }
        }
        _blobStoreVersion = in.readShort();
        _typeName = in.readUTF();

        if (fromInitialLoad && onlyIndexedPart &&
                cacheManager.getBlobStoreInternalCache().getBlobStoreInternalCacheFilter() != null) {
            onlyIndexedPart = !cacheManager.getBlobStoreInternalCache()
                    .getBlobStoreInternalCacheFilter().isRelevantType(_typeName);
        }

        if (onlyIndexedPart && fromInitialLoad) {
            final TypeData typeData = cacheManager.getTypeData(cacheManager.getEngine().getTypeManager().getServerTypeDesc(_typeName));
            onlyIndexedPart &= !typeData.isUsingQueryExtensionIndexManager();
        }

        _entryTypeCode = in.readByte();
        _scn = in.readLong();
        _transient = (flags & FLAG_TRANSIENT) == FLAG_TRANSIENT;
        if ((flags & FLAG_ORDER) == FLAG_ORDER)
            _order = in.readInt();

        if ((flags & FLAG_VERSIONID) == FLAG_VERSIONID)
            _versionID = in.readInt();
        else
            _versionID = 1;

        if ((flags & FLAG_EXPIRATION) == FLAG_EXPIRATION)
            _expirationTime = in.readLong();
        else
            _expirationTime = Long.MAX_VALUE;


        short indexesStoredVersion = in.readShort();
        if (onlyIndexedPart) {
            BlobStoreStorageAdapterClassInfo ci = ((IBlobStoreStorageAdapter) cacheManager.getStorageAdapter()).getBlobStoreStorageAdapterClassInfo(_typeName);
            if (ci == null) {
                onlyIndexedPart = false;
                if (CacheManager.getLogger().isInfoEnabled()) {
                    CacheManager.getLogger().info("Blobstore- full entry loaded since no type was introduced to blobstore type name =" + _typeName + " uid=" + _m_Uid);
                }
            }
            if (ci != null && indexesStoredVersion != ci.getStoredVersion()) {
                onlyIndexedPart = false;
                if (CacheManager.getLogger().isInfoEnabled()) {
                    CacheManager.getLogger().info("Blobstore- full entry loaded since index version changed stored=" + indexesStoredVersion + " current=" + ci.getStoredVersion() + " uid=" + _m_Uid);
                }

            }
        }

        IServerTypeDesc typeDesc = cacheManager.getEngine().getTypeManager().getServerTypeDesc(_typeName);

        _fieldsValues = readObjectArray(in, typeDesc, _fieldsValues);
        if ((flags & FLAG_DYNAMIC_PROPERTIES) == FLAG_DYNAMIC_PROPERTIES)
            _dynamicProperties = readMapStringObject(in, _dynamicProperties);

        if (!onlyIndexedPart) {
            if (CacheManager.getLogger().isDebugEnabled()) {
                CacheManager.getLogger().debug("container [" + cacheManager.getEngine().getFullSpaceName() + "] Blobstore- full entry loaded, uid=" + _m_Uid);
            }
            _fieldsValues = readObjectArray(in, typeDesc, _fieldsValues);
            if ((flags & FLAG_DYNAMIC_PROPERTIES) == FLAG_DYNAMIC_PROPERTIES)
                _dynamicProperties = readMapStringObject(in, _dynamicProperties);
        }
        _onlyIndexesPart = onlyIndexedPart;
    }

    private void readExternalOffHeap(CacheManager cacheManager, String typeName, ObjectInput in, boolean onlyIndexesPart) throws IOException, ClassNotFoundException {
        byte flags = in.readByte();

        byte embeddedSyncInfoFlags = in.readByte();
        if ((embeddedSyncInfoFlags & FLAG_CONTAINS_EMBEDDED_SYNC_INFO) == FLAG_CONTAINS_EMBEDDED_SYNC_INFO) {
            _generationId = IOUtils.readLong(in);
            _sequenceId = IOUtils.readLong(in);
        }

        _blobStoreVersion = IOUtils.readShort(in);
        _entryTypeCode = in.readByte();
        _scn = IOUtils.readLong(in);
        _transient = (flags & FLAG_TRANSIENT) == FLAG_TRANSIENT;
        if ((flags & FLAG_ORDER) == FLAG_ORDER)
            _order = IOUtils.readInt(in);

        if ((flags & FLAG_VERSIONID) == FLAG_VERSIONID)
            _versionID = IOUtils.readInt(in);
        else
            _versionID = 1;

        if ((flags & FLAG_EXPIRATION) == FLAG_EXPIRATION)
            _expirationTime = IOUtils.readLong(in);
        else
            _expirationTime = Long.MAX_VALUE;

        short indexesStoredVersion = IOUtils.readShort(in);
        _typeName = typeName;
        IServerTypeDesc typeDesc = cacheManager.getEngine().getTypeManager().getServerTypeDesc(_typeName);

        _fieldsValues = readObjectArray(in, typeDesc, _fieldsValues);
        if ((flags & FLAG_DYNAMIC_PROPERTIES) == FLAG_DYNAMIC_PROPERTIES)
            _dynamicProperties = readMapStringObject(in, _dynamicProperties);

        if(!onlyIndexesPart){
            _fieldsValues = readObjectArray(in, typeDesc, _fieldsValues);
            if ((flags & FLAG_DYNAMIC_PROPERTIES) == FLAG_DYNAMIC_PROPERTIES)
                _dynamicProperties = readMapStringObject(in, _dynamicProperties);
        }
    }


    private static final byte FLAG_RECOVERABLE = ((byte) 1) << 0;
    private static final byte FLAG_ORDER = ((byte) 1) << 1;
    private static final byte FLAG_VERSIONID = ((byte) 1) << 2;
    private static final byte FLAG_EXPIRATION = ((byte) 1) << 3;
    private static final byte FLAG_DYNAMIC_PROPERTIES = ((byte) 1) << 4;

    private static final byte FLAG_TRANSIENT = ((byte) 1) << 5;

    private byte buildFlags() {
        byte flags = 0;

        if (_recoverable)
            flags |= FLAG_RECOVERABLE;
        if (_order != 0)
            flags |= FLAG_ORDER;
        if (_versionID != 1)
            flags |= FLAG_VERSIONID;
        if (_expirationTime != Long.MAX_VALUE)
            flags |= FLAG_EXPIRATION;
        if (_dynamicProperties != null)
            flags |= FLAG_DYNAMIC_PROPERTIES;
        if (_transient)
            flags |= FLAG_TRANSIENT;

        return flags;
    }

    private final static byte FLAG_CONTAINS_EMBEDDED_SYNC_INFO = ((byte) 1) << 0;
    private final static byte FLAG_PHANTOM_OP = ((byte) 1) << 1;
    private final static byte FLAG_PART_OF_MULTIPLE_OP = ((byte) 1) << 2;

    public byte buildEmbeddedSyncInfoFlags() {
        byte flags = 0;
        if (_generationId != 0)
            flags |= FLAG_CONTAINS_EMBEDDED_SYNC_INFO;
        if (_phantom)
            flags |= FLAG_PHANTOM_OP;
        if (_partOfMultipleUidsInfo)
            flags |= FLAG_PART_OF_MULTIPLE_OP;
        return flags;
    }


    public interface Serializer {
        void write(Object object, ObjectOutput out) throws IOException;

        Object read(ObjectInput in) throws IOException, ClassNotFoundException;
    }

    private static Map<Class<?>, Serializer> initSerializers() {
        Map<Class<?>, Serializer> types = new HashMap<>();
        types.put(int.class, new Serializer() {
            @Override
            public void write(Object object, ObjectOutput out) throws IOException {
                out.writeInt((Integer) object);
            }

            @Override
            public Object read(ObjectInput in) throws IOException {
                return in.readInt();
            }
        });
        types.put(long.class, new Serializer() {
            @Override
            public void write(Object object, ObjectOutput out) throws IOException {
                out.writeLong((Long) object);
            }

            @Override
            public Object read(ObjectInput in) throws IOException {
                return in.readLong();
            }
        });
        types.put(float.class, new Serializer() {
            @Override
            public void write(Object object, ObjectOutput out) throws IOException {
                out.writeFloat((Float) object);
            }

            @Override
            public Object read(ObjectInput in) throws IOException {
                return in.readFloat();
            }
        });
        types.put(boolean.class, new Serializer() {
            @Override
            public void write(Object object, ObjectOutput out) throws IOException {
                out.writeBoolean((Boolean) object);
            }

            @Override
            public Object read(ObjectInput in) throws IOException {
                return in.readBoolean();
            }
        });
        types.put(char.class, new Serializer() {
            @Override
            public void write(Object object, ObjectOutput out) throws IOException {
                out.writeChar((Character) object);
            }

            @Override
            public Object read(ObjectInput in) throws IOException {
                return in.readChar();
            }
        });
        types.put(short.class, new Serializer() {
            @Override
            public void write(Object object, ObjectOutput out) throws IOException {
                out.writeShort((Short) object);
            }

            @Override
            public Object read(ObjectInput in) throws IOException {
                return in.readShort();
            }
        });
        types.put(double.class, new Serializer() {
            @Override
            public void write(Object object, ObjectOutput out) throws IOException {
                out.writeDouble((Double) object);
            }

            @Override
            public Object read(ObjectInput in) throws IOException {
                return in.readDouble();
            }
        });
        types.put(Integer.class, new Serializer() {
            @Override
            public void write(Object object, ObjectOutput out) throws IOException {
                out.writeInt((Integer) object);
            }

            @Override
            public Object read(ObjectInput in) throws IOException {
                return in.readInt();
            }
        });
        types.put(Long.class, new Serializer() {
            @Override
            public void write(Object object, ObjectOutput out) throws IOException {
                out.writeLong((Long) object);
            }

            @Override
            public Object read(ObjectInput in) throws IOException {
                return in.readLong();
            }
        });
        types.put(Float.class, new Serializer() {
            @Override
            public void write(Object object, ObjectOutput out) throws IOException {
                out.writeFloat((Float) object);
            }

            @Override
            public Object read(ObjectInput in) throws IOException {
                return in.readFloat();
            }
        });
        types.put(Boolean.class, new Serializer() {
            @Override
            public void write(Object object, ObjectOutput out) throws IOException {
                out.writeBoolean((Boolean) object);
            }

            @Override
            public Object read(ObjectInput in) throws IOException {
                return in.readBoolean();
            }
        });
        types.put(Character.class, new Serializer() {
            @Override
            public void write(Object object, ObjectOutput out) throws IOException {
                out.writeChar((Character) object);
            }

            @Override
            public Object read(ObjectInput in) throws IOException {
                return in.readChar();
            }
        });
        types.put(Short.class, new Serializer() {
            @Override
            public void write(Object object, ObjectOutput out) throws IOException {
                out.writeShort((Short) object);
            }

            @Override
            public Object read(ObjectInput in) throws IOException {
                return in.readShort();
            }
        });
        types.put(Double.class, new Serializer() {
            @Override
            public void write(Object object, ObjectOutput out) throws IOException {
                out.writeDouble((Double) object);
            }

            @Override
            public Object read(ObjectInput in) throws IOException {
                return in.readDouble();
            }
        });
        types.put(String.class, new Serializer() {
            @Override
            public void write(Object object, ObjectOutput out) throws IOException {
                String input = (String) object;
                if (input.length() <= 20000) //UTF limitation
                {
                    out.writeBoolean(true);
                    out.writeUTF(input);
                } else {
                    out.writeBoolean(false);
                    out.writeObject(input);
                }
            }

            @Override
            public Object read(ObjectInput in) throws IOException, ClassNotFoundException {
                if (in.readBoolean())
                    return in.readUTF();
                else
                    return in.readObject();
            }
        });
        types.put(MarshObject.class, new Serializer() {
            @Override
            public void write(Object object, ObjectOutput out) throws IOException {
                byte[] data = ((BinaryWrapper) object).getBytes();
                out.writeInt(data.length);
                if (data.length > 0) {
                    out.write(data);
//                	out.flush();
                }
            }

            @Override
            public Object read(ObjectInput in) throws IOException {
                byte[] data = new byte[in.readInt()];
                if (data.length > 0) {
                    in.readFully(data);
                }
                return new MarshObject(data);
            }
        });
        // Use Marsh object serializer as default for BinaryWrapper interface
        types.put(BinaryWrapper.class, types.get(MarshObject.class));
        types.put(CompressedMarshObject.class, new Serializer() {
            @Override
            public void write(Object object, ObjectOutput out) throws IOException {
                byte[] data = ((BinaryWrapper) object).getBytes();
                out.writeInt(data.length);
                if (data.length > 0) {
                    out.write(data);
//                	out.flush();
                }
            }

            @Override
            public Object read(ObjectInput in) throws IOException {
                byte[] data = new byte[in.readInt()];
                if (data.length > 0) {
                    in.readFully(data);
                }
                return new CompressedMarshObject(data);
            }
        });
        return types;
    }
}
