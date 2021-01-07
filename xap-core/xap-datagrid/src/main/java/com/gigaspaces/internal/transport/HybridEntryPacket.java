package com.gigaspaces.internal.transport;

import com.gigaspaces.internal.io.IOArrayException;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.metadata.EntryType;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.metadata.PropertyInfo;
import com.gigaspaces.internal.query.ICustomQuery;
import com.gigaspaces.internal.server.storage.HybridPropertiesHolder;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.j_spaces.core.EntrySerializationException;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;

@com.gigaspaces.api.InternalApi
public class HybridEntryPacket extends AbstractEntryPacket {
    private static final long serialVersionUID = 1L;
    private static final short FLAG_CLASSNAME = 1 << 0;
    private static final short FLAG_UID = 1 << 1;
    private static final short FLAG_VERSION = 1 << 2;
    private static final short FLAG_TIME_TO_LIVE = 1 << 3;
    private static final short FLAG_MULTIPLE_UIDS = 1 << 4;
    private static final short FLAG_FIELDS_VALUES = 1 << 5;
    private static final short FLAG_FIFO = 1 << 6;
    private static final short FLAG_TRANSIENT = 1 << 7;
    private static final short FLAG_NO_WRITE_LEASE = 1 << 8;
    private static final short FLAG_RETURN_ONLY_UIDS = 1 << 9;
    private static final short FLAG_CUSTOM_QUERY = 1 << 10;
    private static final short FLAG_DYNAMIC_PROPERTIES = 1 << 11;
    protected String _typeName;
    protected boolean _returnOnlyUIDs;
    private HybridPropertiesHolder propertiesHolder;
    private Map<String, Object> _dynamicProperties;
    private String _uid;
    private int _version;
    private long _timeToLive;
    private boolean _transient;
    private ICustomQuery _customQuery;
    //for read/take multiple - on output: UIDS of entries to read/take,
    //on input- the result UIDs of the operation if ReturnOnlyUids option is used
    // this field is valid only if the classname is null
    private String[] _multipleUIDs;
    // Deprecated:
    private boolean _noWriteLease;
    private boolean _fifo;

    /**
     * Default constructor required by {@link java.io.Externalizable}.
     */
    public HybridEntryPacket() {
    }

    public HybridEntryPacket(ITypeDesc typeDesc, EntryType entryType, Object[] fixedProperties, Map<String, Object> dynamicProperties,
                             String uid, int version, long timeToLive, boolean isTransient) {
        this(typeDesc, entryType, dynamicProperties, uid, version, timeToLive, isTransient, new HybridPropertiesHolder(typeDesc, fixedProperties));
    }

    public HybridEntryPacket(ITypeDesc typeDesc, EntryType entryType, Map<String, Object> dynamicProperties,
                             String uid, int version, long timeToLive, boolean isTransient, HybridPropertiesHolder propertiesHolder) {
        super(typeDesc, entryType);
        _typeName = typeDesc.getTypeName();
        _dynamicProperties = dynamicProperties;
        _uid = uid;
        _version = version;
        _timeToLive = timeToLive;
        _transient = isTransient;
        _noWriteLease = false;
        _fifo = false;
        this.propertiesHolder = propertiesHolder;

    }

    protected HybridEntryPacket(ITypeDesc typeDesc, Object[] values) {
        this(typeDesc, new HybridPropertiesHolder(typeDesc, values));
    }

    protected HybridEntryPacket(ITypeDesc typeDesc, HybridPropertiesHolder propertiesHolder) {
        super(typeDesc, typeDesc.getObjectType());
        this._typeName = typeDesc.getTypeName();
        this.propertiesHolder = propertiesHolder;
    }

    /**
     * Returns a shallow copy of this <tt>EntryPacket</tt> instance. And also only clone the values
     * array.
     *
     * @return a shallow clone of this <tt>EntryPacket</tt> instance.
     */
    @Override
    public IEntryPacket clone() {
        IEntryPacket packet = super.clone();
        ((HybridEntryPacket) packet).setPropertiesHolder(propertiesHolder.clone());
        return packet;
    }

    public TransportPacketType getPacketType() {
        return TransportPacketType.ENTRY_PACKET;
    }

    public String getTypeName() {
        return _typeName;
    }

    public String getUID() {
        return _uid;
    }

    public void setUID(String uid) {
        _uid = uid;
    }

    public String[] getMultipleUIDs() {
        return _multipleUIDs;
    }

    public void setMultipleUIDs(String[] multipleUIDs) {
        this._multipleUIDs = multipleUIDs;
    }

    public boolean isReturnOnlyUids() {
        return _returnOnlyUIDs;
    }

    public void setReturnOnlyUIDs(boolean returnOnlyUIDs) {
        this._returnOnlyUIDs = returnOnlyUIDs;
    }

    public Map<String, Object> getDynamicProperties() {
        return _dynamicProperties;
    }

    public void setDynamicProperties(Map<String, Object> dynamicProperties) {
        _dynamicProperties = dynamicProperties;
    }

    public Object[] getFieldValues() {
        return propertiesHolder.getFixedProperties(getTypeDescriptor());
    }

    public void setFieldsValues(Object[] values) {
        if (getTypeDescriptor() == null && getTypeName() == null) {
            propertiesHolder.setFixedProperties(values);
        } else {
            propertiesHolder.setFixedProperties(getTypeDescriptor(), values);
        }
    }

    public Object getFieldValue(int index) {
        try {
            return this.propertiesHolder.getFixedProperty(this.getTypeDescriptor(), index);
        } catch (Exception e) {
            throw new IllegalStateException("The field values array was not properly set", e);
        }
    }

    public void setFieldValue(int index, Object value) {
        try {
            if (getTypeDescriptor() == null && getTypeName() == null) {
                propertiesHolder.setFixedProperty(index, value);
            } else {
                this.propertiesHolder.setFixedProperty(_typeDesc, index, value);
            }
        } catch (Exception e) {
            throw new IllegalStateException("The field values array was not properly set", e);
        }
    }

    public boolean isTransient() {
        return _transient;
    }

    public boolean isFifo() {
        return _fifo;
    }

    public int getVersion() {
        return _version;
    }

    public void setVersion(int version) {
        this._version = version;
    }

    public long getTTL() {
        return _timeToLive;
    }

    public void setTTL(long ttl) {
        _timeToLive = ttl;
    }

    public boolean isNoWriteLease() {
        return _noWriteLease;
    }

    public ICustomQuery getCustomQuery() {
        return _customQuery;
    }

    public void setCustomQuery(ICustomQuery customQuery) {
        _customQuery = customQuery;
    }

    private short buildFlags(PlatformLogicalVersion version) {
        short flags = 0;

        if (_typeName != null)
            flags |= FLAG_CLASSNAME;
        if (_uid != null)
            flags |= FLAG_UID;
        if (_version != 0)
            flags |= FLAG_VERSION;
        if (_timeToLive != 0)
            flags |= FLAG_TIME_TO_LIVE;
        if (_multipleUIDs != null)
            flags |= FLAG_MULTIPLE_UIDS;
        if (propertiesHolder != null)
            flags |= FLAG_FIELDS_VALUES;
        if (_fifo)
            flags |= FLAG_FIFO;
        if (_transient)
            flags |= FLAG_TRANSIENT;
        if (_noWriteLease)
            flags |= FLAG_NO_WRITE_LEASE;
        if (_returnOnlyUIDs)
            flags |= FLAG_RETURN_ONLY_UIDS;
        if (_customQuery != null)
            flags |= FLAG_CUSTOM_QUERY;
        if (_dynamicProperties != null)
            flags |= FLAG_DYNAMIC_PROPERTIES;

        return flags;
    }

    private boolean calcPropertiesHolderFlag(PlatformLogicalVersion version) {
        if (propertiesHolder == null)
            return false;

        if (version.lessThan(PlatformLogicalVersion.v15_8_0) && propertiesHolder.allNulls())
            return false;

        return true;
    }

    @Override
    public void writeToSwap(ObjectOutput out) throws IOException {
        super.writeToSwap(out);

        serializePacket(out, PlatformLogicalVersion.getLogicalVersion());
    }

    @Override
    public void readFromSwap(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readFromSwap(in);

        deserializePacket(in, PlatformLogicalVersion.getLogicalVersion());
    }

    @Override
    protected void writeExternal(ObjectOutput out, PlatformLogicalVersion version)
            throws IOException {
        super.writeExternal(out, version);

        serializePacket(out, version);
    }

    private final void serializePacket(ObjectOutput out,
                                       PlatformLogicalVersion version) {
        try {
            out.writeShort(buildFlags(version));

            if (_typeName != null)
                IOUtils.writeRepetitiveString(out, _typeName);
            if (_uid != null)
                IOUtils.writeString(out, _uid);
            if (_version != 0)
                out.writeInt(_version);
            if (_timeToLive != 0)
                out.writeLong(_timeToLive);
            if (_multipleUIDs != null)
                IOUtils.writeStringArray(out, _multipleUIDs);
            if (propertiesHolder != null)
                IOUtils.writeObject(out, propertiesHolder);
            if (_dynamicProperties != null)
                IOUtils.writeObject(out, _dynamicProperties);
            if (_customQuery != null)
                IOUtils.writeObject(out, _customQuery);
        } catch (EntrySerializationException e) {
            throw e;
        } catch (Exception e) {
            String className = _typeName == null ? "." : _typeName;
            throw new EntrySerializationException("Failed to serialize Entry " + className, e);
        }
    }

    @Override
    protected void readExternal(ObjectInput in, PlatformLogicalVersion version)
            throws IOException, ClassNotFoundException {
        super.readExternal(in, version);

        deserializePacket(in, version);
    }

    private final void deserializePacket(ObjectInput in, PlatformLogicalVersion version) {
        try {
            final short flags = in.readShort();

            this._fifo = (flags & FLAG_FIFO) != 0;
            this._transient = (flags & FLAG_TRANSIENT) != 0;
            this._noWriteLease = (flags & FLAG_NO_WRITE_LEASE) != 0;
            this._returnOnlyUIDs = (flags & FLAG_RETURN_ONLY_UIDS) != 0;

            if ((flags & FLAG_CLASSNAME) != 0)
                _typeName = IOUtils.readRepetitiveString(in);
            if ((flags & FLAG_UID) != 0)
                _uid = IOUtils.readString(in);
            if ((flags & FLAG_VERSION) != 0)
                _version = in.readInt();
            if ((flags & FLAG_TIME_TO_LIVE) != 0)
                _timeToLive = in.readLong();
            if ((flags & FLAG_MULTIPLE_UIDS) != 0)
                _multipleUIDs = IOUtils.readStringArray(in);
            if ((flags & FLAG_FIELDS_VALUES) != 0)
                propertiesHolder = IOUtils.readObject(in);
            if ((flags & FLAG_DYNAMIC_PROPERTIES) != 0)
                _dynamicProperties = IOUtils.readObject(in);
            if ((flags & FLAG_CUSTOM_QUERY) != 0)
                _customQuery = IOUtils.readObject(in);
        } catch (EntrySerializationException e) {
            throw e;
        } catch (Exception e) {
            String className = _typeName != null ? _typeName : ".";
            throw new EntrySerializationException("Failed to deserialize Entry " + className, e);
        }
    }

    private EntrySerializationException createPropertySerializationException(IOArrayException e, boolean isSerialize) {
        final int index = e.getIndex();
        String message = "Failed to " + (isSerialize ? "serialize" : "deserialize") +
                " property #" + index;

        ITypeDesc typeDesc = getTypeDescriptor();
        if (typeDesc != null) {
            int numOfProperties = typeDesc.getNumOfFixedProperties();
            if (index < 0 || index >= numOfProperties)
                message += " (index out of bound: number of properties=" + numOfProperties + ")";
            else {
                PropertyInfo property = typeDesc.getFixedProperty(index);
                message += " (Name=[" + property.getName() + "], Type=[" + property.getTypeName() + "])";
            }
        }

        message += " in entry of type [" + this.getTypeName() + "].";
        if (isSerialize)
            message += " Value=[" + String.valueOf(getFieldValue(index)) + "]";

        return new EntrySerializationException(message, e.getCause());
    }

    /**
     * true if the entry packet has an array of fixed properties
     */
    @Override
    public boolean hasFixedPropertiesArray() {
        return true;
    }

    @Override
    public boolean allNullFieldValues() {
        return propertiesHolder.allNulls();
    }


    public HybridPropertiesHolder getPropertiesHolder() {
        return propertiesHolder;
    }


    public void setPropertiesHolder(HybridPropertiesHolder holder) {
        this.propertiesHolder = holder;
    }

    @Override
    public boolean isHybrid() {
        return true;
    }
}
