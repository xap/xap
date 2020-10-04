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

package com.gigaspaces.internal.cluster.node.impl.packets.data.operations;

import com.gigaspaces.internal.cluster.node.IReplicationInBatchContext;
import com.gigaspaces.internal.cluster.node.IReplicationInContext;
import com.gigaspaces.internal.cluster.node.ReplicationInContentContext;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInFacade;
import com.gigaspaces.internal.cluster.node.impl.ReplicationSingleOperationType;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationTransactionalPacketEntryData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.ITransactionalBatchExecutionCallback;
import com.gigaspaces.internal.cluster.node.impl.packets.data.ITransactionalExecutionCallback;
import com.gigaspaces.internal.cluster.node.impl.packets.data.ReplicationPacketDataMediator;
import com.gigaspaces.internal.cluster.node.impl.view.EntryPacketServerEntryAdapter;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.core.ObjectTypes;
import com.j_spaces.core.cluster.ReplicationOperationType;

import net.jini.core.transaction.Transaction;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

@com.gigaspaces.api.InternalApi
public class UpdateReplicationPacketData
        extends SingleReplicationPacketData implements IReplicationTransactionalPacketEntryData {
    private static final long serialVersionUID = 1L;
    private boolean _overrideVersion;
    protected short _flags;
    protected transient IEntryData _currentEntryData;
    protected transient IEntryData _previousEntryData;
    private IEntryPacket _previousEntryPacket;
    private transient boolean _serializeFullContent;
    private transient long _expirationTime;


    public UpdateReplicationPacketData() {
    }

    public UpdateReplicationPacketData(IEntryPacket entry, boolean fromGateway, boolean overrideVersion, IEntryData previousEntryData, short flags, long expirationTime, IEntryData currentEntryData) {
        super(entry, fromGateway);
        _overrideVersion = overrideVersion;
        _previousEntryData = previousEntryData;
        _currentEntryData = currentEntryData;
        _flags = flags;
        _expirationTime = expirationTime;
    }

    public void execute(IReplicationInContext context,
                        IReplicationInFacade inReplicationHandler, ReplicationPacketDataMediator dataMediator) throws Exception {
        try {
            inReplicationHandler.inUpdateEntry(context, getEntryPacket(), getPreviousEntryPacket(), false, _overrideVersion, _flags);
        } finally {
            ReplicationInContentContext contentContext = context.getContentContext();
            if (contentContext != null && contentContext.getSecondaryEntryData() != null) {
                _currentEntryData = contentContext.getMainEntryData();
                _previousEntryData = contentContext.getSecondaryEntryData();
                contentContext.clear();
            }
        }

    }

    public void executeTransactional(IReplicationInContext context,
                                     ITransactionalExecutionCallback transactionExecutionCallback,
                                     Transaction transaction, boolean twoPhaseCommit) throws Exception {
        try {
            transactionExecutionCallback.updateEntry(context, transaction, twoPhaseCommit, getEntryPacket(), getPreviousEntryPacket(), false, _overrideVersion, _flags);
        } finally {
            ReplicationInContentContext contentContext = context.getContentContext();
            if (contentContext != null && contentContext.getSecondaryEntryData() != null) {
                _currentEntryData = contentContext.getMainEntryData();
                _previousEntryData = contentContext.getSecondaryEntryData();
                contentContext.clear();
            }
        }
    }

    public void batchExecuteTransactional(IReplicationInBatchContext context,
                                          ITransactionalBatchExecutionCallback executionCallback)
            throws Exception {
        executionCallback.updateEntry(context, getEntryPacket(), getPreviousEntryPacket(), false, _flags);
    }

    public boolean beforeDelayedReplication() {
        return updateEntryPacketTimeToLiveIfNeeded(_expirationTime);
    }

    @Override
    protected int getFilterObjectType(IServerTypeDesc serverTypeDesc) {
        return ObjectTypes.ENTRY;
    }

    @Override
    protected ReplicationOperationType getFilterReplicationOpType() {
        return ReplicationOperationType.UPDATE;
    }

    public ReplicationSingleOperationType getOperationType() {
        return ReplicationSingleOperationType.UPDATE;
    }

    @Override
    public String toString() {
        return "UPDATE: " + getEntryPacket();
    }

    @Override
    public UpdateReplicationPacketData clone() {
        UpdateReplicationPacketData clone = (UpdateReplicationPacketData) super.clone();
        clone._overrideVersion = this._overrideVersion;
        clone._flags = this._flags;
        return clone;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readExternal(in);

        if (LRMIInvocationContext.getEndpointLogicalVersion().lessThan(PlatformLogicalVersion.v9_1_0))
            realExternalPre91(in);
        else
            readExternalPost91(in);

        _expirationTime = !getEntryPacket().isExternalizableEntryPacket() && getEntryPacket().getTTL() != Long.MAX_VALUE
                ? getEntryPacket().getTTL() + SystemTime.timeMillis() : Long.MAX_VALUE;
    }

    private void realExternalPre91(ObjectInput in) throws IOException, ClassNotFoundException {
        _overrideVersion = in.readBoolean();
        if (in.readBoolean())
            _previousEntryData = IOUtils.readObject(in);
        _flags = in.readShort();
    }

    private void readExternalPost91(ObjectInput in) throws IOException, ClassNotFoundException {
        _overrideVersion = in.readBoolean();
        _flags = in.readShort();
        if (in.readBoolean() /* serializeFullContent */)
            deserializePreviousEntryData(in);
    }

    protected void deserializePreviousEntryData(ObjectInput in)
            throws IOException, ClassNotFoundException {
        final boolean hasPreviousEntryDataBeenSerialzed = in.readBoolean();

        if (!hasPreviousEntryDataBeenSerialzed)
            return;

        Object[] previousFixedValues = deserializePreviousFixedValues(in);

        DynamicPropertiesDeserializationData data = deserializePreviousDynamicProperties(in);
        Map<String, Object> previousDynamicProperties = data._serializedPreviousDynamicProperties;
        boolean previousDynamicPropertiesExisted = data._previousDynamicPropertiesExisted;
        _previousEntryData = createPreviousEntryData(previousFixedValues, previousDynamicProperties, previousDynamicPropertiesExisted);
    }

    private Object[] deserializePreviousFixedValues(ObjectInput in) throws IOException, ClassNotFoundException {
        Object[] serializedPreviousFixedProperties = null;
        if (in.readBoolean() /* fixed properties were serialized */) {
            int arrayLength = in.readInt();
            serializedPreviousFixedProperties = new Object[arrayLength];
            for (int i = 0; i < serializedPreviousFixedProperties.length; i++) {
                final boolean useSerializedValue = in.readBoolean();

                if (useSerializedValue)
                    serializedPreviousFixedProperties[i] = IOUtils.readObject(in);
                else
                    serializedPreviousFixedProperties[i] = getEntryPacket().getFieldValues()[i];
            }

        }
        return serializedPreviousFixedProperties;
    }

    private DynamicPropertiesDeserializationData deserializePreviousDynamicProperties(ObjectInput in) throws IOException, ClassNotFoundException {
        Map<String, Object> serializedPreviousDynamicProperties = null;
        boolean previousDynamicPropertiesExisted = false;
        if (in.readBoolean() /* dynamic properties were serialized */) {
            previousDynamicPropertiesExisted = true;
            int size = in.readInt();
            serializedPreviousDynamicProperties = new HashMap<String, Object>();

            for (int i = 0; i < size; i++) {
                String key = IOUtils.readString(in);
                Object value = IOUtils.readObject(in);
                if (value != null) {
                    serializedPreviousDynamicProperties.put(key, value);
                } else {
                    final boolean itemAddedLater = in.readBoolean();
                    if (itemAddedLater)
                        serializedPreviousDynamicProperties.put(key, ItemAddedLaterSerializationMarker.INSTANCE);
                    else
                        serializedPreviousDynamicProperties.put(key, value);
                }
            }

            if (getEntryPacket().getDynamicProperties() != null) {
                for (Entry<String, Object> dynamicProperty : getEntryPacket().getDynamicProperties().entrySet()) {
                    if (!serializedPreviousDynamicProperties.containsKey(dynamicProperty.getKey()))
                        serializedPreviousDynamicProperties.put(dynamicProperty.getKey(), dynamicProperty.getValue());
                }

                for (Iterator<Entry<String, Object>> previousIterator = serializedPreviousDynamicProperties.entrySet().iterator(); previousIterator.hasNext(); ) {
                    Entry<String, Object> previousDynamicProperty = previousIterator.next();
                    if (previousDynamicProperty.getValue() == ItemAddedLaterSerializationMarker.INSTANCE)
                        previousIterator.remove();
                }
            }
        } else {
            // dynamic properties were not serialized, we now test whether we need to use the new ones or were they null
            previousDynamicPropertiesExisted = in.readBoolean();
        }

        return new DynamicPropertiesDeserializationData(previousDynamicPropertiesExisted, serializedPreviousDynamicProperties);
    }

    private static class DynamicPropertiesDeserializationData {
        public DynamicPropertiesDeserializationData(
                boolean previousDynamicPropertiesExisted,
                Map<String, Object> serializedPreviousDynamicProperties) {
            _previousDynamicPropertiesExisted = previousDynamicPropertiesExisted;
            _serializedPreviousDynamicProperties = serializedPreviousDynamicProperties;
        }

        boolean _previousDynamicPropertiesExisted;
        Map<String, Object> _serializedPreviousDynamicProperties;
    }

    private EntryPacketServerEntryAdapter createPreviousEntryData(
            Object[] previousFixedValues,
            Map<String, Object> previousDynamicProperties,
            boolean previousDynamicPropertiesExisted) {
        _previousEntryPacket = getEntryPacket().clone();
        if (previousFixedValues != null)
            _previousEntryPacket.setFieldsValues(previousFixedValues);


        if (previousDynamicProperties != null)
            _previousEntryPacket.setDynamicProperties(previousDynamicProperties);
        else if (!previousDynamicPropertiesExisted)
            _previousEntryPacket.setDynamicProperties(null);


        if (_previousEntryPacket.hasPreviousVersion())
            _previousEntryPacket.setVersion(_previousEntryPacket.getPreviousVersion());
        else
            _previousEntryPacket.setVersion(_previousEntryPacket.getVersion() - 1);

        return new EntryPacketServerEntryAdapter(_previousEntryPacket);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        if(_serializeFullContent){
            getEntryPacket().getFieldValues();
            getEntryPacket().setBinaryFields(null);
        }
        super.writeExternal(out);

        if (LRMIInvocationContext.getEndpointLogicalVersion().lessThan(PlatformLogicalVersion.v9_1_0))
            writeExternalPre91(out);
        else
            writeExternalPost91(out);
    }

    private void writeExternalPre91(ObjectOutput out) throws IOException {
        out.writeBoolean(_overrideVersion);
        out.writeBoolean(_serializeFullContent);
        if (_serializeFullContent)
            writePreviousEntryData(out);
        out.writeShort(_flags);
    }

    protected void writePreviousEntryData(ObjectOutput out) throws IOException {
        serializeEntryData(_previousEntryData, out);
    }

    protected void readPreviousEntryData(ObjectInput in) throws IOException, ClassNotFoundException {
        _previousEntryData = deserializeEntryData(in);
    }

    private void writeExternalPost91(ObjectOutput out) throws IOException {
        out.writeBoolean(_overrideVersion);
        out.writeShort(_flags);
        out.writeBoolean(_serializeFullContent);
        if (_serializeFullContent)
            serializePreviousEntryData(out);
    }

    protected void serializePreviousEntryData(ObjectOutput out) throws IOException {
        // flag to indicate whether the previousEntryData was written
        if (_previousEntryData != null) {
            out.writeBoolean(true);
            serializePreviousFixedValues(out);
            serializePreviousDynamicProperties(out);
        } else {
            out.writeBoolean(false);
        }

    }

    private void serializePreviousFixedValues(ObjectOutput out) throws IOException {
        Object[] serializedPreviousFixedProperties = null;

        Object[] fixedProperties = getEntryPacket().getFieldValues();
        Object[] previousFixedProperties = _previousEntryData.getFixedPropertiesValues();

        for (int i = 0; i < fixedProperties.length; i++) {
            if (fixedProperties[i] != null) {
                if (previousFixedProperties[i] == null) {
                    serializedPreviousFixedProperties = instantiateArrayIfNeeded(serializedPreviousFixedProperties, fixedProperties.length);
                    serializedPreviousFixedProperties[i] = ItemAddedLaterSerializationMarker.INSTANCE;
                } else if (!previousFixedProperties[i].equals(fixedProperties[i])) {
                    serializedPreviousFixedProperties = instantiateArrayIfNeeded(serializedPreviousFixedProperties, fixedProperties.length);
                    serializedPreviousFixedProperties[i] = previousFixedProperties[i];
                }
            } else if (previousFixedProperties[i] != null) {
                serializedPreviousFixedProperties = instantiateArrayIfNeeded(serializedPreviousFixedProperties, fixedProperties.length);
                serializedPreviousFixedProperties[i] = previousFixedProperties[i];
            }
        }

        // flag to indicate whether previous fixed values were serialized
        final boolean serialize = serializedPreviousFixedProperties != null;
        out.writeBoolean(serialize);
        if (serialize) {
            out.writeInt(serializedPreviousFixedProperties.length);
            for (int i = 0; i < serializedPreviousFixedProperties.length; i++) {
                if (serializedPreviousFixedProperties[i] == ItemAddedLaterSerializationMarker.INSTANCE) {
                    out.writeBoolean(true);
                    IOUtils.writeObject(out, null);
                } else if (serializedPreviousFixedProperties[i] != null) {
                    out.writeBoolean(true);
                    IOUtils.writeObject(out, serializedPreviousFixedProperties[i]);
                } else {
                    out.writeBoolean(false);
                }
            }
        }
    }

    private static Object[] instantiateArrayIfNeeded(Object[] array, int length) {
        if (array == null)
            return new Object[length];
        else
            return array;
    }

    private void serializePreviousDynamicProperties(ObjectOutput out) throws IOException {
        Map<String, Object> serializedPreviousDynamicProperties = null;

        if (getEntryPacket().getDynamicProperties() != null && _previousEntryData.getDynamicProperties() != null) {
            for (Entry<String, Object> dynamicProperty : getEntryPacket().getDynamicProperties().entrySet()) {
                boolean previousValueExists = _previousEntryData.getDynamicProperties().containsKey(dynamicProperty.getKey());
                Object previousValue = _previousEntryData.getDynamicProperties().get(dynamicProperty.getKey());

                if (!previousValueExists) {
                    serializedPreviousDynamicProperties = instantiateMapIfNeeded(serializedPreviousDynamicProperties);
                    serializedPreviousDynamicProperties.put(dynamicProperty.getKey(), ItemAddedLaterSerializationMarker.INSTANCE);
                } else if (previousValue == null || !previousValue.equals(dynamicProperty.getValue())) {
                    serializedPreviousDynamicProperties = instantiateMapIfNeeded(serializedPreviousDynamicProperties);
                    serializedPreviousDynamicProperties.put(dynamicProperty.getKey(), previousValue);
                }
            }

            for (Entry<String, Object> previousDynamicProperty : _previousEntryData.getDynamicProperties().entrySet()) {
                if (!getEntryPacket().getDynamicProperties().containsKey(previousDynamicProperty.getKey())) {
                    serializedPreviousDynamicProperties = instantiateMapIfNeeded(serializedPreviousDynamicProperties);
                    serializedPreviousDynamicProperties.put(previousDynamicProperty.getKey(), previousDynamicProperty.getValue());
                }
            }
        } else if (_previousEntryData.getDynamicProperties() != null) {
            serializedPreviousDynamicProperties = _previousEntryData.getDynamicProperties();
        }

        // flag to indicate whether previous dynamic values were serialized
        final boolean serialize = serializedPreviousDynamicProperties != null;
        out.writeBoolean(serialize);
        if (serialize) {
            out.writeInt(serializedPreviousDynamicProperties.size());
            for (Entry<String, Object> previousDynamicProperty : serializedPreviousDynamicProperties.entrySet()) {
                if (previousDynamicProperty.getValue() == ItemAddedLaterSerializationMarker.INSTANCE) {
                    IOUtils.writeString(out, previousDynamicProperty.getKey());
                    IOUtils.writeObject(out, null);
                    out.writeBoolean(true);
                } else if (previousDynamicProperty.getValue() == null) {
                    IOUtils.writeString(out, previousDynamicProperty.getKey());
                    IOUtils.writeObject(out, null);
                    out.writeBoolean(false);
                } else {
                    IOUtils.writeString(out, previousDynamicProperty.getKey());
                    IOUtils.writeObject(out, previousDynamicProperty.getValue());
                }
            }
        } else {
            // here we mark whether we need to read the current dynamic properties into the previous 
            // or were they null
            out.writeBoolean(_previousEntryData.getDynamicProperties() != null);
        }
    }

    private static Map<String, Object> instantiateMapIfNeeded(Map<String, Object> map) {
        if (map == null)
            return new HashMap<String, Object>();
        else
            return map;
    }

    @Override
    public void readFromSwap(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readFromSwap(in);
        _overrideVersion = in.readBoolean();
        deserializePreviousEntryData(in);
        _flags = in.readShort();
        _expirationTime = in.readLong();
        restoreCurrentEntryData();
    }

    protected void restoreCurrentEntryData() {
        _currentEntryData = new EntryPacketServerEntryAdapter(getEntryPacket());
    }

    @Override
    public void writeToSwap(ObjectOutput out) throws IOException {
        if(_previousEntryData != null){
            getEntryPacket().getFieldValues();
            getEntryPacket().setBinaryFields(null);
        }
        super.writeToSwap(out);
        out.writeBoolean(_overrideVersion);
        serializePreviousEntryData(out);
        out.writeShort(_flags);
        out.writeLong(_expirationTime);
    }

    @Override
    public IEntryData getMainEntryData() {
        return _currentEntryData;
    }

    @Override
    public IEntryData getSecondaryEntryData() {
        return _previousEntryData;
    }

    public IEntryPacket getPreviousEntryPacket() {
        return _previousEntryPacket;
    }

    @Override
    public boolean filterIfNotPresentInReplicaState() {
        return true;
    }

    public void serializeFullContent() {
        _serializeFullContent = true;
    }

    public boolean isSerializeFullContent() {
        return _serializeFullContent;
    }

    public boolean isOverrideVersion() {
        return _overrideVersion;
    }

    public short getFlags() {
        return _flags;
    }

    public long getExpirationTime() {
        return _expirationTime;
    }

}
