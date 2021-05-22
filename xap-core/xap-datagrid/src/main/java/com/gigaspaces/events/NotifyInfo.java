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

package com.gigaspaces.events;

import com.gigaspaces.internal.client.cache.CustomInfo;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.server.space.redolog.storage.bytebuffer.ISwapExternalizable;
import com.gigaspaces.internal.utils.Textualizable;
import com.gigaspaces.internal.utils.Textualizer;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.gigaspaces.serialization.SmartExternalizable;
import com.j_spaces.core.client.INotifyDelegatorFilter;
import com.j_spaces.core.client.Modifiers;
import com.j_spaces.core.client.NotifyModifiers;
import com.j_spaces.core.cluster.ClusterPolicy;

import net.jini.core.event.RemoteEventListener;
import net.jini.id.UuidFactory;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.rmi.MarshalledObject;

/**
 * Holds a set of space.notify related parameters.
 *
 * @author asy ronen
 * @version 1.0
 * @since 6.0
 */
@com.gigaspaces.api.InternalApi
public class NotifyInfo implements SmartExternalizable, ISwapExternalizable, Textualizable {
    private static final long serialVersionUID = 9213790201720650891L;

    private Boolean _replicateTemplate;
    private Boolean _triggerNotifyTemplate;
    private boolean _guaranteedNotifications;
    private boolean _broadcast;
    private boolean _returnOnlyUids;

    private Integer _batchSize;
    private Integer _batchPendingThreshold;
    private Long _batchTime;

    private int _notifyType;
    private int _modifiers;
    private String _templateUID;
    private RemoteEventListener _listener;
    private INotifyDelegatorFilter _filter;
    private MarshalledObject<?> _handback;
    private CustomInfo _customInfo;
    private String _tag;

    /**
     * Default constructor required by Externalizable.
     */
    public NotifyInfo() {
    }

    public NotifyInfo(RemoteEventListener listener, int notifyType) {
        this(listener, notifyType, false, null);
    }

    public NotifyInfo(RemoteEventListener listener, int notifyType, boolean fifo) {
        this(listener, notifyType, fifo, null);
    }

    public NotifyInfo(RemoteEventListener listener, int notifyType, boolean fifo, MarshalledObject<?> handback) {
        this._listener = listener;
        this._notifyType = notifyType;
        setFifo(fifo);
        this._handback = handback;
    }

    public NotifyInfo(RemoteEventListener listener, NotifyActionType notifyType, EventSessionConfig config,
                      MarshalledObject<?> handback, INotifyDelegatorFilter filter, String tag) {
        this(listener, notifyType.getModifier(), config.isFifo(), handback);
        this._filter = filter;
        this._replicateTemplate = config.isReplicateNotifyTemplate();
        this._triggerNotifyTemplate = config.isTriggerNotifyTemplate();
        this._guaranteedNotifications = config.isGuaranteedNotifications();
        //setReturnPrevValue(config.isNotifyPreviousValueOnUpdate());
        if (config.isBatching()) {
            this._batchSize = config.getBatchSize();
            this._batchTime = config.getBatchTime();
            this._batchPendingThreshold = config.getBatchPendingThreshold();
        }
        this._tag = tag;
    }

    public NotifyInfo(RemoteEventListener listener, NotifyInfo notifyInfo) {
        this._replicateTemplate = notifyInfo._replicateTemplate;
        this._triggerNotifyTemplate = notifyInfo._triggerNotifyTemplate;
        this._guaranteedNotifications = notifyInfo._guaranteedNotifications;
        this._broadcast = notifyInfo._broadcast;
        this._returnOnlyUids = notifyInfo._returnOnlyUids;
        this._modifiers = notifyInfo._modifiers;
        this._batchSize = notifyInfo._batchSize;
        this._batchPendingThreshold = notifyInfo._batchPendingThreshold;
        this._batchTime = notifyInfo._batchTime;
        this._notifyType = notifyInfo._notifyType;
        this._templateUID = notifyInfo._templateUID;
        this._listener = listener;
        this._filter = notifyInfo._filter;
        this._handback = notifyInfo._handback;
        this._tag = notifyInfo._tag;
    }

    @Override
    public String toString() {
        return Textualizer.toString(this);
    }

    @Override
    public void toText(Textualizer textualizer) {
        textualizer.append("notifyType", _notifyType);
        textualizer.append("templateUid", _templateUID);
        textualizer.append("broadcast", _broadcast);
        textualizer.append("fifo", isFifo());
        textualizer.append("returnOnlyUids", _returnOnlyUids);
        textualizer.append("batchSize", _batchSize);
        textualizer.append("batchPendingThreshold", _batchPendingThreshold);
        textualizer.append("batchTime", _batchTime);
        textualizer.append("replicateTemplate", _replicateTemplate);
        textualizer.append("triggerNotifyTemplate", _triggerNotifyTemplate);
        textualizer.append("guaranteed", _guaranteedNotifications);
        textualizer.append("handback", _handback);
        textualizer.append("returnPrevValue", isReturnPrevValue());
        textualizer.append("tag", getTag());
    }

    public Boolean getReplicateTemplate() {
        return _replicateTemplate;
    }

    public void setReplicateTemplate(Boolean replicateTemplate) {
        this._replicateTemplate = replicateTemplate;
    }

    public Boolean getNotifyTemplate() {
        return _triggerNotifyTemplate;
    }

    public void setNotifyTemplate(Boolean notifyTemplate) {
        this._triggerNotifyTemplate = notifyTemplate;
    }

    public boolean isGuaranteedNotifications() {
        return _guaranteedNotifications;
    }

    public void setGuaranteedNotifications(boolean guaranteedNotifications) {
        _guaranteedNotifications = guaranteedNotifications;
    }

    public boolean isBroadcast() {
        return _broadcast;
    }

    public void setBroadcast(boolean broadcast) {
        this._broadcast = broadcast;
    }

    public boolean isReturnOnlyUids() {
        return _returnOnlyUids;
    }

    public NotifyInfo setReturnOnlyUids(boolean returnOnlyUids) {
        this._returnOnlyUids = returnOnlyUids;
        return this;
    }

    public boolean isFifo() {
        return Modifiers.contains(_modifiers, Modifiers.FIFO);
    }

    public void setFifo(boolean fifo) {
        if (fifo)
            _modifiers = Modifiers.add(_modifiers, Modifiers.FIFO);
        else
            _modifiers = Modifiers.remove(_modifiers, Modifiers.FIFO);
    }

    public boolean isReturnPrevValue() {
        return Modifiers.contains(_modifiers, Modifiers.RETURN_PREV_ON_UPDATE_NOTIFY);
    }

    public void setReturnPrevValue(boolean returnPrevValue) {
        if (returnPrevValue)
            _modifiers = Modifiers.add(_modifiers, Modifiers.RETURN_PREV_ON_UPDATE_NOTIFY);
        else
            _modifiers = Modifiers.remove(_modifiers, Modifiers.RETURN_PREV_ON_UPDATE_NOTIFY);
    }

    public int getNotifyType() {
        return _notifyType;
    }

    public void setNotifyType(int notifyType) {
        this._notifyType = notifyType;
    }

    public int getModifiers() {
        return _modifiers;
    }

    public void setModifiers(int modifiers) {
        this._modifiers = modifiers;
    }

    public String getTemplateUID() {
        return _templateUID;
    }

    public void setTemplateUID(String templateUID) {
        this._templateUID = templateUID;
    }

    public String getOrInitTemplateUID() {
        if (_templateUID == null)
            _templateUID = UuidFactory.generate().toString();
        return _templateUID;
    }

    public RemoteEventListener getListener() {
        return _listener;
    }

    public void setListener(RemoteEventListener listener) {
        this._listener = listener;
    }

    public INotifyDelegatorFilter getFilter() {
        return _filter;
    }

    public void setFilter(INotifyDelegatorFilter filter) {
        this._filter = filter;
    }

    public MarshalledObject<?> getHandback() {
        return _handback;
    }

    public void setHandback(MarshalledObject<?> handback) {
        this._handback = handback;
    }

    public CustomInfo getCustomInfo() {
        return _customInfo;
    }

    public NotifyInfo setCustomInfo(CustomInfo customInfo) {
        this._customInfo = customInfo;
        return this;
    }

    public boolean isBatching() {
        return _batchSize != null && _batchTime != null && _batchPendingThreshold != null;
    }

    public int getBatchSize() {
        return _batchSize != null ? _batchSize : -1;
    }

    public long getBatchTime() {
        return _batchTime != null ? _batchTime : -1;
    }

    public int getBatchPendingThreshold() {
        return _batchPendingThreshold != null ? _batchPendingThreshold : -1;
    }

    public void setBatchParams(int size, long time, int pendingThreshold) {
        this._batchSize = size;
        this._batchTime = time;
        this._batchPendingThreshold = pendingThreshold;
    }

    public String getTag() {
        return _tag;
    }

    public void applyDefaults(ClusterPolicy clusterPolicy) {
        // if user hasn't specify replication and notification - use the default values.
        if (_triggerNotifyTemplate == null) {
            if (clusterPolicy != null && clusterPolicy.m_ReplicationPolicy != null)
                _triggerNotifyTemplate = clusterPolicy.m_ReplicationPolicy.m_TriggerNotifyTemplates;
            else
                _triggerNotifyTemplate = false;
        }

        if (_replicateTemplate == null) {
            if (clusterPolicy != null && clusterPolicy.m_ReplicationPolicy != null)
                _replicateTemplate = clusterPolicy.m_ReplicationPolicy.m_ReplicateNotifyTemplates;
            else
                _replicateTemplate = false;
        }
    }

    public void validateModifiers() {
        if ((NotifyModifiers.isRematchedUpdate(_notifyType) || NotifyModifiers.isMatchedUpdate(_notifyType)) && NotifyModifiers.isUpdate(_notifyType))
            throw new IllegalArgumentException(NotifyActionType.NOTIFY_MATCHED_UPDATE + " or " + NotifyActionType.NOTIFY_REMATCHED_UPDATE + " modifiers cannot be used with "
                    + NotifyActionType.NOTIFY_UPDATE + " modifier for the same notify template");
    }

    private static final short FLAG_IS_REPLICATE_SET = 1 << 0;
    private static final short FLAG_REPLICATE_NOTIFY = 1 << 1;
    private static final short FLAG_TRIGGER_NOTIFY_TEMPLATE = 1 << 2;
    private static final short FLAG_BATCH = 1 << 3;
    private static final short FLAG_MODIFIERS = 1 << 4;
    private static final short FLAG_HAND_BACK = 1 << 5;
    private static final short FLAG_FILTER = 1 << 6;
    private static final short FLAG_BROADCAST = 1 << 7;
    private static final short FLAG_TEMPLATE_UID = 1 << 8;
    private static final short FLAG_IS_TRIGGER_SET = 1 << 9;
    private static final short FLAG_GUARANTEED_NOTIFY = 1 << 10;
    private static final short FLAG_RETURN_ONLY_UIDS = 1 << 11;
    private static final short FLAG_CUSTOM_INFO = 1 << 12;
    private static final short FLAG_TAG_STRING = 1 << 13;

    /**
     * This flag has been superseeded by FLAG_MODIFIERS - they are never used together.
     *
     * @deprecated
     */
    private static final short FLAG_FIFO = 1 << 4;

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        PlatformLogicalVersion version = LRMIInvocationContext.getEndpointLogicalVersion();
        if (version.greaterOrEquals(PlatformLogicalVersion.v9_5_0))
            serialize(out, false);
        else
            writeExternalV9_0_2(out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        PlatformLogicalVersion version = LRMIInvocationContext.getEndpointLogicalVersion();
        if (version.greaterOrEquals(PlatformLogicalVersion.v9_5_0))
            deserialize(in, false);
        else
            readExternalV9_0_2(in);
    }

    @Override
    public void writeToSwap(ObjectOutput out) throws IOException {
        serialize(out, true);
    }

    @Override
    public void readFromSwap(ObjectInput in) throws IOException, ClassNotFoundException {
        deserialize(in, true);
    }

    private void serialize(ObjectOutput out, boolean swap)
            throws IOException {
        writeExternalV9_5_0(out);
    }

    private void writeExternalV9_5_0(ObjectOutput out)
            throws IOException {
        final short flags = buildFlagsV9_5_0();
        out.writeShort(flags);

        out.writeInt(_notifyType);
        out.writeObject(_listener);

        if (_modifiers != 0)
            out.writeInt(_modifiers);
        if (_templateUID != null)
            IOUtils.writeString(out, _templateUID);
        if (_filter != null)
            out.writeObject(_filter);

        if (_handback != null) {
            if (out instanceof ObjectOutputStream)
                ((ObjectOutputStream) out).writeUnshared(_handback);
            else
                out.writeObject(_handback);
        }

        if ((flags & FLAG_BATCH) != 0) {
            out.writeInt(_batchSize);
            out.writeLong(_batchTime);
            out.writeInt(_batchPendingThreshold);
        }
        if (_customInfo != null)
            IOUtils.writeObject(out, _customInfo);

        if (_tag != null) {
            IOUtils.writeString(out, _tag);
        }
    }

    private void writeExternalV9_0_2(ObjectOutput out)
            throws IOException {
        final short flags = buildFlagsV9_0_2();
        out.writeShort(flags);

        out.writeInt(_notifyType);
        out.writeObject(_listener);

        if (_modifiers != 0)
            out.writeInt(_modifiers);
        if (_templateUID != null)
            IOUtils.writeString(out, _templateUID);
        if (_filter != null)
            out.writeObject(_filter);

        if (_handback != null) {
            if (out instanceof ObjectOutputStream)
                ((ObjectOutputStream) out).writeUnshared(_handback);
            else
                out.writeObject(_handback);
        }

        if ((flags & FLAG_BATCH) != 0) {
            out.writeInt(_batchSize);
            out.writeLong(_batchTime);
            out.writeInt(_batchPendingThreshold);
        }
    }

    private void deserialize(ObjectInput in, boolean swap)
            throws IOException, ClassNotFoundException {
        readExternalV9_5_0(in);
    }

    private void readExternalV9_5_0(ObjectInput in)
            throws IOException, ClassNotFoundException {
        final short flags = in.readShort();

        _notifyType = in.readInt();
        _listener = (RemoteEventListener) in.readObject();

        if ((flags & FLAG_MODIFIERS) != 0)
            _modifiers = in.readInt();
        if ((flags & FLAG_TEMPLATE_UID) != 0)
            _templateUID = IOUtils.readString(in);
        if ((flags & FLAG_FILTER) != 0)
            _filter = (INotifyDelegatorFilter) in.readObject();
        if ((flags & FLAG_HAND_BACK) != 0) {
            if (in instanceof ObjectInputStream)
                _handback = (MarshalledObject<?>) ((ObjectInputStream) in).readUnshared();
            else
                _handback = (MarshalledObject<?>) in.readObject();
        }

        if ((flags & FLAG_BATCH) != 0) {
            _batchSize = in.readInt();
            _batchTime = in.readLong();
            _batchPendingThreshold = in.readInt();
        }
        if ((flags & FLAG_CUSTOM_INFO) != 0)
            _customInfo = IOUtils.readObject(in);

        if ((flags & FLAG_TAG_STRING) != 0) {
            _tag = IOUtils.readString(in);
        }

        _broadcast = (flags & FLAG_BROADCAST) != 0;
        _guaranteedNotifications = (flags & FLAG_GUARANTEED_NOTIFY) != 0;
        _returnOnlyUids = (flags & FLAG_RETURN_ONLY_UIDS) != 0;
        if ((flags & FLAG_IS_REPLICATE_SET) != 0)
            _replicateTemplate = (flags & FLAG_REPLICATE_NOTIFY) != 0;
        if ((flags & FLAG_IS_TRIGGER_SET) != 0)
            _triggerNotifyTemplate = (flags & FLAG_TRIGGER_NOTIFY_TEMPLATE) != 0;
    }

    private void readExternalV9_0_2(ObjectInput in)
            throws IOException, ClassNotFoundException {
        final short flags = in.readShort();

        _notifyType = in.readInt();
        _listener = (RemoteEventListener) in.readObject();

        if ((flags & FLAG_MODIFIERS) != 0)
            _modifiers = in.readInt();
        if ((flags & FLAG_TEMPLATE_UID) != 0)
            _templateUID = IOUtils.readString(in);
        if ((flags & FLAG_FILTER) != 0)
            _filter = (INotifyDelegatorFilter) in.readObject();
        if ((flags & FLAG_HAND_BACK) != 0) {
            if (in instanceof ObjectInputStream)
                _handback = (MarshalledObject<?>) ((ObjectInputStream) in).readUnshared();
            else
                _handback = (MarshalledObject<?>) in.readObject();
        }

        if ((flags & FLAG_BATCH) != 0) {
            _batchSize = in.readInt();
            _batchTime = in.readLong();
            _batchPendingThreshold = in.readInt();
        }

        _broadcast = (flags & FLAG_BROADCAST) != 0;
        _guaranteedNotifications = (flags & FLAG_GUARANTEED_NOTIFY) != 0;
        _returnOnlyUids = (flags & FLAG_RETURN_ONLY_UIDS) != 0;
        if ((flags & FLAG_IS_REPLICATE_SET) != 0)
            _replicateTemplate = (flags & FLAG_REPLICATE_NOTIFY) != 0;
        if ((flags & FLAG_IS_TRIGGER_SET) != 0)
            _triggerNotifyTemplate = (flags & FLAG_TRIGGER_NOTIFY_TEMPLATE) != 0;
    }

    private short buildFlagsV9_5_0() {
        short flags = 0;

        if (_replicateTemplate != null) {
            flags |= FLAG_IS_REPLICATE_SET;
            if (_replicateTemplate)
                flags |= FLAG_REPLICATE_NOTIFY;
        }

        if (_triggerNotifyTemplate != null) {
            flags |= FLAG_IS_TRIGGER_SET;
            if (_triggerNotifyTemplate)
                flags |= FLAG_TRIGGER_NOTIFY_TEMPLATE;
        }

        if (_batchSize != null && _batchTime != null && _batchPendingThreshold != null)
            flags |= FLAG_BATCH;
        if (_modifiers != 0)
            flags |= FLAG_MODIFIERS;
        if (_returnOnlyUids)
            flags |= FLAG_RETURN_ONLY_UIDS;
        if (_handback != null)
            flags |= FLAG_HAND_BACK;
        if (_filter != null)
            flags |= FLAG_FILTER;
        if (_broadcast)
            flags |= FLAG_BROADCAST;
        if (_templateUID != null)
            flags |= FLAG_TEMPLATE_UID;
        if (_guaranteedNotifications)
            flags |= FLAG_GUARANTEED_NOTIFY;
        if (_customInfo != null)
            flags |= FLAG_CUSTOM_INFO;
        if (_tag != null)
            flags |= FLAG_TAG_STRING;

        return flags;
    }

    private short buildFlagsV9_0_2() {
        short flags = 0;

        if (_replicateTemplate != null) {
            flags |= FLAG_IS_REPLICATE_SET;
            if (_replicateTemplate)
                flags |= FLAG_REPLICATE_NOTIFY;
        }

        if (_triggerNotifyTemplate != null) {
            flags |= FLAG_IS_TRIGGER_SET;
            if (_triggerNotifyTemplate)
                flags |= FLAG_TRIGGER_NOTIFY_TEMPLATE;
        }

        if (_batchSize != null && _batchTime != null && _batchPendingThreshold != null)
            flags |= FLAG_BATCH;
        if (_modifiers != 0)
            flags |= FLAG_MODIFIERS;
        if (_returnOnlyUids)
            flags |= FLAG_RETURN_ONLY_UIDS;
        if (_handback != null)
            flags |= FLAG_HAND_BACK;
        if (_filter != null)
            flags |= FLAG_FILTER;
        if (_broadcast)
            flags |= FLAG_BROADCAST;
        if (_templateUID != null)
            flags |= FLAG_TEMPLATE_UID;
        if (_guaranteedNotifications)
            flags |= FLAG_GUARANTEED_NOTIFY;

        return flags;
    }

    private short buildFlagsV9_0_0() {
        short flags = 0;

        if (_replicateTemplate != null) {
            flags |= FLAG_IS_REPLICATE_SET;
            if (_replicateTemplate)
                flags |= FLAG_REPLICATE_NOTIFY;
        }

        if (_triggerNotifyTemplate != null) {
            flags |= FLAG_IS_TRIGGER_SET;
            if (_triggerNotifyTemplate)
                flags |= FLAG_TRIGGER_NOTIFY_TEMPLATE;
        }

        if (_batchSize != null && _batchTime != null && _batchPendingThreshold != null)
            flags |= FLAG_BATCH;
        if (isFifo())
            flags |= FLAG_FIFO;
        if (_returnOnlyUids)
            flags |= FLAG_RETURN_ONLY_UIDS;
        if (_handback != null)
            flags |= FLAG_HAND_BACK;
        if (_filter != null)
            flags |= FLAG_FILTER;
        if (_broadcast)
            flags |= FLAG_BROADCAST;
        if (_templateUID != null)
            flags |= FLAG_TEMPLATE_UID;
        if (_guaranteedNotifications)
            flags |= FLAG_GUARANTEED_NOTIFY;

        return flags;
    }
}
