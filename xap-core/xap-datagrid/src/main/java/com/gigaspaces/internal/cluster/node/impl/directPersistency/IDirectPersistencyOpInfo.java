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

package com.gigaspaces.internal.cluster.node.impl.directPersistency;

import com.gigaspaces.internal.cluster.node.impl.directPersistency.embeddedSyncList.IEmbeddedSyncOpInfo;
import com.gigaspaces.serialization.SmartExternalizable;

import java.util.List;

/**
 * Created by yechielf on 28/04/2015.
 */
public interface IDirectPersistencyOpInfo extends SmartExternalizable {

    long getGenerationId();

    boolean isMultiUids();

    String getUid();

    List<String> getUids();

    /* -1 if not set */
    Long getRedoKey();

    boolean hasRedoKey();

    void setRedoKey(long redokey);

    boolean isInMainList();

    void setInMainList();

    boolean isPersisted();

    void setPersisted();

    long getSequenceNumber();

    int getSegmentNumber();

    int getOrderWithinSegment();

    //specific for embedded synclist
    boolean isConfirmedByRemote(IDirectPersistencySyncHandler handler);

    void setEmbeddedSyncOpInfo(IEmbeddedSyncOpInfo embeddedSyncOpInfo);

    IEmbeddedSyncOpInfo getEmbeddedSyncOpInfo();
}
