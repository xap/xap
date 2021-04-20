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

package com.gigaspaces.internal.server.metadata;

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.metrics.LongCounter;

/**
 * @author Niv Ingberg
 * @since 7.0
 */
public interface IServerTypeDesc {
    String ROOT_TYPE_NAME = Object.class.getName();
    String ROOT_SYSTEM_TYPE_NAME = "com.gigaspaces.SystemType";

    int getTypeId();

    String getTypeName();

    boolean isRootType();

    ITypeDesc getTypeDesc();

    void setTypeDesc(ITypeDesc typeDesc);

    boolean isActive();

    boolean isInactive();

    void inactivateType();

    boolean isFifoSupported();

    IServerTypeDesc[] getSuperTypes();

    IServerTypeDesc[] getAssignableTypes();

    boolean hasSubTypes();

    IServerTypeDesc createCopy(IServerTypeDesc superType);

    void addSubType(IServerTypeDesc subType);

    void removeSubType(IServerTypeDesc subType);

    short getServerTypeDescCode();

    boolean isMaybeOutdated();

    void setMaybeOutdated();

    LongCounter getReadCounter();

    LongCounter getRAMReadCounter();
}
