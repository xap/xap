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

package com.gigaspaces.internal.client.mutators;

import com.gigaspaces.client.mutators.SpaceEntryMutator;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.utils.Textualizable;
import com.gigaspaces.internal.utils.Textualizer;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Niv Ingberg
 * @since 9.1
 */
public abstract class SpaceEntryPathMutator extends SpaceEntryMutator implements SmartExternalizable, Textualizable {
    private static final long serialVersionUID = 1L;

    private String _path;

    /**
     * if true then path will not be cached
     * @since 14.2.0 part of fix for GS-13776
     */
    private boolean _disablePathCaching = false;

    protected SpaceEntryPathMutator() {
    }

    protected SpaceEntryPathMutator(String path) {
        this();
        this._path = path;
    }

    public String getPath() {
        return _path;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        final PlatformLogicalVersion version = LRMIInvocationContext.getEndpointLogicalVersion();
        if (version.patchSameOrGreater(PlatformLogicalVersion.v12_3_0_PATCH4) || /*equal to v12_3_0_PATCH4*/
            version.greaterOrEquals(PlatformLogicalVersion.v14_2_0)) {
            out.writeBoolean(_disablePathCaching);
        }

        if( _disablePathCaching ) {
            IOUtils.writeString(out, _path);
        }
        else{
            IOUtils.writeRepetitiveString(out, _path);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        final PlatformLogicalVersion version = LRMIInvocationContext.getEndpointLogicalVersion();

        if (version.patchSameOrGreater(PlatformLogicalVersion.v12_3_0_PATCH4) || /*equal to v12_3_0_PATCH4*/
            version.greaterOrEquals(PlatformLogicalVersion.v14_2_0)) {
            _disablePathCaching = in.readBoolean();
        }

        if( _disablePathCaching ) {
            _path = IOUtils.readString(in);
        }
        else{
            _path = IOUtils.readRepetitiveString(in);
        }
    }

    @Override
    public void toText(Textualizer textualizer) {
        textualizer.append("path", _path);
    }

    @Override
    public String toString() {
        return Textualizer.toString(this);
    }

    /**
     * Defines if path will not be cached
     * @since 14.2.0
     */
    public SpaceEntryPathMutator disablePathCaching() {
        this._disablePathCaching = true;
        return this;
    }
}