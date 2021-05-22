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

package com.gigaspaces.internal.cluster.node.impl.replica;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @since 9.0.1
 */
@com.gigaspaces.api.InternalApi
public class CurrentStageInfo implements SmartExternalizable {

    private static final long serialVersionUID = 1L;

    private String _stageName;
    private String _nextStageName;
    private boolean _isLastStage;

    public CurrentStageInfo() {
    }

    public CurrentStageInfo(String stageName, String nextStageName, boolean isLastStage) {
        this._stageName = stageName;
        this._nextStageName = nextStageName;
        this._isLastStage = isLastStage;
    }

    public String getStageName() {
        return _stageName;
    }

    public String getNextStageName() {
        return _nextStageName;
    }

    public boolean isLastStage() {
        return _isLastStage;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeString(out, _stageName);
        IOUtils.writeString(out, _nextStageName);
        out.writeBoolean(_isLastStage);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this._stageName = IOUtils.readString(in);
        this._nextStageName = IOUtils.readString(in);
        this._isLastStage = in.readBoolean();
    }

}
