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

package com.gigaspaces.management.space;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Encapsulates space query details.
 *
 * @author Niv Ingberg
 * @since 9.5.0
 */
@com.gigaspaces.api.InternalApi
public class SpaceQueryDetails implements SmartExternalizable {
    private static final long serialVersionUID = 1L;

    private String _description;

    /**
     * Required for Externalizable
     */
    public SpaceQueryDetails() {
    }

    public SpaceQueryDetails(String description) {
        this._description = description;
    }

    public String getDescription() {
        return _description;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeString(out, _description);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this._description = IOUtils.readString(in);
    }
}
