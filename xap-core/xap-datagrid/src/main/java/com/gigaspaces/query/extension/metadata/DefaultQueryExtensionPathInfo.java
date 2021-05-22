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

package com.gigaspaces.query.extension.metadata;

import com.gigaspaces.query.extension.QueryExtensionProvider;
import com.gigaspaces.query.extension.SpaceQueryExtension;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Niv Ingberg
 * @since 11.0
 */
@com.gigaspaces.api.InternalApi
public class DefaultQueryExtensionPathInfo extends QueryExtensionPathInfo implements SmartExternalizable {
    // serialVersionUID should never be changed.
    private static final long serialVersionUID = 1L;

    private transient Class<? extends QueryExtensionProvider> providerClass;

    /**
     * Required for Externalizable
     */
    public DefaultQueryExtensionPathInfo() {

    }

    public DefaultQueryExtensionPathInfo(Class<? extends QueryExtensionProvider> providerClass) {
        this.providerClass = providerClass;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    }

    @Override
    public Class<? extends QueryExtensionProvider> getQueryExtensionProviderClass() {
        return providerClass;
    }

    @Override
    public boolean isIndexed() {
        return true;
    }
}
