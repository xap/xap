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


package com.gigaspaces.admin.quiesce;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Boris
 * @see {@link com.gigaspaces.admin.quiesce.QuiesceToken}
 * @since 10.1.0
 */
@com.gigaspaces.api.InternalApi
public class DefaultQuiesceToken implements QuiesceToken, SmartExternalizable {

    private static final long serialVersionUID = 1L;

    private String token;

    public DefaultQuiesceToken() {
        this("");
    }

    public DefaultQuiesceToken(String token) {
        this.token = token;
    }

    public String getToken() {
        return token;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DefaultQuiesceToken that = (DefaultQuiesceToken) o;

        if (token != null ? !token.equals(that.token) : that.token != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return token != null ? token.hashCode() : 0;
    }

    @Override
    public String toString() {
        return token;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeString(out, token);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        token = IOUtils.readString(in);
    }
}
