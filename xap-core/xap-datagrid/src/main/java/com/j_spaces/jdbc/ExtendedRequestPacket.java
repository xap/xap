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

package com.j_spaces.jdbc;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.serialization.SmartExternalizable;
import com.j_spaces.jdbc.driver.GPreparedStatement.PreparedValuesCollection;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

@com.gigaspaces.api.InternalApi
public class ExtendedRequestPacket
        extends RequestPacket implements SmartExternalizable {
    private static final long serialVersionUID = 1L;

    private Integer modifiers;

    public ExtendedRequestPacket() {
        super();
    }


    public ExtendedRequestPacket(RequestPacket packet) {
        setType(packet.getType());
        setStatement(packet.getStatement());
        setPreparedValues(packet.getPreparedValues());
        setPreparedValuesCollection(packet.getPreparedValuesCollection());
        setModifiers(packet.getModifiers());
    }


    public Integer getModifiers() {
        return modifiers;
    }

    public void setModifiers(Integer modifiers) {
        this.modifiers = modifiers;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeObject(out, getType());
        IOUtils.writeString(out, getStatement());
        IOUtils.writeObjectArray(out, getPreparedValues());
        IOUtils.writeObject(out, getPreparedValuesCollection());
        IOUtils.writeObject(out, modifiers);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        setType((Type) IOUtils.readObject(in));
        setStatement(IOUtils.readString(in));
        setPreparedValues(IOUtils.readObjectArray(in));
        setPreparedValuesCollection((PreparedValuesCollection) IOUtils.readObject(in));
        modifiers = IOUtils.readObject(in);
    }

}
