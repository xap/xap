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

package com.j_spaces.jdbc.parser;

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.j_spaces.jdbc.SQLUtil;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.SQLException;
import java.util.TreeMap;

/**
 * This is a Prepared node. it represents a literal node when used in a prepared statement. in
 * addition to the value object, it holds the index of the parameter in the statement.
 *
 * @author Michael Mitrani, 2Train4, 2004
 */
@com.gigaspaces.api.InternalApi
public class PreparedNode extends LiteralNode {
    private static final long serialVersionUID = 1L;

    private int index;

    public PreparedNode() {
    }

    public PreparedNode(Object value) {
        this(value, -1);
    }

    public PreparedNode(Object value, int index) {
        super(value);
        this.index = index;
    }

    public int getIndex() {
        return index;
    }

    public boolean isPreparedValue() {
        return true;
    }

    @Override
    public void prepareValues(Object[] values) throws SQLException {
        if (value != null)
            throw new SQLException("Prepared value already set!", "GSP", -109);

        if (index > values.length) {
            throw new SQLException("Prepared value missing!", "GSP", -110);
        }

        this.value = values[index - 1];
    }


    public String prepareTemplateValues(TreeMap values, String colName) throws SQLException {

        if (value != null)
            throw new SQLException("Prepared value already set!", "GSP", -109);
        this.value = values.remove(colName);

        return null;
    }

    @Override
    public Object getConvertedObject(ITypeDesc typeDesc, String propertyName) throws SQLException {
        return SQLUtil.cast(typeDesc, propertyName, value, true);
    }

    //override the clone method in ValueNode. we need the index only.
    public Object clone() {
        return new PreparedNode(null, this.index);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeInt(index);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        index = in.readInt();
    }
}

