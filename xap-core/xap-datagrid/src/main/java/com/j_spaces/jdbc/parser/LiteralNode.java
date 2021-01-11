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

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.j_spaces.jdbc.SQLUtil;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.Date;
import java.sql.SQLException;
import java.time.LocalDate;


/**
 * This is the Literal node. it holds an object which is the value of the condition
 *
 * @author Michael Mitrani, 2Train4, 2004
 */
@com.gigaspaces.api.InternalApi
public class LiteralNode extends ValueNode {
    private static final long serialVersionUID = 1L;

    protected Object value;

    public LiteralNode() {
    }

    public LiteralNode(Object value) {
        super();
        this.value = value;
    }

    public LiteralNode(String left, String op, Integer right) {
        super();
        if (left.trim().equalsIgnoreCase("CURRENT_DATE")) {
            LocalDate localDate = LocalDate.now();
            if (op != null) {
                if (op.equals("+")) {
                    localDate = localDate.plusDays(right);
                } else if (op.equals("-")) {
                    localDate = localDate.minusDays(right);
                }
            }
            this.value = Date.valueOf(localDate);
        } else {
            this.value = left;
        }
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public boolean isPreparedValue() {
        return false;
    }

    @Override
    public String toString() {
        return value != null ? value.toString() : null;
    }

    /**
     * Gets the node's object converted to the provided property's object type.
     */
    public Object getConvertedObject(ITypeDesc typeDesc, String propertyName) throws SQLException {
        return SQLUtil.cast(typeDesc, propertyName, value, false);
    }

    @Override
    public Object clone() {
        return new LiteralNode(value);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        IOUtils.writeObject(out, value);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        value = IOUtils.readObject(in);
    }
}
