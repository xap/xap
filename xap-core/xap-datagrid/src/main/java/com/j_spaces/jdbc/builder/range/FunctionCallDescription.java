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

package com.j_spaces.jdbc.builder.range;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.query.sql.functions.SqlFunctionExecutionContext;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Created by Barak Bar Orion on 2/9/16.
 *
 * @since 11.0
 */
@com.gigaspaces.api.InternalApi
public class FunctionCallDescription implements SmartExternalizable, SqlFunctionExecutionContext {

    private static final long serialVersionUID = 1L;
    private String name;
    private List<Object> args;
    private int columnIndex;

    public FunctionCallDescription() {
        args = Collections.emptyList();
    }

    public FunctionCallDescription(String name, int columnIndex, List<Object> args) {
        this.name = name;
        this.columnIndex = columnIndex;
        this.args = args;
    }

    public String getName() {
        return name;
    }

    public void setArgs(List<Object> args) {
        this.args = args;
    }

    public int getColumnIndex() {
        return columnIndex;
    }

    @Override
    public int getNumberOfArguments() {
        return args.size();
    }

    @Override
    public Object getArgument(int index) {
        return args.get(index);
    }

    public List<Object> getArgs() {
        return args;
    }

    public FunctionCallDescription setColumnValue(Object value) {
        this.args.set(columnIndex, value);
        return this;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeString(out, name);
        out.writeInt(columnIndex);
        out.writeObject(args);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        name = IOUtils.readString(in);
        columnIndex = in.readInt();
        //noinspection unchecked
        args = (List<Object>) in.readObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FunctionCallDescription that = (FunctionCallDescription) o;
        return columnIndex == that.columnIndex &&
                Objects.equals(name, that.name) &&
                Objects.equals(args, that.args);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, args, columnIndex);
    }
}
