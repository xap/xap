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


package com.gigaspaces.query.aggregators;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.query.sql.functions.SqlFunction;
import com.gigaspaces.query.sql.functions.SqlFunctionExecutionContext;
import com.j_spaces.jdbc.FunctionCallColumn;
import com.j_spaces.jdbc.SQLFunctions;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * @author Alon Shoham
 * @since 15.8.0
 */
public class SingleValueFunctionAggregator<T extends Serializable & Comparable> extends AbstractPathAggregator<T> {

    private static final long serialVersionUID = 1L;

    private Object result;
    private boolean isSet;
    private FunctionCallColumn functionCallColumn;

    public SingleValueFunctionAggregator(FunctionCallColumn functionCallColumn) {
        super();
        this.functionCallColumn = functionCallColumn;
    }

    public SingleValueFunctionAggregator() {
    }

    @Override
    public String getDefaultAlias() {
        return functionCallColumn.getFunctionName() + "(" + getPath() + ")";
    }

    @Override
    public void aggregate(SpaceEntriesAggregatorContext context) {
        if (!isSet) {
            result = apply(getPathValue(context));
            isSet = true;
        }
    }

    @Override
    public void aggregateIntermediateResult(T partitionResult) {
        result = functionCallColumn.apply(partitionResult);
    }

    @Override
    public T getIntermediateResult() {
        return (T) result;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        IOUtils.writeObject(out, result);
        out.writeBoolean(isSet);
        IOUtils.writeObject(out, functionCallColumn);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        result = IOUtils.readObject(in);
        isSet = in.readBoolean();
        functionCallColumn = IOUtils.readObject(in);
    }


    public Object apply(Object value){
        return functionCallColumn.apply(value);
    }
}
