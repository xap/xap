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

import java.io.Serializable;
import java.util.function.Function;

/**
 * @author Anna Pavtulov
 * @since 10.0
 */

public class SingleValueFunctionAggregator<T extends Serializable & Comparable> extends AbstractPathAggregator<T> {

    private static final long serialVersionUID = 1L;

    private transient T result;
    private transient boolean isSet;
    private transient Function<T,T> function;
    private transient String functionName;

    public SingleValueFunctionAggregator(Function<T, T> function, String functionName) {
        super();
        this.function = function;
        this.functionName = functionName;
    }

    @Override
    public String getDefaultAlias() {
        return functionName + "(" + getPath() + ")";
    }

    @Override
    public void aggregate(SpaceEntriesAggregatorContext context) {
        T value = (T) getPathValue(context);
        if (!isSet) {
            result = function.apply(value);
            isSet = true;
        }
    }

    @Override
    public void aggregateIntermediateResult(T partitionResult) {
        result = function.apply(partitionResult);
    }

    @Override
    public T getIntermediateResult() {
        return result;
    }
}
