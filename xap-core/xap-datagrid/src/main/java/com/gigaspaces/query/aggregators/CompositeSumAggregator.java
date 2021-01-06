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
import com.gigaspaces.internal.utils.math.MutableNumber;
import com.j_spaces.jdbc.SumColumn;
import com.j_spaces.jdbc.ValueSelectColumn;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


/**
 * @author Yohana Khoury
 * @since 15.8.0
 */
public class CompositeSumAggregator extends AbstractPathAggregator<MutableNumber> {
    private SumColumn funcColumn;
    private transient MutableNumber result;


    public CompositeSumAggregator() {
    }

    public CompositeSumAggregator(SumColumn funcColumn) {
        this.funcColumn = funcColumn;
    }


    @Override
    public String getDefaultAlias() {
        return funcColumn.getAlias();
    }

    @Override
    public void aggregate(SpaceEntriesAggregatorContext context) {
        calc(context, funcColumn);
    }

    private void calc(SpaceEntriesAggregatorContext context, SumColumn sumColumn) {
        if (sumColumn.getLeft() instanceof SumColumn) {
            calc(context, (SumColumn) sumColumn.getLeft());
        } else {
            Number left = (Number) context.getPathValue(sumColumn.getLeft().getName());
            add(left);
        }

        if (sumColumn.getRight() instanceof SumColumn) {
            calc(context, (SumColumn) sumColumn.getRight());
        } else if (sumColumn.getRight() instanceof ValueSelectColumn) {
            if (sumColumn.getOperator().equals("-")) {
                remove(((Number) sumColumn.getRight().getValue()));
            } else {
                add(((Number) sumColumn.getRight().getValue()));
            }
        } else {
            Number right = (Number) context.getPathValue(sumColumn.getRight().getName());
            if (sumColumn.getOperator().equals("-")) {
                remove(right);
            } else {
                add(right);
            }
        }
    }

    @Override
    public MutableNumber getIntermediateResult() {
        return result;
    }

    @Override
    public Object getFinalResult() {
        return result != null ? result.toNumber() : null;
    }

    @Override
    public void aggregateIntermediateResult(MutableNumber partitionResult) {
        if (result == null)
            result = partitionResult;
        else
            result.add(partitionResult.toNumber());
    }

    private void add(Number number) {
        if (number != null) {
            if (result == null)
                result = MutableNumber.fromClass(number.getClass(), true);
            result.add(number);
        }
    }

    private void remove(Number number) {
        if (number != null) {
            if (result == null)
                result = MutableNumber.fromClass(number.getClass(), true);
            result.remove(number);
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        IOUtils.writeObject(out, funcColumn);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        funcColumn = IOUtils.readObject(in);
    }
}
