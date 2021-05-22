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
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Niv Ingberg
 * @since 10.0
 */

public class AverageAggregator extends AbstractPathAggregator<AverageAggregator.AverageTuple> {

    private static final long serialVersionUID = 1L;

    private transient AverageTuple result;

    @Override
    public String getDefaultAlias() {
        return "avg(" + getPath() + ")";
    }

    @Override
    public void aggregate(SpaceEntriesAggregatorContext context) {
        Number value = (Number) getPathValue(context);
        if (value != null)
            result = result != null ? result.add(value, 1) : new AverageTuple(value);
    }

    @Override
    public void aggregateIntermediateResult(AverageTuple partitionResult) {
        if (result == null)
            result = partitionResult;
        else
            result = result.add(partitionResult.sum.toNumber(), partitionResult.count);
    }

    @Override
    public AverageTuple getIntermediateResult() {
        return result;
    }

    @Override
    public Object getFinalResult() {
        return result == null ? null : result.getAverage();
    }

    public static class AverageTuple implements SmartExternalizable {

        private static final long serialVersionUID = 1L;

        private MutableNumber sum;
        private long count;

        public AverageTuple() {
        }

        public AverageTuple(Number sum) {
            this.sum = MutableNumber.fromClass(sum.getClass(), true);
            this.sum.add(sum);
            this.count = 1;
        }

        public AverageTuple add(Number deltaSum, long deltaCount) {
            this.count += deltaCount;
            this.sum.add(deltaSum);
            return this;
        }

        public Number getAverage() {
            if (count == 0)
                return null;
            return sum.calcDivision(count);
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeLong(count);
            IOUtils.writeObject(out, sum);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            this.count = in.readLong();
            this.sum = IOUtils.readObject(in);
        }
    }
}
