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
import com.gigaspaces.internal.query.RawEntry;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Niv Ingberg
 * @since 10.0
 */

public class MinEntryAggregator extends AbstractPathAggregator<MinEntryAggregator.MinEntryScannerResult> {

    private static final long serialVersionUID = 1L;

    private transient MinEntryScannerResult result;

    @Override
    public String getDefaultAlias() {
        return "minEntry(" + getPath() + ")";
    }

    @Override
    public void aggregate(SpaceEntriesAggregatorContext context) {
        Comparable value = (Comparable) getPathValue(context);
        if (result == null)
            result = new MinEntryScannerResult(context.getRawEntry(), value);
        else if (result.value.compareTo(value) > 0) {
            result.entry = context.getRawEntry();
            result.value = value;
        }
    }

    @Override
    public void aggregateIntermediateResult(MinEntryScannerResult partitionResult) {
        result = result == null || result.value.compareTo(partitionResult.value) > 0 ? partitionResult : result;
    }

    @Override
    public MinEntryScannerResult getIntermediateResult() {
        return result;
    }

    @Override
    public Object getFinalResult() {
        return result == null ? null : toObject(result.getRawEntry());
    }

    public static class MinEntryScannerResult implements SmartExternalizable {

        private static final long serialVersionUID = 1L;

        private RawEntry entry;
        private Comparable value;

        public MinEntryScannerResult() {
        }

        private MinEntryScannerResult(RawEntry entry, Comparable value) {
            this.entry = entry;
            this.value = value;
        }

        public RawEntry getRawEntry() {
            return entry;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            IOUtils.writeObject(out, entry);
            IOUtils.writeObject(out, value);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            this.entry = IOUtils.readObject(in);
            this.value = IOUtils.readObject(in);
        }
    }
}
