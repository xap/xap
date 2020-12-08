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
import com.gigaspaces.internal.query.predicate.ISpacePredicate;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public abstract class SingleValueRange extends Range {
    // serialVersionUID should never be changed.
    private static final long serialVersionUID = 1L;

    private Object value;

    protected SingleValueRange(String colPath, FunctionCallDescription functionCallDescription, Object value, ISpacePredicate predicate) {
        super(colPath, functionCallDescription, predicate);
        this.value = value;
    }

    public SingleValueRange() {
        super();
    }

    /**
     * @return the value
     */
    public Object getValue() {
        return value;
    }

    /**
     * @param value the value to set
     */
    public void setValue(Object value) {
        this.value = value;
    }

    @Override
    public boolean isRelevantForAllIndexValuesOptimization() {
        return true;
    }


    @Override
    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        super.readExternal(in);

        value = IOUtils.readObject(in);
    }

    @Override
    public void writeExternal(ObjectOutput out)
            throws IOException {
        super.writeExternal(out);

        IOUtils.writeObject(out, value);
    }
}