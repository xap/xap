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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Iterator;
import java.util.Map;

/**
 * @author anna
 * @since 10.1
 */

public class DistinctResult implements SmartExternalizable, Iterable<RawEntry> {

    private static final long serialVersionUID = 1L;

    private Map<DistinctPropertiesKey, RawEntry> map;

    /**
     * Required for Externalizable
     */
    public DistinctResult() {
    }

    public DistinctResult(Map<DistinctPropertiesKey, RawEntry> map) {
        this.map = map;
    }

    protected Map<DistinctPropertiesKey, RawEntry> getMap() {
        return map;
    }

    public int size() {
        return map.size();
    }

    @Override
    public Iterator<RawEntry> iterator() {
        return map.values().iterator();
    }

    public RawEntry get(Object... keys) {
        return map.get(new DistinctPropertiesKey(keys));
    }

    public RawEntry get(DistinctPropertiesKey key) {
        return map.get(key);
    }


    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeObject(out, map);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.map = IOUtils.readObject(in);
    }
}
