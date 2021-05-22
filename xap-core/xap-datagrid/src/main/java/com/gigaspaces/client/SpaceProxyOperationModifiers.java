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


package com.gigaspaces.client;

import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;

/**
 * Base class for space proxy operations modifiers.
 *
 * NOTE: This class is intended for internal usage only.
 *
 * @author Niv Ingberg
 * @since 9.0.1
 */
public abstract class SpaceProxyOperationModifiers implements SmartExternalizable {
    private static final long serialVersionUID = 1L;

    private int _code;

    public SpaceProxyOperationModifiers() {
    }

    protected SpaceProxyOperationModifiers(int code) {
        this._code = code;
    }

    protected SpaceProxyOperationModifiers(SpaceProxyOperationModifiers m1, SpaceProxyOperationModifiers m2) {
        this._code = m1._code | m2._code;
    }

    protected SpaceProxyOperationModifiers(SpaceProxyOperationModifiers m1, SpaceProxyOperationModifiers m2, SpaceProxyOperationModifiers m3) {
        this._code = m1._code | m2._code | m3._code;
    }

    protected SpaceProxyOperationModifiers(SpaceProxyOperationModifiers... modifiers) {
        int code = 0;
        for (SpaceProxyOperationModifiers m : modifiers)
            code |= m._code;
        this._code = code;
    }

    public int getCode() {
        return _code;
    }

    /**
     * Checks if the specified modifier is set.
     *
     * @return true if the specified modifier is set, false otherwise.
     */
    protected boolean contains(SpaceProxyOperationModifiers modifiers) {
        return (this._code & modifiers._code) != 0;
    }

    protected int add(SpaceProxyOperationModifiers modifiers) {
        return this._code | modifiers._code;
    }

    protected int remove(SpaceProxyOperationModifiers modifiers) {
        return this._code & ~modifiers._code;
    }

    @SuppressWarnings("unchecked")
    protected <T extends SpaceProxyOperationModifiers> T createIfNeeded(int modifiers) {
        Map<Integer, SpaceProxyOperationModifiers> cache = getCache();
        T cachedValue = (T) cache.get(modifiers);
        if (cachedValue != null)
            return cachedValue;

        T newValue = (T) create(modifiers);
        cache.put(modifiers, newValue);
        return newValue;
    }

    protected abstract SpaceProxyOperationModifiers create(int modifiers);

    protected abstract Map<Integer, SpaceProxyOperationModifiers> getCache();

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass())
            return false;

        SpaceProxyOperationModifiers other = (SpaceProxyOperationModifiers) obj;

        return _code == other._code;
    }

    @Override
    public int hashCode() {
        return _code;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(_code);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _code = in.readInt();
    }
}
