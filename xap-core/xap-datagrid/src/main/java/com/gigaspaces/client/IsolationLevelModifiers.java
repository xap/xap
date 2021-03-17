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

import com.j_spaces.core.client.Modifiers;

/**
 * Base class for isolation level based space proxy operations modifiers.
 *
 * NOTE: This class is intended for internal usage only.
 *
 * @author Dan Kilman
 * @since 9.5
 */
abstract public class IsolationLevelModifiers extends SpaceProxyOperationModifiers {
    private static final long serialVersionUID = 1L;

    private static final int REPEATABLE_READ = Modifiers.NONE;

    private static final int ALL_ISOLATION_LEVELS =
            Modifiers.DIRTY_READ |
                    Modifiers.READ_COMMITTED |
                    REPEATABLE_READ;

    public IsolationLevelModifiers() {
    }

    protected IsolationLevelModifiers(int code) {
        super(code);
    }

    protected IsolationLevelModifiers(IsolationLevelModifiers m1, IsolationLevelModifiers m2) {
        super(m1, m2);
    }

    protected IsolationLevelModifiers(IsolationLevelModifiers m1, IsolationLevelModifiers m2, IsolationLevelModifiers m3) {
        super(m1, m2, m3);
    }

    protected IsolationLevelModifiers(IsolationLevelModifiers... modifiers) {
        super(modifiers);
    }

    @SuppressWarnings("unchecked")
    public <T extends IsolationLevelModifiers> T setIsolationLevel(IsolationLevelModifiers isolationLevel) {
        if (isolationLevel == null)
            return (T)this;
        int isolationLevelCode = isolationLevel.getCode();
        if (!(isolationLevelCode == Modifiers.DIRTY_READ ||
                isolationLevelCode == Modifiers.READ_COMMITTED ||
                isolationLevelCode == REPEATABLE_READ ||
                isolationLevelCode == Modifiers.RUN_ON_IO_THREAD
                )) {
            throw new IllegalArgumentException("isolationLevel must be one of: REPEATABLE_READ, DIRTY_READ, READ_COMMITTED. Got ["
                    + isolationLevelCode + " instead");
        }

        return (T) createIfNeeded(Modifiers.remove(getCode(), ALL_ISOLATION_LEVELS) | isolationLevelCode);
    }

    /**
     * Checks if this instance contains the {@link #REPEATABLE_READ} setting.
     *
     * @return true if this instance contains the {@link #REPEATABLE_READ} setting, false otherwise.
     */
    public boolean isRepeatableRead() {
        return !isReadCommitted() && !isDirtyRead();
    }

    /**
     * Checks if this instance contains the {@link #READ_COMMITTED} setting.
     *
     * @return true if this instance contains the {@link #READ_COMMITTED} setting, false otherwise.
     */
    public boolean isReadCommitted() {
        return Modifiers.contains(getCode(), Modifiers.READ_COMMITTED);
    }

    /**
     * Checks if this instance contains the {@link #DIRTY_READ} setting.
     *
     * @return true if this instance contains the {@link #DIRTY_READ} setting, false otherwise.
     */
    public boolean isDirtyRead() {
        return Modifiers.contains(getCode(), Modifiers.DIRTY_READ);
    }

}
