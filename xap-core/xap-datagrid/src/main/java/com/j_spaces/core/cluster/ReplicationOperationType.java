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


package com.j_spaces.core.cluster;

import com.gigaspaces.internal.utils.CollectionUtils;

import java.util.Set;

/**
 * Represents all the possible operation types that can be replicated.
 *
 * @author asy ronen
 * @version 1.0
 * @since 6.02
 */
public enum ReplicationOperationType {
    WRITE(false), TAKE(true), EXTEND_LEASE(false), UPDATE(false), DISCARD(false),
    LEASE_EXPIRATION(false), NOTIFY(true), TRANSACTION(false), EVICT(true), CHANGE(false);

    private static final Set<ReplicationOperationType> FULL_PERMISSIONS = CollectionUtils.toUnmodifiableSet(
            WRITE, TAKE, EXTEND_LEASE, UPDATE, LEASE_EXPIRATION, NOTIFY, TRANSACTION, EVICT, CHANGE
    );

    private final boolean template;

    ReplicationOperationType(boolean template) {
        this.template = template;
    }

    public boolean isTemplate() {
        return template;
    }

    public static final Set<ReplicationOperationType> getFullPermissions() {
        return FULL_PERMISSIONS;
    }
}
