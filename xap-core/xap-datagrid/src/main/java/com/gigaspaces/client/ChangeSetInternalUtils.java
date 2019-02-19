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

import com.gigaspaces.client.mutators.SpaceEntryMutator;
import com.gigaspaces.internal.client.mutators.SpaceEntryPathMutator;

import java.util.Collection;
import java.util.Set;

/**
 * @author eitany
 * @since 9.1
 */
@com.gigaspaces.api.InternalApi
public class ChangeSetInternalUtils {

    public static Collection<SpaceEntryMutator> getMutators(ChangeSet changeSet) {

        Collection<SpaceEntryMutator> mutators = changeSet.getMutators();
        Set<String> nonCachedPaths = changeSet.getNonCachedPaths();
        if( !nonCachedPaths.isEmpty() ) {
            for (SpaceEntryMutator spaceEntryMutator : mutators) {
                if (spaceEntryMutator instanceof SpaceEntryPathMutator) {
                    SpaceEntryPathMutator spaceEntryPathMutator =
                        (SpaceEntryPathMutator) spaceEntryMutator;
                    String path = spaceEntryPathMutator.getPath();
                    if ( nonCachedPaths.contains(path)) {
                        spaceEntryPathMutator.disablePathCaching();
                    }
                }
            }
        }

        return mutators;
    }

    public static ChangeSet fromParameters(Collection<SpaceEntryMutator> mutators, long lease) {
        return new ChangeSet(mutators, lease);
    }

    public static long getLease(ChangeSet changeSet) {
        return changeSet.getLease();
    }

    public static ChangeModifiers modifierFromCode(int code) {
        return new ChangeModifiers(code);
    }

}
