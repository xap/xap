/*
 *
 * Copyright 2005 Sun Microsystems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package net.jini.discovery;

public class LookupGroups {
    private static final String[] ALL_GROUPS = null;
    private static final String[] NO_GROUPS = new String[0];

    /**
     * Convenience constant used to request that attempts be made to discover all lookup services
     * that are within range, and which belong to any group.
     */
    public static String[] all() {
        return ALL_GROUPS;
    }

    /**
     * Convenience constant used to request that discovery by group membership be halted (or not
     * started, if the group discovery mechanism is simply being instantiated).
     */
    public static String[] none() {
        return NO_GROUPS;
    }
}
