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

package com.gigaspaces.internal.cluster.node.impl;

/**
 * @author Niv Ingberg
 * @since 12.0
 */
@com.gigaspaces.api.InternalApi
public class ReplicationUtils {

    public String toChannelName(String lookupName) {
        return lookupName;
    }

    public String toShortLookupName(String lookupName) {
        if (lookupName == null)
            return null;
        String[] split = lookupName.split(":");
        if (split[0].contains("_container")) {
            if (split.length < 2)
                throw new IllegalArgumentException("Illegal space name '" + lookupName + "', should be space_name_container:space_name.");
            if (split[1].contains("LocalView")) {
                String lastSection = split[split.length -1];
                int indexOfUnderScore = lastSection.lastIndexOf("_");
                int indexOfDashAfterUnderscore = lastSection.substring(indexOfUnderScore).indexOf("-") + indexOfUnderScore;
                return "LocalView" + lastSection.substring(indexOfUnderScore, indexOfDashAfterUnderscore);
            }

            return split[0].replace("_container", "");
        }
        if (split.length > 1 && split[1].contains("NotifyDur")) {
            String lastSection = split[split.length -1];
            int indexOfUnderScore = lastSection.lastIndexOf("_");
            int indexOfDashAfterUnderscore = lastSection.substring(indexOfUnderScore).indexOf("-") + indexOfUnderScore;
            return "NotifyDur" + lastSection.substring(indexOfUnderScore, indexOfDashAfterUnderscore);
        }
        return lookupName;
    }
}
