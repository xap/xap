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

import com.gigaspaces.internal.cluster.node.impl.groups.sync.ISyncReplicationGroupOutContext;
import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.extension.XapExtensions;
import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.logger.Constants;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@com.gigaspaces.api.InternalApi
public class ReplicationLogUtils {
    public static String toShortLookupName(String lookupName) {
        return XapExtensions.getInstance().getReplicationUtils().toShortLookupName(lookupName);
    }

    public static String packetsToLogString(
            List<IReplicationOrderedPacket> packets) {
        StringBuilder result = new StringBuilder();
        for (IReplicationOrderedPacket packet : packets) {
            result.append(packet);
            result.append(", ");
        }
        if (packets.isEmpty())
            result.append("NONE");
        else
            result.setLength(result.length() - 2);
        return result.toString();
    }

    public static String packetsToLogString(
            ISyncReplicationGroupOutContext context) {
        if (context.isSinglePacket())
            return context.getSinglePacket().toString();
        return packetsToLogString(context.getOrderedPackets());
    }

    public static Logger createOutgoingChannelSpecificLogger(String myLookupName, String targetLookupName, String groupName) {
        return LoggerFactory.getLogger(Constants.LOGGER_REPLICATION_CHANNEL
                + ".out." + toShortLookupName(myLookupName)
                + "." + toShortGroupName(groupName) + "."
                + toShortLookupName(targetLookupName));
    }

    public static Logger createIncomingChannelSpecificLogger(String myLookupName, String targetLookupName, String groupName) {
        return LoggerFactory.getLogger(Constants.LOGGER_REPLICATION_CHANNEL
                + ".in." + toShortLookupName(myLookupName)
                + "." + toShortGroupName(groupName) + "."
                + toShortLookupName(targetLookupName));
    }

    public static Logger createChannelSpecificLogger(String myLookupName, String targetLookupName, String groupName) {
        return LoggerFactory.getLogger(Constants.LOGGER_REPLICATION_CHANNEL
                + "." + toShortLookupName(myLookupName)
                + "." + toShortGroupName(groupName) + "."
                + toShortLookupName(targetLookupName));
    }

    public static Logger createChannelSpecificVerboseLogger(String myLookupName, String targetLookupName, String groupName) {
        return LoggerFactory.getLogger(Constants.LOGGER_REPLICATION_CHANNEL_VERBOSE
                + "." + toShortLookupName(myLookupName)
                + "." + toShortGroupName(groupName) + "."
                + toShortLookupName(targetLookupName));
    }

    public static String toShortGroupName(String groupName) {
        if (!StringUtils.hasText(groupName))
            return groupName;
        if (!groupName.contains(":"))
            return groupName;
        return groupName.substring(groupName.lastIndexOf(":") + 1);
    }
}
