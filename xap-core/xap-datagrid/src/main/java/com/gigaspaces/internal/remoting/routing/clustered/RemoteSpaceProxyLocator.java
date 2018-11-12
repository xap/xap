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

package com.gigaspaces.internal.remoting.routing.clustered;

import com.gigaspaces.internal.quiesce.QuiesceTokenProvider;
import com.gigaspaces.internal.server.space.IRemoteSpace;
import com.gigaspaces.logger.Constants;
import com.j_spaces.core.client.FinderException;
import com.j_spaces.core.client.LookupFinder;
import com.j_spaces.core.client.SpaceFinder;
import com.j_spaces.core.client.SpaceURL;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author idan
 * @since 9.0.0
 */
@SuppressWarnings("deprecation")
@com.gigaspaces.api.InternalApi
public class RemoteSpaceProxyLocator implements RemoteOperationsExecutorProxyLocator {
    private static final long LOOKUP_TIMEOUT = LookupFinder.SINGLE_LOOKUP_TIMEOUT;
    private final Logger _logger;

    private final SpaceURL _spaceURL;
    private final Map<String, SpaceURL> _membersUrls;

    private final QuiesceTokenProvider _quiesceTokenProvider;

    public RemoteSpaceProxyLocator(String name, SpaceURL spaceUrl, QuiesceTokenProvider quiesceTokenProvider) {
        this._logger = Logger.getLogger(Constants.LOGGER_SPACEPROXY_ROUTER_LOOKUP + '.' + name);
        this._spaceURL = spaceUrl;
        this._membersUrls = new ConcurrentHashMap<String, SpaceURL>();
        this._quiesceTokenProvider = quiesceTokenProvider;
    }

    public SpaceURL getMemberUrl(String memberName) {
        SpaceURL memberURL = _membersUrls.get(memberName);
        if (memberURL != null)
            return memberURL;

        memberURL = _spaceURL.clone();
        String[] names = memberName.split(":");
        memberURL.setContainerName(names[0]);
        memberURL.setSpaceName(names[1]);
        memberURL.refreshUrlString();
        _membersUrls.put(memberName, memberURL);
        return memberURL;
    }


    public RemoteOperationsExecutorProxy locateMember(String memberName, String spaceUuid, LookupType lookupType) {
        SpaceURL memberURL = getMemberUrl(memberName);
        if (_logger.isLoggable(Level.FINEST))
            _logger.log(Level.FINEST, "Looking for server " + memberName + " at [" + memberURL + "]...");

        try {
            IRemoteSpace proxy = SpaceFinder.findJiniSpace(memberURL, spaceUuid, memberURL.getCustomProperties(), LOOKUP_TIMEOUT, lookupType);
            if (_logger.isLoggable(Level.FINE))
                _logger.log(Level.FINE, "Server " + memberName + " was found at [" + memberURL + "].");
            return new RemoteOperationsExecutorProxy(memberName, proxy, _quiesceTokenProvider);
        } catch (FinderException e) {
            if (_logger.isLoggable(Level.FINEST))
                _logger.log(Level.FINEST, "Could not find server " + memberName + " at " + memberURL + ".", e);
            else if (_logger.isLoggable(Level.FINER))
                _logger.log(Level.FINER, "Could not find server " + memberName + " at " + memberURL + ".");

            return null;
        }
    }

    @Override
    public RemoteOperationsExecutorProxy locateMember(String memberName, LookupType lookupType) {
            return locateMember(memberName, null, lookupType);
        }
}
