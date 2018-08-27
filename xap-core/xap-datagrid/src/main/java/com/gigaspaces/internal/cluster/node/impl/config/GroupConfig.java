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

package com.gigaspaces.internal.cluster.node.impl.config;

import java.util.Arrays;


@com.gigaspaces.api.InternalApi
public class GroupConfig {

    private final String _name;
    private final String[] _membersLookupNames;
    private int _historyLength = 50; //TODO configurable?
    private boolean _enableNetworkCompression;

    public GroupConfig(String groupName, String... membersLookupNames) {
        _name = groupName;
        _membersLookupNames = membersLookupNames;
    }

    public GroupConfig(String groupName, boolean enableNetworkCompression, String... membersLookupNames) {
        _name = groupName;
        _membersLookupNames = membersLookupNames;
        _enableNetworkCompression = enableNetworkCompression;
    }

    public String[] getMembersLookupNames() {
        return _membersLookupNames;
    }

    public String getName() {
        return _name;
    }

    public int getHistoryLength() {
        return _historyLength;
    }

    public void setHistoryLength(int historyLength) {
        _historyLength = historyLength;
    }

    public boolean isNetworkCompressionEnabled() {
        return _enableNetworkCompression;
    }

    public void setEnableNetworkCompression(boolean enableNetworkCompression) {
        this._enableNetworkCompression = enableNetworkCompression;
    }

    @Override
    public String toString() {
        return "GroupConfig [_name=" + _name + ", _membersLookupNames="
                + Arrays.toString(_membersLookupNames) + ", _historyLength="
                + _historyLength + ", _enableNetworkCompression=" + _enableNetworkCompression +  "]";
    }

}