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
package com.gigaspaces.internal.server.space;

import com.gigaspaces.attribute_store.AttributeStore;
import com.gigaspaces.internal.zookeeper.ZNodePathFactory;
import org.slf4j.Logger;

import java.io.IOException;

/**
 * Created by tamirs
 * on 5/9/17.
 */
public class ZookeeperLastPrimaryHandler {

    private static final String SEPARATOR = "~";

    private final Logger _logger;
    private final String _attributeStoreKey;
    private final String attributeStoreValue;
    private final AttributeStore _attributeStore;

    public ZookeeperLastPrimaryHandler(SpaceImpl spaceImpl, AttributeStore attributeStore, Logger logger) {
        this._logger = logger;
        this._attributeStoreKey = toPath(spaceImpl.getName(), String.valueOf(spaceImpl.getPartitionIdOneBased()));
        this.attributeStoreValue = toId(spaceImpl.getInstanceId(), spaceImpl.getSpaceUuid().toString());
        this._attributeStore = attributeStore;
    }


    public void removeLastPrimaryRecord() throws IOException {
        _logger.info("Removing key ["+_attributeStoreKey+"] from ZK");
        _attributeStore.remove(_attributeStoreKey);
    }

    public void setMeAsLastPrimary() throws IOException {
        String previousLastPrimary = _attributeStore.set(_attributeStoreKey, attributeStoreValue);
        if (_logger.isInfoEnabled())
            _logger.info("Set as last primary ["+ attributeStoreValue +"] for key ["+_attributeStoreKey+"] in ZK. Previous last primary is ["+previousLastPrimary+"]");
    }

    public boolean isLastPrimary() {
        try {
            return attributeStoreValue.equals(getLastPrimaryName());
        } catch (IOException e) {
            _logger.warn("Failed to get last primary from ZK", e);
            return false;
        }
    }

    public String getLastPrimaryName() throws IOException {
        return _attributeStore.get(_attributeStoreKey);
    }

    public String getLastPrimaryNameMemoryXtend() throws IOException {
        String lastPrimary = this._attributeStore.get(_attributeStoreKey);
        if(lastPrimary == null)
            return null;

        String[] tokens = lastPrimary.split(SEPARATOR);
        if (tokens.length == 2)
            return tokens[0];

        _logger.warn("Invalid last primary value [" + lastPrimary + "] - expected " + toId("<instance_id>","<service_id>"));
        return null;
    }

    public String getAttributeStoreKey() {
        return _attributeStoreKey;
    }

    public static String getSeparator() {
        return SEPARATOR;
    }

    public static String toPath(String spaceName, String partitionId) {
        return ZNodePathFactory.space(spaceName, "leader-election", partitionId, "leader");
    }

    public static String toId(String instanceId, String uid) {
        return instanceId + SEPARATOR + uid;
    }
}
