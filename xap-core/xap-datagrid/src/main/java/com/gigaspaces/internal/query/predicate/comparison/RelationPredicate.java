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

package com.gigaspaces.internal.query.predicate.comparison;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.cache.QueryExtensionIndexManagerWrapper;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @since 11.0 Created by Barak Bar Orion 8/24/15.
 */

@com.gigaspaces.api.InternalApi
public class RelationPredicate extends ScalarSpacePredicate {
    private static final long serialVersionUID = 1L;
    private String namespace;
    private String typeName;
    private String path;
    private String op;
    private transient QueryExtensionIndexManagerWrapper _handler;
    private transient CacheManager _cacheManager;


    public RelationPredicate() {
    }

    @Override
    public boolean requiresCacheManagerForExecution() {
        return true;
    }

    @Override
    public void setCacheManagerForExecution(CacheManager cacheManager) {
        this._cacheManager = cacheManager;
    }

    public RelationPredicate(String namespace, String typeName, String path, String op, Object value /* todo change to shape*/) {
        super(value, null);
        this.namespace = namespace;
        this.typeName = typeName;
        this.path = path;
        this.op = op;
    }

    @Override
    protected boolean match(Object actual, Object expected) {
        if (actual == null) {
            return false;
        }
        if (_handler == null) {
            _handler = _cacheManager.getQueryExtensionManager(namespace);
            if (_handler == null) {
                throw new IllegalStateException("Unknown namespace [" + namespace + "]");
            }
        }
        return _handler.filter(typeName, path, op, actual, expected);
    }

    @Override
    protected String getOperatorName() {
        return namespace + ":" + op;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        IOUtils.writeString(out, namespace);
        IOUtils.writeString(out, typeName);
        if(LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v12_1_0)) {
            IOUtils.writeString(out, path);
        }
        IOUtils.writeString(out, op);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        namespace = IOUtils.readString(in);
        typeName = IOUtils.readString(in);
        if(LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v12_1_0)) {
            path = IOUtils.readString(in);
        }
        op = IOUtils.readString(in);
    }
}
