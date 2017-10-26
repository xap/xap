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

package com.gigaspaces.internal.cluster.node.impl.packets.data;

import com.gigaspaces.events.NotifyInfo;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.transport.ITemplatePacket;

@com.gigaspaces.api.InternalApi
public class SpaceEngineFixFacade
        implements IDataConsumeFixFacade {

    private final SpaceEngine _spaceEngine;

    public SpaceEngineFixFacade(SpaceEngine spaceEngine) {
        _spaceEngine = spaceEngine;
    }

    public void write(IEntryPacket entryPacket) throws Exception {
        _spaceEngine.write(entryPacket,
                null /* txn */,
                entryPacket.getTTL(),
                0 /*modifiers*/,
                true /* fromRepl */,
                false,
                null);
    }

    public void insertNotifyTemplate(ITemplatePacket notifyTemplate,
                                     String uid, NotifyInfo notifyInfo) throws Exception {
        _spaceEngine.notify(notifyTemplate,
                notifyTemplate.getTTL(),
                true /* fromRepl */,
                uid,
                null,
                notifyInfo);

    }

    public void addTypeDesc(ITypeDesc typeDescriptor) throws Exception {
        _spaceEngine.getTypeManager().addTypeDesc(typeDescriptor);
        if (_spaceEngine.getCacheManager().isBlobStoreCachePolicy()) //need to be stored in case blobStore recovery will be used
            _spaceEngine.getCacheManager().getStorageAdapter().introduceDataType(typeDescriptor);
    }

}
