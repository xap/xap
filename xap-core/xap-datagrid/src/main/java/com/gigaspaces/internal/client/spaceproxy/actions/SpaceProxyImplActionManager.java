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

package com.gigaspaces.internal.client.spaceproxy.actions;

import com.gigaspaces.internal.client.spaceproxy.SpaceProxyImpl;
import com.gigaspaces.internal.client.spaceproxy.executors.SpaceProxyImplTypeDescriptorActionsExecutor;
import com.gigaspaces.internal.client.spaceproxy.executors.TypeDescriptorActionsProxyExecutor;

@com.gigaspaces.api.InternalApi
public class SpaceProxyImplActionManager extends AbstractSpaceProxyActionManager<SpaceProxyImpl> {
    public SpaceProxyImplActionManager(SpaceProxyImpl spaceProxy) {
        super(spaceProxy);
    }

    @Override
    protected TypeDescriptorActionsProxyExecutor<SpaceProxyImpl> createTypeDescriptorActionsExecutor() {
        return new SpaceProxyImplTypeDescriptorActionsExecutor();
    }

    @Override
    protected AdminProxyAction<SpaceProxyImpl> createAdminProxyAction() {
        return new SpaceProxyImplAdminAction();
    }

    @Override
    protected CountClearProxyAction<SpaceProxyImpl> createCountClearProxyAction() {
        return new SpaceProxyImplCountClearAction();
    }

    @Override
    protected ReadTakeProxyAction<SpaceProxyImpl> createReadTakeProxyAction() {
        return new SpaceProxyImplReadTakeAction();
    }

    @Override
    protected ReadTakeMultipleProxyAction<SpaceProxyImpl> createReadTakeMultipleProxyAction() {
        return new SpaceProxyImplReadTakeMultipleAction();
    }

    @Override
    protected ReadTakeByIdsProxyAction<SpaceProxyImpl> createReadTakeByIdsProxyAction() {
        return new SpaceProxyImplReadTakeByIdsAction();
    }

    @Override
    protected SnapshotProxyAction<SpaceProxyImpl> createSnapshotProxyAction() {
        return new SpaceProxyImplSnapshotAction();
    }

    @Override
    protected WriteProxyAction<SpaceProxyImpl> createWriteProxyAction() {
        return new SpaceProxyImplWriteAction();
    }

    @Override
    protected ReadTakeEntriesUidsProxyAction<SpaceProxyImpl> createReadTakeEntriesUidsProxyAction() {
        return new SpaceProxyImplReadTakeEntriesUidsAction();
    }

    @Override
    protected ChangeProxyAction<SpaceProxyImpl> createChangeProxyAction() {
        return new SpaceProxyImplChangeAction();
    }

    @Override
    protected AggregateProxyAction<SpaceProxyImpl> createAggregateAction() {
        return new SpaceProxyImplAggregateAction();
    }
}
