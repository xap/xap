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

package com.gigaspaces.internal.client.spaceproxy.actioninfo;

import com.gigaspaces.client.ChangeSet;
import com.gigaspaces.client.ChangeSetInternalUtils;
import com.gigaspaces.client.mutators.SpaceEntryMutator;
import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.j_spaces.core.client.Modifiers;

import net.jini.core.transaction.Transaction;

import java.util.Collection;


/**
 * @author eitany
 * @since 9.1
 */
@com.gigaspaces.api.InternalApi
public class ChangeProxyActionInfo extends QueryProxyActionInfo {

    public final long timeout;
    public final long lease;
    public final Collection<SpaceEntryMutator> mutators;

    public ChangeProxyActionInfo(ISpaceProxy spaceProxy, Object template, ChangeSet changeSet,
                                 Transaction txn, long timeout, com.gigaspaces.client.ChangeModifiers modifiers) {
        super(spaceProxy, template, txn, modifiers.getCode(), true);

        if (template == null)
            throw new IllegalArgumentException("change operation cannot accept null template.");

        this.timeout = timeout;
        if (timeout < 0)
            throw new IllegalArgumentException("timeout parameter must be greater than or equal to zero.");
        this.mutators = ChangeSetInternalUtils.getMutators(changeSet);
        this.lease = ChangeSetInternalUtils.getLease(changeSet);
        if (lease < 0)
            throw new IllegalArgumentException("lease parameter must be greater than or equal to zero.");
        if (this.mutators.isEmpty() && this.lease == 0)
            throw new IllegalArgumentException("change operation cannot accept empty changeSet.");

        final boolean oneWay = Modifiers.contains(this.modifiers, Modifiers.ONE_WAY);
        if (oneWay) {
            if (txn != null || spaceProxy.getContextTransaction() != null)
                throw new UnsupportedOperationException("Oneway change is not supported when the change is being done under a transaction.");
            if (Modifiers.contains(this.modifiers, Modifiers.RETURN_DETAILED_CHANGE_RESULT))
                throw new IllegalArgumentException("Oneway change is not allowed with ChangeModifiers.RETURN_DETAILED_RESULTS modifier.");
        }

        if (_devLogger.isTraceEnabled()) {
            if (verifyLogScannedEntriesCountParams(this.timeout))
                this.modifiers |= Modifiers.LOG_SCANNED_ENTRIES_COUNT;
        }
    }

    public Collection<SpaceEntryMutator> getMutators() {
        return mutators;
    }
}
