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

package org.openspaces.jpa.openjpa;

import org.apache.openjpa.kernel.BrokerImpl;
import org.apache.openjpa.kernel.OpCallbacks;
import org.apache.openjpa.kernel.StateManager;
import org.apache.openjpa.kernel.StateManagerImpl;
import org.apache.openjpa.lib.util.Localizer;
import org.apache.openjpa.meta.ClassMetaData;
import org.apache.openjpa.meta.FieldMetaData;
import org.apache.openjpa.util.OpenJPAException;
import org.apache.openjpa.util.UserException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * GigaSpaces OpenJPA's Broker implementation.
 *
 * @author Idan Moyal
 * @since 8.0.1
 */
public class Broker extends BrokerImpl {
    //
    private static final long serialVersionUID = 1L;

    public Broker() {
        super();

    }

    /**
     * Create a state manager for the given oid and metadata.
     */
    @Override
    protected StateManagerImpl newStateManagerImpl(Object oid, ClassMetaData meta) {
        return new StateManager(oid, meta, this);
    }

    /**
     * Persist the provided Collection elements.
     */
    public void persistCollection(Collection<?> collection, boolean explicit, OpCallbacks call,
                                  StateManager ownerStateManager, FieldMetaData fieldMetaData) {

        if (collection.isEmpty())
            return;

        beginOperation(true);
        List<Exception> exceps = null;
        try {
            assertWriteOperation();

            for (Object object : collection) {
                try {
                    StateManager stateManager = (StateManager) persist(object, null, explicit, call);
                    stateManager.setOwnerInformation(ownerStateManager, fieldMetaData);
                } catch (UserException e) {
                    if (exceps == null)
                        exceps = new ArrayList<Exception>();
                    exceps.add(e);
                }
            }
        } finally {
            endOperation();
        }

        // Throw exception if needed
        if (exceps != null && !exceps.isEmpty()) {
            boolean fatal = false;
            Throwable[] throwables = exceps.toArray(new Throwable[exceps.size()]);
            for (int i = 0; i < throwables.length; i++) {
                if (throwables[i] instanceof OpenJPAException
                        && ((OpenJPAException) throwables[i]).isFatal())
                    fatal = true;
            }

            Localizer loc = Localizer.forPackage(BrokerImpl.class);
            OpenJPAException err = new UserException(loc.get("nested-exceps"));
            throw err.setNestedThrowables(throwables).setFatal(fatal);
        }

    }


}
