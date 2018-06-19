/*******************************************************************************
 * Copyright (c) 2012 GigaSpaces Technologies Ltd. All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ******************************************************************************/
package org.apache.openjpa.kernel;

import org.apache.openjpa.meta.ClassMetaData;
import org.apache.openjpa.meta.FieldMetaData;
import org.apache.openjpa.meta.JavaTypes;
import org.apache.openjpa.meta.ValueMetaData;
import org.openspaces.jpa.openjpa.Broker;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

/**
 * A GigaSpaces extended version of OpenJPA's StateManager which has an additional member for
 * storing an entity's owner reference for supporting GigaSpaces owned relationships model.
 *
 * @author Idan Moyal
 * @since 8.0.1
 */
public class StateManager extends StateManagerImpl {
    //
    private static final long serialVersionUID = 1L;

    /**
     * Used for storing the relationship's owner state manager. If the state manager is the root of
     * the relationship & therefore has no owner, the value is null.
     */
    private StateManager _ownerStateManager;

    private FieldMetaData _ownerMetaData;
    private boolean _cleared = false;

    public StateManager(Object id, ClassMetaData meta, BrokerImpl broker) {
        super(id, meta, broker);
    }

    /**
     * Sets the state manager's owner information (state manager & field meta data)
     *
     * @param ownerStateManager The owner's state manager.
     * @param ownerMetaData     The owner's field meta data.
     */
    public void setOwnerInformation(StateManager ownerStateManager, FieldMetaData ownerMetaData) {
        this._ownerStateManager = ownerStateManager;
        this._ownerMetaData = ownerMetaData;

    }

    /**
     * @return The state manager's Owner state manager.
     */
    public StateManager getOwnerStateManager() {
        return _ownerStateManager;
    }

    /**
     * @return The state manager Owner's field meta data.
     */
    public FieldMetaData getOwnerMetaData() {
        return _ownerMetaData;
    }

    /**
     * Called after an instance is persisted by a user through the broker. Cascades the persist
     * operation to fields marked {@link ValueMetaData#CASCADE_IMMEDIATE}.
     */
    @Override
    void cascadePersist(OpCallbacks call) {
        FieldMetaData[] fmds = getMetaData().getFields();
        for (int i = 0; i < fmds.length; i++) {
            if (!getLoaded().get(i))
                continue;

            if (fmds[i].getCascadePersist() == ValueMetaData.CASCADE_IMMEDIATE
                    || fmds[i].getKey().getCascadePersist() == ValueMetaData.CASCADE_IMMEDIATE
                    || fmds[i].getElement().getCascadePersist() == ValueMetaData.CASCADE_IMMEDIATE) {
                cascadePersist(i, call, fetchField(i, false));
            }
        }
    }

    /**
     * Cascade-persists the provided value and setting an owner for the owned relationships entities
     * (One-to-one & One-to-many).
     *
     * @param field The field to cascade persist.
     * @param call  Operation call back.
     * @param value The field value to cascade persist.
     */
    private void cascadePersist(int field, OpCallbacks call, Object value) {
        if (value == null)
            return;

        FieldMetaData fmd = getMetaData().getField(field);
        Broker broker = (Broker) getBroker();
        switch (fmd.getDeclaredTypeCode()) {
            case JavaTypes.PC:
            case JavaTypes.PC_UNTYPED:
                if (!broker.isDetachedNew() && broker.isDetached(value))
                    return; // allow but ignore
                StateManager stateManager = (StateManager) broker.persist(value, null, true, call);
                // Set owner reference for one-to-one relationship only
                if (fmd.getAssociationType() == FieldMetaData.ONE_TO_ONE)
                    stateManager.setOwnerInformation(this, fmd);
                break;
            case JavaTypes.ARRAY:
                broker.persistCollection(Arrays.asList((Object[]) value), true, call, this, fmd);
                break;
            case JavaTypes.COLLECTION:
                broker.persistCollection((Collection<?>) value, true, call, this, fmd);
                break;
            case JavaTypes.MAP:
                if (fmd.getKey().getCascadePersist()
                        == ValueMetaData.CASCADE_IMMEDIATE)
                    broker.persistCollection(((Map<?, ?>) value).keySet(), true, call, this, fmd);
                if (fmd.getElement().getCascadePersist()
                        == ValueMetaData.CASCADE_IMMEDIATE)
                    broker.persistCollection(((Map<?, ?>) value).values(), true, call, this, fmd);
                break;
        }
    }

    @Override
    void clearFields() {
        super.clearFields();
        _cleared = true;
    }

    /**
     * Gets whether the fields of this state manager have been cleared.
     *
     * @return true if fields were cleared, otherwise false.
     */
    public boolean isCleared() {
        return _cleared;
    }

    /**
     * Resets the cleared fields states to false.
     */
    public void resetClearedState() {
        this._cleared = false;
    }


}
