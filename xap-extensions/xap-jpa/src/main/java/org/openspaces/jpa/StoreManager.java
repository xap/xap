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

package org.openspaces.jpa;

import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.internal.client.QueryResultTypeInternal;
import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.client.spaceproxy.metadata.ObjectType;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.metadata.SpaceTypeInfo;
import com.gigaspaces.internal.metadata.SpaceTypeInfoRepository;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.internal.transport.TemplatePacketFactory;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.client.ReadModifiers;
import com.j_spaces.core.client.UpdateModifiers;
import com.j_spaces.jdbc.QueryProcessorFactory;
import com.j_spaces.jdbc.driver.GConnection;

import net.jini.core.entry.UnusableEntryException;
import net.jini.core.lease.Lease;
import net.jini.core.transaction.Transaction;
import net.jini.core.transaction.TransactionException;
import net.jini.core.transaction.TransactionFactory;

import org.apache.openjpa.abstractstore.AbstractStoreManager;
import org.apache.openjpa.conf.OpenJPAConfiguration;
import org.apache.openjpa.enhance.PersistenceCapable;
import org.apache.openjpa.kernel.FetchConfiguration;
import org.apache.openjpa.kernel.LockManager;
import org.apache.openjpa.kernel.OpenJPAStateManager;
import org.apache.openjpa.kernel.PCState;
import org.apache.openjpa.kernel.QueryLanguages;
import org.apache.openjpa.kernel.StateManager;
import org.apache.openjpa.kernel.StoreQuery;
import org.apache.openjpa.kernel.exps.ExpressionParser;
import org.apache.openjpa.lib.rop.ResultObjectProvider;
import org.apache.openjpa.meta.ClassMetaData;
import org.apache.openjpa.meta.FieldMetaData;
import org.apache.openjpa.util.ApplicationIds;
import org.openspaces.jpa.openjpa.SpaceConfiguration;
import org.openspaces.jpa.openjpa.StoreManagerQuery;
import org.openspaces.jpa.openjpa.StoreManagerSQLQuery;

import java.lang.reflect.Method;
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Stack;

import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;

/**
 * A GigaSpaces back-end implementation for OpenJPA. Responsible for storing and fetching data from
 * GigaSpaces using space API.
 *
 * @author idan
 * @since 8.0
 */
@SuppressWarnings("unchecked")
public class StoreManager extends AbstractStoreManager {
    //
    private Transaction _transaction = null;
    private static final Map<Class<?>, Integer> _classesRelationStatus = new HashMap<Class<?>, Integer>();
    private static final HashSet<Class<?>> _processedClasses = new HashSet<Class<?>>();
    private GConnection _connection;
    private RelationsManager _relationsManager;

    public StoreManager() {
        _relationsManager = new RelationsManager();
    }

    @Override
    protected void open() {
        // Specific gigaspaces initialization (space proxy)
        getConfiguration().initialize();
    }

    @Override
    protected Collection<String> getUnsupportedOptions() {
        Collection<String> unsupportedOptions = super.getUnsupportedOptions();
        unsupportedOptions.remove(OpenJPAConfiguration.OPTION_ID_DATASTORE);
        unsupportedOptions.remove(OpenJPAConfiguration.OPTION_OPTIMISTIC);
        unsupportedOptions.remove(OpenJPAConfiguration.OPTION_INC_FLUSH);
        return unsupportedOptions;
    }

    @Override
    public boolean syncVersion(OpenJPAStateManager sm, Object edata) {
        try {
            // If there's no version field return false
            // Object will be loaded from space by OpenJPA
            if (!getConfiguration().getOptimistic() || sm.getMetaData().getVersionField() == null)
                return false;

            // Verify version
            IEntryPacket result = readObjectFromSpace(sm);
            if (result == null)
                return false;

            Object spaceVersion = result.getVersion();
            if (spaceVersion == null)
                throw new IllegalStateException("Entity of type: " + result.getTypeName() + " with Id: " + result.getID()
                        + " expected to have a value in its version property.");

            return sm.getVersion().equals(spaceVersion);

        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public void begin() {
        try {
            if (_transaction != null) {
                if (getConfiguration().getOptimistic())
                    return;
                throw new TransactionException("Attempted to start a new transaction when there's already an active transaction.");
            }
            long timeout = (getConfiguration().getLockTimeout() == 0) ?
                    Lease.FOREVER : getConfiguration().getLockTimeout();
            _transaction = (TransactionFactory.create(getConfiguration().getTransactionManager(),
                    timeout)).transaction;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public void commit() {
        try {
            long timeout = (getConfiguration().getLockTimeout() == 0) ?
                    Lease.FOREVER : getConfiguration().getLockTimeout();
            _transaction.commit(timeout);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        } finally {
            _transaction = null;
        }
    }

    @Override
    public void rollback() {
        try {
            _transaction.abort(Long.MAX_VALUE);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        } finally {
            _transaction = null;
        }
    }

    @Override
    public void beginOptimistic() {
        // Do nothing... (a transaction for rollback purpose will be started on flush)
    }

    @Override
    public void rollbackOptimistic() {
        if (_transaction != null)
            rollback();
    }

    @Override
    public StoreQuery newQuery(String language) {
        ExpressionParser ep = QueryLanguages.parserForLanguage(language);

        if (ep != null)
            return new StoreManagerQuery(ep, this);

        if (QueryLanguages.LANG_SQL.equals(language)) {
            return new StoreManagerSQLQuery(this);
        }

        return null;
    }

    @Override
    protected OpenJPAConfiguration newConfiguration() {
        return new SpaceConfiguration();
    }

    public SpaceConfiguration getConfiguration() {
        return (SpaceConfiguration) getContext().getConfiguration();
    }

    /**
     * Returns whether the state manager's managed object exists in space.
     */
    public boolean exists(OpenJPAStateManager sm, Object edata) {
        ClassMetaData cm = sm.getMetaData();
        final Object[] ids = ApplicationIds.toPKValues(sm.getObjectId(), cm);
        ISpaceProxy proxy = (ISpaceProxy) getConfiguration().getSpace();
        try {
            Object result = proxy.readById(cm.getDescribedType().getName(), ids[0], null, _transaction,
                    0, ReadModifiers.DIRTY_READ, false, QueryResultTypeInternal.EXTERNAL_ENTRY, null);
            return result != null;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public boolean isCached(List<Object> oids, BitSet edata) {
        return false;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Collection loadAll(Collection sms, PCState state, int load, FetchConfiguration fetch, Object edata) {
        final List<Object> failedIds = new ArrayList<Object>();
        for (OpenJPAStateManager sm : (Collection<OpenJPAStateManager>) sms) {
            BitSet fields = new BitSet(sm.getMetaData().getFields().length);
            LockManager lm = sm.getContext().getLockManager();
            if (!load(sm, fields, fetch, lm.getLockLevel(sm), edata))
                failedIds.add(sm.getId());
        }
        return failedIds;
    }

    @Override
    public boolean initialize(OpenJPAStateManager sm, PCState state,
                              FetchConfiguration fetchConfiguration, Object edata) {

        final ClassMetaData cm = sm.getMetaData();
        try {
            // If we already have the result and only need to initialize.. (relevant for nested objects & JPQL)
            IEntryPacket result =
                    (edata == null) ? readObjectFromSpace(sm) : (IEntryPacket) edata;
            if (result == null)
                return false;

            // Initialize
            sm.initialize(cm.getDescribedType(), state);
            loadFields(sm, result, cm.getFields());
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }

        return true;
    }

    /**
     * Loads the provided IEntryPacket field values to the provided StateManager. Note that for
     * gaining better performance OneToOne & OneToMany relationships are loaded but not initialized
     * (lazy initialization).
     *
     * @param sm    The state manager.
     * @param entry The IEntryPacket containing the field values.
     * @param fms   The fields meta data.
     */
    private void loadFields(OpenJPAStateManager sm, IEntryPacket entry, FieldMetaData[] fms) {
        int spacePropertyIndex = -1;

        for (int i = 0; i < fms.length; i++) {
            //ignore version which is not part of the entry packet
            if (fms[i].isVersion())
                continue;

            spacePropertyIndex++;

            // Skip primary keys and non-persistent keys
            if (fms[i].isPrimaryKey() || sm.getLoaded().get(fms[i].getIndex()))
                continue;

            Integer associationType = _classesRelationStatus.get(fms[i].getElement().getDeclaredType());
            if (associationType != null)
                fms[i].setAssociationType(associationType);

            // Handle one-to-one
            if (fms[i].getAssociationType() == FieldMetaData.ONE_TO_ONE) {
                sm.store(i, entry.getFieldValue(spacePropertyIndex));
                sm.getLoaded().set(fms[i].getIndex(), false);

                // Handle one-to-many
            } else if (fms[i].getAssociationType() == FieldMetaData.ONE_TO_MANY) {
                sm.store(i, entry.getFieldValue(spacePropertyIndex));
                sm.getLoaded().set(fms[i].getIndex(), false);

                // Handle embedded property
            } else if (fms[i].isEmbeddedPC()) {
                loadEmbeddedObject(fms[i], sm, entry.getFieldValue(spacePropertyIndex));

                // Otherwise, store the value as is
            } else {
                sm.store(i, entry.getFieldValue(spacePropertyIndex));
            }
        }

        sm.setVersion(entry.getVersion());
        ((StateManager) sm).resetClearedState();
    }

    /**
     * Loads a One-to-one relationship object to the provided owner's state manager.
     *
     * @param fmd        The owner's field meta data.
     * @param sm         The owner's state manager.
     * @param fieldValue The One-to-one field value to load into the owner's state manager.
     */
    private void loadOneToOneObject(FieldMetaData fmd, OpenJPAStateManager sm, Object fieldValue) {
        if (fieldValue == null) {
            sm.storeObject(fmd.getIndex(), null);
        } else {
            final ISpaceProxy proxy = (ISpaceProxy) getConfiguration().getSpace();
            final IEntryPacket entry = proxy.getDirectProxy().getTypeManager().getEntryPacketFromObject(
                    fieldValue, ObjectType.POJO);
            final ClassMetaData cmd = fmd.getDeclaredTypeMetaData();
            final Object oid = ApplicationIds.fromPKValues(new Object[]{entry.getID()}, cmd);
            final BitSet exclude = new BitSet(cmd.getFields().length);
            final Object managedObject = getContext().find(oid, null, exclude, entry, 0);
            _relationsManager.setOwnerStateManagerForPersistentInstance(managedObject, sm, fmd);
            sm.storeObject(fmd.getIndex(), managedObject);
        }
    }

    /**
     * Loads an embedded object field.
     *
     * @param fmd        The embedded field meta data.
     * @param sm         The parent object state manager.
     * @param fieldValue The value to load for the embedded field.
     */
    private void loadEmbeddedObject(FieldMetaData fmd, OpenJPAStateManager sm, Object fieldValue) {
        if (fieldValue == null) {
            sm.storeObject(fmd.getIndex(), null);
        } else {
            if (fieldValue != null) {
                final OpenJPAStateManager em = ctx.embed(null, null, sm, fmd);
                ((StateManager) em).setOwnerInformation((StateManager) sm, fmd);
                sm.storeObject(fmd.getIndex(), em.getManagedInstance());
                final ISpaceProxy proxy = (ISpaceProxy) getConfiguration().getSpace();
                final IEntryPacket entry = proxy.getDirectProxy().getTypeManager().getEntryPacketFromObject(
                        fieldValue, ObjectType.POJO);
                loadFields(em, entry, fmd.getDeclaredTypeMetaData().getFields());
            }
        }
    }

    /**
     * Loads One-to-many relationship objects to the owner's state manager.
     *
     * @param fmd        The One-to-many field's meta data.
     * @param sm         The owner's state manager.
     * @param fieldValue The value to be stored for the current field.
     */
    private void loadOneToManyObjects(FieldMetaData fmd, OpenJPAStateManager sm, Object fieldValue) {
        final Object collection = sm.newProxy(fmd.getIndex());

        if (fieldValue != null) {
            final ISpaceProxy proxy = (ISpaceProxy) getConfiguration().getSpace();
            final ClassMetaData cmd = fmd.getElement().getDeclaredTypeMetaData();
            final BitSet exclude = new BitSet(cmd.getFields().length);

            // Initialize each of the collection's items
            for (Object item : (Collection<?>) fieldValue) {
                final IEntryPacket entry = proxy.getDirectProxy().getTypeManager().getEntryPacketFromObject(
                        item, ObjectType.POJO);
                final Object oid = ApplicationIds.fromPKValues(new Object[]{entry.getID()}, cmd);
                // Initialize a state manager for the current item
                final Object managedObject = getContext().find(oid, null, exclude, entry, 0);
                _relationsManager.setOwnerStateManagerForPersistentInstance(managedObject, sm, fmd);
                ((Collection<Object>) collection).add(managedObject);
            }
        }
        sm.storeObject(fmd.getIndex(), collection);
    }

    /**
     * Reads an IEntryPacket implementation from space according to the provided StateManager.
     *
     * @param sm The state manager.
     * @return The IEntryPacket implementation for the provided StateManager.
     */
    private IEntryPacket readObjectFromSpace(OpenJPAStateManager sm)
            throws UnusableEntryException, TransactionException, InterruptedException, RemoteException {
        IEntryPacket result;
        final ISpaceProxy proxy = (ISpaceProxy) getConfiguration().getSpace();
        final ITypeDesc typeDescriptor = proxy.getDirectProxy().getTypeManager().getTypeDescByName(
                sm.getMetaData().getDescribedType().getName());
        final Object[] ids = ApplicationIds.toPKValues(sm.getObjectId(), sm.getMetaData());
        final int readModifier = (_transaction != null) ? getConfiguration().getReadModifier()
                : ReadModifiers.REPEATABLE_READ;

        ITemplatePacket template;
        if (typeDescriptor.isAutoGenerateId())
            template = TemplatePacketFactory.createUidPacket((String) ids[0], null, 0, QueryResultTypeInternal.OBJECT_JAVA);
        else
            template = TemplatePacketFactory.createIdPacket(ids[0], null, 0, typeDescriptor, QueryResultTypeInternal.OBJECT_JAVA, null);
        result = (IEntryPacket) proxy.read(template, _transaction, 0, readModifier);
        return result;
    }

    /**
     * This method loads specific fields from the data store for updating them. Note: The state
     * manager's fields are cleared.
     */
    @Override
    public boolean load(OpenJPAStateManager sm, BitSet fields, FetchConfiguration fetch, int lockLevel, Object context) {
        final ClassMetaData cm = sm.getMetaData();
        final StateManager stateManager = (StateManager) sm;
        final SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(cm.getDescribedType());
        final StateManager gsm = (StateManager) sm;
        try {
            if (!gsm.isCleared()) {
                loadSpecificFields(sm, fields, typeInfo);
                return true;
            } else {
                // If this is a relationship owner, read object from space and lazy initialize its fields
                // And initialize the fields specified in the provided 'fields' BitSet
                if (stateManager.getOwnerStateManager() == null) {
                    final IEntryPacket entry = readObjectFromSpace(sm);
                    if (entry == null)
                        return false;
                    loadFields(sm, entry, cm.getFields());
                    loadSpecificFields(sm, fields, typeInfo);
                    return true;

                    // If this is an owned instance (Owner->Pet), read owner from space and find
                    // the instance according to its Id and load & initialize its fields.
                } else {
                    // Save route to owner state manager
                    Stack<StateManager> sms = new Stack<StateManager>();
                    StateManager stateManagerToRead = stateManager;
                    while (stateManagerToRead.getOwnerStateManager() != null) {
                        sms.push(stateManagerToRead);
                        stateManagerToRead = stateManagerToRead.getOwnerStateManager();
                    }
                    final IEntryPacket entry = readObjectFromSpace(stateManagerToRead);
                    if (entry == null)
                        return false;
                    // Find the desired instance 
                    final IEntryPacket foundEntryPacket = _relationsManager.findObjectInEntry(stateManagerToRead, entry, sms);
                    if (foundEntryPacket == null)
                        return false;
                    loadFields(sm, foundEntryPacket, sm.getMetaData().getFields());
                    loadSpecificFields(sm, fields, typeInfo);
                }
                return true;
            }
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    /**
     * Load the fields specified in the provided 'fields' BitSet.
     *
     * @param sm       The state manager to load fields for.
     * @param fields   The fields to load.
     * @param typeInfo {@link SpaceTypeInfo} used for reflection.
     */
    private void loadSpecificFields(OpenJPAStateManager sm, BitSet fields, final SpaceTypeInfo typeInfo) {
        for (FieldMetaData fmd : sm.getMetaData().getFields()) {
            if (fields.get(fmd.getIndex())) {
                Object instance = sm.getManagedInstance();

                // Remove state manager before using reflection
                ((PersistenceCapable) instance).pcReplaceStateManager(null);
                Object value = typeInfo.getProperty(fmd.getName()).getValue(sm.getManagedInstance());
                ((PersistenceCapable) instance).pcReplaceStateManager(sm);

                if (fmd.getAssociationType() == FieldMetaData.ONE_TO_MANY) {
                    loadOneToManyObjects(fmd, sm, value);

                } else if (fmd.getAssociationType() == FieldMetaData.ONE_TO_ONE) {
                    loadOneToOneObject(fmd, sm, value);

                } else if (fmd.isEmbeddedPC()) {
                    loadEmbeddedObject(fmd, sm, value);
                }
            }
        }
    }

    @Override
    public ResultObjectProvider executeExtent(ClassMetaData classmetadata, boolean flag,
                                              FetchConfiguration fetchconfiguration) {
        return null;
    }

    /**
     * Flushes changes to GigaSpaces. Returns a list of exceptions that occurred.
     */
    @SuppressWarnings({"rawtypes"})
    @Override
    protected Collection flush(Collection pNew, Collection pNewUpdated, Collection pNewFlushedDeleted,
                               Collection pDirty, Collection pDeleted) {

        if (getContext().getBroker().getOptimistic() && _transaction == null)
            begin();

        IJSpace space = getConfiguration().getSpace();

        ArrayList<Exception> exceptions = new ArrayList<Exception>();

        if (_relationsManager.shouldInitializeClassesRelationStatus())
            _relationsManager.initializeClassesRelationStatus();

        if (pNew.size() > 0)
            handleNewObjects(pNew, space);

        if (pDirty.size() > 0)
            handleUpdatedObjects(pDirty, exceptions, space);

        if (pDeleted.size() > 0)
            handleDeletedObjects(pDeleted, exceptions, space);

        return exceptions;
    }

    /**
     * Clears the removed objects from the space.
     */
    private void handleDeletedObjects(Collection<OpenJPAStateManager> sms, ArrayList<Exception> exceptions, IJSpace space) {
        for (OpenJPAStateManager sm : sms) {
            ClassMetaData cm = sm.getMetaData();
            if (_classesRelationStatus.containsKey(cm.getDescribedType()))
                continue;
            try {
                // Remove object from space
                final Object[] ids = ApplicationIds.toPKValues(sm.getObjectId(), cm);
                final ISpaceProxy proxy = (ISpaceProxy) space;
                final ITypeDesc typeDescriptor = proxy.getDirectProxy().getTypeManager().getTypeDescByName(sm.getMetaData().getDescribedType().getName());
                final Object routing = sm.fetch(typeDescriptor.getRoutingPropertyId());
                ITemplatePacket template;
                if (typeDescriptor.isAutoGenerateId())
                    template = TemplatePacketFactory.createUidPacket((String) ids[0], routing, 0, QueryResultTypeInternal.OBJECT_JAVA);
                else
                    template = TemplatePacketFactory.createIdPacket(ids[0], routing, 0, typeDescriptor, QueryResultTypeInternal.OBJECT_JAVA, null);

                int result = proxy.clear(template, _transaction, 0);
                if (result != 1)
                    throw new Exception("Unable to clear object from space.");
            } catch (Exception e) {
                exceptions.add(e);
            }
        }
    }

    /**
     * Partially updates dirty fields to the space.
     */
    private void handleUpdatedObjects(Collection<OpenJPAStateManager> sms, ArrayList<Exception> exceptions, IJSpace space) {
        // Generate a template for each state manager and use partial update for updating..
        HashSet<OpenJPAStateManager> stateManagersToRestore = new HashSet<OpenJPAStateManager>();
        for (OpenJPAStateManager sm : sms) {
            final ClassMetaData cm = sm.getMetaData();
            try {
                // Find relationship owner and flush it to space
                if (_classesRelationStatus.containsKey(cm.getDescribedType())) {
                    final FieldOwnerInformation ownerInformation = _relationsManager.getStateManagerToUpdate((StateManager) sm);
                    final IEntryPacket entry = getEntryPacketFromStateManager(space, ownerInformation.getStateManager());
                    // Write changes to the space
                    for (FieldMetaData fmd : cm.getFields()) {
                        _relationsManager.initializeOwnerReferencesForField((StateManager) sm, fmd);
                    }
                    _relationsManager.removeOwnedEntitiesStateManagers(stateManagersToRestore, ownerInformation.getStateManager());

                    if (ownerInformation.getStateManager().getVersion() != null)
                        entry.setVersion((Integer) ownerInformation.getStateManager().getVersion());

                    final FieldMetaData[] fmds = ownerInformation.getStateManager().getMetaData().getFields();
                    int spacePropertyIndex = -1;
                    int routingPropertyIndex = entry.getTypeDescriptor().getRoutingPropertyId();

                    for (int i = 0; i < fmds.length; i++) {
                        //ignore version which is not part of the entry packet
                        if (fmds[i].isVersion())
                            continue;
                        spacePropertyIndex++;
                        if (i != ownerInformation.getMetaData().getIndex() && !fmds[i].isPrimaryKey() && spacePropertyIndex != routingPropertyIndex) {
                            entry.setFieldValue(spacePropertyIndex, null);
                        }
                    }

                    space.write(entry, _transaction, Lease.FOREVER, 0, UpdateModifiers.PARTIAL_UPDATE);

                    //update the version
                    ownerInformation.getStateManager().setVersion(entry.getVersion());

                } else {
                    // Create an entry packet from the updated POJO and set all the fields
                    // but the updated & primary key to null.
                    final IEntryPacket entry = getEntryPacketFromStateManager(space, sm);
                    final FieldMetaData[] fmds = cm.getFields();

                    int spacePropertyIndex = -1;
                    int routingPropertyIndex = entry.getTypeDescriptor().getRoutingPropertyId();

                    for (int i = 0; i < fmds.length; i++) {
                        //ignore version which is not part of the entry packet
                        if (fmds[i].isVersion())
                            continue;

                        spacePropertyIndex++;
                        if (!sm.getDirty().get(i) && !fmds[i].isPrimaryKey() && spacePropertyIndex != routingPropertyIndex) {
                            entry.setFieldValue(spacePropertyIndex, null);
                        } else {
                            _relationsManager.initializeOwnerReferencesForField((StateManager) sm, fmds[i]);
                        }
                    }

                    if (sm.getVersion() != null)
                        entry.setVersion((Integer) sm.getVersion());
                    // Write changes to the space
                    space.write(entry, _transaction, Lease.FOREVER, 0, UpdateModifiers.PARTIAL_UPDATE);

                    //update the version
                    sm.setVersion(entry.getVersion());

                }
            } catch (Exception e) {
                exceptions.add(e);
            } finally {
                _relationsManager.restoreRemovedStateManagers(stateManagersToRestore);
            }
        }
    }

    /**
     * Gets an {@link IEntryPacket} instance from the provided {@link OpenJPAStateManager}'s managed
     * instance. The conversion is made after removing the state manager from the managed object
     * because its fields are accessed using reflection and this might cause problems due to
     * OpenJPA's entities enhancement.
     *
     * @param space Space instance the conversion will be called for (using its type manager).
     * @param sm    The state manager whose managed object will be converted.
     * @return An {@link IEntryPacket} instance representing the managed object.
     */
    private IEntryPacket getEntryPacketFromStateManager(IJSpace space, OpenJPAStateManager sm) {
        try {
            final ISpaceProxy proxy = (ISpaceProxy) space;
            sm.getPersistenceCapable().pcReplaceStateManager(null);
            IEntryPacket entry = proxy.getDirectProxy().getTypeManager().getEntryPacketFromObject(
                    sm.getManagedInstance(), ObjectType.POJO);
            return entry;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        } finally {
            sm.getPersistenceCapable().pcReplaceStateManager(sm);
        }
    }

    /**
     * Converts the provided {@link Entity} to an {@link IEntryPacket} instance. The conversion is
     * made after removing the entity's state manager (if it exists) and returning it after the
     * conversion is made due to conflicts between reflection and OpenJPA's enhancement.
     *
     * @param entity The {@link Entity} to convert.
     * @return An {@link IEntryPacket} instance representing the provided entity.
     */
    private IEntryPacket getEntryPacketFromEntity(Object entity) {
        PersistenceCapable pc = (PersistenceCapable) entity;
        final ISpaceProxy proxy = (ISpaceProxy) getConfiguration().getSpace();
        IEntryPacket entry;

        if (pc.pcGetStateManager() != null) {
            OpenJPAStateManager sm = (OpenJPAStateManager) pc.pcGetStateManager();
            try {
                pc.pcReplaceStateManager(null);
                entry = proxy.getDirectProxy().getTypeManager().getEntryPacketFromObject(
                        entity, ObjectType.POJO);
            } finally {
                pc.pcReplaceStateManager(sm);
            }
        } else {
            entry = proxy.getDirectProxy().getTypeManager().getEntryPacketFromObject(
                    entity, ObjectType.POJO);
        }
        return entry;
    }

    /**
     * Writes new persistent objects to the space.
     */
    private void handleNewObjects(Collection<OpenJPAStateManager> sms, IJSpace space) {
        final HashMap<Class<?>, ArrayList<Object>> objectsToWriteByType = new HashMap<Class<?>, ArrayList<Object>>();
        final ArrayList<OpenJPAStateManager> stateManagersToRestore = new ArrayList<OpenJPAStateManager>();
        Class<?> previousType = null;
        ArrayList<Object> currentList = null;
        for (OpenJPAStateManager sm : sms) {
            // If the current object is in a relation skip it
            if (_classesRelationStatus.containsKey(sm.getMetaData().getDescribedType())) {
                continue;
            }
            // If the object has managed instances in its fields we need to remove the state manager from these instances
            // since they are serialized when written to space and can cause a deadlock when written
            // by writeMultiple.
            _relationsManager.removeOwnedEntitiesStateManagers(stateManagersToRestore, sm);

            // In order to use writeMultiple we need to gather each type's instances to its own list
            if (!sm.getMetaData().getDescribedType().equals(previousType)) {
                currentList = objectsToWriteByType.get(sm.getMetaData().getDescribedType());
                if (currentList == null) {
                    currentList = new ArrayList<Object>();
                    objectsToWriteByType.put(sm.getMetaData().getDescribedType(), currentList);
                }
                previousType = sm.getMetaData().getDescribedType();
            }

            // Each persisted class should have its state manager removed
            // before being written to space since gigaspaces reflection conflicts with
            // OpenJPA's class monitoring.
            sm.getPersistenceCapable().pcReplaceStateManager(null);
            stateManagersToRestore.add(sm);
            currentList.add(sm.getManagedInstance());
        }
        // Write objects to space in batches by type
        try {
            for (Map.Entry<Class<?>, ArrayList<Object>> entry : objectsToWriteByType.entrySet()) {
                space.writeMultiple(entry.getValue().toArray(), _transaction, Lease.FOREVER, UpdateModifiers.WRITE_ONLY);
            }
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        } finally {
            // Restore the removed state managers.
            _relationsManager.restoreRemovedStateManagers(stateManagersToRestore);
        }

        // If optimistic locking is enabled, set version for written states
        // (The version is already saved in its property but not in the states version field)
        if (getConfiguration().getOptimistic()) {
            for (Map.Entry<Class<?>, ArrayList<Object>> entry : objectsToWriteByType.entrySet()) {
                for (Object obj : entry.getValue()) {
                    PersistenceCapable pc = (PersistenceCapable) obj;
                    OpenJPAStateManager sm = (OpenJPAStateManager) pc.pcGetStateManager();
                    if (sm.getMetaData().getVersionField() == null)
                        break;

                    Object version = sm.fetch(sm.getMetaData().getVersionField().getIndex());
                    sm.setVersion(version);
                }
            }
        }
    }

    /**
     * Validates the provided class' annotations. Currently the only validation performed is for @Id
     * & @SpaceId annotations that must be declared on the same getter.
     */
    private void validateClassAnnotations(Class<?> type) {
        // Validation is only relevant for Entities
        if (type.getAnnotation(Entity.class) == null)
            return;

        for (Method getter : type.getMethods()) {

            if (!getter.getName().startsWith("get"))
                continue;

            SpaceId spaceId = getter.getAnnotation(SpaceId.class);
            boolean hasJpaId = getter.getAnnotation(Id.class) != null || getter.getAnnotation(EmbeddedId.class) != null;
            if (spaceId != null || hasJpaId) {
                if (!hasJpaId || spaceId == null)
                    throw new IllegalArgumentException("SpaceId and Id annotations must both be declared on the same property in JPA entities in type: " + type.getName());
                if (spaceId.autoGenerate()) {
                    GeneratedValue generatedValue = getter.getAnnotation(GeneratedValue.class);
                    if (generatedValue == null || generatedValue.strategy() != GenerationType.IDENTITY)
                        throw new IllegalArgumentException(
                                "SpaceId with autoGenerate=true annotated property should also have a JPA GeneratedValue annotation with strategy = GenerationType.IDENTITY.");
                }
                break;
            }
        }
    }

    /**
     * Initializes an ExternalEntry result as a state managed Pojo. (used by JPQL's query executor)
     */
    public Object loadObject(ClassMetaData classMetaData, IEntryPacket entry) {
        // Get object id
        Object[] ids = new Object[1];
        ids[0] = entry.getID();
        Object objectId = ApplicationIds.fromPKValues(ids, classMetaData);
        return getContext().find(objectId, null, null, entry, 0);
    }

    /**
     * Gets the current active transaction.
     */
    public Transaction getCurrentTransaction() {
        return _transaction;
    }

    /**
     * Gets a JDBC connection using the configuration's space instance. Each store manager has its
     * own Connection for Multithreaded reasons.
     */
    public GConnection getJdbcConnection() throws SQLException {
        if (_connection == null) {
            Properties connectionProperties = new Properties();
            connectionProperties.put(QueryProcessorFactory.COM_GIGASPACES_EMBEDDED_QP_ENABLED, "true");
            _connection = GConnection.getInstance(getConfiguration().getSpace(), connectionProperties);
            if (_connection.getAutoCommit())
                _connection.setAutoCommit(false);
        }
        return _connection;
    }

    /**
     * Gets the class relation status (one-to-one etc..) for the provided type.
     */
    public synchronized int getClassRelationStatus(Class<?> type) {
        // In case relations status was not initialized already..
        if (_relationsManager.shouldInitializeClassesRelationStatus())
            _relationsManager.initializeClassesRelationStatus();
        // Get relation status..
        Integer relationStatus = _classesRelationStatus.get(type);
        return (relationStatus == null) ? FieldMetaData.MANAGE_NONE : relationStatus;
    }


    /**
     * Keeps information for a field's owner.
     */
    private static class FieldOwnerInformation {
        private StateManager stateManager;
        private FieldMetaData metaData;

        public FieldOwnerInformation(StateManager stateManager, FieldMetaData metaData) {
            this.stateManager = stateManager;
            this.metaData = metaData;
        }

        public StateManager getStateManager() {
            return stateManager;
        }

        public FieldMetaData getMetaData() {
            return metaData;
        }
    }

    /**
     * StoreManager's relationships manager. Provides methods for handling relationships in
     * GigaSpaces owned relationships model.
     */
    private class RelationsManager {

        public RelationsManager() {
        }

        /**
         * Removes owned entities state managers (before writing them to space due to serialization
         * deadlock problem). The removed state managers are kept in the provided collection for
         * restoring them later.
         *
         * @param stateManagersToRestore The collection for storing the removed state managers.
         * @param sm                     The owning entity's state manager.
         */
        public void removeOwnedEntitiesStateManagers(Collection<OpenJPAStateManager> stateManagersToRestore,
                                                     OpenJPAStateManager sm) {
            // Remove the state manager from objects in relation for making their serialization not
            // handled by OpenJPA which can cause a deadlock when writing to space.
            // The deadlock is caused because when serializing a monitored instance, OpenJPA takes over
            // and attempts to access an already locked layer in OpenJPA's hierarchy which causes
            // a deadlock.
            for (FieldMetaData fmd : sm.getMetaData().getFields()) {
                if (!sm.getLoaded().get(fmd.getIndex()))
                    continue;
                if (fmd.isEmbeddedPC()) {
                    Object value = sm.fetch(fmd.getDeclaredIndex());
                    if (value != null) {
                        PersistenceCapable pc = (PersistenceCapable) value;
                        OpenJPAStateManager stateManager = (OpenJPAStateManager) pc.pcGetStateManager();
                        removeOwnedEntitiesStateManagers(stateManagersToRestore, stateManager);
                        pc.pcReplaceStateManager(null);
                        stateManagersToRestore.add(stateManager);
                    }
                } else if (fmd.getAssociationType() == FieldMetaData.ONE_TO_MANY || isPersistentCollection(fmd)) {
                    Collection<?> collection = (Collection<?>) sm.fetch(fmd.getIndex());
                    if (collection != null) {
                        for (Object item : collection) {
                            PersistenceCapable pc = (PersistenceCapable) item;
                            OpenJPAStateManager stateManager = (OpenJPAStateManager) pc.pcGetStateManager();
                            if (stateManager != null) {
                                // Set relationship owner
                                setOwnerStateManagerForPersistentInstance(item, sm, fmd);
                                removeOwnedEntitiesStateManagers(stateManagersToRestore, stateManager);
                                stateManagersToRestore.add(stateManager);
                                pc.pcReplaceStateManager(null);
                            }
                        }
                    }
                } else if (fmd.getAssociationType() == FieldMetaData.ONE_TO_ONE) {
                    Object value = sm.fetch(fmd.getIndex());
                    if (value != null) {
                        setOwnerStateManagerForPersistentInstance(value, sm, fmd);
                        PersistenceCapable pc = (PersistenceCapable) value;
                        OpenJPAStateManager stateManager = (OpenJPAStateManager) pc.pcGetStateManager();
                        removeOwnedEntitiesStateManagers(stateManagersToRestore, stateManager);
                        stateManagersToRestore.add(stateManager);
                        pc.pcReplaceStateManager(null);
                    }
                }
            }
        }

        /**
         * @return whether the provided field meta data describes a persistent collection which is
         * not a one to many relationship. usually declared with the PersistentCollection
         * annotation.
         */
        private boolean isPersistentCollection(FieldMetaData fmd) {
            return Collection.class.isAssignableFrom(fmd.getDeclaredType()) && fmd.getElement() != null
                    && PersistenceCapable.class.isAssignableFrom(fmd.getElement().getDeclaredType());
        }

        /**
         * Sets the provided state manager as the managed object's owner.
         *
         * @param managedObject The managed object to set the owner for.
         * @param sm            The owner's state manager.
         */
        public void setOwnerStateManagerForPersistentInstance(Object managedObject, OpenJPAStateManager sm, FieldMetaData fmd) {
            StateManager stateManager = (StateManager) ((PersistenceCapable) managedObject).pcGetStateManager();
            if (stateManager == null)
                throw new IllegalStateException("Attempted to set an Owner back-reference for an unmanaged instance: "
                        + managedObject.toString() + " of type: " + managedObject.getClass().getName());
            stateManager.setOwnerInformation((StateManager) sm, fmd);
        }

        /**
         * Sets the provided state manager as the owner for the provided field value.
         *
         * @param sm  The owner's state manager.
         * @param fmd The field's value the owner will be set for.
         */
        public void initializeOwnerReferencesForField(StateManager sm, FieldMetaData fmd) {
            if (fmd.getAssociationType() == FieldMetaData.ONE_TO_MANY) {
                Collection<?> collection = (Collection<?>) sm.fetch(fmd.getIndex());
                if (collection != null) {
                    for (Object item : collection) {
                        if (item != null) {
                            _relationsManager.setOwnerStateManagerForPersistentInstance(item, sm, fmd);
                        }
                    }
                }
            } else if (fmd.getAssociationType() == FieldMetaData.ONE_TO_ONE || fmd.isEmbeddedPC()) {
                Object value = sm.fetch(fmd.getIndex());
                if (value != null) {
                    _relationsManager.setOwnerStateManagerForPersistentInstance(value, sm, fmd);
                }
            }
        }

        /**
         * Attempts to find the super-owner of the provided state manager in a relationship to
         * update. Throws an exception if such a state manager doesn't exist.
         *
         * @param sm The owned relationship state manager.
         * @return The super-owner state manager of the relationship.
         */
        public FieldOwnerInformation getStateManagerToUpdate(StateManager sm) {
            final Integer associationType = _classesRelationStatus.get(sm.getMetaData().getDescribedType());
            if (associationType == null)
                throw new IllegalStateException("Error updating: " + sm.getMetaData().getClass().getName()
                        + " with id: " + sm.getId());
            final StateManager ownerStateManager = sm.getOwnerStateManager();
            if (ownerStateManager != null) {
                if (associationType == FieldMetaData.ONE_TO_MANY) {
                    for (FieldMetaData fmd : ownerStateManager.getMetaData().getFields()) {
                        if (fmd.getElement().getDeclaredType().equals(sm.getMetaData().getDescribedType())) {
                            Collection<?> collection = (Collection<?>) ownerStateManager.fetch(fmd.getIndex());
                            if (collection == null || !collection.contains(sm.getManagedInstance()))
                                break;
                            if (ownerStateManager.getOwnerStateManager() != null)
                                return getStateManagerToUpdate(ownerStateManager);
                            return new FieldOwnerInformation(ownerStateManager, fmd);
                        }
                    }
                } else if (associationType == FieldMetaData.ONE_TO_ONE) {
                    for (FieldMetaData fmd : ownerStateManager.getMetaData().getFields()) {
                        if (fmd.getDeclaredType().equals(sm.getMetaData().getDescribedType())) {
                            Object value = ownerStateManager.fetch(fmd.getIndex());
                            if (value == null || !value.equals(sm.getManagedInstance()))
                                break;
                            if (ownerStateManager.getOwnerStateManager() != null)
                                return getStateManagerToUpdate(ownerStateManager);
                            return new FieldOwnerInformation(ownerStateManager, fmd);
                        }
                    }
                }
            }
            throw new IllegalStateException("Attempted to update an owned entity: "
                    + sm.getMetaData().getClass().getName() + " with Id: " + sm.getId() + " which has no owner.");
        }

        /**
         * Restores state managers for the provided collection of state managers.
         *
         * @param stateManagersToRestore State managers collection to restore.
         */
        public void restoreRemovedStateManagers(Collection<OpenJPAStateManager> stateManagersToRestore) {
            for (OpenJPAStateManager sm : stateManagersToRestore) {
                sm.getPersistenceCapable().pcReplaceStateManager(sm);
            }
        }

        /**
         * Collects information on current OpenJPA listed class meta data list. On every call to
         * flush() the method is called & checks if there are new classes to initialize.
         */
        public synchronized void initializeClassesRelationStatus() {
            if (!shouldInitializeClassesRelationStatus())
                return;
            // Collect information regarding relationships.
            // Eventually classes which are in a relation should not be saved to the space
            // since we only support owned relationships and these instances will be saved as nested instances
            // of their owning instance.
            ClassMetaData[] cms = getConfiguration().getMetaDataRepositoryInstance().getMetaDatas();
            for (ClassMetaData cm : cms) {
                // Process class
                if (!_processedClasses.contains(cm.getDescribedType())) {
                    for (FieldMetaData fmd : cm.getFields()) {
                        if (fmd.getAssociationType() == FieldMetaData.ONE_TO_ONE) {
                            if (!_classesRelationStatus.containsKey(fmd.getDeclaredType())) {
                                _classesRelationStatus.put(fmd.getDeclaredType(), FieldMetaData.ONE_TO_ONE);
                            }
                        } else if (fmd.getAssociationType() == FieldMetaData.ONE_TO_MANY) {
                            if (!_classesRelationStatus.containsKey(fmd.getDeclaredType())) {
                                _classesRelationStatus.put(fmd.getElement().getDeclaredType(), FieldMetaData.ONE_TO_MANY);
                            }
                        } else if (fmd.getAssociationType() == FieldMetaData.MANY_TO_MANY) {
                            throw new IllegalArgumentException("Many-to-many is not supported.");
                        }
                    }
                    validateClassAnnotations(cm.getDescribedType());
                    _processedClasses.add(cm.getDescribedType());
                }
            }
        }

        /**
         * Gets whether classes relations status is not complete and should be synchronized. OpenJPA
         * creates class meta data only after an entity is persisted for the first time.
         */
        public boolean shouldInitializeClassesRelationStatus() {
            return getConfiguration().getMetaDataRepositoryInstance().getMetaDatas().length != _processedClasses.size();
        }

        /**
         * Attempts to find the instance represented by the provided state manager in the provided
         * relationships tree.
         *
         * @param sm  The state manager holding the instance to find.
         * @param sms The state managers which potentially holds the instance to find.
         * @return The found instance, otherwise null.
         */
        public IEntryPacket findObjectInEntry(StateManager sm, IEntryPacket entry, Stack<StateManager> sms) {
            final StateManager tempStateManager = sms.pop();
            final SpaceTypeInfo ownedTypeInfo = SpaceTypeInfoRepository.getTypeInfo(tempStateManager.getMetaData().getDescribedType());
            final FieldMetaData[] fms = sm.getMetaData().getFields();
            final FieldMetaData fmd = tempStateManager.getOwnerMetaData();

            if (fmd == null)
                throw new IllegalStateException("Owner field meta data is not set for entity of type: "
                        + tempStateManager.getMetaData().getDescribedType().getName() + " with Id: "
                        + tempStateManager.getId());

            // Find space property index (skip version fields..)
            int spacePropertyIndex = -1;
            for (int i = 0; i <= fmd.getIndex(); i++) {
                if (fms[i].isVersion())
                    continue;
                spacePropertyIndex++;
            }

            // One-to-many
            if (fmd.getAssociationType() == FieldMetaData.ONE_TO_MANY) {
                final Object id = ApplicationIds.toPKValues(tempStateManager.getId(), tempStateManager.getMetaData())[0];
                final Collection<?> values = (Collection<?>) entry.getFieldValue(spacePropertyIndex);
                if (values != null) {
                    for (Object item : values) {
                        Object itemId = ownedTypeInfo.getIdProperty().getValue(item);
                        if (id.equals(itemId)) {
                            final IEntryPacket entryPacket = getEntryPacketFromEntity(item);
                            return (sms.isEmpty()) ? entryPacket : findObjectInEntry(tempStateManager, entryPacket, sms);
                        }
                    }
                }

                // One-to-one
            } else if (fmd.getAssociationType() == FieldMetaData.ONE_TO_ONE) {
                final Object id = ApplicationIds.toPKValues(tempStateManager.getId(), tempStateManager.getMetaData())[0];
                final Object value = entry.getFieldValue(spacePropertyIndex);
                if (value != null) {
                    Object objectId = ownedTypeInfo.getIdProperty().getValue(value);
                    if (id.equals(objectId)) {
                        final IEntryPacket entryPacket = getEntryPacketFromEntity(value);
                        return (sms.isEmpty()) ? entryPacket : findObjectInEntry(tempStateManager, entryPacket, sms);
                    }
                }

                // Embedded
            } else if (fmd.isEmbeddedPC()) {
                final Object value = entry.getFieldValue(spacePropertyIndex);
                final IEntryPacket entryPacket = getEntryPacketFromEntity(value);
                return (sms.isEmpty()) ? entryPacket : findObjectInEntry(tempStateManager, entryPacket, sms);
            }

            // Object not found..
            return null;
        }
    }

}
