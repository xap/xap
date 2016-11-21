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


package org.openspaces.core;

import com.gigaspaces.admin.quiesce.QuiesceToken;
import com.gigaspaces.async.AsyncFuture;
import com.gigaspaces.async.AsyncFutureListener;
import com.gigaspaces.async.AsyncResultFilter;
import com.gigaspaces.async.AsyncResultsReducer;
import com.gigaspaces.async.FutureFactory;
import com.gigaspaces.client.ChangeModifiers;
import com.gigaspaces.client.ChangeResult;
import com.gigaspaces.client.ChangeSet;
import com.gigaspaces.client.ClearModifiers;
import com.gigaspaces.client.CountModifiers;
import com.gigaspaces.client.IsolationLevelModifiers;
import com.gigaspaces.client.ReadByIdsResult;
import com.gigaspaces.client.ReadByIdsResultImpl;
import com.gigaspaces.client.ReadModifiers;
import com.gigaspaces.client.TakeByIdsResult;
import com.gigaspaces.client.TakeByIdsResultImpl;
import com.gigaspaces.client.TakeModifiers;
import com.gigaspaces.client.WriteModifiers;
import com.gigaspaces.client.iterator.SpaceIterator;
import com.gigaspaces.events.DataEventSession;
import com.gigaspaces.events.DataEventSessionFactory;
import com.gigaspaces.events.EventSessionConfig;
import com.gigaspaces.internal.client.QueryResultTypeInternal;
import com.gigaspaces.internal.client.cache.ISpaceCache;
import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.utils.ObjectUtils;
import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.query.ISpaceQuery;
import com.gigaspaces.query.IdQuery;
import com.gigaspaces.query.IdsQuery;
import com.gigaspaces.query.QueryResultType;
import com.gigaspaces.query.aggregators.AggregationResult;
import com.gigaspaces.query.aggregators.AggregationSet;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.LeaseContext;

import net.jini.core.transaction.Transaction;

import org.openspaces.core.exception.DefaultExceptionTranslator;
import org.openspaces.core.exception.ExceptionTranslator;
import org.openspaces.core.executor.DistributedTask;
import org.openspaces.core.executor.Task;
import org.openspaces.core.executor.TaskRoutingProvider;
import org.openspaces.core.executor.internal.ExecutorMetaDataProvider;
import org.openspaces.core.executor.internal.InternalDistributedSpaceTaskWrapper;
import org.openspaces.core.executor.internal.InternalSpaceTaskWrapper;
import org.openspaces.core.internal.InternalGigaSpace;
import org.openspaces.core.transaction.DefaultTransactionProvider;
import org.openspaces.core.transaction.TransactionProvider;
import org.openspaces.core.transaction.internal.InternalAsyncFuture;
import org.openspaces.core.transaction.internal.InternalAsyncFutureListener;
import org.openspaces.core.util.SpaceUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.util.Assert;

import java.io.IOException;
import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.concurrent.Future;

/**
 * Default implementation of {@link GigaSpace}. Constructed with {@link com.j_spaces.core.IJSpace},
 * {@link org.openspaces.core.transaction.TransactionProvider} and {@link
 * org.openspaces.core.exception.ExceptionTranslator}. <p/> <p>Operations are delegated to {@link
 * com.j_spaces.core.IJSpace} with transactions acquired using {@link
 * org.openspaces.core.transaction.TransactionProvider}. Any exceptions thrown during the operations
 * are translated using {@link org.openspaces.core.exception.ExceptionTranslator}. <p/> <p>Allows to
 * set default timeouts for read and take operations and default lease for write operation.
 *
 * @author kimchy
 */
public class DefaultGigaSpace implements GigaSpace, InternalGigaSpace {

    private String name;

    final private ISpaceProxy space;

    final private TransactionProvider txProvider;

    final private boolean implicitTxProvider;

    final private ExceptionTranslator exTranslator;

    final private GigaSpaceTypeManager typeManager;

    private long defaultReadTimeout = 0;

    private long defaultTakeTimeout = 0;

    private long defaultWriteLease = Long.MAX_VALUE;

    private final int springIsolationLevel;

    private final int defaultIsolationLevel;

    private WriteModifiers defaultWriteModifiers;

    private ReadModifiers defaultReadModifiers;

    private TakeModifiers defaultTakeModifiers;

    private CountModifiers defaultCountModifiers;

    private ClearModifiers defaultClearModifiers;

    private ChangeModifiers defaultChangeModifiers;

    final private ExecutorMetaDataProvider executorMetaDataProvider = new ExecutorMetaDataProvider();

    private DefaultGigaSpace clusteredGigaSpace;

    /**
     * Constructs a new DefaultGigaSpace implementation.
     *
     * @param configurer            The transaction provider for declarative transaction ex.
     */
    public DefaultGigaSpace(GigaSpaceConfigurer configurer) {
        this.space = initSpace(configurer);
        this.txProvider = configurer.getTxProvider() != null
                ? configurer.getTxProvider()
                : new DefaultTransactionProvider(configurer.getTransactionManager());
        this.implicitTxProvider = configurer.getTxProvider() == null;
        this.name = configurer.getName() != null ? configurer.getName() : space.getName();
        this.exTranslator = configurer.getExTranslator() != null ? configurer.getExTranslator() : new DefaultExceptionTranslator();
        this.typeManager = new DefaultGigaSpaceTypeManager(this.space, this.exTranslator);

        // set the default read take modifiers according to the default isolation level
        this.springIsolationLevel = configurer.getDefaultIsolationLevel();
        this.defaultIsolationLevel =
                IsolationLevelHelpers.convertSpringToSpaceIsolationLevel(springIsolationLevel, space.getReadModifiers());
        this.defaultCountModifiers = IsolationLevelHelpers.toCountModifiers(this.defaultIsolationLevel);
        this.defaultReadModifiers = IsolationLevelHelpers.toReadModifiers(this.defaultIsolationLevel);

        setDefaultReadTimeout(configurer.getDefaultReadTimeout());
        setDefaultTakeTimeout(configurer.getDefaultTakeTimeout());
        setDefaultWriteLease(configurer.getDefaultWriteLease());
        setDefaultWriteModifiers(configurer.getDefaultWriteModifiers());
        setDefaultReadModifiers(configurer.getDefaultReadModifiers());
        setDefaultTakeModifiers(configurer.getDefaultTakeModifiers());
        setDefaultCountModifiers(configurer.getDefaultCountModifiers());
        setDefaultClearModifiers(configurer.getDefaultClearModifiers());
        setDefaultChangeModifiers(configurer.getDefaultChangeModifiers());
    }

    private DefaultGigaSpace(IJSpace space, DefaultGigaSpace other) {
        this.space = (ISpaceProxy) space;
        this.txProvider = other.txProvider;
        this.implicitTxProvider = other.implicitTxProvider;
        this.exTranslator = other.exTranslator;
        this.typeManager = new DefaultGigaSpaceTypeManager(this.space, this.exTranslator);

        this.defaultIsolationLevel = other.defaultIsolationLevel;
        this.springIsolationLevel = other.springIsolationLevel;
        this.name = other.name;
        this.defaultReadTimeout = other.defaultReadTimeout;
        this.defaultTakeTimeout = other.defaultTakeTimeout;
        this.defaultWriteLease = other.defaultWriteLease;
        this.defaultChangeModifiers = other.defaultChangeModifiers;
        this.defaultClearModifiers = other.defaultClearModifiers;
        this.defaultCountModifiers = other.defaultCountModifiers;
        this.defaultReadModifiers = other.defaultReadModifiers;
        this.defaultWriteModifiers = other.defaultWriteModifiers;
        this.defaultTakeModifiers = other.defaultTakeModifiers;
    }

    private static ISpaceProxy initSpace(GigaSpaceConfigurer configurer) {
        ISpaceProxy space = (ISpaceProxy) configurer.getSpace();
        Assert.notNull(space, "space property is required");
        boolean clustered = configurer.getClustered() != null ? configurer.getClustered() : space instanceof ISpaceCache || SpaceUtils.isRemoteProtocol(space);
        return !clustered && space.isClustered() ? (ISpaceProxy)SpaceUtils.getClusterMemberSpace(space) : space;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    @Override
    public boolean isSecured() {
        return this.space.isSecured();
    }

    /**
     * Sets the default read timeout when executing {@link #read(Object)} or {@link
     * #readIfExists(Object)} operations.
     */
    public void setDefaultReadTimeout(long defaultReadTimeout) {
        this.defaultReadTimeout = defaultReadTimeout;
    }


    /**
     * Gets the default read timeout when executing {@link #read(Object)} or {@link
     * #readIfExists(Object)} operations.
     */
    public long getDefaultReadTimeout() {
        return defaultReadTimeout;
    }

    /**
     * Sets the default take timeout when executing {@link #take(Object)} or {@link
     * #takeIfExists(Object)} operations.
     */
    public void setDefaultTakeTimeout(long defaultTakeTimeout) {
        this.defaultTakeTimeout = defaultTakeTimeout;
    }


    /**
     * Gets the default take timeout when executing {@link #take(Object)} or {@link
     * #takeIfExists(Object)} operations.
     */
    public long getDefaultTakeTimeout() {
        return defaultTakeTimeout;
    }

    /**
     * Sets the default write lease when executing {@link #write(Object)}.
     */
    public void setDefaultWriteLease(long defaultWriteLease) {
        this.defaultWriteLease = defaultWriteLease;
    }

    /**
     * Gets the default write lease when executing {@link #write(Object)}.
     */
    public long getDefaultWriteLease() {
        return defaultWriteLease;
    }

    /**
     * Gets the defaultIsolationLevel.
     */
    public int getDefaultIsolationLevel() {
        return defaultIsolationLevel;
    }

    /**
     * Sets the default {@link WriteModifiers} when excecution {@link #write(Object)}
     */
    public void setDefaultWriteModifiers(WriteModifiers modifiers) {
        this.defaultWriteModifiers = modifiers != null ? modifiers : WriteModifiers.NONE;
    }

    /**
     * Sets the default {@link ClearModifiers} when excecution {@link #write(Object)}
     */
    public void setDefaultClearModifiers(ClearModifiers modifiers) {
        this.defaultClearModifiers = modifiers != null ? modifiers : ClearModifiers.NONE;
    }

    /**
     * Sets the default {@link CountModifiers} when excecution {@link #count(Object)}
     */
    public void setDefaultCountModifiers(CountModifiers modifiers) {
        this.defaultCountModifiers = modifiers == null || modifiers.getCode() == 0
                ? IsolationLevelHelpers.toCountModifiers(defaultIsolationLevel)
                : validateWithDefaultIsolationLevel(modifiers, "count");
    }

    /**
     * Sets the default {@link ReadModifiers} when excecution {@link #read(Object)}
     */
    public void setDefaultReadModifiers(ReadModifiers modifiers) {
        this.defaultReadModifiers = modifiers == null || modifiers.getCode() == 0
                ? IsolationLevelHelpers.toReadModifiers(defaultIsolationLevel)
                : validateWithDefaultIsolationLevel(modifiers, "read");
    }

    /**
     * Sets the default {@link TakeModifiers} when excecution {@link #take(Object)}
     */
    public void setDefaultTakeModifiers(TakeModifiers modifiers) {
        this.defaultTakeModifiers = modifiers != null ? modifiers : TakeModifiers.NONE;
    }

    /**
     * Sets the default {@link ChangeModifiers} when excecution {@link #change(Object, ChangeSet)}
     */
    public void setDefaultChangeModifiers(ChangeModifiers modifiers) {
        this.defaultChangeModifiers = modifiers != null ? modifiers : ChangeModifiers.NONE;
    }

    private <T extends IsolationLevelModifiers> T validateWithDefaultIsolationLevel(T modifiers, String name) {
        if ((springIsolationLevel == TransactionDefinition.ISOLATION_REPEATABLE_READ && !modifiers.isRepeatableRead()) ||
                (springIsolationLevel == TransactionDefinition.ISOLATION_READ_COMMITTED && !modifiers.isReadCommitted()) ||
                (springIsolationLevel == TransactionDefinition.ISOLATION_READ_UNCOMMITTED && !modifiers.isDirtyRead())) {
            throw new IllegalArgumentException("Cannot configure conflicting isolation level and " +
                    "default " + name + " modifiers." + StringUtils.NEW_LINE +
                    "defaultIsolationLevel [" + defaultIsolationLevel + "]" + StringUtils.NEW_LINE +
                    "default " + name + " modifiers [" + modifiers.getCode() + "]");
        }
        return modifiers;
    }

    // GigaSpace interface Methods

    public WriteModifiers getDefaultWriteModifiers() {
        return defaultWriteModifiers;
    }

    public ClearModifiers getDefaultClearModifiers() {
        return defaultClearModifiers;
    }

    public CountModifiers getDefaultCountModifiers() {
        return IsolationLevelHelpers.mergeWithIsolationLevelModifiersIfNeeded(defaultCountModifiers, this);
    }

    public ReadModifiers getDefaultReadModifiers() {
        return IsolationLevelHelpers.mergeWithIsolationLevelModifiersIfNeeded(defaultReadModifiers, this);
    }

    public TakeModifiers getDefaultTakeModifiers() {
        return defaultTakeModifiers;
    }

    public ChangeModifiers getDefaultChangeModifiers() {
        return defaultChangeModifiers;
    }

    public IJSpace getSpace() {
        return this.space;
    }

    public GigaSpace getClustered() {
        if (clusteredGigaSpace != null) {
            return clusteredGigaSpace;
        }
        if (this.space.isClustered()) {
            clusteredGigaSpace = this;
        } else {
            final DefaultGigaSpace newClusteredGigaSpace;
            try {
                newClusteredGigaSpace = new DefaultGigaSpace(this.space.getDirectProxy().getClusteredProxy(), this);
            } catch (Exception e) {
                throw new InvalidDataAccessApiUsageException("Failed to get clustered Space from actual space", e);
            }
            //GS-8287: try to assign the created single clustered GigaSpace instance to the volatile reference
            //but avoid locking at creation - we don't promise a single instance being returned. 
            if (clusteredGigaSpace == null) {
                clusteredGigaSpace = newClusteredGigaSpace;
            }
        }
        return this.clusteredGigaSpace;
    }

    public TransactionProvider getTxProvider() {
        return this.txProvider;
    }

    public ExceptionTranslator getExceptionTranslator() {
        return this.exTranslator;
    }

    public void clear(Object template) throws DataAccessException {
        try {
            //noinspection deprecation
            space.clear(template, getCurrentTransaction(), defaultClearModifiers.getCode());
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    public int clear(Object template, int modifiers) throws DataAccessException {
        try {
            return space.clear(template, getCurrentTransaction(), modifiers);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    public int clear(Object template, ClearModifiers modifiers) throws DataAccessException {
        try {
            return space.clear(template, getCurrentTransaction(), modifiers.getCode());
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    public int count(Object template) throws DataAccessException {
        try {
            return space.count(template, getCurrentTransaction(), getDefaultCountModifiers().getCode());
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    public int count(Object template, int modifiers) throws DataAccessException {
        try {
            return space.count(template, getCurrentTransaction(), modifiers);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    public int count(Object template, CountModifiers modifiers) throws DataAccessException {
        try {
            return space.count(template, getCurrentTransaction(), modifiers.getCode());
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    public <T> ISpaceQuery<T> snapshot(Object entry) throws DataAccessException {
        try {
            return space.snapshot(entry);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    public <T> T readById(Class<T> clazz, Object id) {
        return readById(clazz, id, null, defaultReadTimeout, getDefaultReadModifiers().getCode());
    }

    public <T> T readById(Class<T> clazz, Object id, Object routing) {
        return readById(clazz, id, routing, defaultReadTimeout, getDefaultReadModifiers().getCode());
    }

    public <T> T readById(Class<T> clazz, Object id, Object routing, long timeout) {
        return readById(clazz, id, routing, timeout, getDefaultReadModifiers().getCode());
    }

    @SuppressWarnings("unchecked")
    public <T> T readById(Class<T> clazz, Object id, Object routing, long timeout, int modifiers) {
        try {
            return (T) space.readById(
                    ObjectUtils.assertArgumentNotNull(clazz, "class").getName(), id, routing, getCurrentTransaction(), timeout, modifiers, false, QueryResultTypeInternal.NOT_SET, null);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T readById(Class<T> clazz, Object id, Object routing, long timeout, ReadModifiers modifiers) {
        try {
            return (T) space.readById(
                    ObjectUtils.assertArgumentNotNull(clazz, "class").getName(), id, routing, getCurrentTransaction(), timeout, modifiers.getCode(), false, QueryResultTypeInternal.NOT_SET, null);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T readById(IdQuery<T> query) throws DataAccessException {
        try {
            return (T) space.readById(query.getTypeName(), query.getId(), query.getRouting(), getCurrentTransaction(), defaultReadTimeout, getDefaultReadModifiers().getCode(), false, toInternal(query.getQueryResultType()), query.getProjections());
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T readById(IdQuery<T> query, long timeout) throws DataAccessException {
        try {
            return (T) space.readById(query.getTypeName(), query.getId(), query.getRouting(), getCurrentTransaction(), timeout, getDefaultReadModifiers().getCode(), false, toInternal(query.getQueryResultType()), query.getProjections());
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T readById(IdQuery<T> query, long timeout, int modifiers) throws DataAccessException {
        try {
            return (T) space.readById(query.getTypeName(), query.getId(), query.getRouting(), getCurrentTransaction(), timeout, modifiers, false, toInternal(query.getQueryResultType()), query.getProjections());
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T readById(IdQuery<T> query, long timeout, ReadModifiers modifiers) throws DataAccessException {
        try {
            return (T) space.readById(query.getTypeName(), query.getId(), query.getRouting(), getCurrentTransaction(), timeout, modifiers.getCode(), false, toInternal(query.getQueryResultType()), query.getProjections());
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    public <T> T read(T template) throws DataAccessException {
        return read(template, defaultReadTimeout);
    }

    public <T> T read(T template, long timeout) throws DataAccessException {
        return read(template, timeout, getDefaultReadModifiers().getCode());
    }

    @SuppressWarnings("unchecked")
    public <T> T read(T template, long timeout, int modifiers) throws DataAccessException {
        try {
            return (T) space.read(template, getCurrentTransaction(), timeout, modifiers);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T read(T template, long timeout, ReadModifiers modifiers) throws DataAccessException {
        try {
            return (T) space.read(template, getCurrentTransaction(), timeout, modifiers.getCode());
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    public <T> T read(ISpaceQuery<T> template) throws DataAccessException {
        return read(template, defaultReadTimeout);
    }

    public <T> T read(ISpaceQuery<T> template, long timeout) throws DataAccessException {
        return read(template, timeout, getDefaultReadModifiers().getCode());
    }

    @SuppressWarnings("unchecked")
    public <T> T read(ISpaceQuery<T> template, long timeout, int modifiers) throws DataAccessException {
        try {
            return (T) space.read(template, getCurrentTransaction(), timeout, modifiers);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T read(ISpaceQuery<T> template, long timeout, ReadModifiers modifiers) throws DataAccessException {
        try {
            return (T) space.read(template, getCurrentTransaction(), timeout, modifiers.getCode());
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    public <T> AsyncFuture<T> asyncRead(T template) throws DataAccessException {
        return asyncRead(template, defaultReadTimeout, getDefaultReadModifiers().getCode(), null);
    }

    public <T> AsyncFuture<T> asyncRead(T template, AsyncFutureListener<T> listener) throws DataAccessException {
        return asyncRead(template, defaultReadTimeout, getDefaultReadModifiers().getCode(), listener);
    }

    public <T> AsyncFuture<T> asyncRead(T template, long timeout) throws DataAccessException {
        return asyncRead(template, timeout, getDefaultReadModifiers().getCode(), null);
    }

    public <T> AsyncFuture<T> asyncRead(T template, long timeout, AsyncFutureListener<T> listener) throws DataAccessException {
        return asyncRead(template, timeout, getDefaultReadModifiers().getCode(), listener);
    }

    public <T> AsyncFuture<T> asyncRead(T template, long timeout, int modifiers) throws DataAccessException {
        return asyncRead(template, timeout, modifiers, null);
    }

    public <T> AsyncFuture<T> asyncRead(T template, long timeout, ReadModifiers modifiers) throws DataAccessException {
        return asyncRead(template, timeout, modifiers.getCode(), null);
    }

    public <T> AsyncFuture<T> asyncRead(T template, long timeout, int modifiers, AsyncFutureListener<T> listener) throws DataAccessException {
        Transaction tx = getCurrentTransaction();
        try {
            return wrapFuture(space.asyncRead(template, tx, timeout, modifiers, wrapListener(listener, tx)), tx);
        } catch (RemoteException e) {
            throw exTranslator.translate(e);
        }
    }

    public <T> AsyncFuture<T> asyncRead(T template, long timeout, ReadModifiers modifiers, AsyncFutureListener<T> listener) throws DataAccessException {
        Transaction tx = getCurrentTransaction();
        try {
            return wrapFuture(space.asyncRead(template, tx, timeout, modifiers.getCode(), wrapListener(listener, tx)), tx);
        } catch (RemoteException e) {
            throw exTranslator.translate(e);
        }
    }

    @Override
    public <T> AsyncFuture<T> asyncRead(ISpaceQuery<T> template) throws DataAccessException {
        return asyncRead(template, defaultReadTimeout, getDefaultReadModifiers().getCode(), (AsyncFutureListener<T>) null);
    }

    @Override
    public <T> AsyncFuture<T> asyncRead(ISpaceQuery<T> template, AsyncFutureListener<T> listener) throws DataAccessException {
        return asyncRead(template, defaultReadTimeout, getDefaultReadModifiers().getCode(), listener);
    }

    @Override
    public <T> AsyncFuture<T> asyncRead(ISpaceQuery<T> template, long timeout) throws DataAccessException {
        return asyncRead(template, timeout, getDefaultReadModifiers().getCode(), (AsyncFutureListener<T>) null);
    }

    @Override
    public <T> AsyncFuture<T> asyncRead(ISpaceQuery<T> template, long timeout, AsyncFutureListener<T> listener) throws DataAccessException {
        return asyncRead(template, timeout, getDefaultReadModifiers().getCode(), listener);
    }

    @Override
    public <T> AsyncFuture<T> asyncRead(ISpaceQuery<T> template, long timeout, int modifiers) throws DataAccessException {
        return asyncRead(template, timeout, modifiers, (AsyncFutureListener<T>) null);
    }

    @Override
    public <T> AsyncFuture<T> asyncRead(ISpaceQuery<T> template, long timeout, ReadModifiers modifiers) throws DataAccessException {
        return asyncRead(template, timeout, modifiers.getCode(), (AsyncFutureListener<T>) null);
    }

    @Override
    public <T> AsyncFuture<T> asyncRead(ISpaceQuery<T> template, long timeout, int modifiers, AsyncFutureListener<T> listener) throws DataAccessException {
        Transaction tx = getCurrentTransaction();
        try {
            return wrapFuture(space.asyncRead(template, tx, timeout, modifiers, wrapListener(listener, tx)), tx);
        } catch (RemoteException e) {
            throw exTranslator.translate(e);
        }
    }

    @Override
    public <T> AsyncFuture<T> asyncRead(ISpaceQuery<T> template, long timeout, ReadModifiers modifiers, AsyncFutureListener<T> listener) throws DataAccessException {
        Transaction tx = getCurrentTransaction();
        try {
            return wrapFuture(space.asyncRead(template, tx, timeout, modifiers.getCode(), wrapListener(listener, tx)), tx);
        } catch (RemoteException e) {
            throw exTranslator.translate(e);
        }
    }

    public <T> T readIfExistsById(Class<T> clazz, Object id) {
        return readIfExistsById(clazz, id, null, defaultReadTimeout, getDefaultReadModifiers().getCode());
    }

    public <T> T readIfExistsById(Class<T> clazz, Object id, Object routing) {
        return readIfExistsById(clazz, id, routing, defaultReadTimeout, getDefaultReadModifiers().getCode());
    }

    public <T> T readIfExistsById(Class<T> clazz, Object id, Object routing, long timeout) {
        return readIfExistsById(clazz, id, routing, timeout, getDefaultReadModifiers().getCode());
    }

    @SuppressWarnings("unchecked")
    public <T> T readIfExistsById(Class<T> clazz, Object id, Object routing, long timeout, int modifiers) {
        try {
            return (T) space.readById(
                    ObjectUtils.assertArgumentNotNull(clazz, "class").getName(), id, routing, getCurrentTransaction(), timeout, modifiers, true, QueryResultTypeInternal.NOT_SET, null);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T readIfExistsById(Class<T> clazz, Object id, Object routing, long timeout, ReadModifiers modifiers) {
        try {
            return (T) space.readById(
                    ObjectUtils.assertArgumentNotNull(clazz, "class").getName(), id, routing, getCurrentTransaction(), timeout, modifiers.getCode(), true, QueryResultTypeInternal.NOT_SET, null);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T readIfExistsById(IdQuery<T> query) throws DataAccessException {
        try {
            return (T) space.readById(query.getTypeName(), query.getId(), query.getRouting(), getCurrentTransaction(), defaultReadTimeout, getDefaultReadModifiers().getCode(), true, toInternal(query.getQueryResultType()), query.getProjections());
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T readIfExistsById(IdQuery<T> query, long timeout) throws DataAccessException {
        try {
            return (T) space.readById(query.getTypeName(), query.getId(), query.getRouting(), getCurrentTransaction(), timeout, getDefaultReadModifiers().getCode(), true, toInternal(query.getQueryResultType()), query.getProjections());
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T readIfExistsById(IdQuery<T> query, long timeout, int modifiers) throws DataAccessException {
        try {
            return (T) space.readById(query.getTypeName(), query.getId(), query.getRouting(), getCurrentTransaction(), timeout, modifiers, true, toInternal(query.getQueryResultType()), query.getProjections());
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T readIfExistsById(IdQuery<T> query, long timeout, ReadModifiers modifiers) throws DataAccessException {
        try {
            return (T) space.readById(query.getTypeName(), query.getId(), query.getRouting(), getCurrentTransaction(), timeout, modifiers.getCode(), true, toInternal(query.getQueryResultType()), query.getProjections());
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    public <T> T readIfExists(T template) throws DataAccessException {
        return readIfExists(template, defaultReadTimeout);
    }

    public <T> T readIfExists(T template, long timeout) throws DataAccessException {
        return readIfExists(template, timeout, getDefaultReadModifiers().getCode());
    }

    @SuppressWarnings("unchecked")
    public <T> T readIfExists(T template, long timeout, int modifiers) throws DataAccessException {
        try {
            return (T) space.readIfExists(template, getCurrentTransaction(), timeout, modifiers);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T readIfExists(T template, long timeout, ReadModifiers modifiers) throws DataAccessException {
        try {
            return (T) space.readIfExists(template, getCurrentTransaction(), timeout, modifiers.getCode());
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    public <T> T readIfExists(ISpaceQuery<T> template) throws DataAccessException {
        return readIfExists(template, defaultReadTimeout);
    }

    public <T> T readIfExists(ISpaceQuery<T> template, long timeout) throws DataAccessException {
        return readIfExists(template, timeout, getDefaultReadModifiers().getCode());
    }

    @SuppressWarnings("unchecked")
    public <T> T readIfExists(ISpaceQuery<T> template, long timeout, int modifiers) throws DataAccessException {
        try {
            return (T) space.readIfExists(template, getCurrentTransaction(), timeout, modifiers);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T readIfExists(ISpaceQuery<T> template, long timeout, ReadModifiers modifiers) throws DataAccessException {
        try {
            return (T) space.readIfExists(template, getCurrentTransaction(), timeout, modifiers.getCode());
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    public <T> T[] readMultiple(T template) throws DataAccessException {
        return readMultiple(template, Integer.MAX_VALUE);
    }

    public <T> T[] readMultiple(T template, int maxEntries) throws DataAccessException {
        return readMultiple(template, maxEntries, getDefaultReadModifiers().getCode());
    }

    @SuppressWarnings("unchecked")
    public <T> T[] readMultiple(T template, int maxEntries, int modifiers) throws DataAccessException {
        try {
            return (T[]) space.readMultiple(template, getCurrentTransaction(), maxEntries, modifiers);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T[] readMultiple(T template, int maxEntries, ReadModifiers modifiers) throws DataAccessException {
        try {
            return (T[]) space.readMultiple(template, getCurrentTransaction(), maxEntries, modifiers.getCode());
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    public <T> T[] readMultiple(ISpaceQuery<T> template) throws DataAccessException {
        return readMultiple(template, Integer.MAX_VALUE);
    }

    public <T> T[] readMultiple(ISpaceQuery<T> template, int maxEntries) throws DataAccessException {
        return readMultiple(template, maxEntries, getDefaultReadModifiers().getCode());
    }

    @SuppressWarnings("unchecked")
    public <T> T[] readMultiple(ISpaceQuery<T> template, int maxEntries, int modifiers) throws DataAccessException {
        try {
            return (T[]) space.readMultiple(template, getCurrentTransaction(), maxEntries, modifiers);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T[] readMultiple(ISpaceQuery<T> template, int maxEntries, ReadModifiers modifiers) throws DataAccessException {
        try {
            return (T[]) space.readMultiple(template, getCurrentTransaction(), maxEntries, modifiers.getCode());
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    public <T> T takeById(Class<T> clazz, Object id) {
        return takeById(clazz, id, null, defaultTakeTimeout, defaultTakeModifiers.getCode());
    }

    public <T> T takeById(Class<T> clazz, Object id, Object routing) {
        return takeById(clazz, id, routing, defaultTakeTimeout, defaultTakeModifiers.getCode());
    }

    public <T> T takeById(Class<T> clazz, Object id, Object routing, long timeout) {
        return takeById(clazz, id, routing, timeout, defaultTakeModifiers.getCode());
    }

    @SuppressWarnings("unchecked")
    public <T> T takeById(Class<T> clazz, Object id, Object routing, long timeout, int modifiers) {
        try {
            return (T) space.takeById(
                    ObjectUtils.assertArgumentNotNull(clazz, "class").getName(), id, routing, 0, getCurrentTransaction(), timeout, modifiers, false, QueryResultTypeInternal.NOT_SET, null);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T takeById(Class<T> clazz, Object id, Object routing, long timeout, TakeModifiers modifiers) {
        try {
            return (T) space.takeById(
                    ObjectUtils.assertArgumentNotNull(clazz, "class").getName(), id, routing, 0, getCurrentTransaction(), timeout, modifiers.getCode(), false, QueryResultTypeInternal.NOT_SET, null);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T takeById(IdQuery<T> query) throws DataAccessException {
        try {
            return (T) space.takeById(query.getTypeName(), query.getId(), query.getRouting(), query.getVersion(),
                    getCurrentTransaction(), defaultTakeTimeout, defaultTakeModifiers.getCode(),
                    false, toInternal(query.getQueryResultType()), query.getProjections());
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T takeById(IdQuery<T> query, long timeout) throws DataAccessException {
        try {
            return (T) space.takeById(query.getTypeName(), query.getId(), query.getRouting(), query.getVersion(),
                    getCurrentTransaction(), timeout, defaultTakeModifiers.getCode(), false,
                    toInternal(query.getQueryResultType()), query.getProjections());
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T takeById(IdQuery<T> query, long timeout, int modifiers) throws DataAccessException {
        try {
            return (T) space.takeById(query.getTypeName(), query.getId(), query.getRouting(), query.getVersion(),
                    getCurrentTransaction(), timeout, modifiers, false,
                    toInternal(query.getQueryResultType()), query.getProjections());
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T takeById(IdQuery<T> query, long timeout, TakeModifiers modifiers) throws DataAccessException {
        try {
            return (T) space.takeById(query.getTypeName(), query.getId(), query.getRouting(), query.getVersion(),
                    getCurrentTransaction(), timeout, modifiers.getCode(), false,
                    toInternal(query.getQueryResultType()), query.getProjections());
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    public <T> T take(T template) throws DataAccessException {
        return take(template, defaultTakeTimeout);
    }

    public <T> T take(T template, long timeout) throws DataAccessException {
        return take(template, timeout, defaultTakeModifiers.getCode());
    }

    @SuppressWarnings("unchecked")
    public <T> T take(T template, long timeout, int modifiers) throws DataAccessException {
        try {
            return (T) space.take(template, getCurrentTransaction(), timeout, modifiers, false);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T take(T template, long timeout, TakeModifiers modifiers) throws DataAccessException {
        try {
            return (T) space.take(template, getCurrentTransaction(), timeout, modifiers.getCode(), false);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    public <T> T take(ISpaceQuery<T> template) throws DataAccessException {
        return take(template, defaultTakeTimeout);
    }

    public <T> T take(ISpaceQuery<T> template, long timeout) throws DataAccessException {
        return take(template, timeout, defaultTakeModifiers.getCode());
    }

    @SuppressWarnings("unchecked")
    public <T> T take(ISpaceQuery<T> template, long timeout, int modifiers) throws DataAccessException {
        try {
            return (T) space.take(template, getCurrentTransaction(), timeout, modifiers, false);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T take(ISpaceQuery<T> template, long timeout, TakeModifiers modifiers) throws DataAccessException {
        try {
            return (T) space.take(template, getCurrentTransaction(), timeout, modifiers.getCode(), false);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }


    public <T> AsyncFuture<T> asyncTake(T template) throws DataAccessException {
        return asyncTake(template, defaultTakeTimeout, defaultTakeModifiers.getCode(), null);
    }

    public <T> AsyncFuture<T> asyncTake(T template, AsyncFutureListener<T> listener) throws DataAccessException {
        return asyncTake(template, defaultTakeTimeout, defaultTakeModifiers.getCode(), listener);
    }

    public <T> AsyncFuture<T> asyncTake(T template, long timeout) throws DataAccessException {
        return asyncTake(template, timeout, defaultTakeModifiers.getCode(), null);
    }

    public <T> AsyncFuture<T> asyncTake(T template, long timeout, AsyncFutureListener<T> listener) throws DataAccessException {
        return asyncTake(template, timeout, defaultTakeModifiers.getCode(), listener);
    }

    public <T> AsyncFuture<T> asyncTake(T template, long timeout, int modifiers) throws DataAccessException {
        return asyncTake(template, timeout, modifiers, null);
    }

    public <T> AsyncFuture<T> asyncTake(T template, long timeout, TakeModifiers modifiers) throws DataAccessException {
        return asyncTake(template, timeout, modifiers.getCode(), null);
    }

    public <T> AsyncFuture<T> asyncTake(T template, long timeout, int modifiers, AsyncFutureListener<T> listener) throws DataAccessException {
        Transaction tx = getCurrentTransaction();
        try {
            return wrapFuture(space.asyncTake(template, tx, timeout, modifiers, wrapListener(listener, tx)), tx);
        } catch (RemoteException e) {
            throw exTranslator.translate(e);
        }
    }

    public <T> AsyncFuture<T> asyncTake(T template, long timeout, TakeModifiers modifiers, AsyncFutureListener<T> listener) throws DataAccessException {
        Transaction tx = getCurrentTransaction();
        try {
            return wrapFuture(space.asyncTake(template, tx, timeout, modifiers.getCode(), wrapListener(listener, tx)), tx);
        } catch (RemoteException e) {
            throw exTranslator.translate(e);
        }
    }

    public <T> AsyncFuture<T> asyncTake(ISpaceQuery<T> template) throws DataAccessException {
        return asyncTake(template, defaultTakeTimeout, defaultTakeModifiers.getCode(), (AsyncFutureListener<T>) null);
    }

    public <T> AsyncFuture<T> asyncTake(ISpaceQuery<T> template, AsyncFutureListener<T> listener) throws DataAccessException {
        return asyncTake(template, defaultTakeTimeout, defaultTakeModifiers.getCode(), listener);
    }

    public <T> AsyncFuture<T> asyncTake(ISpaceQuery<T> template, long timeout) throws DataAccessException {
        return asyncTake(template, timeout, defaultTakeModifiers.getCode(), (AsyncFutureListener<T>) null);
    }

    public <T> AsyncFuture<T> asyncTake(ISpaceQuery<T> template, long timeout, AsyncFutureListener<T> listener) throws DataAccessException {
        return asyncTake(template, timeout, defaultTakeModifiers.getCode(), listener);
    }

    public <T> AsyncFuture<T> asyncTake(ISpaceQuery<T> template, long timeout, int modifiers) throws DataAccessException {
        return asyncTake(template, timeout, modifiers, (AsyncFutureListener<T>) null);
    }

    public <T> AsyncFuture<T> asyncTake(ISpaceQuery<T> template, long timeout, TakeModifiers modifiers) throws DataAccessException {
        return asyncTake(template, timeout, modifiers.getCode(), (AsyncFutureListener<T>) null);
    }

    public <T> AsyncFuture<T> asyncTake(ISpaceQuery<T> template, long timeout, int modifiers, AsyncFutureListener<T> listener) throws DataAccessException {
        Transaction tx = getCurrentTransaction();
        try {
            return wrapFuture(space.asyncTake(template, tx, timeout, modifiers, wrapListener(listener, tx)), tx);
        } catch (RemoteException e) {
            throw exTranslator.translate(e);
        }
    }

    public <T> AsyncFuture<T> asyncTake(ISpaceQuery<T> template, long timeout, TakeModifiers modifiers, AsyncFutureListener<T> listener) throws DataAccessException {
        Transaction tx = getCurrentTransaction();
        try {
            return wrapFuture(space.asyncTake(template, tx, timeout, modifiers.getCode(), wrapListener(listener, tx)), tx);
        } catch (RemoteException e) {
            throw exTranslator.translate(e);
        }
    }

    public <T> T takeIfExistsById(Class<T> clazz, Object id) {
        return takeIfExistsById(clazz, id, null, defaultTakeTimeout, defaultTakeModifiers.getCode());
    }

    public <T> T takeIfExistsById(Class<T> clazz, Object id, Object routing) {
        return takeIfExistsById(clazz, id, routing, defaultTakeTimeout, defaultTakeModifiers.getCode());
    }

    public <T> T takeIfExistsById(Class<T> clazz, Object id, Object routing, long timeout) {
        return takeIfExistsById(clazz, id, routing, timeout, defaultTakeModifiers.getCode());
    }

    @SuppressWarnings("unchecked")
    public <T> T takeIfExistsById(Class<T> clazz, Object id, Object routing, long timeout, int modifiers) {
        try {
            return (T) space.takeById(
                    ObjectUtils.assertArgumentNotNull(clazz, "class").getName(), id, routing, 0, getCurrentTransaction(), timeout, modifiers, true, QueryResultTypeInternal.NOT_SET, null);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T takeIfExistsById(Class<T> clazz, Object id, Object routing, long timeout, TakeModifiers modifiers) {
        try {
            return (T) space.takeById(
                    ObjectUtils.assertArgumentNotNull(clazz, "class").getName(), id, routing, 0, getCurrentTransaction(), timeout, modifiers.getCode(), true, QueryResultTypeInternal.NOT_SET, null);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T takeIfExistsById(IdQuery<T> query) throws DataAccessException {
        try {
            return (T) space.takeById(query.getTypeName(), query.getId(), query.getRouting(), query.getVersion(), getCurrentTransaction(), defaultTakeTimeout, defaultTakeModifiers.getCode(), true, toInternal(query.getQueryResultType()), query.getProjections());
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T takeIfExistsById(IdQuery<T> query, long timeout) throws DataAccessException {
        try {
            return (T) space.takeById(query.getTypeName(), query.getId(), query.getRouting(), query.getVersion(), getCurrentTransaction(), timeout, defaultTakeModifiers.getCode(), true, toInternal(query.getQueryResultType()), query.getProjections());
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T takeIfExistsById(IdQuery<T> query, long timeout, int modifiers) throws DataAccessException {
        try {
            return (T) space.takeById(query.getTypeName(), query.getId(), query.getRouting(), query.getVersion(), getCurrentTransaction(), timeout, modifiers, true, toInternal(query.getQueryResultType()), query.getProjections());
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T takeIfExistsById(IdQuery<T> query, long timeout, TakeModifiers modifiers) throws DataAccessException {
        try {
            return (T) space.takeById(query.getTypeName(), query.getId(), query.getRouting(), query.getVersion(), getCurrentTransaction(), timeout, modifiers.getCode(), true, toInternal(query.getQueryResultType()), query.getProjections());
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    public <T> T takeIfExists(T template) throws DataAccessException {
        return takeIfExists(template, defaultTakeTimeout, defaultTakeModifiers.getCode());
    }

    public <T> T takeIfExists(T template, long timeout) throws DataAccessException {
        return takeIfExists(template, timeout, defaultTakeModifiers.getCode());
    }

    @SuppressWarnings("unchecked")
    public <T> T takeIfExists(T template, long timeout, int modifiers) throws DataAccessException {
        try {
            return (T) space.take(template, getCurrentTransaction(), timeout, modifiers, true);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T takeIfExists(T template, long timeout, TakeModifiers modifiers) throws DataAccessException {
        try {
            return (T) space.take(template, getCurrentTransaction(), timeout, modifiers.getCode(), true);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    public <T> T takeIfExists(ISpaceQuery<T> template) throws DataAccessException {
        return takeIfExists(template, defaultTakeTimeout, defaultTakeModifiers.getCode());
    }

    public <T> T takeIfExists(ISpaceQuery<T> template, long timeout) throws DataAccessException {
        return takeIfExists(template, timeout, defaultTakeModifiers.getCode());
    }

    @SuppressWarnings("unchecked")
    public <T> T takeIfExists(ISpaceQuery<T> template, long timeout, int modifiers) throws DataAccessException {
        try {
            return (T) space.take(template, getCurrentTransaction(), timeout, modifiers, true);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T takeIfExists(ISpaceQuery<T> template, long timeout, TakeModifiers modifiers) throws DataAccessException {
        try {
            return (T) space.take(template, getCurrentTransaction(), timeout, modifiers.getCode(), true);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    public <T> T[] takeMultiple(T template) throws DataAccessException {
        return takeMultiple(template, Integer.MAX_VALUE);
    }

    @SuppressWarnings("unchecked")
    public <T> T[] takeMultiple(T template, int maxEntries) throws DataAccessException {
        try {
            return (T[]) space.takeMultiple(template, getCurrentTransaction(), maxEntries, defaultTakeModifiers.getCode());
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    public <T> T[] takeMultiple(ISpaceQuery<T> template) throws DataAccessException {
        return takeMultiple(template, Integer.MAX_VALUE);
    }

    @SuppressWarnings("unchecked")
    public <T> T[] takeMultiple(ISpaceQuery<T> template, int maxEntries) throws DataAccessException {
        try {
            return (T[]) space.takeMultiple(template, getCurrentTransaction(), maxEntries, defaultTakeModifiers.getCode());
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T[] takeMultiple(T template, int maxEntries, int modifiers) throws DataAccessException {
        try {
            return (T[]) space.takeMultiple(template, getCurrentTransaction(), maxEntries, modifiers);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T[] takeMultiple(T template, int maxEntries, TakeModifiers modifiers) throws DataAccessException {
        try {
            return (T[]) space.takeMultiple(template, getCurrentTransaction(), maxEntries, modifiers.getCode());
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T[] takeMultiple(ISpaceQuery<T> template, int maxEntries, int modifiers) throws DataAccessException {
        try {
            return (T[]) space.takeMultiple(template, getCurrentTransaction(), maxEntries, modifiers);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T[] takeMultiple(ISpaceQuery<T> template, int maxEntries, TakeModifiers modifiers) throws DataAccessException {
        try {
            return (T[]) space.takeMultiple(template, getCurrentTransaction(), maxEntries, modifiers.getCode());
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    public <T> LeaseContext<T> write(T entry) throws DataAccessException {
        return write(entry, defaultWriteLease);
    }

    @SuppressWarnings("unchecked")
    public <T> LeaseContext<T> write(T entry, long lease) throws DataAccessException {
        try {
            return space.write(entry, getCurrentTransaction(), lease, 0, defaultWriteModifiers.getCode());
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    public <T> LeaseContext<T> write(T entry, WriteModifiers modifiers) throws DataAccessException {
        try {
            return space.write(entry, getCurrentTransaction(), defaultWriteLease, 0, modifiers.getCode());
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> LeaseContext<T> write(T entry, long lease, long timeout, int modifiers) throws DataAccessException {
        try {
            return space.write(entry, getCurrentTransaction(), lease, timeout, modifiers);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    public <T> LeaseContext<T> write(T entry, long lease, long timeout, WriteModifiers modifiers) throws DataAccessException {
        try {
            return space.write(entry, getCurrentTransaction(), lease, timeout, modifiers.getCode());
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @Override
    public <T> AggregationResult aggregate(ISpaceQuery<T> query, AggregationSet aggregationSet) {
        try {
            return space.aggregate(query, aggregationSet, getCurrentTransaction(), getDefaultReadModifiers().getCode());
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @Override
    public <T> ChangeResult<T> change(ISpaceQuery<T> query, ChangeSet changeSet) {
        try {
            return space.change(query, changeSet, getCurrentTransaction(), 0, defaultChangeModifiers);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @Override
    public <T> ChangeResult<T> change(ISpaceQuery<T> query, ChangeSet changeSet, ChangeModifiers modifiers) {
        try {
            return space.change(query, changeSet, getCurrentTransaction(), 0, modifiers);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    ;

    @Override
    public <T> ChangeResult<T> change(ISpaceQuery<T> query, ChangeSet changeSet, long timeout) {
        try {
            return space.change(query, changeSet, getCurrentTransaction(), timeout, defaultChangeModifiers);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @Override
    public <T> ChangeResult<T> change(ISpaceQuery<T> query, ChangeSet changeSet, ChangeModifiers modifiers, long timeout) {
        try {
            return space.change(query, changeSet, getCurrentTransaction(), timeout, modifiers);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    ;

    @Override
    public <T> Future<ChangeResult<T>> asyncChange(ISpaceQuery<T> query, ChangeSet changeSet) {
        try {
            return space.asyncChange(query, changeSet, getCurrentTransaction(), 0, defaultChangeModifiers, null);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @Override
    public <T> Future<ChangeResult<T>> asyncChange(ISpaceQuery<T> query, ChangeSet changeSet,
                                                   AsyncFutureListener<ChangeResult<T>> listener) {
        try {
            return space.asyncChange(query, changeSet, getCurrentTransaction(), 0, defaultChangeModifiers, listener);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @Override
    public <T> Future<ChangeResult<T>> asyncChange(ISpaceQuery<T> query, ChangeSet changeSet, long timeout) {
        try {
            return space.asyncChange(query, changeSet, getCurrentTransaction(), timeout, defaultChangeModifiers, null);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @Override
    public <T> Future<ChangeResult<T>> asyncChange(ISpaceQuery<T> query, ChangeSet changeSet, long timeout,
                                                   AsyncFutureListener<ChangeResult<T>> listener) {
        try {
            return space.asyncChange(query, changeSet, getCurrentTransaction(), timeout, defaultChangeModifiers, listener);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @Override
    public <T> Future<ChangeResult<T>> asyncChange(ISpaceQuery<T> query, ChangeSet changeSet, ChangeModifiers modifiers) {
        try {
            return space.asyncChange(query, changeSet, getCurrentTransaction(), 0, modifiers, null);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @Override
    public <T> Future<ChangeResult<T>> asyncChange(ISpaceQuery<T> query, ChangeSet changeSet, ChangeModifiers modifiers,
                                                   AsyncFutureListener<ChangeResult<T>> listener) {
        try {
            return space.asyncChange(query, changeSet, getCurrentTransaction(), 0, modifiers, listener);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @Override
    public <T> Future<ChangeResult<T>> asyncChange(ISpaceQuery<T> query, ChangeSet changeSet, ChangeModifiers modifiers,
                                                   long timeout) {
        try {
            return space.asyncChange(query, changeSet, getCurrentTransaction(), timeout, modifiers, null);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @Override
    public <T> Future<ChangeResult<T>> asyncChange(ISpaceQuery<T> query, ChangeSet changeSet, ChangeModifiers modifiers,
                                                   long timeout, AsyncFutureListener<ChangeResult<T>> listener) {
        try {
            return space.asyncChange(query, changeSet, getCurrentTransaction(), timeout, modifiers, listener);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @Override
    public <T> ChangeResult<T> change(T template, ChangeSet changeSet) {
        try {
            return space.change(template, changeSet, getCurrentTransaction(), 0, defaultChangeModifiers);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @Override
    public <T> ChangeResult<T> change(T template, ChangeSet changeSet, ChangeModifiers modifiers) {
        try {
            return space.change(template, changeSet, getCurrentTransaction(), 0, modifiers);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    ;

    @Override
    public <T> ChangeResult<T> change(T template, ChangeSet changeSet, long timeout) {
        try {
            return space.change(template, changeSet, getCurrentTransaction(), timeout, defaultChangeModifiers);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @Override
    public <T> ChangeResult<T> change(T template, ChangeSet changeSet, ChangeModifiers modifiers, long timeout) {
        try {
            return space.change(template, changeSet, getCurrentTransaction(), timeout, modifiers);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    ;

    @Override
    public <T> Future<ChangeResult<T>> asyncChange(T template, ChangeSet changeSet) {
        try {
            return space.asyncChange(template, changeSet, getCurrentTransaction(), 0, defaultChangeModifiers, null);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @Override
    public <T> Future<ChangeResult<T>> asyncChange(T template, ChangeSet changeSet,
                                                   AsyncFutureListener<ChangeResult<T>> listener) {
        try {
            return space.asyncChange(template, changeSet, getCurrentTransaction(), 0, defaultChangeModifiers, listener);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @Override
    public <T> Future<ChangeResult<T>> asyncChange(T template, ChangeSet changeSet, long timeout) {
        try {
            return space.asyncChange(template, changeSet, getCurrentTransaction(), timeout, defaultChangeModifiers, null);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @Override
    public <T> Future<ChangeResult<T>> asyncChange(T template, ChangeSet changeSet, long timeout,
                                                   AsyncFutureListener<ChangeResult<T>> listener) {
        try {
            return space.asyncChange(template, changeSet, getCurrentTransaction(), timeout, defaultChangeModifiers, listener);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @Override
    public <T> Future<ChangeResult<T>> asyncChange(T template, ChangeSet changeSet, ChangeModifiers modifiers) {
        try {
            return space.asyncChange(template, changeSet, getCurrentTransaction(), 0, modifiers, null);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @Override
    public <T> Future<ChangeResult<T>> asyncChange(T template, ChangeSet changeSet, ChangeModifiers modifiers,
                                                   AsyncFutureListener<ChangeResult<T>> listener) {
        try {
            return space.asyncChange(template, changeSet, getCurrentTransaction(), 0, modifiers, listener);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @Override
    public <T> Future<ChangeResult<T>> asyncChange(T template, ChangeSet changeSet, ChangeModifiers modifiers,
                                                   long timeout) {
        try {
            return space.asyncChange(template, changeSet, getCurrentTransaction(), timeout, modifiers, null);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @Override
    public <T> Future<ChangeResult<T>> asyncChange(T template, ChangeSet changeSet, ChangeModifiers modifiers,
                                                   long timeout, AsyncFutureListener<ChangeResult<T>> listener) {
        try {
            return space.asyncChange(template, changeSet, getCurrentTransaction(), timeout, modifiers, listener);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }


    public <T> LeaseContext<T>[] writeMultiple(T[] entries) throws DataAccessException {
        return writeMultiple(entries, defaultWriteLease);
    }

    public <T> LeaseContext<T>[] writeMultiple(T[] entries, WriteModifiers modifiers) throws DataAccessException {
        return writeMultiple(entries, defaultWriteLease, modifiers);
    }

    @SuppressWarnings("unchecked")
    public <T> LeaseContext<T>[] writeMultiple(T[] entries, long lease) throws DataAccessException {
        try {
            return space.writeMultiple(entries, getCurrentTransaction(), lease, null, 0, defaultWriteModifiers.getCode());
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> LeaseContext<T>[] writeMultiple(T[] entries, long lease, int updateModifiers) throws DataAccessException {
        try {
            return space.writeMultiple(entries, getCurrentTransaction(), lease, null, 0, updateModifiers);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> LeaseContext<T>[] writeMultiple(T[] entries, long lease, WriteModifiers modifiers) throws DataAccessException {
        try {
            return space.writeMultiple(entries, getCurrentTransaction(), lease, null, 0, modifiers.getCode());
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> LeaseContext<T>[] writeMultiple(T[] entries, long lease, long timeout, WriteModifiers modifiers) throws DataAccessException {
        try {
            return space.writeMultiple(entries, getCurrentTransaction(), lease, null, timeout, modifiers.getCode());
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> LeaseContext<T>[] writeMultiple(T[] entries, long[] leases, int updateModifiers) throws DataAccessException {
        try {
            return space.writeMultiple(entries, getCurrentTransaction(), Long.MIN_VALUE, leases, 0, updateModifiers);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> LeaseContext<T>[] writeMultiple(T[] entries, long[] leases, WriteModifiers modifiers) throws DataAccessException {
        try {
            return space.writeMultiple(entries, getCurrentTransaction(), Long.MIN_VALUE, leases, 0, modifiers.getCode());
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> LeaseContext<T>[] writeMultiple(T[] entries, long[] leases, long timeout, WriteModifiers modifiers) throws DataAccessException {
        try {
            return space.writeMultiple(entries, getCurrentTransaction(), Long.MIN_VALUE, leases, timeout, modifiers.getCode());
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }


    public IteratorBuilder iterator() {
        return new IteratorBuilder(this);
    }

    @Override
    public <T> SpaceIterator<T> iterator(T template) {
        return iterator(template, SpaceIterator.getDefaultBatchSize(), getDefaultReadModifiers());
    }

    @Override
    public <T> SpaceIterator<T> iterator(T template, int batchSize) {
        return iterator(template, batchSize, getDefaultReadModifiers());
    }

    @Override
    public <T> SpaceIterator<T> iterator(T template, int batchSize, ReadModifiers modifiers) {
        try {
            return new SpaceIterator<T>(space, template, getCurrentTransaction(), batchSize, modifiers);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @Override
    public <T> SpaceIterator<T> iterator(ISpaceQuery<T> template) {
        return iterator(template, SpaceIterator.getDefaultBatchSize(), getDefaultReadModifiers());
    }

    @Override
    public <T> SpaceIterator<T> iterator(ISpaceQuery<T> template, int batchSize) {
        return iterator(template, batchSize, getDefaultReadModifiers());
    }

    @Override
    public <T> SpaceIterator<T> iterator(ISpaceQuery<T> template, int batchSize, ReadModifiers modifiers) {
        try {
            return new SpaceIterator<T>(space, template, getCurrentTransaction(), batchSize, modifiers);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    public <T extends Serializable> AsyncFuture<T> execute(Task<T> task) {
        return execute(task, (AsyncFutureListener<T>) null);
    }

    public <T extends Serializable> AsyncFuture<T> execute(Task<T> task, AsyncFutureListener<T> listener) {
        if (task instanceof DistributedTask) {
            return distExecute((DistributedTask) task, listener);
        }
        Object routing = null;
        if (task instanceof TaskRoutingProvider) {
            Object optionalRouting = executorMetaDataProvider.findRouting(((TaskRoutingProvider) task).getRouting());
            if (optionalRouting != null) {
                routing = optionalRouting;
            }
        }
        if (routing == null) {
            routing = executorMetaDataProvider.findRouting(task);
        }
        return execute(task, routing, listener);
    }

    public <T extends Serializable> AsyncFuture<T> execute(Task<T> task, Object routing) {
        return execute(task, routing, (AsyncFutureListener<T>) null);
    }

    public <T extends Serializable> AsyncFuture<T> execute(Task<T> task, Object routing, AsyncFutureListener<T> listener) {
        Object optionalRouting = executorMetaDataProvider.findRouting(routing);
        if (optionalRouting != null) {
            routing = optionalRouting;
        }
        try {
            Transaction tx = getCurrentTransaction();
            return wrapFuture(space.execute(new InternalSpaceTaskWrapper<T>(task, routing), routing, tx, wrapListener(listener, tx)), tx);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    public <T extends Serializable, R> AsyncFuture<R> execute(DistributedTask<T, R> task, Object... routings) {
        AsyncFutureListener<R> listener = null;
        int numberOfRoutings = routings.length;
        if (routings.length > 0 && routings[routings.length - 1] instanceof AsyncFutureListener) {
            listener = (AsyncFutureListener<R>) routings[routings.length - 1];
            numberOfRoutings -= 1;
        }
        if (numberOfRoutings == 0) {
            return execute(task, listener);
        }
        AsyncFuture<T>[] futures = new AsyncFuture[numberOfRoutings];
        Transaction tx = getCurrentTransaction();
        for (int i = 0; i < numberOfRoutings; i++) {
            try {
                Object routing = routings[i];
                Object optionalRouting = executorMetaDataProvider.findRouting(routing);
                if (optionalRouting != null) {
                    routing = optionalRouting;
                }
                futures[i] = space.execute(new InternalSpaceTaskWrapper<T>(task, routing), routing, tx, (AsyncFutureListener) null);
            } catch (Exception e) {
                throw exTranslator.translate(e);
            }
        }
        AsyncFuture<R> result;
        if (task instanceof AsyncResultFilter) {
            result = FutureFactory.create(futures, task, (AsyncResultFilter<T>) task);
        } else {
            result = FutureFactory.create(futures, task);
        }
        result = wrapFuture(result, tx);
        if (listener != null) {
            result.setListener(wrapListener(listener, tx));
        }
        return result;
    }

    public <T extends Serializable, R> AsyncFuture<R> execute(DistributedTask<T, R> task) {
        return distExecute(task, null);
    }

    public <T extends Serializable, R> AsyncFuture<R> execute(DistributedTask<T, R> task, AsyncFutureListener<R> listener) {
        return distExecute(task, listener);
    }

    public <T extends Serializable, R> AsyncFuture<R> distExecute(DistributedTask<T, R> task, AsyncFutureListener<R> listener) {
        try {
            Transaction tx = getCurrentTransaction();
            InternalDistributedSpaceTaskWrapper<T, R> taskWrapper = new InternalDistributedSpaceTaskWrapper<T, R>(task);
            return wrapFuture(space.execute(taskWrapper, taskWrapper.getRouting(), tx, wrapListener(listener, tx)), tx);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    public <T extends Serializable, R> ExecutorBuilder<T, R> executorBuilder(AsyncResultsReducer<T, R> reducer) {
        return new ExecutorBuilder<T, R>(this, reducer);
    }

    // Support methods

    public Transaction getCurrentTransaction() {
        Transaction.Created txCreated = txProvider.getCurrentTransaction(this, space);
        if (txCreated != null) {
            return txCreated.transaction;
        }
        return null;
    }

    /**
     * Gets the isolation level from the current running transaction (enabling the usage of Spring
     * declarative isolation level settings). If there is no transaction in progress or the
     * transaction isolation is {@link org.springframework.transaction.TransactionDefinition#ISOLATION_DEFAULT}
     * will use the default isolation level associated with this class.
     */
    public int getModifiersForIsolationLevel() {
        int currentSpringIsolationLevel = txProvider.getCurrentTransactionIsolationLevel(this);
        return IsolationLevelHelpers.convertSpringToSpaceIsolationLevel(currentSpringIsolationLevel, defaultIsolationLevel);
    }

    @Override
    public String toString() {
        return space.toString();
    }

    public <T> AsyncFutureListener<T> wrapListener(AsyncFutureListener<T> listener, Transaction tx) {
        if (listener == null) {
            return null;
        }
        if (tx == null) {
            return new InternalAsyncFutureListener<T>(this, listener);
        }
        return InternalAsyncFutureListener.wrapIfNeeded(listener, this);
    }

    public <T> AsyncFuture<T> wrapFuture(AsyncFuture<T> future, Transaction tx) {
        return new InternalAsyncFuture<T>(future, this, tx);
    }

    @SuppressWarnings("unchecked")
    public <T> ReadByIdsResult<T> readByIds(Class<T> clazz, Object[] ids) {
        try {
            return new ReadByIdsResultImpl<T>((T[]) space.readByIds(
                    ObjectUtils.assertArgumentNotNull(clazz, "class").getName(), ids, null, getCurrentTransaction(), getDefaultReadModifiers().getCode(), QueryResultTypeInternal.NOT_SET, false, null));
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> ReadByIdsResult<T> readByIds(Class<T> clazz, Object[] ids, int modifiers) {
        try {
            return new ReadByIdsResultImpl<T>((T[]) space.readByIds(
                    ObjectUtils.assertArgumentNotNull(clazz, "class").getName(), ids, null, getCurrentTransaction(), modifiers, QueryResultTypeInternal.NOT_SET, false, null));
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> ReadByIdsResult<T> readByIds(Class<T> clazz, Object[] ids, Object routing, int modifiers) {
        try {
            return new ReadByIdsResultImpl<T>((T[]) space.readByIds(
                    ObjectUtils.assertArgumentNotNull(clazz, "class").getName(), ids, routing, getCurrentTransaction(), modifiers, QueryResultTypeInternal.NOT_SET, false, null));
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> ReadByIdsResult<T> readByIds(Class<T> clazz, Object[] ids, Object routing, ReadModifiers modifiers) {
        try {
            return new ReadByIdsResultImpl<T>((T[]) space.readByIds(
                    ObjectUtils.assertArgumentNotNull(clazz, "class").getName(), ids, routing, getCurrentTransaction(), modifiers.getCode(), QueryResultTypeInternal.NOT_SET, false, null));
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> ReadByIdsResult<T> readByIds(Class<T> clazz, Object[] ids, Object[] routings, int modifiers) {
        try {
            return new ReadByIdsResultImpl<T>((T[]) space.readByIds(
                    ObjectUtils.assertArgumentNotNull(clazz, "class").getName(), ids, routings, getCurrentTransaction(), modifiers, QueryResultTypeInternal.NOT_SET, false, null));
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> ReadByIdsResult<T> readByIds(Class<T> clazz, Object[] ids, Object[] routings, ReadModifiers modifiers) {
        try {
            return new ReadByIdsResultImpl<T>((T[]) space.readByIds(
                    ObjectUtils.assertArgumentNotNull(clazz, "class").getName(), ids, routings, getCurrentTransaction(), modifiers.getCode(), QueryResultTypeInternal.NOT_SET, false, null));
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> ReadByIdsResult<T> readByIds(Class<T> clazz, Object[] ids, Object routing) {
        try {
            return new ReadByIdsResultImpl<T>((T[]) space.readByIds(
                    ObjectUtils.assertArgumentNotNull(clazz, "class").getName(), ids, routing, getCurrentTransaction(), getDefaultReadModifiers().getCode(), QueryResultTypeInternal.NOT_SET, false, null));
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> ReadByIdsResult<T> readByIds(Class<T> clazz, Object[] ids, Object[] routings) {
        try {
            return new ReadByIdsResultImpl<T>((T[]) space.readByIds(
                    ObjectUtils.assertArgumentNotNull(clazz, "class").getName(), ids, routings, getCurrentTransaction(), getDefaultReadModifiers().getCode(), QueryResultTypeInternal.NOT_SET, false, null));
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> ReadByIdsResult<T> readByIds(IdsQuery<T> query) throws DataAccessException {
        try {
            if (query.getRouting() != null)
                return new ReadByIdsResultImpl<T>((T[]) space.readByIds(query.getTypeName(), query.getIds(), query.getRouting(), getCurrentTransaction(), getDefaultReadModifiers().getCode(), toInternal(query.getQueryResultType()), false, query.getProjections()));
            else
                return new ReadByIdsResultImpl<T>((T[]) space.readByIds(query.getTypeName(), query.getIds(), query.getRoutings(), getCurrentTransaction(), getDefaultReadModifiers().getCode(), toInternal(query.getQueryResultType()), false, query.getProjections()));
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> ReadByIdsResult<T> readByIds(IdsQuery<T> query, int modifiers) throws DataAccessException {
        try {
            if (query.getRouting() != null)
                return new ReadByIdsResultImpl<T>((T[]) space.readByIds(query.getTypeName(), query.getIds(), query.getRouting(), getCurrentTransaction(), modifiers, toInternal(query.getQueryResultType()), false, query.getProjections()));
            else
                return new ReadByIdsResultImpl<T>((T[]) space.readByIds(query.getTypeName(), query.getIds(), query.getRoutings(), getCurrentTransaction(), modifiers, toInternal(query.getQueryResultType()), false, query.getProjections()));
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> ReadByIdsResult<T> readByIds(IdsQuery<T> query, ReadModifiers modifiers) throws DataAccessException {
        try {
            if (query.getRouting() != null)
                return new ReadByIdsResultImpl<T>((T[]) space.readByIds(query.getTypeName(), query.getIds(), query.getRouting(), getCurrentTransaction(), modifiers.getCode(), toInternal(query.getQueryResultType()), false, query.getProjections()));
            else
                return new ReadByIdsResultImpl<T>((T[]) space.readByIds(query.getTypeName(), query.getIds(), query.getRoutings(), getCurrentTransaction(), modifiers.getCode(), toInternal(query.getQueryResultType()), false, query.getProjections()));
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> TakeByIdsResult<T> takeByIds(Class<T> clazz, Object[] ids, int modifiers) {
        try {
            return new TakeByIdsResultImpl<T>((T[]) space.takeByIds(
                    ObjectUtils.assertArgumentNotNull(clazz, "class").getName(), ids, null, getCurrentTransaction(), modifiers, QueryResultTypeInternal.NOT_SET, false, null));
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> TakeByIdsResult<T> takeByIds(Class<T> clazz, Object[] ids, Object routing, int modifiers) {
        try {
            return new TakeByIdsResultImpl<T>((T[]) space.takeByIds(
                    ObjectUtils.assertArgumentNotNull(clazz, "class").getName(), ids, routing, getCurrentTransaction(), modifiers, QueryResultTypeInternal.NOT_SET, false, null));
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> TakeByIdsResult<T> takeByIds(Class<T> clazz, Object[] ids, Object routing, TakeModifiers modifiers) {
        try {
            return new TakeByIdsResultImpl<T>((T[]) space.takeByIds(
                    ObjectUtils.assertArgumentNotNull(clazz, "class").getName(), ids, routing, getCurrentTransaction(), modifiers.getCode(), QueryResultTypeInternal.NOT_SET, false, null));
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> TakeByIdsResult<T> takeByIds(Class<T> clazz, Object[] ids, Object[] routings, int modifiers) {
        try {
            return new TakeByIdsResultImpl<T>((T[]) space.takeByIds(
                    ObjectUtils.assertArgumentNotNull(clazz, "class").getName(), ids, routings, getCurrentTransaction(), modifiers, QueryResultTypeInternal.NOT_SET, false, null));
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> TakeByIdsResult<T> takeByIds(Class<T> clazz, Object[] ids, Object[] routings, TakeModifiers modifiers) {
        try {
            return new TakeByIdsResultImpl<T>((T[]) space.takeByIds(
                    ObjectUtils.assertArgumentNotNull(clazz, "class").getName(), ids, routings, getCurrentTransaction(), modifiers.getCode(), QueryResultTypeInternal.NOT_SET, false, null));
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> TakeByIdsResult<T> takeByIds(Class<T> clazz, Object[] ids) {
        try {
            return new TakeByIdsResultImpl<T>((T[]) space.takeByIds(
                    ObjectUtils.assertArgumentNotNull(clazz, "class").getName(), ids, null, getCurrentTransaction(), defaultTakeModifiers.getCode(), QueryResultTypeInternal.NOT_SET, false, null));
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> TakeByIdsResult<T> takeByIds(Class<T> clazz, Object[] ids, Object routing) {
        try {
            return new TakeByIdsResultImpl<T>((T[]) space.takeByIds(
                    ObjectUtils.assertArgumentNotNull(clazz, "class").getName(), ids, routing, getCurrentTransaction(), defaultTakeModifiers.getCode(), QueryResultTypeInternal.NOT_SET, false, null));
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> TakeByIdsResult<T> takeByIds(Class<T> clazz, Object[] ids, Object[] routings) {
        try {
            return new TakeByIdsResultImpl<T>((T[]) space.takeByIds(
                    ObjectUtils.assertArgumentNotNull(clazz, "class").getName(), ids, routings, getCurrentTransaction(), defaultTakeModifiers.getCode(), QueryResultTypeInternal.NOT_SET, false, null));
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> TakeByIdsResult<T> takeByIds(IdsQuery<T> query) throws DataAccessException {
        try {
            if (query.getRouting() != null)
                return new TakeByIdsResultImpl<T>((T[]) space.takeByIds(query.getTypeName(), query.getIds(), query.getRouting(), getCurrentTransaction(), defaultTakeModifiers.getCode(), toInternal(query.getQueryResultType()), false, query.getProjections()));
            else
                return new TakeByIdsResultImpl<T>((T[]) space.takeByIds(query.getTypeName(), query.getIds(), query.getRoutings(), getCurrentTransaction(), defaultTakeModifiers.getCode(), toInternal(query.getQueryResultType()), false, query.getProjections()));
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> TakeByIdsResult<T> takeByIds(IdsQuery<T> query, int modifiers) throws DataAccessException {
        try {
            if (query.getRouting() != null)
                return new TakeByIdsResultImpl<T>((T[]) space.takeByIds(query.getTypeName(), query.getIds(), query.getRouting(), getCurrentTransaction(), modifiers, toInternal(query.getQueryResultType()), false, query.getProjections()));
            else
                return new TakeByIdsResultImpl<T>((T[]) space.takeByIds(query.getTypeName(), query.getIds(), query.getRoutings(), getCurrentTransaction(), modifiers, toInternal(query.getQueryResultType()), false, query.getProjections()));
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> TakeByIdsResult<T> takeByIds(IdsQuery<T> query, TakeModifiers modifiers) throws DataAccessException {
        try {
            if (query.getRouting() != null)
                return new TakeByIdsResultImpl<T>((T[]) space.takeByIds(query.getTypeName(), query.getIds(), query.getRouting(), getCurrentTransaction(), modifiers.getCode(), toInternal(query.getQueryResultType()), false, query.getProjections()));
            else
                return new TakeByIdsResultImpl<T>((T[]) space.takeByIds(query.getTypeName(), query.getIds(), query.getRoutings(), getCurrentTransaction(), modifiers.getCode(), toInternal(query.getQueryResultType()), false, query.getProjections()));
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    @Override
    public DataEventSession newDataEventSession() {
        return newDataEventSession(new EventSessionConfig());
    }

    @Override
    public DataEventSession newDataEventSession(EventSessionConfig config) {
        try {
            return DataEventSessionFactory.create(getSpace(), config);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    public GigaSpaceTypeManager getTypeManager() {
        return typeManager;
    }


    private static QueryResultTypeInternal toInternal(QueryResultType queryResultType) {
        if (queryResultType == null)
            return null;
        if (queryResultType == QueryResultType.DEFAULT || queryResultType == QueryResultType.NOT_SET)
            return QueryResultTypeInternal.NOT_SET;
        if (queryResultType == QueryResultType.OBJECT)
            return QueryResultTypeInternal.OBJECT_JAVA;
        if (queryResultType == QueryResultType.DOCUMENT)
            return QueryResultTypeInternal.DOCUMENT_ENTRY;

        throw new IllegalArgumentException("Unsupported query result type: " + queryResultType);
    }

    @Override
    public void setQuiesceToken(QuiesceToken token) {
        space.getDirectProxy().setQuiesceToken(token);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> ISpaceQuery<T> prepareTemplate(Object template) {
        return (ISpaceQuery<T>) space.getDirectProxy().prepareTemplate(template);
    }

    public void close() throws IOException {
        if (implicitTxProvider)
            txProvider.close();
    }
}
