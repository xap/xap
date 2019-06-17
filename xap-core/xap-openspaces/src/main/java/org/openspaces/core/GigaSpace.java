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
import com.gigaspaces.async.AsyncResultsReducer;
import com.gigaspaces.client.ChangeModifiers;
import com.gigaspaces.client.ChangeResult;
import com.gigaspaces.client.ChangeSet;
import com.gigaspaces.client.ClearModifiers;
import com.gigaspaces.client.CountModifiers;
import com.gigaspaces.client.ReadByIdsResult;
import com.gigaspaces.client.ReadModifiers;
import com.gigaspaces.client.TakeByIdsResult;
import com.gigaspaces.client.TakeModifiers;
import com.gigaspaces.client.WriteModifiers;
import com.gigaspaces.client.iterator.SpaceIterator;
import com.gigaspaces.events.DataEventSession;
import com.gigaspaces.events.EventSessionConfig;
import com.gigaspaces.query.ISpaceQuery;
import com.gigaspaces.query.IdQuery;
import com.gigaspaces.query.IdsQuery;
import com.gigaspaces.query.aggregators.AggregationResult;
import com.gigaspaces.query.aggregators.AggregationSet;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.LeaseContext;

import net.jini.core.transaction.Transaction;

import org.openspaces.core.exception.ExceptionTranslator;
import org.openspaces.core.executor.DistributedTask;
import org.openspaces.core.executor.Task;
import org.openspaces.core.transaction.TransactionProvider;
import org.springframework.dao.DataAccessException;

import java.io.Serializable;
import java.util.concurrent.Future;

/**
 * Provides a simpler interface of {@link IJSpace} extension utilizing GigaSpaces extended and
 * simplified programming model. Most operations revolve around the use of Objects allowing to use
 * GigaSpaces support for POJOs. <p/> <p>Though this interface has a single implementation it is
 * still important to work against the interface as it allows for simpler testing and mocking. <p/>
 * <p>Transaction management is implicit and works in a declarative manner. Operations do not accept
 * a transaction object, and will automatically use the {@link TransactionProvider} in order to
 * acquire the current running transaction. If there is no current running transaction the operation
 * will be executed without a transaction. <p/> <p>Operations throw a {@link
 * org.springframework.dao.DataAccessException} allowing for simplified development model as it is a
 * runtime exception. The cause of the exception can be acquired from the GigaSpace exception.
 *
 * @author kimchy
 * @see com.gigaspaces.query.ISpaceQuery
 * @see com.j_spaces.core.client.SQLQuery
 * @see org.openspaces.core.transaction.TransactionProvider
 * @see org.openspaces.core.DefaultGigaSpace
 * @see org.springframework.dao.DataAccessException
 */
public interface GigaSpace {

    /**
     * Returns the name of the GigaSpace. If it is configured with Spring for example, will return
     * the bean name, if not, will default to the space name.
     */
    String getName();

    /**
     * Returns the space name.
     * @since 14.5
     */
    String getSpaceName();

    /**
     * Returns wether or not this space is secured.
     *
     * @return true if this space is secured, false otherwise.
     **/
    boolean isSecured();

    /**
     * Returns the <code>IJSpace</code> used by this GigaSpace implementation to delegate different
     * space operations. <p/> <p>Allows the use of space operations that are not exposed by this
     * interface, as well as use as a parameter to other low level GigaSpace components. <p/> <p>If
     * a transaction object is required for low level operations (as low level operations do not
     * have declarative transaction ex) the {@link #getTxProvider()} should be used to acquire the
     * current running transaction.
     */
    IJSpace getSpace();

    /**
     * Returns a clustered view of this {@link org.openspaces.core.GigaSpace} instance. <p/>
     * <pre>
     * GigaSpace gigaSpace = new GigaSpaceConfigurer(space).clustered(true).gigaSpace();
     * </pre>
     * <p/> <p/> If this instance is already a clustered view (was initially constructed using the
     * clustered flag), will return the same instance being used to issue the call. <p/> If this is
     * a GigaSpace that works directly with a cluster member, will return a clustered view (as if it
     * was constructed with the clustered flag set). Note that this method might return different
     * instances when being called by different threads. <p/> <p/>
     * <pre>
     * GigaSpace nonClusteredViewOfGigaSpace= ... // acquire non-clustered view of a clustered
     * space
     * GigaSpace space=nonClusteredViewofGigaSpace.getClustered();
     * // space != nonClusteredViewOfSpace
     * </pre>
     * <p/>
     * <pre>
     * GigaSpace clusteredViewOfGigaSpace= ... // acquire clustered view of a clustered space
     * GigaSpace space=clusteredViewofGigaSpace.getClustered();
     * // space == clusteredViewOfSpace
     * </pre>
     *
     * @see GigaSpaceConfigurer#clustered(boolean)
     */
    GigaSpace getClustered();

    /**
     * Returns the transaction provider allowing to access the current running transaction.
     */
    TransactionProvider getTxProvider();

    /**
     * Returns the current running transaction. Can be <code>null</code> if no transaction is in
     * progress.
     */
    Transaction getCurrentTransaction();

    /**
     * Returns the exception translator associated with this GigaSpace instance.
     */
    ExceptionTranslator getExceptionTranslator();

    /**
     * Gets the default {@link WriteModifiers} set during this {@link GigaSpace} configuration. This
     * value is configured either by using a {@link GigaSpaceConfigurer} or through the pu.xml.
     */
    WriteModifiers getDefaultWriteModifiers();

    /**
     * Gets the default {@link ClearModifiers} set during this {@link GigaSpace} configuration. This
     * value is configured either by using a {@link GigaSpaceConfigurer} or through the pu.xml.
     */
    ClearModifiers getDefaultClearModifiers();

    /**
     * Gets the default {@link CountModifiers} set during this {@link GigaSpace} configuration. This
     * value is configured either by using a {@link GigaSpaceConfigurer} or through the pu.xml. If
     * there is an active transaction and that transaction has any isolation level set on it, that
     * isolation level will be merged into the returned modifiers with any previously set isolation
     * level on the modifiers overriden.
     */
    CountModifiers getDefaultCountModifiers();

    /**
     * Gets the default {@link ReadModifiers} set during this {@link GigaSpace} configuration. This
     * value is configured either by using a {@link GigaSpaceConfigurer} or through the pu.xml. If
     * there is an active transaction and that transaction has any isolation level set on it, that
     * isolation level will be merged into the returned modifiers with any previously set isolation
     * level on the modifiers overriden.
     */
    ReadModifiers getDefaultReadModifiers();

    /**
     * Gets the default {@link TakeModifiers} set during this {@link GigaSpace} configuration. This
     * value is configured either by using a {@link GigaSpaceConfigurer} or through the pu.xml.
     */
    TakeModifiers getDefaultTakeModifiers();

    /**
     * Gets the default {@link ChangeModifiers} set during this {@link GigaSpace} configuration.
     * This value is configured either by using a {@link GigaSpaceConfigurer} or through the
     * pu.xml.
     */
    ChangeModifiers getDefaultChangeModifiers();

    /**
     * Removes the entries that match the specified template and the specified transaction from this
     * space. <p/> <p>If the clear operation conducted without transaction (null as value) it will
     * clear all entries that are not under transaction. Therefore entries under transaction would
     * not be removed from the space. <p/> <p>The clear operation supports inheritance, therefore
     * template class matching objects and its sub classes matching objects are part of the
     * candidates population to be removed from the space. You can in fact clean all space objects
     * (that are not under transaction) by calling: <code>gigaSpace.clear(null)</code>. <p/>
     * <p>Notice: The clear operation does not remove notify templates, i.e. registration for
     * notifications.
     *
     * @param template the template to use for matching
     * @throws DataAccessException In the event of an error, DataAccessException will wrap a
     *                             ClearException, accessible via DataAccessException.getRootCause().
     */
    void clear(Object template) throws DataAccessException;

    /**
     * Removes the entries that match the specified template and the specified transaction from this
     * space. <p/> <p>If the clear operation conducted without transaction (null as value) it will
     * clear all entries that are not under transaction. Therefore entries under transaction would
     * not be removed from the space. <p/> <p>The clear operation supports inheritance, therefore
     * template class matching objects and its sub classes matching objects are part of the
     * candidates population to be removed from the space. You can in fact remove all space objects
     * (that are not under transaction) by calling: <code>gigaSpace.clear(null)</code>. <p/>
     * <p>Notice: The clear operation does not remove notify templates i.e. registration for
     * notifications.
     *
     * @param template  the template to use for matching
     * @param modifiers one or a union of {@link ClearModifiers}.
     * @return The number of cleared entries
     * @throws DataAccessException In the event of an error, DataAccessException will wrap a
     *                             ClearException, accessible via DataAccessException.getRootCause().
     * @since 9.0.1
     */
    int clear(Object template, ClearModifiers modifiers);

    /**
     * Count any matching entry from the space. If a running within a transaction will count all the
     * entries visible under the transaction.
     *
     * @param template The template used for matching. Matching is done against the template with
     *                 <code>null</code> fields being wildcards ("match anything") other fields
     *                 being values ("match exactly on the serialized form").
     * @return The number of matching entries
     */
    int count(Object template) throws DataAccessException;

    /**
     * Count any matching entries from the space. If this is running within a transaction will count
     * all the entries visible under the transaction. <p/> <p>Allows to specify modifiers using
     * {@link ReadModifiers} which allows to programmatically control the isolation level this count
     * operation will be performed under.
     *
     * @param template  The template used for matching. Matching is done against the template with
     *                  <code>null</code> fields being wildcards ("match anything") other fields
     *                  being values ("match exactly on the serialized form").
     * @param modifiers one or a union of {@link CountModifiers}.
     * @return The number of matching entries
     * @since 9.0.1
     */
    int count(Object template, CountModifiers modifiers) throws DataAccessException;

    /**
     * <p>The process of serializing an entry for transmission to a JavaSpaces service will be
     * identical if the same entry is used twice. This is most likely to be an issue with templates
     * that are used repeatedly to search for entries with read or take. <p/> <p>The client-side
     * implementations of read and take cannot reasonably avoid this duplicated effort, since they
     * have no efficient way of checking whether the same template is being used without intervening
     * modification. <p/> <p>The snapshot method gives the JavaSpaces service implementor a way to
     * reduce the impact of repeated use of the same entry. Invoking snapshot with an Entry will
     * return another Entry object that contains a snapshot of the original entry. Using the
     * returned snapshot entry is equivalent to using the unmodified original entry in all
     * operations on the same JavaSpaces service. <p/> <p>Modifications to the original entry will
     * not affect the snapshot. You can snapshot a null template; snapshot may or may not return
     * null given a null template. The entry returned from snapshot will be guaranteed equivalent to
     * the original unmodified object only when used with the space. Using the snapshot with any
     * other JavaSpaces service will generate an <code>IllegalArgumentException</code> unless the
     * other space can use it because of knowledge about the JavaSpaces service that generated the
     * snapshot. <p/> <p>The snapshot will be a different object from the original, may or may not
     * have the same hash code, and equals may or may not return true when invoked with the original
     * object, even if the original object is unmodified. A snapshot is guaranteed to work only
     * within the virtual machine in which it was generated. If a snapshot is passed to another
     * virtual machine (for example, in a parameter of an RMI call), using it--even with the same
     * JavaSpaces service--may generate an <code>IllegalArgumentException</code>.
     *
     * @param entry The entry to snapshot
     * @return The snapshot
     * @deprecated since 10.1.0 - usage alternatives: 1. {@link #prepareTemplate(Object)} if one
     * needs to prepare and cache the template as pre-processed packet without accessing the server.
     * 2. {@link GigaSpaceTypeManager#registerTypeDescriptor(Class)} or {@link
     * GigaSpaceTypeManager#registerTypeDescriptor(com.gigaspaces.metadata.SpaceTypeDescriptor)} if
     * one wants to introduce (register) a new type to the space. 3. It's possible to combine (1)
     * and (2) to achieve full snapshot functionality
     */
    @Deprecated
    <T> ISpaceQuery<T> snapshot(Object entry) throws DataAccessException;

    /**
     * Read an object from the space matching its id and the class. Returns <code>null</code> if
     * there is no match. <p/> <p>The timeout is the default timeout this interface is configured
     * with (using its factory) and defaults to 0. <p/> <p>Note, if the space is partitioned, and
     * the Entry has a specific property for its routing value, the operation will broadcast to all
     * partitions. The {@link #readById(Class, Object, Object)} can be used to specify the routing.
     *
     * @param clazz The class of the entry
     * @param id    The id of the entry
     * @return A matching object, or <code>null</code> if no matching is found within the timeout
     * value.
     */
    <T> T readById(Class<T> clazz, Object id) throws DataAccessException;

    /**
     * Read an object from the space matching its id, the class and the routing value. Returns
     * <code>null</code> if there is no match. <p/> <p>The timeout is the default timeout this
     * interface is configured with (using its factory) and defaults to 0.
     *
     * @param clazz   The class of the entry
     * @param id      The id of the entry
     * @param routing The routing value
     * @return A matching object, or <code>null</code> if no matching is found within the timeout
     * value.
     */
    <T> T readById(Class<T> clazz, Object id, Object routing) throws DataAccessException;

    /**
     * Read an object from the space matching its id, the class and the routing value. Returns
     * <code>null</code> if there is no match within the specified timeout.
     *
     * @param clazz   The class of the entry
     * @param id      The id of the entry
     * @param routing The routing value
     * @param timeout The timeout value to wait for a matching entry if it does not exists within
     *                the space
     * @return A matching object, or <code>null</code> if no matching is found within the timeout
     * value.
     */
    <T> T readById(Class<T> clazz, Object id, Object routing, long timeout) throws DataAccessException;

    /**
     * Read an object from the space matching its id, the class and the routing value. Returns
     * <code>null</code> if there is no match within the specified timeout.
     *
     * @param clazz     The class of the entry
     * @param id        The id of the entry
     * @param routing   The routing value
     * @param timeout   The timeout value to wait for a matching entry if it does not exists within
     *                  the space
     * @param modifiers one or a union of {@link ReadModifiers}.
     * @return A matching object, or <code>null</code> if no matching is found within the timeout
     * value.
     * @since 9.0.1
     */
    <T> T readById(Class<T> clazz, Object id, Object routing, long timeout, ReadModifiers modifiers) throws DataAccessException;

    /**
     * Read an object from the space matching the specified id query. Returns <code>null</code> if
     * there is no match. <p/> <p>The timeout is the default timeout this interface is configured
     * with (using its factory) and defaults to 0. <p/> <p>Note, if the space is partitioned, and
     * the Entry has a specific property for its routing value, the operation will broadcast to all
     * partitions. The {@link #readById(Class, Object, Object)} can be used to specify the routing.
     *
     * @param query Query to search by.
     * @return A matching object, or <code>null</code> if no matching is found within the timeout
     * value.
     * @since 8.0
     */
    <T> T readById(IdQuery<T> query) throws DataAccessException;

    /**
     * Read an object from the space matching the specified id query. Returns <code>null</code> if
     * there is no match within the specified timeout.
     *
     * @param query   Query to search by.
     * @param timeout The timeout value to wait for a matching entry if it does not exists within
     *                the space
     * @return A matching object, or <code>null</code> if no matching is found within the timeout
     * value.
     * @since 8.0
     */
    <T> T readById(IdQuery<T> query, long timeout) throws DataAccessException;

    /**
     * Read an object from the space matching the specified id query. Returns <code>null</code> if
     * there is no match within the specified timeout.
     *
     * @param query     Query to search by.
     * @param timeout   The timeout value to wait for a matching entry if it does not exists within
     *                  the space
     * @param modifiers one or a union of {@link ReadModifiers}.
     * @return A matching object, or <code>null</code> if no matching is found within the timeout
     * value.
     * @since 9.0.1
     */
    <T> T readById(IdQuery<T> query, long timeout, ReadModifiers modifiers) throws DataAccessException;

    /**
     * Read any matching object from the space, blocking until one exists. Return <code>null</code>
     * if the timeout expires. <p/> <p>Note, the timeout is the default timeout this interface is
     * configured with (using its factory) and defaults to 0.
     *
     * @param template The template used for matching. Matching is done against template with
     *                 <code>null</code> fields being wildcards ("match anything") other fields
     *                 being values ("match exactly on the serialized form").
     * @return A copy of the object read from the space.
     */
    <T> T read(T template) throws DataAccessException;

    /**
     * Read any matching object from the space, blocking until one exists. Return <code>null</code>
     * if the timeout expires.
     *
     * @param template The template used for matching. Matching is done against template with
     *                 <code>null</code> fields being wildcards ("match anything") other fields
     *                 being values ("match exactly on the serialized form").
     * @param timeout  How long the client is willing to wait for a transactionally proper matching
     *                 object. A timeout of 0 means to wait no time at all.
     * @return A copy of the object read from the space.
     */
    <T> T read(T template, long timeout) throws DataAccessException;

    /**
     * Read any matching object from the space, blocking until one exists. Return <code>null</code>
     * if the timeout expires. <p/> <p>Overloads {@link #read(Object, long)} by adding a
     * <code>modifiers</code> parameter. Equivalent when called with the default modifier - {@link
     * ReadModifiers#REPEATABLE_READ}. Modifiers are used to define the behavior of a read
     * operation.
     *
     * @param template  The template used for matching. Matching is done against template with
     *                  <code>null</code> fields being wildcards ("match anything") other fields
     *                  being values ("match exactly on the serialized form").
     * @param timeout   How long the client is willing to wait for a transactionally proper matching
     *                  object. A timeout of 0 means to wait no time at all.
     * @param modifiers one or a union of {@link ReadModifiers}.
     * @return A copy of the object read from the space.
     * @since 9.0.1
     */
    <T> T read(T template, long timeout, ReadModifiers modifiers) throws DataAccessException;

    /**
     * Read any matching object from the space, blocking until one exists. Return <code>null</code>
     * if the timeout expires. <p/> <p>Note, the timeout is the default timeout this interface is
     * configured with (using its factory) and defaults to 0.
     *
     * @param template A query to be executed against the space. Most common one is {@link
     *                 com.j_spaces.core.client.SQLQuery}.
     * @return A copy of the object read from the space.
     */
    <T> T read(ISpaceQuery<T> template) throws DataAccessException;

    /**
     * Read any matching object from the space, blocking until one exists. Return <code>null</code>
     * if the timeout expires.
     *
     * @param template A query to be executed against the space. Most common one is {@link
     *                 com.j_spaces.core.client.SQLQuery}.
     * @param timeout  How long the client is willing to wait for a transactionally proper matching
     *                 object. A timeout of 0 means to wait no time at all.
     * @return A copy of the object read from the space.
     */
    <T> T read(ISpaceQuery<T> template, long timeout) throws DataAccessException;

    /**
     * Read any matching object from the space, blocking until one exists. Return <code>null</code>
     * if the timeout expires. <p/> <p>Overloads {@link #read(Object, long)} by adding a
     * <code>modifiers</code> parameter. Equivalent when called with the default modifier - {@link
     * ReadModifiers#REPEATABLE_READ}. Modifiers are used to define the behavior of a read
     * operation.
     *
     * @param template  A query to be executed against the space. Most common one is {@link
     *                  com.j_spaces.core.client.SQLQuery}.
     * @param timeout   How long the client is willing to wait for a transactionally proper matching
     *                  object. A timeout of 0 means to wait no time at all.
     * @param modifiers one or a union of {@link ReadModifiers}.
     * @return A copy of the object read from the space.
     * @since 9.0.1
     */
    <T> T read(ISpaceQuery<T> template, long timeout, ReadModifiers modifiers) throws DataAccessException;

    /**
     * Reads any matching entry from the space in an asynchronous manner. Returns immediately with a
     * future. The future can then be used to check if there is a match or not. Once a match is
     * found or the timeout expires, the future will return a result (<code>null</code> in case
     * there was no match). <p/> <p>Note, the timeout is the default timeout this interface is
     * configured with (using its factory) and defaults to 0.
     *
     * @param template The template used for matching. Matching is done against template with
     *                 <code>null</code> fields being wildcards ("match anything") other fields
     *                 being values ("match exactly on the serialized form").
     * @return A copy of the object read from the space.
     */
    <T> AsyncFuture<T> asyncRead(T template) throws DataAccessException;

    /**
     * Reads any matching entry from the space in an asynchronous manner. Returns immediately with a
     * future. The future can then be used to check if there is a match or not. Once a match is
     * found or the timeout expires, the future will return a result (<code>null</code> in case
     * there was no match). <p/> <p>Note, the timeout is the default timeout this interface is
     * configured with (using its factory) and defaults to 0.
     *
     * @param template The template used for matching. Matching is done against template with
     *                 <code>null</code> fields being wildcards ("match anything") other fields
     *                 being values ("match exactly on the serialized form").
     * @param listener A listener to be notified when a result arrives
     * @return A copy of the object read from the space.
     */
    <T> AsyncFuture<T> asyncRead(T template, AsyncFutureListener<T> listener) throws DataAccessException;

    /**
     * Reads any matching entry from the space in an asynchronous manner. Returns immediately with a
     * future. The future can then be used to check if there is a match or not. Once a match is
     * found or the timeout expires, the future will return a result (<code>null</code> in case
     * there was no match).
     *
     * @param template The template used for matching. Matching is done against template with
     *                 <code>null</code> fields being wildcards ("match anything") other fields
     *                 being values ("match exactly on the serialized form").
     * @param timeout  How long the client is willing to wait for a transactionally proper matching
     *                 object. A timeout of 0 means to wait no time at all.
     * @return A copy of the object read from the space.
     */
    <T> AsyncFuture<T> asyncRead(T template, long timeout) throws DataAccessException;

    /**
     * Reads any matching entry from the space in an asynchronous manner. Returns immediately with a
     * future. The future can then be used to check if there is a match or not. Once a match is
     * found or the timeout expires, the future will return a result (<code>null</code> in case
     * there was no match).
     *
     * @param template The template used for matching. Matching is done against template with
     *                 <code>null</code> fields being wildcards ("match anything") other fields
     *                 being values ("match exactly on the serialized form").
     * @param timeout  How long the client is willing to wait for a transactionally proper matching
     *                 object. A timeout of 0 means to wait no time at all.
     * @param listener A listener to be notified when a result arrives
     * @return A copy of the object read from the space.
     */
    <T> AsyncFuture<T> asyncRead(T template, long timeout, AsyncFutureListener<T> listener) throws DataAccessException;

    /**
     * Reads any matching entry from the space in an asynchronous manner. Returns immediately with a
     * future. The future can then be used to check if there is a match or not. Once a match is
     * found or the timeout expires, the future will return a result (<code>null</code> in case
     * there was no match). <p/> <p>Overloads {@link #asyncRead(Object, long)} by adding a
     * <code>modifiers</code> parameter. Equivalent when called with the default modifier - {@link
     * ReadModifiers#REPEATABLE_READ}. Modifiers are used to define the behavior of a read
     * operation.
     *
     * @param template  The template used for matching. Matching is done against template with
     *                  <code>null</code> fields being wildcards ("match anything") other fields
     *                  being values ("match exactly on the serialized form").
     * @param timeout   How long the client is willing to wait for a transactionally proper matching
     *                  object. A timeout of 0 means to wait no time at all.
     * @param modifiers one or a union of {@link ReadModifiers}.
     * @return A copy of the object read from the space.
     * @since 9.0.1
     */
    <T> AsyncFuture<T> asyncRead(T template, long timeout, ReadModifiers modifiers) throws DataAccessException;

    /**
     * Reads any matching entry from the space in an asynchronous manner. Returns immediately with a
     * future. The future can then be used to check if there is a match or not. Once a match is
     * found or the timeout expires, the future will return a result (<code>null</code> in case
     * there was no match). <p/> <p>Overloads {@link #asyncRead(Object, long)} by adding a
     * <code>modifiers</code> parameter. Equivalent when called with the default modifier - {@link
     * ReadModifiers#REPEATABLE_READ}. Modifiers are used to define the behavior of a read
     * operation.
     *
     * @param template  The template used for matching. Matching is done against template with
     *                  <code>null</code> fields being wildcards ("match anything") other fields
     *                  being values ("match exactly on the serialized form").
     * @param timeout   How long the client is willing to wait for a transactionally proper matching
     *                  object. A timeout of 0 means to wait no time at all.
     * @param modifiers one or a union of {@link ReadModifiers}.
     * @param listener  A listener to be notified when a result arrives
     * @return A copy of the object read from the space.
     * @since 9.0.1
     */
    <T> AsyncFuture<T> asyncRead(T template, long timeout, ReadModifiers modifiers, AsyncFutureListener<T> listener) throws DataAccessException;

    /**
     * Reads any matching entry from the space in an asynchronous manner. Returns immediately with a
     * future. The future can then be used to check if there is a match or not. Once a match is
     * found or the timeout expires, the future will return a result (<code>null</code> in case
     * there was no match). <p/> <p>Note, the timeout is the default timeout this interface is
     * configured with (using its factory) and defaults to 0.
     *
     * @param template A query to be executed against the space. Most common one is {@link
     *                 com.j_spaces.core.client.SQLQuery}.
     * @return A copy of the object read from the space.
     */
    <T> AsyncFuture<T> asyncRead(ISpaceQuery<T> template) throws DataAccessException;

    /**
     * Reads any matching entry from the space in an asynchronous manner. Returns immediately with a
     * future. The future can then be used to check if there is a match or not. Once a match is
     * found or the timeout expires, the future will return a result (<code>null</code> in case
     * there was no match). <p/> <p>Note, the timeout is the default timeout this interface is
     * configured with (using its factory) and defaults to 0.
     *
     * @param template A query to be executed against the space. Most common one is {@link
     *                 com.j_spaces.core.client.SQLQuery}.
     * @param listener A listener to be notified when a result arrives
     * @return A copy of the object read from the space.
     */
    <T> AsyncFuture<T> asyncRead(ISpaceQuery<T> template, AsyncFutureListener<T> listener) throws DataAccessException;

    /**
     * Reads any matching entry from the space in an asynchronous manner. Returns immediately with a
     * future. The future can then be used to check if there is a match or not. Once a match is
     * found or the timeout expires, the future will return a result (<code>null</code> in case
     * there was no match).
     *
     * @param template A query to be executed against the space. Most common one is {@link
     *                 com.j_spaces.core.client.SQLQuery}.
     * @param timeout  How long the client is willing to wait for a transactionally proper matching
     *                 object. A timeout of 0 means to wait no time at all.
     * @return A copy of the object read from the space.
     */
    <T> AsyncFuture<T> asyncRead(ISpaceQuery<T> template, long timeout) throws DataAccessException;

    /**
     * Reads any matching entry from the space in an asynchronous manner. Returns immediately with a
     * future. The future can then be used to check if there is a match or not. Once a match is
     * found or the timeout expires, the future will return a result (<code>null</code> in case
     * there was no match).
     *
     * @param template A query to be executed against the space. Most common one is {@link
     *                 com.j_spaces.core.client.SQLQuery}.
     * @param timeout  How long the client is willing to wait for a transactionally proper matching
     *                 object. A timeout of 0 means to wait no time at all.
     * @param listener A listener to be notified when a result arrives
     * @return A copy of the object read from the space.
     */
    <T> AsyncFuture<T> asyncRead(ISpaceQuery<T> template, long timeout, AsyncFutureListener<T> listener) throws DataAccessException;

    /**
     * Reads any matching entry from the space in an asynchronous manner. Returns immediately with a
     * future. The future can then be used to check if there is a match or not. Once a match is
     * found or the timeout expires, the future will return a result (<code>null</code> in case
     * there was no match). <p/> <p>Overloads {@link #asyncRead(Object, long)} by adding a
     * <code>modifiers</code> parameter. Equivalent when called with the default modifier - {@link
     * ReadModifiers#REPEATABLE_READ}. Modifiers are used to define the behavior of a read
     * operation.
     *
     * @param template  A query to be executed against the space. Most common one is {@link
     *                  com.j_spaces.core.client.SQLQuery}.
     * @param timeout   How long the client is willing to wait for a transactionally proper matching
     *                  object. A timeout of 0 means to wait no time at all.
     * @param modifiers one or a union of {@link ReadModifiers}.
     * @return A copy of the object read from the space.
     * @since 9.0.1
     */
    <T> AsyncFuture<T> asyncRead(ISpaceQuery<T> template, long timeout, ReadModifiers modifiers) throws DataAccessException;

    /**
     * Reads any matching entry from the space in an asynchronous manner. Returns immediately with a
     * future. The future can then be used to check if there is a match or not. Once a match is
     * found or the timeout expires, the future will return a result (<code>null</code> in case
     * there was no match). <p/> <p>Overloads {@link #asyncRead(Object, long)} by adding a
     * <code>modifiers</code> parameter. Equivalent when called with the default modifier - {@link
     * ReadModifiers#REPEATABLE_READ}. Modifiers are used to define the behavior of a read
     * operation.
     *
     * @param template  A query to be executed against the space. Most common one is {@link
     *                  com.j_spaces.core.client.SQLQuery}.
     * @param timeout   How long the client is willing to wait for a transactionally proper matching
     *                  object. A timeout of 0 means to wait no time at all.
     * @param modifiers one or a union of {@link ReadModifiers}.
     * @param listener  A listener to be notified when a result arrives
     * @return A copy of the object read from the space.
     * @since 9.0.1
     */
    <T> AsyncFuture<T> asyncRead(ISpaceQuery<T> template, long timeout, ReadModifiers modifiers, AsyncFutureListener<T> listener) throws DataAccessException;

    /**
     * Read an object from the space matching its id and the class. Returns <code>null</code> if
     * there is no match. <p/> <p>Matching and timeouts are done as in <code>readById</code>, except
     * that blocking in this call is done only if necessary to wait for transactional state to
     * settle. <p/> <p>The timeout is the default timeout this interface is configured with (using
     * its factory) and defaults to 0. <p/> <p>Note, if the space is partitioned, and the Entry has
     * a specific property for its routing value, the operation will broadcast to all partitions.
     * The {@link #readById(Class, Object, Object)} can be used to specify the routing.
     *
     * @param clazz The class of the entry
     * @param id    The id of the entry
     * @return A matching object, or <code>null</code> if no matching is found within the timeout
     * value.
     */
    <T> T readIfExistsById(Class<T> clazz, Object id) throws DataAccessException;

    /**
     * Read an object from the space matching its id, the class and the routing value. Returns
     * <code>null</code> if there is no match. <p/> <p>Matching and timeouts are done as in
     * <code>readById</code>, except that blocking in this call is done only if necessary to wait
     * for transactional state to settle. <p/> <p>The timeout is the default timeout this interface
     * is configured with (using its factory) and defaults to 0.
     *
     * @param clazz   The class of the entry
     * @param id      The id of the entry
     * @param routing The routing value
     * @return A matching object, or <code>null</code> if no matching is found within the timeout
     * value.
     */
    @SuppressWarnings("unused")
    <T> T readIfExistsById(Class<T> clazz, Object id, Object routing) throws DataAccessException;

    /**
     * Read an object from the space matching its id, the class and the routing value. Returns
     * <code>null</code> if there is no match within the specified timeout. <p/> <p>Matching and
     * timeouts are done as in <code>readById</code>, except that blocking in this call is done only
     * if necessary to wait for transactional state to settle.
     *
     * @param clazz   The class of the entry
     * @param id      The id of the entry
     * @param routing The routing value
     * @param timeout The timeout value to wait for a matching entry if it does not exists within
     *                the space
     * @return A matching object, or <code>null</code> if no matching is found within the timeout
     * value.
     */
    <T> T readIfExistsById(Class<T> clazz, Object id, Object routing, long timeout) throws DataAccessException;

    /**
     * Read an object from the space matching its id, the class and the routing value. Returns
     * <code>null</code> if there is no match within the specified timeout. <p/> <p>Matching and
     * timeouts are done as in <code>readById</code>, except that blocking in this call is done only
     * if necessary to wait for transactional state to settle.
     *
     * @param clazz     The class of the entry
     * @param id        The id of the entry
     * @param routing   The routing value
     * @param timeout   The timeout value to wait for a matching entry if it does not exists within
     *                  the space
     * @param modifiers one or a union of {@link ReadModifiers}.
     * @return A matching object, or <code>null</code> if no matching is found within the timeout
     * value.
     * @since 9.0.1
     */
    <T> T readIfExistsById(Class<T> clazz, Object id, Object routing, long timeout, ReadModifiers modifiers) throws DataAccessException;

    /**
     * Read an object from the space matching the specified id query. Returns <code>null</code> if
     * there is no match. <p/> <p>The timeout is the default timeout this interface is configured
     * with (using its factory) and defaults to 0. <p/> <p>Note, if the space is partitioned, and
     * the Entry has a specific property for its routing value, the operation will broadcast to all
     * partitions. The {@link #readById(Class, Object, Object)} can be used to specify the routing.
     * <p/> <p>Matching and timeouts are done as in <code>readById</code>, except that blocking in
     * this call is done only if necessary to wait for transactional state to settle.
     *
     * @param query Query to search by.
     * @return A matching object, or <code>null</code> if no matching is found within the timeout
     * value.
     * @since 8.0
     */
    <T> T readIfExistsById(IdQuery<T> query) throws DataAccessException;

    /**
     * Read an object from the space matching the specified id query. Returns <code>null</code> if
     * there is no match within the specified timeout. <p/> <p>Matching and timeouts are done as in
     * <code>readById</code>, except that blocking in this call is done only if necessary to wait
     * for transactional state to settle.
     *
     * @param query   Query to search by.
     * @param timeout The timeout value to wait for a matching entry if it does not exists within
     *                the space
     * @return A matching object, or <code>null</code> if no matching is found within the timeout
     * value.
     * @since 8.0
     */
    <T> T readIfExistsById(IdQuery<T> query, long timeout) throws DataAccessException;

    /**
     * Read an object from the space matching the specified id query. Returns <code>null</code> if
     * there is no match within the specified timeout. <p/> <p>Matching and timeouts are done as in
     * <code>readById</code>, except that blocking in this call is done only if necessary to wait
     * for transactional state to settle.
     *
     * @param query     Query to search by.
     * @param timeout   The timeout value to wait for a matching entry if it does not exists within
     *                  the space
     * @param modifiers one or a union of {@link ReadModifiers}.
     * @return A matching object, or <code>null</code> if no matching is found within the timeout
     * value.
     * @since 9.0.1
     */
    <T> T readIfExistsById(IdQuery<T> query, long timeout, ReadModifiers modifiers) throws DataAccessException;

    /**
     * Read any matching object from the space, returning <code>null</code> if there currently is
     * none. Matching and timeouts are done as in <code>read</code>, except that blocking in this
     * call is done only if necessary to wait for transactional state to settle. <p/> <p>Note, the
     * timeout is the default timeout this interface is configured with (using its factory) and
     * defaults to 0.
     *
     * @param template The template used for matching. Matching is done against the template with
     *                 <code>null</code> fields being wildcards ("match anything") other fields
     *                 being values ("match exactly on the serialized form").
     * @return A copy of the object read from the space.
     */
    <T> T readIfExists(T template) throws DataAccessException;

    /**
     * Read any matching object from the space, returning <code>null</code> if there currently is
     * none. Matching and timeouts are done as in <code>read</code>, except that blocking in this
     * call is done only if necessary to wait for transactional state to settle.
     *
     * @param template The template used for matching. Matching is done against template with
     *                 <code>null</code> fields being wildcards ("match anything") other fields
     *                 being values ("match exactly on the serialized form").
     * @param timeout  How long the client is willing to wait for a transactionally proper matching
     *                 object. A timeout of 0 means to wait no time at all.
     * @return A copy of the object read from the space.
     */
    <T> T readIfExists(T template, long timeout) throws DataAccessException;

    /**
     * Read any matching object from the space, returning <code>null</code> if there currently is
     * none. Matching and timeouts are done as in <code>read</code>, except that blocking in this
     * call is done only if necessary to wait for transactional state to settle. <p/> <p>Overloads
     * {@link #read(Object, long)} by adding a <code>modifiers</code> parameter. Equivalent when
     * called with the default modifier - {@link ReadModifiers#REPEATABLE_READ}. Modifiers are used
     * to define the behavior of a read operation.
     *
     * @param template  The template used for matching. Matching is done against template with
     *                  <code>null</code> fields being wildcards ("match anything") other fields
     *                  being values ("match exactly on the serialized form").
     * @param timeout   How long the client is willing to wait for a transactionally proper matching
     *                  object. A timeout of 0 means to wait no time at all.
     * @param modifiers one or a union of {@link ReadModifiers}.
     * @return A copy of the object read from the space.
     * @since 9.0.1
     */
    <T> T readIfExists(T template, long timeout, ReadModifiers modifiers) throws DataAccessException;

    /**
     * Read any matching object from the space, returning <code>null</code> if there currently is
     * none. Matching and timeouts are done as in <code>read</code>, except that blocking in this
     * call is done only if necessary to wait for transactional state to settle. <p/> <p>Note, the
     * timeout is the default timeout this interface is configured with (using its factory) and
     * defaults to 0.
     *
     * @param template A query to be executed against the space. Most common one is {@link
     *                 com.j_spaces.core.client.SQLQuery}.
     * @return A copy of the object read from the space.
     */
    <T> T readIfExists(ISpaceQuery<T> template) throws DataAccessException;

    /**
     * Read any matching object from the space, returning <code>null</code> if there currently is
     * none. Matching and timeouts are done as in <code>read</code>, except that blocking in this
     * call is done only if necessary to wait for transactional state to settle.
     *
     * @param template A query to be executed against the space. Most common one is {@link
     *                 com.j_spaces.core.client.SQLQuery}.
     * @param timeout  How long the client is willing to wait for a transactionally proper matching
     *                 object. A timeout of 0 means to wait no time at all.
     * @return A copy of the object read from the space.
     */
    <T> T readIfExists(ISpaceQuery<T> template, long timeout) throws DataAccessException;

    /**
     * Read any matching object from the space, returning <code>null</code> if there currently is
     * none. Matching and timeouts are done as in <code>read</code>, except that blocking in this
     * call is done only if necessary to wait for transactional state to settle. <p/> <p>Overloads
     * {@link #read(Object, long)} by adding a <code>modifiers</code> parameter. Equivalent when
     * called with the default modifier - {@link ReadModifiers#REPEATABLE_READ}. Modifiers are used
     * to define the behavior of a read operation.
     *
     * @param template  A query to be executed against the space. Most common one is {@link
     *                  com.j_spaces.core.client.SQLQuery}.
     * @param timeout   How long the client is willing to wait for a transactionally proper matching
     *                  object. A timeout of 0 means to wait no time at all.
     * @param modifiers one or a union of {@link ReadModifiers}.
     * @return A copy of the object read from the space.
     * @since 9.0.1
     */
    <T> T readIfExists(ISpaceQuery<T> template, long timeout, ReadModifiers modifiers) throws DataAccessException;

    /**
     * Read any matching entries from the space. Matching is done as in <code>read</code> without
     * timeout (0). Returns an unbounded array of matches. Returns an empty array if no match was
     * found. Same as calling {@link #readMultiple(Object, int) readMultiple(template,
     * Integer.MAX_VALUE)}.
     *
     * @param template The template used for matching. Matching is done against the template with
     *                 <code>null</code> fields being. wildcards ("match anything") other fields
     *                 being values ("match exactly on the serialized form"). The template can also
     *                 be one of the different {@link com.gigaspaces.query.ISpaceQuery} classes
     * @return A copy of the entries read from the space.
     * @throws DataAccessException In the event of a read error, DataAccessException will wrap a
     *                             ReadMultipleException, accessible via DataAccessException.getRootCause().
     */
    <T> T[] readMultiple(T template) throws DataAccessException;

    /**
     * Read any matching entries from the space. Matching is done as in <code>read</code> without
     * timeout (0). Returns an array with matches bound by <code>maxEntries</code>. Returns an empty
     * array if no match was found.
     *
     * @param template   The template used for matching. Matching is done against the template with
     *                   <code>null</code> fields being. wildcards ("match anything") other fields
     *                   being values ("match exactly on the serialized form"). The template can
     *                   also be one of the different {@link com.gigaspaces.query.ISpaceQuery}
     *                   classes
     * @param maxEntries A limit on the number of entries to be returned. Use {@link
     *                   Integer#MAX_VALUE} for the uppermost limit.
     * @return A copy of the entries read from the space.
     * @throws DataAccessException In the event of a read error, DataAccessException will wrap a
     *                             ReadMultipleException, accessible via DataAccessException.getRootCause().
     */
    <T> T[] readMultiple(T template, int maxEntries) throws DataAccessException;

    /**
     * Read any matching entries from the space. Matching is done as in <code>read</code> without
     * timeout (0). Returns an array with matches bound by <code>maxEntries</code>. Returns an empty
     * array if no match was found. <p/> <p>Overloads {@link #readMultiple(Object, int)} by adding a
     * <code>modifiers</code> parameter. Equivalent when called with the default modifier - {@link
     * ReadModifiers#REPEATABLE_READ}. Modifiers are used to define the behavior of a read
     * operation.
     *
     * @param template   The template used for matching. Matching is done against the template with
     *                   <code>null</code> fields being. wildcards ("match anything") other fields
     *                   being values ("match exactly on the serialized form"). The template can
     *                   also be one of the different {@link com.gigaspaces.query.ISpaceQuery}
     *                   classes
     * @param maxEntries A limit on the number of entries to be returned. Use {@link
     *                   Integer#MAX_VALUE} for the uppermost limit.
     * @param modifiers  one or a union of {@link ReadModifiers}.
     * @return A copy of the entries read from the space.
     * @throws DataAccessException In the event of a read error, DataAccessException will wrap a
     *                             ReadMultipleException, accessible via DataAccessException.getRootCause().
     * @since 9.0.1
     */
    <T> T[] readMultiple(T template, int maxEntries, ReadModifiers modifiers) throws DataAccessException;

    /**
     * Read any matching entries from the space. Matching is done as in <code>read</code> without
     * timeout (0). Returns an unbounded array of matches. Returns an empty array if no match was
     * found.
     *
     * @param template A query to be executed against the space. Most common one is {@link
     *                 com.j_spaces.core.client.SQLQuery}.
     * @return A copy of the entries read from the space.
     * @throws DataAccessException In the event of a read error, DataAccessException will wrap a
     *                             ReadMultipleException, accessible via DataAccessException.getRootCause().
     */
    <T> T[] readMultiple(ISpaceQuery<T> template) throws DataAccessException;

    /**
     * Read any matching entries from the space. Matching is done as in <code>read</code> without
     * timeout (0). Returns an array with matches bound by <code>maxEntries</code>. Returns an empty
     * array if no match was found.
     *
     * @param template   A query to be executed against the space. Most common one is {@link
     *                   com.j_spaces.core.client.SQLQuery}.
     * @param maxEntries A limit on the number of entries to be returned. Use {@link
     *                   Integer#MAX_VALUE} for the uppermost limit.
     * @return A copy of the entries read from the space.
     * @throws DataAccessException In the event of a read error, DataAccessException will wrap a
     *                             ReadMultipleException, accessible via DataAccessException.getRootCause().
     */
    <T> T[] readMultiple(ISpaceQuery<T> template, int maxEntries) throws DataAccessException;

    /**
     * Read any matching entries from the space. Matching is done as in <code>read</code> without
     * timeout (0). Returns an array with matches bound by <code>maxEntries</code>. Returns an empty
     * array if no match was found. <p/> <p>Overloads {@link #readMultiple(Object, int)} by adding a
     * <code>modifiers</code> parameter. Equivalent when called with the default modifier - {@link
     * ReadModifiers#REPEATABLE_READ}. Modifiers are used to define the behavior of a read
     * operation.
     *
     * @param template   A query to be executed against the space. Most common one is {@link
     *                   com.j_spaces.core.client.SQLQuery}.
     * @param maxEntries A limit on the number of entries to be returned. Use {@link
     *                   Integer#MAX_VALUE} for the uppermost limit.
     * @param modifiers  one or a union of {@link ReadModifiers}.
     * @return A copy of the entries read from the space.
     * @throws DataAccessException In the event of a read error, DataAccessException will wrap a
     *                             ReadMultipleException, accessible via DataAccessException.getRootCause().
     * @since 9.0.1
     */
    <T> T[] readMultiple(ISpaceQuery<T> template, int maxEntries, ReadModifiers modifiers) throws DataAccessException;

    /**
     * Read objects from the space matching their IDs and the specified class. <p/> <p>Note, if the
     * space is partitioned and the Class defines that routing is not done via the Id property, the
     * operation will broadcast to all partitions. Use {@link #readByIds(Class, Object[], Object)}
     * to specify the routing explicitly and avoid broadcast if needed.
     *
     * @param clazz The class.
     * @param ids   The object IDs array.
     * @return a ReadByIdsResult containing the matched results.
     */
    <T> ReadByIdsResult<T> readByIds(Class<T> clazz, Object[] ids) throws DataAccessException;

    /**
     * Read objects from the space matching their IDs, the specified class and routing key. <p/>
     * <p>Note, if the space is partitioned and the Class defines that routing is not done via the
     * Id property, the operation will be routed according to <code>routingKey</code>. If it is
     * null, the operation will broadcast to all partitions.
     *
     * @param clazz      The class.
     * @param ids        The object IDs array.
     * @param routingKey The routing of the provided object IDs.
     * @return a ReadByIdsResult containing the matched results.
     */
    <T> ReadByIdsResult<T> readByIds(Class<T> clazz, Object[] ids, Object routingKey) throws DataAccessException;

    /**
     * Read objects from the space matching their IDs, the specified class type and routing key,
     * with the provided {@link ReadModifiers}. <p/> <p>Note, if the space is partitioned and the
     * Class defines that routing is not done via the Id property, the operation will be routed
     * according to <code>routingKey</code>. If it is null, the operation will broadcast to all
     * partitions. <p/> <p>{@link ReadModifiers#FIFO} is not supported by this operation - the
     * results are always ordered in correlation with the input IDs array.
     *
     * @param clazz      The class.
     * @param ids        The object IDs array.
     * @param routingKey The routing of the provided object IDs.
     * @param modifiers  The read modifier to use (One or several of {@link ReadModifiers}).
     * @return a ReadByIdsResult containing the matched results.
     * @since 9.0.1
     */
    <T> ReadByIdsResult<T> readByIds(Class<T> clazz, Object[] ids, Object routingKey, ReadModifiers modifiers) throws DataAccessException;

    /**
     * Read objects from the space matching their IDs, the specified class and the routing keys.
     * <p/> <p>Note, if the space is partitioned and the Class defines that routing is not done via
     * the Id property, the operation will be routed according to <code>routingKeys</code>. If it is
     * null, the operation will broadcast to all partitions. <code>routingKeys</code> should be
     * correlated with <code>ids</code>, i.e. their length should be the same, and the routing key
     * of ID i in the IDs array is the element at position i in the routing keys array.
     *
     * @param clazz       The class.
     * @param ids         The object IDs array.
     * @param routingKeys The object routing keys array.
     * @return a ReadByIdsResult containing the matched results.
     */
    <T> ReadByIdsResult<T> readByIds(Class<T> clazz, Object[] ids, Object[] routingKeys) throws DataAccessException;

    /**
     * Read objects from the space matching their IDs, the specified class and the routing keys,
     * with the provided {@link ReadModifiers}. <p/> <p>Note, if the space is partitioned and the
     * Class defines that routing is not done via the Id property, the operation will be routed
     * according to <code>routingKeys</code>. If it is null, the operation will broadcast to all
     * partitions. <code>routingKeys</code> should be correlated with <code>ids</code>, i.e. their
     * length should be the same, and the routing key of ID i in the IDs array is the element at
     * position i in the routing keys array. <p/> <p>{@link ReadModifiers#FIFO} is not supported by
     * this operation - the results are always ordered in correlation with the input IDs array.
     *
     * @param clazz       The class type.
     * @param ids         The objects\ IDs array.
     * @param routingKeys The object routing keys array.
     * @param modifiers   The read modifier to use (One or several of {@link ReadModifiers}).
     * @return a ReadByIdsResult containing the matched results.
     * @since 9.0.1
     */
    <T> ReadByIdsResult<T> readByIds(Class<T> clazz, Object[] ids, Object[] routingKeys, ReadModifiers modifiers) throws DataAccessException;

    /**
     * Read objects from the space matching the specified IDs query.
     *
     * @param query Query to search by.
     * @return a ReadByIdsResult containing the matched results.
     * @since 8.0
     */
    <T> ReadByIdsResult<T> readByIds(IdsQuery<T> query) throws DataAccessException;

    /**
     * Read objects from the space matching the specified IDs query, with the provided {@link
     * ReadModifiers}.
     *
     * @param query     Query to search by.
     * @param modifiers The read modifier to use (One or several of {@link ReadModifiers}).
     * @return a ReadByIdsResult containing the matched results.
     * @since 9.0.1
     */
    <T> ReadByIdsResult<T> readByIds(IdsQuery<T> query, ReadModifiers modifiers) throws DataAccessException;

    /**
     * Take (remove) objects from the space matching their IDs and the specified class. <p/>
     * <p>Note, if the space is partitioned and the Class defines that routing is not done via the
     * Id property, the operation will broadcast to all partitions. Use {@link #readByIds(Class,
     * Object[], Object)} to specify the routing explicitly and avoid broadcast if needed.
     *
     * @param clazz The class.
     * @param ids   The object IDs array.
     * @return a TakeByIdsResult containing the matched results.
     */
    <T> TakeByIdsResult<T> takeByIds(Class<T> clazz, Object[] ids) throws DataAccessException;

    /**
     * Take (remove) objects from the space matching their IDs, the specified class and routing key.
     * <p/> <p>Note, if the space is partitioned and the Class defines that routing is not done via
     * the Id property, the operation will be routed according to <code>routingKey</code>. If it is
     * null, the operation will broadcast to all partitions.
     *
     * @param clazz      The class.
     * @param ids        The object IDs array.
     * @param routingKey The routing of the provided object IDs.
     * @return a TakeByIdsResult containing the matched results.
     */
    <T> TakeByIdsResult<T> takeByIds(Class<T> clazz, Object[] ids, Object routingKey) throws DataAccessException;

    /**
     * Take (remove) objects from the space matching their IDs, the specified class type and routing
     * key, with the provided {@link ReadModifiers}. <p/> <p>Note, if the space is partitioned and
     * the Class defines that routing is not done via the Id property, the operation will be routed
     * according to <code>routingKey</code>. If it is null, the operation will broadcast to all
     * partitions. <p/> <p>{@link ReadModifiers#FIFO} is not supported by this operation - the
     * results are always ordered in correlation with the input IDs array.
     *
     * @param clazz      The class.
     * @param ids        The object IDs array.
     * @param routingKey The routing of the provided object IDs.
     * @param modifiers  The read modifier to use (One or several of {@link ReadModifiers}).
     * @return a TakeByIdsResult containing the matched results.
     */
    <T> TakeByIdsResult<T> takeByIds(Class<T> clazz, Object[] ids, Object routingKey, TakeModifiers modifiers) throws DataAccessException;

    /**
     * Take (remove) objects from the space matching their IDs, the specified class and the routing
     * keys. <p/> <p>Note, if the space is partitioned and the Class defines that routing is not
     * done via the Id property, the operation will be routed according to <code>routingKeys</code>.
     * If it is null, the operation will broadcast to all partitions. <code>routingKeys</code>
     * should be correlated with <code>ids</code>, i.e. their length should be the same, and the
     * routing key of ID i in the IDs array is the element at position i in the routing keys array.
     *
     * @param clazz       The class.
     * @param ids         The object IDs array.
     * @param routingKeys The object routing keys array.
     * @return a TakeByIdsResult containing the matched results.
     */
    <T> TakeByIdsResult<T> takeByIds(Class<T> clazz, Object[] ids, Object[] routingKeys) throws DataAccessException;

    /**
     * Take (remove) objects from the space matching their IDs, the specified class and the routing
     * keys, with the provided {@link ReadModifiers}. <p/> <p>Note, if the space is partitioned and
     * the Class defines that routing is not done via the Id property, the operation will be routed
     * according to <code>routingKeys</code>. If it is null, the operation will broadcast to all
     * partitions. <code>routingKeys</code> should be correlated with <code>ids</code>, i.e. their
     * length should be the same, and the routing key of ID i in the IDs array is the element at
     * position i in the routing keys array. <p/> <p>{@link ReadModifiers#FIFO} is not supported by
     * this operation - the results are always ordered in correlation with the input IDs array.
     *
     * @param clazz       The class type.
     * @param ids         The objects\ IDs array.
     * @param routingKeys The object routing keys array.
     * @param modifiers   The read modifier to use (One or several of {@link ReadModifiers}).
     * @return a TakeByIdsResult containing the matched results.
     * @since 9.0.1
     */
    <T> TakeByIdsResult<T> takeByIds(Class<T> clazz, Object[] ids, Object[] routingKeys, TakeModifiers modifiers) throws DataAccessException;

    /**
     * Take (remove) objects from the space matching the specified IDs query.
     *
     * @param query Query to search by.
     * @return a ReadByIdsResult containing the matched results.
     * @since 8.0
     */
    <T> TakeByIdsResult<T> takeByIds(IdsQuery<T> query) throws DataAccessException;

    /**
     * Take (remove) objects from the space matching the specified IDs query, with the provided
     * {@link ReadModifiers}.
     *
     * @param query     Query to search by.
     * @param modifiers The read modifier to use (One or several of {@link ReadModifiers}).
     * @return a ReadByIdsResult containing the matched results.
     * @since 9.0.1
     */
    <T> TakeByIdsResult<T> takeByIds(IdsQuery<T> query, TakeModifiers modifiers) throws DataAccessException;

    /**
     * Take (remove) an object from the space matching its id and the class. Returns
     * <code>null</code> if there is no match. <p/> <p>The timeout is the default timeout this
     * interface is configured with (using its factory) and defaults to 0. <p/> <p>Note, if the
     * space is partitioned, and the Entry has a specific property for its routing value, the
     * operation will broadcast to all partitions. The {@link #takeById(Class, Object, Object)} can
     * be used to specify the routing.
     *
     * @param clazz The class of the entry
     * @param id    The id of the entry
     * @return A matching object, or <code>null</code> if no matching is found within the timeout
     * value.
     */
    <T> T takeById(Class<T> clazz, Object id) throws DataAccessException;

    /**
     * Take (remove) an object from the space matching its id, the class and the routing value.
     * Returns <code>null</code> if there is no match. <p/> <p>The timeout is the default timeout
     * this interface is configured with (using its factory) and defaults to 0.
     *
     * @param clazz   The class of the entry
     * @param id      The id of the entry
     * @param routing The routing value
     * @return A matching object, or <code>null</code> if no matching is found within the timeout
     * value.
     */
    <T> T takeById(Class<T> clazz, Object id, Object routing) throws DataAccessException;

    /**
     * Take (remove) an object from the space matching its id, the class and the routing value.
     * Returns <code>null</code> if there is no match within the specified timeout.
     *
     * @param clazz   The class of the entry
     * @param id      The id of the entry
     * @param routing The routing value
     * @param timeout The timeout value to wait for a matching entry if it does not exists within
     *                the space
     * @return A matching object, or <code>null</code> if no matching is found within the timeout
     * value.
     */
    <T> T takeById(Class<T> clazz, Object id, Object routing, long timeout) throws DataAccessException;

    /**
     * Take (remove) an object from the space matching its id, the class and the routing value.
     * Returns <code>null</code> if there is no match within the specified timeout.
     *
     * @param clazz     The class of the entry
     * @param id        The id of the entry
     * @param routing   The routing value
     * @param timeout   The timeout value to wait for a matching entry if it does not exists within
     *                  the space
     * @param modifiers one or a union of {@link TakeModifiers}.
     * @return A matching object, or <code>null</code> if no matching is found within the timeout
     * value.
     * @since 9.0.1
     */
    <T> T takeById(Class<T> clazz, Object id, Object routing, long timeout, TakeModifiers modifiers) throws DataAccessException;

    /**
     * Take (remove) an object from the space matching the specified id query. Returns
     * <code>null</code> if there is no match. <p/> <p>The timeout is the default timeout this
     * interface is configured with (using its factory) and defaults to 0. <p/> <p>Note, if the
     * space is partitioned, and the Entry has a specific property for its routing value, the
     * operation will broadcast to all partitions. The {@link #readById(Class, Object, Object)} can
     * be used to specify the routing.
     *
     * @param query Query to search by.
     * @return A matching object, or <code>null</code> if no matching is found within the timeout
     * value.
     * @since 8.0
     */
    <T> T takeById(IdQuery<T> query) throws DataAccessException;

    /**
     * Take (remove) an object from the space matching the specified id query. Returns
     * <code>null</code> if there is no match within the specified timeout.
     *
     * @param query   Query to search by.
     * @param timeout The timeout value to wait for a matching entry if it does not exists within
     *                the space
     * @return A matching object, or <code>null</code> if no matching is found within the timeout
     * value.
     * @since 8.0
     */
    <T> T takeById(IdQuery<T> query, long timeout) throws DataAccessException;

    /**
     * Take (remove) an object from the space matching the specified id query. Returns
     * <code>null</code> if there is no match within the specified timeout.
     *
     * @param query     Query to search by.
     * @param timeout   The timeout value to wait for a matching entry if it does not exists within
     *                  the space
     * @param modifiers one or a union of {@link TakeModifiers}.
     * @return A matching object, or <code>null</code> if no matching is found within the timeout
     * value.
     * @since 9.0.1
     */
    <T> T takeById(IdQuery<T> query, long timeout, TakeModifiers modifiers) throws DataAccessException;

    /**
     * Take (remove) any matching entry from the space, blocking until one exists. Return
     * <code>null</code> if the timeout expires. <p/> <p>Note, the timeout is the default timeout
     * this interface is configured with (using its factory) and defaults to 0.
     *
     * @param template The template used for matching. Matching is done against the template with
     *                 <code>null</code> fields being wildcards ( "match anything") other fields
     *                 being values ("match exactly on the serialized form").
     * @return A removed entry from the space
     */
    <T> T take(T template) throws DataAccessException;

    /**
     * Take (remove) any matching entry from the space, blocking until one exists. Return
     * <code>null</code> if the timeout expires.
     *
     * @param template The template used for matching. Matching is done against the template with
     *                 <code>null</code> fields being wildcards ( "match anything") other fields
     *                 being values ("match exactly on the serialized form").
     * @param timeout  How long the client is willing to wait for a transactionally proper matching
     *                 entry. A timeout of 0 means to wait no time at all.
     * @return A removed entry from the space
     */
    <T> T take(T template, long timeout) throws DataAccessException;

    /**
     * Take (remove) any matching entry from the space, blocking until one exists. Return
     * <code>null</code> if the timeout expires.
     *
     * @param template  The template used for matching. Matching is done against the template with
     *                  <code>null</code> fields being wildcards ( "match anything") other fields
     *                  being values ("match exactly on the serialized form").
     * @param timeout   How long the client is willing to wait for a transactionally proper matching
     *                  entry. A timeout of 0 means to wait no time at all.
     * @param modifiers one or a union of {@link TakeModifiers}.
     * @return A removed entry from the space
     * @since 9.0.1
     */
    <T> T take(T template, long timeout, TakeModifiers modifiers) throws DataAccessException;

    /**
     * Take (remove) any matching entry from the space, blocking until one exists. Return
     * <code>null</code> if the timeout expires. <p/> <p>Note, the timeout is the default timeout
     * this interface is configured with (using its factory) and defaults to 0.
     *
     * @param template A query to be executed against the space. Most common one is {@link
     *                 com.j_spaces.core.client.SQLQuery}.
     * @return A removed entry from the space
     */
    <T> T take(ISpaceQuery<T> template) throws DataAccessException;

    /**
     * Take (remove) any matching entry from the space, blocking until one exists. Return
     * <code>null</code> if the timeout expires.
     *
     * @param template A query to be executed against the space. Most common one is {@link
     *                 com.j_spaces.core.client.SQLQuery}.
     * @param timeout  How long the client is willing to wait for a transactionally proper matching
     *                 entry. A timeout of 0 means to wait no time at all.
     * @return A removed entry from the space
     */
    <T> T take(ISpaceQuery<T> template, long timeout) throws DataAccessException;

    /**
     * Take (remove) any matching entry from the space, blocking until one exists. Return
     * <code>null</code> if the timeout expires.
     *
     * @param template  A query to be executed against the space. Most common one is {@link
     *                  com.j_spaces.core.client.SQLQuery}.
     * @param timeout   How long the client is willing to wait for a transactionally proper matching
     *                  entry. A timeout of 0 means to wait no time at all.
     * @param modifiers one or a union of {@link TakeModifiers}.
     * @return A removed entry from the space
     * @since 9.0.1
     */
    <T> T take(ISpaceQuery<T> template, long timeout, TakeModifiers modifiers) throws DataAccessException;

    /**
     * Take (remove) any matching entry from the space in an asynchronous manner. Returns
     * immediately with a future. The future can then be used to check if there is a match or not.
     * Once a match is found or the timeout expires, the future will return a result
     * (<code>null</code> in case there was no match). <p/> <p>Note, the timeout is the default
     * timeout this interface is configured with (using its factory) and defaults to 0.
     *
     * @param template The template used for matching. Matching is done against the template with
     *                 <code>null</code> fields being wildcards ( "match anything") other fields
     *                 being values ("match exactly on the serialized form").
     * @return A removed entry from the space
     */
    <T> AsyncFuture<T> asyncTake(T template) throws DataAccessException;

    /**
     * Take (remove) any matching entry from the space in an asynchronous manner. Returns
     * immediately with a future. The future can then be used to check if there is a match or not.
     * Once a match is found or the timeout expires, the future will return a result
     * (<code>null</code> in case there was no match). <p/> <p>Note, the timeout is the default
     * timeout this interface is configured with (using its factory) and defaults to 0.
     *
     * @param template The template used for matching. Matching is done against the template with
     *                 <code>null</code> fields being wildcards ( "match anything") other fields
     *                 being values ("match exactly on the serialized form").
     * @param listener A listener to be notified when a result arrives
     * @return A removed entry from the space
     */
    <T> AsyncFuture<T> asyncTake(T template, AsyncFutureListener<T> listener) throws DataAccessException;

    /**
     * Take (remove) any matching entry from the space in an asynchronous manner. Returns
     * immediately with a future. The future can then be used to check if there is a match or not.
     * Once a match is found or the timeout expires, the future will return a result
     * (<code>null</code> in case there was no match).
     *
     * @param template The template used for matching. Matching is done against the template with
     *                 <code>null</code> fields being wildcards ( "match anything") other fields
     *                 being values ("match exactly on the serialized form").
     * @param timeout  How long the client is willing to wait for a transactionally proper matching
     *                 entry. A timeout of 0 means to wait no time at all.
     * @return A removed entry from the space
     */
    <T> AsyncFuture<T> asyncTake(T template, long timeout) throws DataAccessException;

    /**
     * Take (remove) any matching entry from the space in an asynchronous manner. Returns
     * immediately with a future. The future can then be used to check if there is a match or not.
     * Once a match is found or the timeout expires, the future will return a result
     * (<code>null</code> in case there was no match).
     *
     * @param template The template used for matching. Matching is done against the template with
     *                 <code>null</code> fields being wildcards ( "match anything") other fields
     *                 being values ("match exactly on the serialized form").
     * @param timeout  How long the client is willing to wait for a transactionally proper matching
     *                 entry. A timeout of 0 means to wait no time at all.
     * @param listener A listener to be notified when a result arrives
     * @return A removed entry from the space
     */
    <T> AsyncFuture<T> asyncTake(T template, long timeout, AsyncFutureListener<T> listener) throws DataAccessException;

    /**
     * Take (remove) any matching entry from the space in an asynchronous manner. Returns
     * immediately with a future. The future can then be used to check if there is a match or not.
     * Once a match is found or the timeout expires, the future will return a result
     * (<code>null</code> in case there was no match).
     *
     * @param template  The template used for matching. Matching is done against the template with
     *                  <code>null</code> fields being wildcards ( "match anything") other fields
     *                  being values ("match exactly on the serialized form").
     * @param timeout   How long the client is willing to wait for a transactionally proper matching
     *                  entry. A timeout of 0 means to wait no time at all.
     * @param modifiers one or a union of {@link TakeModifiers}.
     * @return A removed entry from the space
     * @since 9.0.1
     */
    <T> AsyncFuture<T> asyncTake(T template, long timeout, TakeModifiers modifiers) throws DataAccessException;

    /**
     * Take (remove) any matching entry from the space in an asynchronous manner. Returns
     * immediately with a future. The future can then be used to check if there is a match or not.
     * Once a match is found or the timeout expires, the future will return a result
     * (<code>null</code> in case there was no match).
     *
     * @param template  The template used for matching. Matching is done against the template with
     *                  <code>null</code> fields being wildcards ( "match anything") other fields
     *                  being values ("match exactly on the serialized form").
     * @param timeout   How long the client is willing to wait for a transactionally proper matching
     *                  entry. A timeout of 0 means to wait no time at all.
     * @param modifiers one or a union of {@link TakeModifiers}.
     * @param listener  A listener to be notified when a result arrives
     * @return A removed entry from the space
     * @since 9.0.1
     */
    <T> AsyncFuture<T> asyncTake(T template, long timeout, TakeModifiers modifiers, AsyncFutureListener<T> listener) throws DataAccessException;

    /**
     * Take (remove) any matching entry from the space in an asynchronous manner. Returns
     * immediately with a future. The future can then be used to check if there is a match or not.
     * Once a match is found or the timeout expires, the future will return a result
     * (<code>null</code> in case there was no match). <p/> <p>Note, the timeout is the default
     * timeout this interface is configured with (using its factory) and defaults to 0.
     *
     * @param template A query to be executed against the space. Most common one is {@link
     *                 com.j_spaces.core.client.SQLQuery}.
     * @return A removed entry from the space
     */
    <T> AsyncFuture<T> asyncTake(ISpaceQuery<T> template) throws DataAccessException;

    /**
     * Take (remove) any matching entry from the space in an asynchronous manner. Returns
     * immediately with a future. The future can then be used to check if there is a match or not.
     * Once a match is found or the timeout expires, the future will return a result
     * (<code>null</code> in case there was no match). <p/> <p>Note, the timeout is the default
     * timeout this interface is configured with (using its factory) and defaults to 0.
     *
     * @param template A query to be executed against the space. Most common one is {@link
     *                 com.j_spaces.core.client.SQLQuery}.
     * @param listener A listener to be notified when a result arrives.
     * @return A removed entry from the space
     */
    <T> AsyncFuture<T> asyncTake(ISpaceQuery<T> template, AsyncFutureListener<T> listener) throws DataAccessException;

    /**
     * Take (remove) any matching entry from the space in an asynchronous manner. Returns
     * immediately with a future. The future can then be used to check if there is a match or not.
     * Once a match is found or the timeout expires, the future will return a result
     * (<code>null</code> in case there was no match).
     *
     * @param template A query to be executed against the space. Most common one is {@link
     *                 com.j_spaces.core.client.SQLQuery}.
     * @param timeout  How long the client is willing to wait for a transactionally proper matching
     *                 entry. A timeout of 0 means to wait no time at all.
     * @return A removed entry from the space
     */
    <T> AsyncFuture<T> asyncTake(ISpaceQuery<T> template, long timeout) throws DataAccessException;

    /**
     * Take (remove) any matching entry from the space in an asynchronous manner. Returns
     * immediately with a future. The future can then be used to check if there is a match or not.
     * Once a match is found or the timeout expires, the future will return a result
     * (<code>null</code> in case there was no match).
     *
     * @param template A query to be executed against the space. Most common one is {@link
     *                 com.j_spaces.core.client.SQLQuery}.
     * @param timeout  How long the client is willing to wait for a transactionally proper matching
     *                 entry. A timeout of 0 means to wait no time at all.
     * @param listener A listener to be notified when a result arrives.
     * @return A removed entry from the space
     */
    <T> AsyncFuture<T> asyncTake(ISpaceQuery<T> template, long timeout, AsyncFutureListener<T> listener) throws DataAccessException;

    /**
     * Take (remove) any matching entry from the space in an asynchronous manner. Returns
     * immediately with a future. The future can then be used to check if there is a match or not.
     * Once a match is found or the timeout expires, the future will return a result
     * (<code>null</code> in case there was no match).
     *
     * @param template  A query to be executed against the space. Most common one is {@link
     *                  com.j_spaces.core.client.SQLQuery}.
     * @param timeout   How long the client is willing to wait for a transactionally proper matching
     *                  entry. A timeout of 0 means to wait no time at all.
     * @param modifiers one or a union of {@link TakeModifiers}.
     * @return A removed entry from the space
     * @since 9.0.1
     */
    <T> AsyncFuture<T> asyncTake(ISpaceQuery<T> template, long timeout, TakeModifiers modifiers) throws DataAccessException;

    /**
     * Take (remove) any matching entry from the space in an asynchronous manner. Returns
     * immediately with a future. The future can then be used to check if there is a match or not.
     * Once a match is found or the timeout expires, the future will return a result
     * (<code>null</code> in case there was no match).
     *
     * @param template  A query to be executed against the space. Most common one is {@link
     *                  com.j_spaces.core.client.SQLQuery}.
     * @param timeout   How long the client is willing to wait for a transactionally proper matching
     *                  entry. A timeout of 0 means to wait no time at all.
     * @param modifiers one or a union of {@link TakeModifiers}.
     * @param listener  A listener to be notified when a result arrives.
     * @return A removed entry from the space
     * @since 9.0.1
     */
    <T> AsyncFuture<T> asyncTake(ISpaceQuery<T> template, long timeout, TakeModifiers modifiers, AsyncFutureListener<T> listener) throws DataAccessException;

    /**
     * Take (remove) an object from the space matching its id and the class. Returns
     * <code>null</code> if there is no match. <p/> <p>Matching and timeouts are done as in
     * <code>takeById</code>, except that blocking in this call is done only if necessary to wait
     * for transactional state to settle. <p/> <p>The timeout is the default timeout this interface
     * is configured with (using its factory) and defaults to 0. <p/> <p>Note, if the space is
     * partitioned, and the Entry has a specific property for its routing value, the operation will
     * broadcast to all partitions. The {@link #takeById(Class, Object, Object)} can be used to
     * specify the routing.
     *
     * @param clazz The class of the entry
     * @param id    The id of the entry
     * @return A matching object, or <code>null</code> if no matching is found within the timeout
     * value.
     */
    <T> T takeIfExistsById(Class<T> clazz, Object id) throws DataAccessException;

    /**
     * Take (remove) an object from the space matching its id, the class and the routing value.
     * Returns <code>null</code> if there is no match. <p/> <p>Matching and timeouts are done as in
     * <code>takeById</code>, except that blocking in this call is done only if necessary to wait
     * for transactional state to settle. <p/> <p>The timeout is the default timeout this interface
     * is configured with (using its factory) and defaults to 0.
     *
     * @param clazz   The class of the entry
     * @param id      The id of the entry
     * @param routing The routing value
     * @return A matching object, or <code>null</code> if no matching is found within the timeout
     * value.
     */
    <T> T takeIfExistsById(Class<T> clazz, Object id, Object routing) throws DataAccessException;

    /**
     * Take (remove) an object from the space matching its id, the class and the routing value.
     * Returns <code>null</code> if there is no match within the specified timeout. <p/> <p>Matching
     * and timeouts are done as in <code>takeById</code>, except that blocking in this call is done
     * only if necessary to wait for transactional state to settle.
     *
     * @param clazz   The class of the entry
     * @param id      The id of the entry
     * @param routing The routing value
     * @param timeout The timeout value to wait for a matching entry if it does not exists within
     *                the space
     * @return A matching object, or <code>null</code> if no matching is found within the timeout
     * value.
     */
    <T> T takeIfExistsById(Class<T> clazz, Object id, Object routing, long timeout) throws DataAccessException;

    /**
     * Take (remove) an object from the space matching its id, the class and the routing value.
     * Returns <code>null</code> if there is no match within the specified timeout. <p/> <p>Matching
     * and timeouts are done as in <code>takeById</code>, except that blocking in this call is done
     * only if necessary to wait for transactional state to settle.
     *
     * @param clazz     The class of the entry
     * @param id        The id of the entry
     * @param routing   The routing value
     * @param timeout   The timeout value to wait for a matching entry if it does not exists within
     *                  the space
     * @param modifiers one or a union of {@link TakeModifiers}.
     * @return A matching object, or <code>null</code> if no matching is found within the timeout
     * value.
     * @since 9.0.1
     */
    <T> T takeIfExistsById(Class<T> clazz, Object id, Object routing, long timeout, TakeModifiers modifiers) throws DataAccessException;

    /**
     * Take (remove) an object from the space matching the specified id query. Returns
     * <code>null</code> if there is no match. <p/> <p>The timeout is the default timeout this
     * interface is configured with (using its factory) and defaults to 0. <p/> <p>Note, if the
     * space is partitioned, and the Entry has a specific property for its routing value, the
     * operation will broadcast to all partitions. The {@link #readById(Class, Object, Object)} can
     * be used to specify the routing. <p/> <p>Matching and timeouts are done as in
     * <code>readById</code>, except that blocking in this call is done only if necessary to wait
     * for transactional state to settle.
     *
     * @param query Query to search by.
     * @return A matching object, or <code>null</code> if no matching is found within the timeout
     * value.
     * @since 8.0
     */
    <T> T takeIfExistsById(IdQuery<T> query) throws DataAccessException;

    /**
     * Take (remove) an object from the space matching the specified id query. Returns
     * <code>null</code> if there is no match within the specified timeout. <p/> <p>Matching and
     * timeouts are done as in <code>readById</code>, except that blocking in this call is done only
     * if necessary to wait for transactional state to settle.
     *
     * @param query   Query to search by.
     * @param timeout The timeout value to wait for a matching entry if it does not exists within
     *                the space
     * @return A matching object, or <code>null</code> if no matching is found within the timeout
     * value.
     * @since 8.0
     */
    <T> T takeIfExistsById(IdQuery<T> query, long timeout) throws DataAccessException;

    /**
     * Take (remove) an object from the space matching the specified id query. Returns
     * <code>null</code> if there is no match within the specified timeout. <p/> <p>Matching and
     * timeouts are done as in <code>readById</code>, except that blocking in this call is done only
     * if necessary to wait for transactional state to settle.
     *
     * @param query     Query to search by.
     * @param timeout   The timeout value to wait for a matching entry if it does not exists within
     *                  the space
     * @param modifiers one or a union of {@link TakeModifiers}.
     * @return A matching object, or <code>null</code> if no matching is found within the timeout
     * value.
     * @since 9.0.1
     */
    <T> T takeIfExistsById(IdQuery<T> query, long timeout, TakeModifiers modifiers) throws DataAccessException;

    /**
     * Take (remove) any matching entry from the space, returning <code>null</code> if there
     * currently is none. Matching and timeouts are done as in <code>take</code>, except that
     * blocking in this call is done only if necessary to wait for transactional state to settle.
     * <p/> <p>Note, the timeout is the default timeout this interface is configured with (using its
     * factory) and defaults to 0.
     *
     * @param template The template used for matching. Matching is done against the template with
     *                 <code>null</code> fields being wildcards ( "match anything") other fields
     *                 being values ("match exactly on the serialized form").
     * @return A removed entry from the space
     */
    <T> T takeIfExists(T template) throws DataAccessException;

    /**
     * Take (remove) any matching entry from the space, returning <code>null</code> if there
     * currently is none. Matching and timeouts are done as in <code>take</code>, except that
     * blocking in this call is done only if necessary to wait for transactional state to settle.
     *
     * @param template The template used for matching. Matching is done against the template with
     *                 <code>null</code> fields being wildcards ( "match anything") other fields
     *                 being values ("match exactly on the serialized form").
     * @param timeout  How long the client is willing to wait for a transactionally proper matching
     *                 entry. A timeout of 0 means to wait no time at all.
     * @return A removed entry from the space
     */
    <T> T takeIfExists(T template, long timeout) throws DataAccessException;

    /**
     * Take (remove) any matching entry from the space, returning <code>null</code> if there
     * currently is none. Matching and timeouts are done as in <code>take</code>, except that
     * blocking in this call is done only if necessary to wait for transactional state to settle.
     *
     * @param template  The template used for matching. Matching is done against the template with
     *                  <code>null</code> fields being wildcards ( "match anything") other fields
     *                  being values ("match exactly on the serialized form").
     * @param timeout   How long the client is willing to wait for a transactionally proper matching
     *                  entry. A timeout of 0 means to wait no time at all.
     * @param modifiers one or a union of {@link TakeModifiers}.
     * @return A removed entry from the space
     * @since 9.0.1
     */
    <T> T takeIfExists(T template, long timeout, TakeModifiers modifiers) throws DataAccessException;

    /**
     * Take (remove) any matching entry from the space, returning <code>null</code> if there
     * currently is none. Matching and timeouts are done as in <code>take</code>, except that
     * blocking in this call is done only if necessary to wait for transactional state to settle.
     * <p/> <p>Note, the timeout is the default timeout this interface is configured with (using its
     * factory) and defaults to 0.
     *
     * @param template A query to be executed against the space. Most common one is {@link
     *                 com.j_spaces.core.client.SQLQuery}.
     * @return A removed entry from the space
     */
    <T> T takeIfExists(ISpaceQuery<T> template) throws DataAccessException;

    /**
     * Take (remove) any matching entry from the space, returning <code>null</code> if there
     * currently is none. Matching and timeouts are done as in <code>take</code>, except that
     * blocking in this call is done only if necessary to wait for transactional state to settle.
     *
     * @param template A query to be executed against the space. Most common one is {@link
     *                 com.j_spaces.core.client.SQLQuery}.
     * @param timeout  How long the client is willing to wait for a transactionally proper matching
     *                 entry. A timeout of 0 means to wait no time at all.
     * @return A removed entry from the space
     */
    <T> T takeIfExists(ISpaceQuery<T> template, long timeout) throws DataAccessException;

    /**
     * Take (remove) any matching entry from the space, returning <code>null</code> if there
     * currently is none. Matching and timeouts are done as in <code>take</code>, except that
     * blocking in this call is done only if necessary to wait for transactional state to settle.
     *
     * @param template  A query to be executed against the space. Most common one is {@link
     *                  com.j_spaces.core.client.SQLQuery}.
     * @param timeout   How long the client is willing to wait for a transactionally proper matching
     *                  entry. A timeout of 0 means to wait no time at all.
     * @param modifiers one or a union of {@link TakeModifiers}.
     * @return A removed entry from the space
     * @since 9.0.1
     */
    <T> T takeIfExists(ISpaceQuery<T> template, long timeout, TakeModifiers modifiers) throws DataAccessException;

    /**
     * Takes (removes) all the entries matching the specified template from this space. Same as
     * calling {@link #takeMultiple(Object, int) takeMultiple(template, Integer.MAX_VALUE)}.
     *
     * @param template The template used for matching. Matching is done against the template with
     *                 <code>null</code> fields being. wildcards ("match anything") other fields
     *                 being values ("match exactly on the serialized form"). The template can also
     *                 be one of the different {@link com.gigaspaces.query.ISpaceQuery} classes
     * @return Removed matched entries from the space
     * @throws DataAccessException In the event of a take error, DataAccessException will wrap a
     *                             TakeMultipleException, accessible via DataAccessException.getRootCause().
     */
    <T> T[] takeMultiple(T template) throws DataAccessException;

    /**
     * Takes (removes) all the entries matching the specified template from this space.
     *
     * @param template   The template used for matching. Matching is done against the template with
     *                   <code>null</code> fields being. wildcards ("match anything") other fields
     *                   being values ("match exactly on the serialized form"). The template can
     *                   also be one of the different {@link com.gigaspaces.query.ISpaceQuery}
     *                   classes
     * @param maxEntries A limit on the number of entries to be returned. Use {@link
     *                   Integer#MAX_VALUE} for the uppermost limit.
     * @return Removed matched entries from the space
     * @throws DataAccessException In the event of a take error, DataAccessException will wrap a
     *                             TakeMultipleException, accessible via DataAccessException.getRootCause().
     */
    <T> T[] takeMultiple(T template, int maxEntries) throws DataAccessException;

    /**
     * Takes (removes) all the entries matching the specified template from this space.
     *
     * @param template   The template used for matching. Matching is done against the template with
     *                   <code>null</code> fields being. wildcards ("match anything") other fields
     *                   being values ("match exactly on the serialized form"). The template can
     *                   also be one of the different {@link com.gigaspaces.query.ISpaceQuery}
     *                   classes
     * @param maxEntries A limit on the number of entries to be returned. Use {@link
     *                   Integer#MAX_VALUE} for the uppermost limit.
     * @param modifiers  one or a union of {@link TakeModifiers}.
     * @return Removed matched entries from the space
     * @throws DataAccessException In the event of a take error, DataAccessException will wrap a
     *                             TakeMultipleException, accessible via DataAccessException.getRootCause().
     * @since 9.0.1
     */
    <T> T[] takeMultiple(T template, int maxEntries, TakeModifiers modifiers) throws DataAccessException;

    /**
     * Takes (removes) all the entries matching the specified template from this space.
     *
     * @param template A query to be executed against the space. Most common one is {@link
     *                 com.j_spaces.core.client.SQLQuery}.
     * @return Removed matched entries from the space
     * @throws DataAccessException In the event of a take error, DataAccessException will wrap a
     *                             TakeMultipleException, accessible via DataAccessException.getRootCause().
     */
    <T> T[] takeMultiple(ISpaceQuery<T> template) throws DataAccessException;

    /**
     * Takes (removes) all the entries matching the specified template from this space.
     *
     * @param template   A query to be executed against the space. Most common one is {@link
     *                   com.j_spaces.core.client.SQLQuery}.
     * @param maxEntries A limit on the number of entries to be returned. Use {@link
     *                   Integer#MAX_VALUE} for the uppermost limit.
     * @return Removed matched entries from the space
     * @throws DataAccessException In the event of a take error, DataAccessException will wrap a
     *                             TakeMultipleException, accessible via DataAccessException.getRootCause().
     */
    <T> T[] takeMultiple(ISpaceQuery<T> template, int maxEntries) throws DataAccessException;

    /**
     * Takes (removes) all the entries matching the specified template from this space.
     *
     * @param template   A query to be executed against the space. Most common one is {@link
     *                   com.j_spaces.core.client.SQLQuery}.
     * @param maxEntries A limit on the number of entries to be returned. Use {@link
     *                   Integer#MAX_VALUE} for the uppermost limit.
     * @param modifiers  one or a union of {@link TakeModifiers}.
     * @return Removed matched entries from the space
     * @throws DataAccessException In the event of a take error, DataAccessException will wrap a
     *                             TakeMultipleException, accessible via DataAccessException.getRootCause().
     * @since 9.0.1
     */
    <T> T[] takeMultiple(ISpaceQuery<T> template, int maxEntries, TakeModifiers modifiers) throws DataAccessException;

    /**
     * Writes a new object to the space, returning its {@link com.j_spaces.core.LeaseContext}. <p/>
     * <p>By default uses the {@link WriteModifiers#UPDATE_OR_WRITE} modifier (see {@link
     * #write(Object, long, long, WriteModifiers)}. In order to force the operation to perform "write" only (a
     * bit more performant), the {@link WriteModifiers#WRITE_ONLY} modifier can be used resulting in
     * an {@link org.openspaces.core.EntryAlreadyInSpaceException} if the entry already exists in
     * the space (it must have an id for this exception to be raised). <p/> <p>If the object has a
     * primary key that isn't auto - generated, and the modifiers value is one of {@link
     * WriteModifiers#UPDATE_OR_WRITE}, {@link WriteModifiers#UPDATE_ONLY} or {@link
     * WriteModifiers#PARTIAL_UPDATE}, it will call to the update method, returning a LeaseContext
     * holder. This lease is unknown to the grantor, unless an actual 'write' was performed. <p/>
     * <p>The entry will be written using the default lease this interface is configured with (using
     * the its factory). In order to explicitly define the lease, please use {@link #write(Object,
     * long)}.
     *
     * @param entry The entry to write to the space
     * @return A usable <code>Lease</code> on a successful write, or <code>null</code> if performed
     * with the proxy configured with NoWriteLease flag. <p>when {@link
     * WriteModifiers#UPDATE_OR_WRITE} modifier is applied, {@link LeaseContext#getObject()} returns
     * <code>null</code> on a successful write or the previous value - on successful update (Only if
     * {@link WriteModifiers#RETURN_PREV_ON_UPDATE} is used, otherwise null).  {@link
     * org.openspaces.core.UpdateOperationTimeoutException} is thrown if timeout occurred while
     * trying to update the object.
     */
    <T> LeaseContext<T> write(T entry) throws DataAccessException;

    /**
     * Writes a new object to the space, returning its {@link com.j_spaces.core.LeaseContext}. <p/>
     * <p>By default uses the {@link WriteModifiers#UPDATE_OR_WRITE} modifier (see {@link
     * #write(Object, long, long, WriteModifiers)}. In order to force the operation to perform "write" only (a
     * bit more performant), the {@link WriteModifiers#WRITE_ONLY} modifier can be used resulting in
     * an {@link org.openspaces.core.EntryAlreadyInSpaceException} if the entry already exists in
     * the space (it must have an id for this exception to be raised). <p/> <p>If the object has a
     * primary key that isn't auto - generated, and the modifiers value is one of {@link
     * WriteModifiers#UPDATE_OR_WRITE}, {@link WriteModifiers#UPDATE_ONLY} or {@link
     * WriteModifiers#PARTIAL_UPDATE}, it will call to the update method, returning a LeaseContext
     * holder. This lease is unknown to the grantor, unless an actual 'write' was performed.
     *
     * @param entry The entry to write to the space
     * @param lease The lease the entry will be written with, in <b>milliseconds</b>.
     * @return A usable <code>Lease</code> on a successful write, or <code>null</code> if performed
     * with the proxy configured with NoWriteLease flag. <p>when {@link
     * WriteModifiers#UPDATE_OR_WRITE} modifier is applied, {@link LeaseContext#getObject()} returns
     * <code>null</code> on a successful write or the previous value - on successful update (Only if
     * {@link WriteModifiers#RETURN_PREV_ON_UPDATE} is used, otherwise null).  {@link
     * org.openspaces.core.UpdateOperationTimeoutException} is thrown if timeout occurred while
     * trying to update the object.
     */
    <T> LeaseContext<T> write(T entry, long lease) throws DataAccessException;

    /**
     * Writes a new object to the space, returning its {@link com.j_spaces.core.LeaseContext}. <p/>
     * <p>By default uses the {@link WriteModifiers#UPDATE_OR_WRITE} modifier (see {@link
     * #write(Object, long, long, WriteModifiers)}. In order to force the operation to perform "write" only (a
     * bit more performant), the {@link WriteModifiers#WRITE_ONLY} modifier can be used resulting in
     * an {@link org.openspaces.core.EntryAlreadyInSpaceException} if the entry already exists in
     * the space (it must have an id for this exception to be raised). <p/> <p>If the object has a
     * primary key that isn't auto - generated, and the modifiers value is one of {@link
     * WriteModifiers#UPDATE_OR_WRITE}, {@link WriteModifiers#UPDATE_ONLY} or {@link
     * WriteModifiers#PARTIAL_UPDATE}, it will call to the update method, returning a LeaseContext
     * holder. This lease is unknown to the grantor, unless an actual 'write' was performed. <p/>
     * <p>The entry will be written using the default lease this interface is configured with (using
     * the its factory). In order to explicitly define the lease, please use {@link #write(Object,
     * long)}.
     *
     * @param entry     The entry to write to the space
     * @param modifiers one or a union of {@link WriteModifiers}.
     * @return A usable <code>Lease</code> on a successful write, or <code>null</code> if performed
     * with the proxy configured with NoWriteLease flag. <p>when {@link
     * WriteModifiers#UPDATE_OR_WRITE} modifier is applied, {@link LeaseContext#getObject()} returns
     * <code>null</code> on a successful write or the previous value - on successful update (Only if
     * {@link WriteModifiers#RETURN_PREV_ON_UPDATE} is used, otherwise null).  {@link
     * org.openspaces.core.UpdateOperationTimeoutException} is thrown if timeout occurred while
     * trying to update the object.
     * @since 9.0.1
     */
    <T> LeaseContext<T> write(T entry, WriteModifiers modifiers) throws DataAccessException;

    /**
     * Writes a new object to the space, returning its {@link com.j_spaces.core.LeaseContext}. <p/>
     * <p>By default uses the {@link com.gigaspaces.client.WriteModifiers#UPDATE_OR_WRITE} modifier.
     * In order to force the operation to perform "write" only (a bit more performant), the {@link
     * com.gigaspaces.client.WriteModifiers#WRITE_ONLY} modifier can be used resulting in an {@link
     * org.openspaces.core.EntryAlreadyInSpaceException} if the entry already exists in the space
     * (it must have an id for this exception to be raised). <p/> <p>If the object has a primary key
     * that isn't auto - generated, and the modifiers value is one of {@link
     * WriteModifiers#UPDATE_OR_WRITE}, {@link WriteModifiers#UPDATE_ONLY} or {@link
     * WriteModifiers#PARTIAL_UPDATE}, it will call to the update method, returning a LeaseContext
     * holder. This lease is unknown to the grantor, unless an actual 'write' was performed.
     *
     * @param entry     The entry to write to the space
     * @param lease     The lease the entry will be written with, in <b>milliseconds</b>.
     * @param timeout   The timeout of an update operation, in <b>milliseconds</b>. If the entry is
     *                  locked by another transaction wait for the specified number of milliseconds
     *                  for it to be released.
     * @param modifiers one or a union of {@link WriteModifiers}.
     * @return A usable <code>Lease</code> on a successful write, or <code>null</code> if performed
     * with the proxy configured with NoWriteLease flag. <p>when {@link
     * WriteModifiers#UPDATE_OR_WRITE} modifier is applied, {@link LeaseContext#getObject()} returns
     * <code>null</code> on a successful write or the previous value - on successful update (Only if
     * {@link WriteModifiers#RETURN_PREV_ON_UPDATE} is used, otherwise null).  {@link
     * org.openspaces.core.UpdateOperationTimeoutException} is thrown if timeout occurred while
     * trying to update the object.
     * @since 9.0.1
     */
    <T> LeaseContext<T> write(T entry, long lease, long timeout, WriteModifiers modifiers) throws DataAccessException;

    /**
     * Writes the specified entries to this space. <p/> <p>The entry will be written using the
     * default lease this interface is configured with (using the its factory). In order to
     * explicitly define the lease, please use {@link #writeMultiple(Object[], long)}.
     *
     * @param entries The entries to write to the space.
     * @return Leases for the written entries
     * @throws DataAccessException In the event of a write error, DataAccessException will wrap a
     *                             WriteMultipleException, accessible via DataAccessException.getRootCause().
     */
    <T> LeaseContext<T>[] writeMultiple(T[] entries) throws DataAccessException;

    /**
     * Writes the specified entries to this space.
     *
     * @param entries The entries to write to the space.
     * @param lease   The lease the entry will be written with, in <b>milliseconds</b>.
     * @return Leases for the written entries
     * @throws DataAccessException In the event of a write error, DataAccessException will wrap a
     *                             WriteMultipleException, accessible via DataAccessException.getRootCause().
     */
    <T> LeaseContext<T>[] writeMultiple(T[] entries, long lease) throws DataAccessException;

    /**
     * Writes the specified entries to this space.
     *
     * @param entries   The entries to write to the space.
     * @param modifiers one or a union of {@link WriteModifiers}.
     * @return Leases for the written entries
     * @throws DataAccessException In the event of a write error, DataAccessException will wrap a
     *                             WriteMultipleException, accessible via DataAccessException.getRootCause().
     * @since 9.0.1
     */
    <T> LeaseContext<T>[] writeMultiple(T[] entries, WriteModifiers modifiers) throws DataAccessException;

    /**
     * Writes the specified entries to this space. <p/> Same as a single write but for a group of
     * entities sharing the same transaction (if any), applied with the same specified operation
     * modifier. The semantics of a single write and a batch update are similar - the return value
     * for each corresponds to it's cell in the returned array. see <code>'returns'</code> for
     * possible return values.
     *
     * @param entries   the entries to write.
     * @param lease     the requested lease time, in milliseconds
     * @param modifiers one or a union of {@link WriteModifiers}.
     * @return array in which each cell is corresponding to the written entry at the same index in
     * the entries array, each cell is a usable <code>Lease</code> on a successful write, or
     * <code>null</code> if performed with NoWriteLease attribute. <p>when {@link
     * WriteModifiers#UPDATE_OR_WRITE} modifier is applied, <ul> <li>{@link
     * LeaseContext#getObject()} returns: <ul> <li>null - on a successful write <li>previous value -
     * on successful update (Only if {@link WriteModifiers#RETURN_PREV_ON_UPDATE} is used, otherwise
     * null) </ul> <li>or, OperationTimeoutException - thrown if timeout occurred </ul>
     * @throws DataAccessException In the event of a write error, DataAccessException will wrap a
     *                             WriteMultipleException, accessible via DataAccessException.getRootCause().
     * @since 9.0.1
     */
    <T> LeaseContext<T>[] writeMultiple(T[] entries, long lease, WriteModifiers modifiers) throws DataAccessException;

    /**
     * Writes the specified entries to this space. <p/> Same as a single write but for a group of
     * entities sharing the same transaction (if any), applied with the same specified operation
     * modifier. The semantics of a single write and a batch update are similar - the return value
     * for each corresponds to it's cell in the returned array. see <code>'returns'</code> for
     * possible return values.
     *
     * @param entries   the entries to write.
     * @param lease     the requested lease time, in milliseconds
     * @param timeout   The timeout of the update operations, in <b>milliseconds</b>. If entries are
     *                  locked by another transaction
     * @param modifiers one or a union of {@link WriteModifiers}.
     * @return array in which each cell is corresponding to the written entry at the same index in
     * the entries array, each cell is a usable <code>Lease</code> on a successful write, or
     * <code>null</code> if performed with NoWriteLease attribute. <p>when {@link
     * WriteModifiers#UPDATE_OR_WRITE} modifier is applied, <ul> <li>{@link
     * LeaseContext#getObject()} returns: <ul> <li>null - on a successful write <li>previous value -
     * on successful update (Only if {@link WriteModifiers#RETURN_PREV_ON_UPDATE} is used, otherwise
     * null) </ul> <li>or, OperationTimeoutException - thrown if timeout occurred </ul>
     * @throws DataAccessException In the event of a write error, DataAccessException will wrap a
     *                             WriteMultipleException, accessible via DataAccessException.getRootCause().
     * @since 9.6
     */
    <T> LeaseContext<T>[] writeMultiple(T[] entries, long lease, long timeout, WriteModifiers modifiers) throws DataAccessException;

    /**
     * Writes the specified entries to this space. <p/> Same as a single write but for a group of
     * entities sharing the same transaction (if any), applied with the same specified operation
     * modifier. The semantics of a single write and a batch update are similar - the return value
     * for each corresponds to it's cell in the returned array. see <code>'returns'</code> for
     * possible return values.
     *
     * @param entries   the entries to write.
     * @param leases    the requested lease time per entry, in milliseconds
     * @param modifiers one or a union of {@link WriteModifiers}.
     * @return array in which each cell is corresponding to the written entry at the same index in
     * the entries array, each cell is a usable <code>Lease</code> on a successful write, or
     * <code>null</code> if performed with NoWriteLease attribute. <p>when {@link
     * WriteModifiers#UPDATE_OR_WRITE} modifier is applied, <ul> <li>{@link
     * LeaseContext#getObject()} returns: <ul> <li>null - on a successful write <li>previous value -
     * on successful update (Only if {@link WriteModifiers#RETURN_PREV_ON_UPDATE} is used, otherwise
     * null) </ul> <li>or, OperationTimeoutException - thrown if timeout occurred </ul>
     * @throws DataAccessException In the event of a write error, DataAccessException will wrap a
     *                             WriteMultipleException, accessible via DataAccessException.getRootCause().
     * @since 9.0.1
     */
    <T> LeaseContext<T>[] writeMultiple(T[] entries, long[] leases, WriteModifiers modifiers) throws DataAccessException;

    /**
     * Writes the specified entries to this space. <p/> Same as a single write but for a group of
     * entities sharing the same transaction (if any), applied with the same specified operation
     * modifier. The semantics of a single write and a batch update are similar - the return value
     * for each corresponds to it's cell in the returned array. see <code>'returns'</code> for
     * possible return values.
     *
     * @param entries   the entries to write.
     * @param leases    the requested lease time per entry, in milliseconds
     * @param timeout   The timeout of the update operations, in <b>milliseconds</b>. If entries are
     *                  locked by another transaction
     * @param modifiers one or a union of {@link WriteModifiers}.
     * @return array in which each cell is corresponding to the written entry at the same index in
     * the entries array, each cell is a usable <code>Lease</code> on a successful write, or
     * <code>null</code> if performed with NoWriteLease attribute. <p>when {@link
     * WriteModifiers#UPDATE_OR_WRITE} modifier is applied, <ul> <li>{@link
     * LeaseContext#getObject()} returns: <ul> <li>null - on a successful write <li>previous value -
     * on successful update (Only if {@link WriteModifiers#RETURN_PREV_ON_UPDATE} is used, otherwise
     * null) </ul> <li>or, OperationTimeoutException - thrown if timeout occurred </ul>
     * @throws DataAccessException In the event of a write error, DataAccessException will wrap a
     *                             WriteMultipleException, accessible via DataAccessException.getRootCause().
     * @since 9.6
     */
    <T> LeaseContext<T>[] writeMultiple(T[] entries, long[] leases, long timeout, WriteModifiers modifiers) throws DataAccessException;

    /**
     * Returns an iterator builder allowing to configure and create a {@link
     * com.j_spaces.core.client.GSIterator} over the Space.
     * @deprecated since 14.0 - use {@link #iterator(Object)} or one of its overloads instead.
     */
    @Deprecated
    IteratorBuilder iterator();

    /**
     * Returns an iterator over the entries in the space which match the specified template.
     *
     * @param template The template used for matching. Matching is done against the template with
     *                 <code>null</code> fields being. wildcards ("match anything") other fields
     *                 being values ("match exactly on the serialized form"). The template can also
     *                 be one of the different {@link com.gigaspaces.query.ISpaceQuery} classes
     * @return An iterator over the entries which match the template.
     */
    <T> SpaceIterator<T> iterator(T template);

    /**
     * Returns an iterator over the entries in the space which match the specified template.
     *
     * @param template  The template used for matching. Matching is done against the template with
     *                  <code>null</code> fields being. wildcards ("match anything") other fields
     *                  being values ("match exactly on the serialized form"). The template can also
     *                  be one of the different {@link com.gigaspaces.query.ISpaceQuery} classes
     * @param batchSize Maximum number of entries to fetch on each batch.
     * @return An iterator over the entries which match the template.
     */
    <T> SpaceIterator<T> iterator(T template, int batchSize);

    /**
     * Returns an iterator over the entries in the space which match the specified template.
     *
     * @param template  The template used for matching. Matching is done against the template with
     *                  <code>null</code> fields being. wildcards ("match anything") other fields
     *                  being values ("match exactly on the serialized form"). The template can also
     *                  be one of the different {@link com.gigaspaces.query.ISpaceQuery} classes
     * @param batchSize Maximum number of entries to fetch on each batch.
     * @param modifiers one or a union of {@link ReadModifiers}.
     * @return An iterator over the entries which match the template.
     */
    <T> SpaceIterator<T> iterator(T template, int batchSize, ReadModifiers modifiers);

    /**
     * Returns an iterator over the entries in the space which match the specified template.
     *
     * @param template A query to be executed against the space. Most common one is {@link
     *                 com.j_spaces.core.client.SQLQuery}.
     * @return An iterator over the entries which match the template.
     */
    <T> SpaceIterator<T> iterator(ISpaceQuery<T> template);

    /**
     * Returns an iterator over the entries in the space which match the specified template.
     *
     * @param template  A query to be executed against the space. Most common one is {@link
     *                  com.j_spaces.core.client.SQLQuery}.
     * @param batchSize Maximum number of entries to fetch on each batch.
     * @return An iterator over the entries which match the template.
     */
    <T> SpaceIterator<T> iterator(ISpaceQuery<T> template, int batchSize);

    /**
     * Returns an iterator over the entries in the space which match the specified template.
     *
     * @param template  A query to be executed against the space. Most common one is {@link
     *                  com.j_spaces.core.client.SQLQuery}.
     * @param batchSize Maximum number of entries to fetch on each batch.
     * @param modifiers one or a union of {@link ReadModifiers}.
     * @return An iterator over the entries which match the template.
     */
    <T> SpaceIterator<T> iterator(ISpaceQuery<T> template, int batchSize, ReadModifiers modifiers);

    /**
     * Executes a task on a specific space node. The space node it will execute on should be
     * controlled by having a method that return the routing value annotated with {@link
     * com.gigaspaces.annotation.pojo.SpaceRouting}. <p/> <p>In order to control the routing
     * externally, use {@link #execute(org.openspaces.core.executor.Task, Object)}. <p/> <p>The
     * space that the task is executed within can be accessible by marking a field with type {@link
     * org.openspaces.core.GigaSpace} using the {@link org.openspaces.core.executor.TaskGigaSpace}
     * annotation. Another option is by implementing the {@link org.openspaces.core.executor.TaskGigaSpaceAware}
     * interface. <p/> <p>Resource injection can be enabled by marking the task with {@link
     * org.openspaces.core.executor.AutowireTask} or with {@link org.openspaces.core.executor.AutowireTaskMarker}.
     * Resources defined within processing unit (space node) the task is executed on are accessible
     * by using either the {@link org.springframework.beans.factory.annotation.Autowired} or
     * <tt>javax.annotation.Resource</tt> annotations (assuming they are enabled using
     * <code>context:annotation-config</code>). Bean life cycle methods, such as {@link
     * org.openspaces.core.cluster.ClusterInfoAware} and {@link org.springframework.context.ApplicationContextAware}
     * are also available.
     *
     * @param task The task to execute
     * @return a Future representing pending completion of the task, and whose <code>get()</code>
     * method will return the task value upon completion.
     */
    <T extends Serializable> AsyncFuture<T> execute(Task<T> task);

    /**
     * Executes a task on a specific space node. The space node it will execute on should be
     * controlled by having a method that return the routing value annotated with {@link
     * com.gigaspaces.annotation.pojo.SpaceRouting}. <p/> <p>In order to control the routing
     * externally, use {@link #execute(org.openspaces.core.executor.Task, Object)}. <p/> <p>The
     * space that the task is executed within can be accessible by marking a field with type {@link
     * org.openspaces.core.GigaSpace} using the {@link org.openspaces.core.executor.TaskGigaSpace}
     * annotation. Another option is by implementing the {@link org.openspaces.core.executor.TaskGigaSpaceAware}
     * interface. <p/> <p>Resource injection can be enabled by marking the task with {@link
     * org.openspaces.core.executor.AutowireTask} or with {@link org.openspaces.core.executor.AutowireTaskMarker}.
     * Resources defined within processing unit (space node) the task is executed on are accessible
     * by using either the {@link org.springframework.beans.factory.annotation.Autowired} or
     * <tt>javax.annotation.Resource</tt> annotations (assuming they are enabled using
     * <code>context:annotation-config</code>). Bean life cycle methods, such as {@link
     * org.openspaces.core.cluster.ClusterInfoAware} and {@link org.springframework.context.ApplicationContextAware}
     * are also available.
     *
     * @param task     The task to execute
     * @param listener A listener to be notified when execution completes
     * @return a Future representing pending completion of the task, and whose <code>get()</code>
     * method will return the task value upon completion.
     */
    <T extends Serializable> AsyncFuture<T> execute(Task<T> task, AsyncFutureListener<T> listener);

    /**
     * Executes a task on a specific space node. The space node it will execute on should is
     * controlled by the routing value provided as a second parameter. <p/> <p>The routing object
     * itself does not have to be the actual routing value, but can be a POJO that defined a method
     * annotated with <code>@SpaceRouting</code> annotation (this works well when wanting to use
     * entries as the routing parameters). <p/> <p>In order to control the using the Task itself,
     * use {@link #execute(org.openspaces.core.executor.Task)}. <p/> <p>The space that the task is
     * executed within can be accessible by marking a field with type {@link
     * org.openspaces.core.GigaSpace} using the {@link org.openspaces.core.executor.TaskGigaSpace}
     * annotation. Another option is by implementing the {@link org.openspaces.core.executor.TaskGigaSpaceAware}
     * interface. <p/> <p>Resource injection can be enabled by marking the task with {@link
     * org.openspaces.core.executor.AutowireTask} or with {@link org.openspaces.core.executor.AutowireTaskMarker}.
     * Resources defined within processing unit (space node) the task is executed on are accessible
     * by using either the {@link org.springframework.beans.factory.annotation.Autowired} or
     * <tt>javax.annotation.Resource</tt> annotations (assuming they are enabled using
     * <code>context:annotation-config</code>). Bean life cycle methods, such as {@link
     * org.openspaces.core.cluster.ClusterInfoAware} and {@link org.springframework.context.ApplicationContextAware}
     * are also available.
     *
     * @param task    The task to execute
     * @param routing The routing value that will control on which node the task will be executed
     *                on
     * @return a Future representing pending completion of the task, and whose <code>get()</code>
     * method will return the task value upon completion.
     */
    <T extends Serializable> AsyncFuture<T> execute(Task<T> task, Object routing);

    /**
     * Executes a task on a specific space node. The space node it will execute on should is
     * controlled by the routing value provided as a second parameter. <p/> <p>The routing object
     * itself does not have to be the actual routing value, but can be a POJO that defined a method
     * annotated with <code>@SpaceRouting</code> annotation (this works well when wanting to use
     * entries as the routing parameters). <p/> <p>In order to control the using the Task itself,
     * use {@link #execute(org.openspaces.core.executor.Task)}. <p/> <p>The space that the task is
     * executed within can be accessible by marking a field with type {@link
     * org.openspaces.core.GigaSpace} using the {@link org.openspaces.core.executor.TaskGigaSpace}
     * annotation. Another option is by implementing the {@link org.openspaces.core.executor.TaskGigaSpaceAware}
     * interface. <p/> <p>Resource injection can be enabled by marking the task with {@link
     * org.openspaces.core.executor.AutowireTask} or with {@link org.openspaces.core.executor.AutowireTaskMarker}.
     * Resources defined within processing unit (space node) the task is executed on are accessible
     * by using either the {@link org.springframework.beans.factory.annotation.Autowired} or
     * <tt>javax.annotation.Resource</tt> annotations (assuming they are enabled using
     * <code>context:annotation-config</code>). Bean life cycle methods, such as {@link
     * org.openspaces.core.cluster.ClusterInfoAware} and {@link org.springframework.context.ApplicationContextAware}
     * are also available.
     *
     * @param task     The task to execute
     * @param routing  The routing value that will control on which node the task will be executed
     *                 on
     * @param listener A listener to be notified when execution completes
     * @return a Future representing pending completion of the task, and whose <code>get()</code>
     * method will return the task value upon completion.
     */
    <T extends Serializable> AsyncFuture<T> execute(Task<T> task, Object routing, AsyncFutureListener<T> listener);

    /**
     * Executes a task on all the nodes that correspond to the list of routing values. The task is
     * executed on each space node with all the results reduced by the {@link
     * org.openspaces.core.executor.DistributedTask#reduce(java.util.List)} operation. <p/> <p>The
     * routing object itself does not have to be the actual routing value, but can be a POJO that
     * defined a method annotated with <code>@SpaceRouting</code> annotation (this works well when
     * wanting to use entries as the routing parameters). <p/> <p>The task can optionally implement
     * {@link com.gigaspaces.async.AsyncResultFilter} that can control if tasks should continue to
     * accumulate or it should break and execute the reduce operation on the results received so
     * far. <p/> <p>The future actual result will be the reduced result of the execution, or the
     * exception thrown during during the reduce operation. The moderator can be used as a mechanism
     * to listen for results as they arrive. <p/> <p>The last parameter can be of type {@link
     * com.gigaspaces.async.AsyncFutureListener} which, this case, it will be used to register a
     * listener to be notified of the result. <p/> <p>The space that the task is executed within can
     * be accessible by marking a field with type {@link org.openspaces.core.GigaSpace} using the
     * {@link org.openspaces.core.executor.TaskGigaSpace} annotation. Another option is by
     * implementing the {@link org.openspaces.core.executor.TaskGigaSpaceAware} interface. <p/>
     * <p>Resource injection can be enabled by marking the task with {@link
     * org.openspaces.core.executor.AutowireTask} or with {@link org.openspaces.core.executor.AutowireTaskMarker}.
     * Resources defined within processing unit (space node) the task is executed on are accessible
     * by using either the {@link org.springframework.beans.factory.annotation.Autowired} or
     * <tt>javax.annotation.Resource</tt> annotations (assuming they are enabled using
     * <code>context:annotation-config</code>). Bean life cycle methods, such as {@link
     * org.openspaces.core.cluster.ClusterInfoAware} and {@link org.springframework.context.ApplicationContextAware}
     * are also available.
     *
     * @param task    The task to execute
     * @param routing A list of routing values, each resulting in an execution of the task on the
     *                space node it corresponds to
     * @return a Future representing pending completion of the task, and whose <code>get()</code>
     * method will return the task value upon completion.
     */
    <T extends Serializable, R> AsyncFuture<R> execute(DistributedTask<T, R> task, Object... routing);

    /**
     * Executes the task on all the primary space nodes within the cluster (broadcast). The task is
     * executed on each space node with all the results reduced by the {@link
     * org.openspaces.core.executor.DistributedTask#reduce(java.util.List)} operation. <p/> <p>The
     * task can optionally implement {@link com.gigaspaces.async.AsyncResultFilter} that can control
     * if tasks should continue to accumulate or it should break and execute the reduce operation on
     * the results received so far. <p/> <p>The future actual result will be the reduced result of
     * the execution, or the exception thrown during during the reduce operation. The moderator can
     * be used as a mechanism to listen for results as they arrive. <p/> <p>The space that the task
     * is executed within can be accessible by marking a field with type {@link
     * org.openspaces.core.GigaSpace} using the {@link org.openspaces.core.executor.TaskGigaSpace}
     * annotation. Another option is by implementing the {@link org.openspaces.core.executor.TaskGigaSpaceAware}
     * interface. <p/> <p>Resource injection can be enabled by marking the task with {@link
     * org.openspaces.core.executor.AutowireTask} or with {@link org.openspaces.core.executor.AutowireTaskMarker}.
     * Resources defined within processing unit (space node) the task is executed on are accessible
     * by using either the {@link org.springframework.beans.factory.annotation.Autowired} or
     * <tt>javax.annotation.Resource</tt> annotations (assuming they are enabled using
     * <code>context:annotation-config</code>). Bean life cycle methods, such as {@link
     * org.openspaces.core.cluster.ClusterInfoAware} and {@link org.springframework.context.ApplicationContextAware}
     * are also available.
     *
     * @param task The task to execute
     * @return a Future representing pending completion of the task, and whose <code>get()</code>
     * method will return the task value upon completion.
     */
    <T extends Serializable, R> AsyncFuture<R> execute(DistributedTask<T, R> task);

    //    /**
    //     * Executes the task on all the primary space nodes within the cluster (broadcast). The task is executed
    //     * on each space node with all the results reduced by the
    //     * {@link org.openspaces.core.executor.DistributedTask#reduce(java.util.List)} operation.
    //     *
    //     * <p>The task can optionally implement {@link com.gigaspaces.async.AsyncResultFilter} that can control
    //     * if tasks should continue to accumulate or it should break and execute the reduce operation on the
    //     * results received so far.
    //     *
    //     * <p>The future actual result will be the reduced result of the execution, or the exception thrown during
    //     * during the reduce operation. The moderator can be used as a mechanism to listen for results as they arrive.
    //     *
    //    * <p>The space that the task is executed within can be accessible by marking a field with type {@link org.openspaces.core.GigaSpace}
    //    * using the {@link org.openspaces.core.executor.TaskGigaSpace} annotation. Another option is by implementing
    //    * the {@link org.openspaces.core.executor.TaskGigaSpaceAware} interface.
    //    *
    //    * <p>Resource injection can be enabled by marking the task with {@link org.openspaces.core.executor.AutowireTask}
    //    * or with {@link org.openspaces.core.executor.AutowireTaskMarker}. Resources defined within processing unit
    //    * (space node) the task is executed on are accessible by using either the {@link org.springframework.beans.factory.annotation.Autowired} or
    //    * <tt>javax.annotation.Resource</tt> annotations (assuming they are enabled using <code>context:annotation-config</code>).
    //    * Bean life cycle methods, such as {@link org.openspaces.core.cluster.ClusterInfoAware} and
    //    * {@link org.springframework.context.ApplicationContextAware} are also available.
    //     *
    //     * @param task     The task to execute
    //     * @param listener A listener to be notified when execution completes
    //     * @return a Future representing pending completion of the task,
    //     *         and whose <code>get()</code> method will return the task value upon completion.
    //     */
    // REMOVE this because of compilation problem, you can still use this API.
    //    <T extends Serializable, R> AsyncFuture<R> execute(DistributedTask<T, R> task, AsyncFutureListener<R> listener);

    /**
     * Constructs an executor builder allowing to accumulate different tasks required to be executed
     * into a single execute mechanism with all the results reduced by the reducer provided. <p/>
     * <p>The reducer can optionally implement {@link com.gigaspaces.async.AsyncResultFilter} that
     * can control if tasks should continue to accumulate or it should break and execute the reduce
     * operation on the results received so far. <p/> <p>The space that the task is executed within
     * can be accessible by marking a field with type {@link org.openspaces.core.GigaSpace} using
     * the {@link org.openspaces.core.executor.TaskGigaSpace} annotation. Another option is by
     * implementing the {@link org.openspaces.core.executor.TaskGigaSpaceAware} interface. <p/>
     * <p>Resource injection can be enabled by marking the task with {@link
     * org.openspaces.core.executor.AutowireTask} or with {@link org.openspaces.core.executor.AutowireTaskMarker}.
     * Resources defined within processing unit (space node) the task is executed on are accessible
     * by using either the {@link org.springframework.beans.factory.annotation.Autowired} or
     * <tt>javax.annotation.Resource</tt> annotations (assuming they are enabled using
     * <code>context:annotation-config</code>). Bean life cycle methods, such as {@link
     * org.openspaces.core.cluster.ClusterInfoAware} and {@link org.springframework.context.ApplicationContextAware}
     * are also available.
     *
     * @param reducer The reducer to reduce the results of all the different tasks added.
     * @return The executor builder.
     */
    <T extends Serializable, R> ExecutorBuilder<T, R> executorBuilder(AsyncResultsReducer<T, R> reducer);

    /**
     * Gets the type manager of this GigaSpace instance.
     *
     * @see org.openspaces.core.GigaSpaceTypeManager
     * @since 8.0
     */
    GigaSpaceTypeManager getTypeManager();

    /**
     * Executes the specified query along with the specified aggregations collocated at the space.
     *
     * @param query          Query to search by
     * @param aggregationSet aggregations to execute
     * @return Aggregations result
     * @see org.openspaces.extensions.QueryExtension
     * @since 10.0
     */
    <T> AggregationResult aggregate(ISpaceQuery<T> query, AggregationSet aggregationSet);

    /**
     * Changes existing objects in space, returning a change result which provides details of the
     * operation affect. The change operation is designed for performance optimization, By allowing
     * to change an existing object unlike with regular updating write operation which usually
     * requires reading the object before applying to update to it. As part of the optimization,
     * when the operation is replicated, on a best effort it will try to replicate only the required
     * data which is needed to apply the changes on the entry in the replicated target. <p/>
     * <p>Modifiers can be used to specify behavior of the change operation, by default uses the
     * {@link ChangeModifiers#NONE} modifier.
     *
     * @param query     Query to search by.
     * @param changeSet Changes to apply to the matched entry.
     * @return A <code>ChangeResult</code> containing the details of the change operation affect.
     * @throws ChangeException In event of a change error.
     * @see org.openspaces.extensions.ChangeExtension
     * @since 9.1
     */
    <T> ChangeResult<T> change(ISpaceQuery<T> query, ChangeSet changeSet);

    /**
     * Changes existing objects in space, returning a change result which provides details of the
     * operation affect. The change operation is designed for performance optimization, By allowing
     * to change an existing object unlike with regular updating write operation which usually
     * requires reading the object before applying to update to it. As part of the optimization,
     * when the operation is replicated, on a best effort it will try to replicate only the required
     * data which is needed to apply the changes on the entry in the replicated target. <p/>
     * <p>Modifiers can be used to specify behavior of the change operation, by default uses the
     * {@link ChangeModifiers#NONE} modifier.
     *
     * @param query     Query to search by.
     * @param changeSet Changes to apply to the matched entry.
     * @param timeout   The timeout of the operation, in <b>milliseconds</b>. If the entry is locked
     *                  by another transaction wait for the specified number of milliseconds for it
     *                  to be released.
     * @return A <code>ChangeResult</code> containing the details of the change operation affect.
     * @throws ChangeException In event of a change error.
     * @see org.openspaces.extensions.ChangeExtension
     * @since 9.1
     */
    <T> ChangeResult<T> change(ISpaceQuery<T> query, ChangeSet changeSet, long timeout);

    /**
     * Changes existing objects in space, returning a change result which provides details of the
     * operation affect. The change operation is designed for performance optimization, By allowing
     * to change an existing object unlike with regular updating write operation which usually
     * requires reading the object before applying to update to it. As part of the optimization,
     * when the operation is replicated, on a best effort it will try to replicate only the required
     * data which is needed to apply the changes on the entry in the replicated target. <p/>
     * <p>Modifiers can be used to specify behavior of the change operation, by default uses the
     * {@link ChangeModifiers#NONE} modifier.
     *
     * @param query     Query to search by.
     * @param changeSet Changes to apply to the matched entry.
     * @param modifiers one or a union of {@link ChangeModifiers}
     * @return A <code>ChangeResult</code> containing the details of the change operation affect.
     * @throws ChangeException In event of a change error.
     * @see org.openspaces.extensions.ChangeExtension
     * @since 9.1
     */
    <T> ChangeResult<T> change(ISpaceQuery<T> query, ChangeSet changeSet, ChangeModifiers modifiers);

    /**
     * Changes existing objects in space, returning a change result which provides details of the
     * operation affect. The change operation is designed for performance optimization, By allowing
     * to change an existing object unlike with regular updating write operation which usually
     * requires reading the object before applying to update to it. As part of the optimization,
     * when the operation is replicated, on a best effort it will try to replicate only the required
     * data which is needed to apply the changes on the entry in the replicated target. <p/>
     * <p>Modifiers can be used to specify behavior of the change operation, by default uses the
     * {@link ChangeModifiers#NONE} modifier.
     *
     * @param query     Query to search by.
     * @param changeSet Changes to apply to the matched entry.
     * @param modifiers one or a union of {@link ChangeModifiers}
     * @param timeout   The timeout of the operation, in <b>milliseconds</b>. If the entry is locked
     *                  by another transaction wait for the specified number of milliseconds for it
     *                  to be released.
     * @return A <code>ChangeResult</code> containing the details of the change operation affect.
     * @throws ChangeException In event of a change error.
     * @see org.openspaces.extensions.ChangeExtension
     * @since 9.1
     */
    <T> ChangeResult<T> change(ISpaceQuery<T> query, ChangeSet changeSet, ChangeModifiers modifiers, long timeout);

    /**
     * Changes existing objects in space in an asynchronous manner, returning immidiately with a
     * future. The future can be used to check the change operation results which provides details
     * of the operation affect. The change operation is designed for performance optimization, By
     * allowing to change an existing object unlike with regular updating write operation which
     * usually requires reading the object before applying to update to it. As part of the
     * optimization, when the operation is replicated, on a best effort it will try to replicate
     * only the required data which is needed to apply the changes on the entry in the replicated
     * target. <p/> <p>Modifiers can be used to specify behavior of the change operation, by default
     * uses the {@link ChangeModifiers#NONE} modifier.
     *
     * @param query     Query to search by.
     * @param changeSet Changes to apply to the matched entry.
     * @return A future containing the details of the change operation affect which arrived
     * asynchronously.
     * @throws ChangeException In event of a change error.
     * @see org.openspaces.extensions.ChangeExtension
     * @since 9.1
     */
    <T> Future<ChangeResult<T>> asyncChange(ISpaceQuery<T> query, ChangeSet changeSet);

    /**
     * Changes existing objects in space in an asynchronous manner, returning immidiately with a
     * future. The future can be used to check the change operation results which provides details
     * of the operation affect. The change operation is designed for performance optimization, By
     * allowing to change an existing object unlike with regular updating write operation which
     * usually requires reading the object before applying to update to it. As part of the
     * optimization, when the operation is replicated, on a best effort it will try to replicate
     * only the required data which is needed to apply the changes on the entry in the replicated
     * target. <p/> <p>Modifiers can be used to specify behavior of the change operation, by default
     * uses the {@link ChangeModifiers#NONE} modifier.
     *
     * @param query     Query to search by.
     * @param changeSet Changes to apply to the matched entry.
     * @param listener  A listener to be notified when a result arrives.
     * @return A future containing the details of the change operation affect which arrived
     * asynchronously.
     * @throws ChangeException In event of a change error.
     * @see org.openspaces.extensions.ChangeExtension
     * @since 9.1
     */
    <T> Future<ChangeResult<T>> asyncChange(ISpaceQuery<T> query, ChangeSet changeSet, AsyncFutureListener<ChangeResult<T>> listener);

    /**
     * Changes existing objects in space in an asynchronous manner, returning immidiately with a
     * future. The future can be used to check the change operation results which provides details
     * of the operation affect. The change operation is designed for performance optimization, By
     * allowing to change an existing object unlike with regular updating write operation which
     * usually requires reading the object before applying to update to it. As part of the
     * optimization, when the operation is replicated, on a best effort it will try to replicate
     * only the required data which is needed to apply the changes on the entry in the replicated
     * target. <p/> <p>Modifiers can be used to specify behavior of the change operation, by default
     * uses the {@link ChangeModifiers#NONE} modifier.
     *
     * @param query     Query to search by.
     * @param changeSet Changes to apply to the matched entry.
     * @param timeout   The timeout of the operation, in <b>milliseconds</b>. If the entry is locked
     *                  by another transaction wait for the specified number of milliseconds for it
     *                  to be released.
     * @return A future containing the details of the change operation affect which arrived
     * asynchronously.
     * @throws ChangeException In event of a change error.
     * @see org.openspaces.extensions.ChangeExtension
     * @since 9.1
     */
    <T> Future<ChangeResult<T>> asyncChange(ISpaceQuery<T> query, ChangeSet changeSet, long timeout);

    /**
     * Changes existing objects in space in an asynchronous manner, returning immidiately with a
     * future. The future can be used to check the change operation results which provides details
     * of the operation affect. The change operation is designed for performance optimization, By
     * allowing to change an existing object unlike with regular updating write operation which
     * usually requires reading the object before applying to update to it. As part of the
     * optimization, when the operation is replicated, on a best effort it will try to replicate
     * only the required data which is needed to apply the changes on the entry in the replicated
     * target. <p/> <p>Modifiers can be used to specify behavior of the change operation, by default
     * uses the {@link ChangeModifiers#NONE} modifier.
     *
     * @param query     Query to search by.
     * @param changeSet Changes to apply to the matched entry.
     * @param timeout   The timeout of the operation, in <b>milliseconds</b>. If the entry is locked
     *                  by another transaction wait for the specified number of milliseconds for it
     *                  to be released.
     * @param listener  A listener to be notified when a result arrives.
     * @return A future containing the details of the change operation affect which arrived
     * asynchronously.
     * @throws ChangeException In event of a change error.
     * @see org.openspaces.extensions.ChangeExtension
     * @since 9.1
     */
    <T> Future<ChangeResult<T>> asyncChange(ISpaceQuery<T> query, ChangeSet changeSet, long timeout, AsyncFutureListener<ChangeResult<T>> listener);

    /**
     * Changes an existing objects in space in an asynchronous manner, returning immidiately with a
     * future. The future can be used to check the change operation results which provides details
     * of the operation affect. The change operation is designed for performance optimization, By
     * allowing to change an existing object unlike with regular updating write operation which
     * usually requires reading the object before applying to update to it. As part of the
     * optimization, when the operation is replicated, on a best effort it will try to replicate
     * only the required data which is needed to apply the changes on the entry in the replicated
     * target. <p/> <p>Modifiers can be used to specify behavior of the change operation, by default
     * uses the {@link ChangeModifiers#NONE} modifier.
     *
     * @param query     Query to search by.
     * @param changeSet Changes to apply to the matched entry.
     * @param modifiers one or a union of {@link ChangeModifiers}
     * @return A future containing the details of the change operation affect which arrived
     * asynchronously.
     * @throws ChangeException In event of a change error.
     * @see org.openspaces.extensions.ChangeExtension
     * @since 9.1
     */
    <T> Future<ChangeResult<T>> asyncChange(ISpaceQuery<T> query, ChangeSet changeSet, ChangeModifiers modifiers);

    /**
     * Changes existing objects in space in an asynchronous manner, returning immidiately with a
     * future. The future can be used to check the change operation results which provides details
     * of the operation affect. The change operation is designed for performance optimization, By
     * allowing to change an existing object unlike with regular updating write operation which
     * usually requires reading the object before applying to update to it. As part of the
     * optimization, when the operation is replicated, on a best effort it will try to replicate
     * only the required data which is needed to apply the changes on the entry in the replicated
     * target. <p/> <p>Modifiers can be used to specify behavior of the change operation, by default
     * uses the {@link ChangeModifiers#NONE} modifier.
     *
     * @param query     Query to search by.
     * @param changeSet Changes to apply to the matched entry.
     * @param modifiers one or a union of {@link ChangeModifiers}
     * @param listener  A listener to be notified when a result arrives.
     * @return A future containing the details of the change operation affect which arrived
     * asynchronously.
     * @throws ChangeException In event of a change error.
     * @see org.openspaces.extensions.ChangeExtension
     * @since 9.1
     */
    <T> Future<ChangeResult<T>> asyncChange(ISpaceQuery<T> query, ChangeSet changeSet, ChangeModifiers modifiers, AsyncFutureListener<ChangeResult<T>> listener);

    /**
     * Changes existing objects in space in an asynchronous manner, returning immidiately with a
     * future. The future can be used to check the change operation results which provides details
     * of the operation affect. The change operation is designed for performance optimization, By
     * allowing to change an existing object unlike with regular updating write operation which
     * usually requires reading the object before applying to update to it. As part of the
     * optimization, when the operation is replicated, on a best effort it will try to replicate
     * only the required data which is needed to apply the changes on the entry in the replicated
     * target. <p/> <p>Modifiers can be used to specify behavior of the change operation, by default
     * uses the {@link ChangeModifiers#NONE} modifier.
     *
     * @param query     Query to search by.
     * @param changeSet Changes to apply to the matched entry.
     * @param modifiers one or a union of {@link ChangeModifiers}
     * @param timeout   The timeout of the operation, in <b>milliseconds</b>. If the entry is locked
     *                  by another transaction wait for the specified number of milliseconds for it
     *                  to be released.
     * @return A future containing the details of the change operation affect which arrived
     * asynchronously.
     * @throws ChangeException In event of a change error.
     * @see org.openspaces.extensions.ChangeExtension
     * @since 9.1
     */
    <T> Future<ChangeResult<T>> asyncChange(ISpaceQuery<T> query, ChangeSet changeSet, ChangeModifiers modifiers, long timeout);

    /**
     * Changes existing objects in space in an asynchronous manner, returning immidiately with a
     * future. The future can be used to check the change operation results which provides details
     * of the operation affect. The change operation is designed for performance optimization, By
     * allowing to change an existing object unlike with regular updating write operation which
     * usually requires reading the object before applying to update to it. As part of the
     * optimization, when the operation is replicated, on a best effort it will try to replicate
     * only the required data which is needed to apply the changes on the entry in the replicated
     * target. <p/> <p>Modifiers can be used to specify behavior of the change operation, by default
     * uses the {@link ChangeModifiers#NONE} modifier.
     *
     * @param query     Query to search by.
     * @param changeSet Changes to apply to the matched entry.
     * @param modifiers one or a union of {@link ChangeModifiers}
     * @param timeout   The timeout of the operation, in <b>milliseconds</b>. If the entry is locked
     *                  by another transaction wait for the specified number of milliseconds for it
     *                  to be released.
     * @param listener  A listener to be notified when a result arrives.
     * @return A future containing the details of the change operation affect which arrived
     * asynchronously.
     * @throws ChangeException In event of a change error.
     * @see org.openspaces.extensions.ChangeExtension
     * @since 9.1
     */
    <T> Future<ChangeResult<T>> asyncChange(ISpaceQuery<T> query, ChangeSet changeSet, ChangeModifiers modifiers, long timeout, AsyncFutureListener<ChangeResult<T>> listener);

    /**
     * Changes existing objects in space, returning a change result which provides details of the
     * operation affect. The change operation is designed for performance optimization, By allowing
     * to change an existing object unlike with regular updating write operation which usually
     * requires reading the object before applying to update to it. As part of the optimization,
     * when the operation is replicated, on a best effort it will try to replicate only the required
     * data which is needed to apply the changes on the entry in the replicated target. <p/>
     * <p>Modifiers can be used to specify behavior of the change operation, by default uses the
     * {@link ChangeModifiers#NONE} modifier.
     *
     * @param template  The template used for matching. Matching is done against the template with
     *                  <code>null</code> fields being. wildcards ("match anything") other fields
     *                  being values ("match exactly on the serialized form"). The template can also
     *                  be one of the different {@link com.gigaspaces.query.ISpaceQuery} classes
     * @param changeSet Changes to apply to the matched entry.
     * @return A <code>ChangeResult</code> containing the details of the change operation affect.
     * @throws ChangeException In event of a change error.
     * @since 9.1
     */
    <T> ChangeResult<T> change(T template, ChangeSet changeSet);

    /**
     * Changes existing objects in space, returning a change result which provides details of the
     * operation affect. The change operation is designed for performance optimization, By allowing
     * to change an existing object unlike with regular updating write operation which usually
     * requires reading the object before applying to update to it. As part of the optimization,
     * when the operation is replicated, on a best effort it will try to replicate only the required
     * data which is needed to apply the changes on the entry in the replicated target. <p/>
     * <p>Modifiers can be used to specify behavior of the change operation, by default uses the
     * {@link ChangeModifiers#NONE} modifier.
     *
     * @param template  The template used for matching. Matching is done against the template with
     *                  <code>null</code> fields being. wildcards ("match anything") other fields
     *                  being values ("match exactly on the serialized form"). The template can also
     *                  be one of the different {@link com.gigaspaces.query.ISpaceQuery} classes
     * @param changeSet Changes to apply to the matched entry.
     * @param timeout   The timeout of the operation, in <b>milliseconds</b>. If the entry is locked
     *                  by another transaction wait for the specified number of milliseconds for it
     *                  to be released.
     * @return A <code>ChangeResult</code> containing the details of the change operation affect.
     * @throws ChangeException In event of a change error.
     * @since 9.1
     */
    <T> ChangeResult<T> change(T template, ChangeSet changeSet, long timeout);

    /**
     * Changes existing objects in space, returning a change result which provides details of the
     * operation affect. The change operation is designed for performance optimization, By allowing
     * to change an existing object unlike with regular updating write operation which usually
     * requires reading the object before applying to update to it. As part of the optimization,
     * when the operation is replicated, on a best effort it will try to replicate only the required
     * data which is needed to apply the changes on the entry in the replicated target. <p/>
     * <p>Modifiers can be used to specify behavior of the change operation, by default uses the
     * {@link ChangeModifiers#NONE} modifier.
     *
     * @param template  The template used for matching. Matching is done against the template with
     *                  <code>null</code> fields being. wildcards ("match anything") other fields
     *                  being values ("match exactly on the serialized form"). The template can also
     *                  be one of the different {@link com.gigaspaces.query.ISpaceQuery} classes
     * @param changeSet Changes to apply to the matched entry.
     * @param modifiers one or a union of {@link ChangeModifiers}
     * @return A <code>ChangeResult</code> containing the details of the change operation affect.
     * @throws ChangeException In event of a change error.
     * @since 9.1
     */
    <T> ChangeResult<T> change(T template, ChangeSet changeSet, ChangeModifiers modifiers);

    /**
     * Changes existing objects in space, returning a change result which provides details of the
     * operation affect. The change operation is designed for performance optimization, By allowing
     * to change an existing object unlike with regular updating write operation which usually
     * requires reading the object before applying to update to it. As part of the optimization,
     * when the operation is replicated, on a best effort it will try to replicate only the required
     * data which is needed to apply the changes on the entry in the replicated target. <p/>
     * <p>Modifiers can be used to specify behavior of the change operation, by default uses the
     * {@link ChangeModifiers#NONE} modifier.
     *
     * @param template  The template used for matching. Matching is done against the template with
     *                  <code>null</code> fields being. wildcards ("match anything") other fields
     *                  being values ("match exactly on the serialized form"). The template can also
     *                  be one of the different {@link com.gigaspaces.query.ISpaceQuery} classes
     * @param changeSet Changes to apply to the matched entry.
     * @param modifiers one or a union of {@link ChangeModifiers}
     * @param timeout   The timeout of the operation, in <b>milliseconds</b>. If the entry is locked
     *                  by another transaction wait for the specified number of milliseconds for it
     *                  to be released.
     * @return A <code>ChangeResult</code> containing the details of the change operation affect.
     * @throws ChangeException In event of a change error.
     * @since 9.1
     */
    <T> ChangeResult<T> change(T template, ChangeSet changeSet, ChangeModifiers modifiers, long timeout);

    /**
     * Changes existing objects in space in an asynchronous manner, returning immidiately with a
     * future. The future can be used to check the change operation results which provides details
     * of the operation affect. The change operation is designed for performance optimization, By
     * allowing to change an existing object unlike with regular updating write operation which
     * usually requires reading the object before applying to update to it. As part of the
     * optimization, when the operation is replicated, on a best effort it will try to replicate
     * only the required data which is needed to apply the changes on the entry in the replicated
     * target. <p/> <p>Modifiers can be used to specify behavior of the change operation, by default
     * uses the {@link ChangeModifiers#NONE} modifier.
     *
     * @param template  The template used for matching. Matching is done against the template with
     *                  <code>null</code> fields being. wildcards ("match anything") other fields
     *                  being values ("match exactly on the serialized form"). The template can also
     *                  be one of the different {@link com.gigaspaces.query.ISpaceQuery} classes
     * @param changeSet Changes to apply to the matched entry.
     * @return A future containing the details of the change operation affect which arrived
     * asynchronously.
     * @throws ChangeException Arrived asynchronously in event of a change error, via future or
     *                         listener.
     * @since 9.1
     */
    <T> Future<ChangeResult<T>> asyncChange(T template, ChangeSet changeSet);

    /**
     * Changes existing objects in space in an asynchronous manner, returning immidiately with a
     * future. The future can be used to check the change operation results which provides details
     * of the operation affect. The change operation is designed for performance optimization, By
     * allowing to change an existing object unlike with regular updating write operation which
     * usually requires reading the object before applying to update to it. As part of the
     * optimization, when the operation is replicated, on a best effort it will try to replicate
     * only the required data which is needed to apply the changes on the entry in the replicated
     * target. <p/> <p>Modifiers can be used to specify behavior of the change operation, by default
     * uses the {@link ChangeModifiers#NONE} modifier.
     *
     * @param template  The template used for matching. Matching is done against the template with
     *                  <code>null</code> fields being. wildcards ("match anything") other fields
     *                  being values ("match exactly on the serialized form"). The template can also
     *                  be one of the different {@link com.gigaspaces.query.ISpaceQuery} classes
     * @param changeSet Changes to apply to the matched entry.
     * @param listener  A listener to be notified when a result arrives.
     * @return A future containing the details of the change operation affect which arrived
     * asynchronously.
     * @throws ChangeException Arrived asynchronously in event of a change error, via future or
     *                         listener.
     * @since 9.1
     */
    <T> Future<ChangeResult<T>> asyncChange(T template, ChangeSet changeSet, AsyncFutureListener<ChangeResult<T>> listener);

    /**
     * Changes existing objects in space in an asynchronous manner, returning immidiately with a
     * future. The future can be used to check the change operation results which provides details
     * of the operation affect. The change operation is designed for performance optimization, By
     * allowing to change an existing object unlike with regular updating write operation which
     * usually requires reading the object before applying to update to it. As part of the
     * optimization, when the operation is replicated, on a best effort it will try to replicate
     * only the required data which is needed to apply the changes on the entry in the replicated
     * target. <p/> <p>Modifiers can be used to specify behavior of the change operation, by default
     * uses the {@link ChangeModifiers#NONE} modifier.
     *
     * @param template  The template used for matching. Matching is done against the template with
     *                  <code>null</code> fields being. wildcards ("match anything") other fields
     *                  being values ("match exactly on the serialized form"). The template can also
     *                  be one of the different {@link com.gigaspaces.query.ISpaceQuery} classes
     * @param changeSet Changes to apply to the matched entry.
     * @param timeout   The timeout of the operation, in <b>milliseconds</b>. If the entry is locked
     *                  by another transaction wait for the specified number of milliseconds for it
     *                  to be released.
     * @return A future containing the details of the change operation affect which arrived
     * asynchronously.
     * @throws ChangeException Arrived asynchronously in event of a change error, via future or
     *                         listener.
     * @since 9.1
     */
    <T> Future<ChangeResult<T>> asyncChange(T template, ChangeSet changeSet, long timeout);

    /**
     * Changes existing objects in space in an asynchronous manner, returning immidiately with a
     * future. The future can be used to check the change operation results which provides details
     * of the operation affect. The change operation is designed for performance optimization, By
     * allowing to change an existing object unlike with regular updating write operation which
     * usually requires reading the object before applying to update to it. As part of the
     * optimization, when the operation is replicated, on a best effort it will try to replicate
     * only the required data which is needed to apply the changes on the entry in the replicated
     * target. <p/> <p>Modifiers can be used to specify behavior of the change operation, by default
     * uses the {@link ChangeModifiers#NONE} modifier.
     *
     * @param template  The template used for matching. Matching is done against the template with
     *                  <code>null</code> fields being. wildcards ("match anything") other fields
     *                  being values ("match exactly on the serialized form"). The template can also
     *                  be one of the different {@link com.gigaspaces.query.ISpaceQuery} classes
     * @param changeSet Changes to apply to the matched entry.
     * @param timeout   The timeout of the operation, in <b>milliseconds</b>. If the entry is locked
     *                  by another transaction wait for the specified number of milliseconds for it
     *                  to be released.
     * @param listener  A listener to be notified when a result arrives.
     * @return A future containing the details of the change operation affect which arrived
     * asynchronously.
     * @throws ChangeException Arrived asynchronously in event of a change error, via future or
     *                         listener.
     * @since 9.1
     */
    <T> Future<ChangeResult<T>> asyncChange(T template, ChangeSet changeSet, long timeout, AsyncFutureListener<ChangeResult<T>> listener);

    /**
     * Changes an existing objects in space in an asynchronous manner, returning immidiately with a
     * future. The future can be used to check the change operation results which provides details
     * of the operation affect. The change operation is designed for performance optimization, By
     * allowing to change an existing object unlike with regular updating write operation which
     * usually requires reading the object before applying to update to it. As part of the
     * optimization, when the operation is replicated, on a best effort it will try to replicate
     * only the required data which is needed to apply the changes on the entry in the replicated
     * target. <p/> <p>Modifiers can be used to specify behavior of the change operation, by default
     * uses the {@link ChangeModifiers#NONE} modifier.
     *
     * @param template  The template used for matching. Matching is done against the template with
     *                  <code>null</code> fields being. wildcards ("match anything") other fields
     *                  being values ("match exactly on the serialized form"). The template can also
     *                  be one of the different {@link com.gigaspaces.query.ISpaceQuery} classes
     * @param changeSet Changes to apply to the matched entry.
     * @param modifiers one or a union of {@link ChangeModifiers}
     * @return A future containing the details of the change operation affect which arrived
     * asynchronously.
     * @throws ChangeException Arrived asynchronously in event of a change error, via future or
     *                         listener.
     * @since 9.1
     */
    <T> Future<ChangeResult<T>> asyncChange(T template, ChangeSet changeSet, ChangeModifiers modifiers);

    /**
     * Changes existing objects in space in an asynchronous manner, returning immidiately with a
     * future. The future can be used to check the change operation results which provides details
     * of the operation affect. The change operation is designed for performance optimization, By
     * allowing to change an existing object unlike with regular updating write operation which
     * usually requires reading the object before applying to update to it. As part of the
     * optimization, when the operation is replicated, on a best effort it will try to replicate
     * only the required data which is needed to apply the changes on the entry in the replicated
     * target. <p/> <p>Modifiers can be used to specify behavior of the change operation, by default
     * uses the {@link ChangeModifiers#NONE} modifier.
     *
     * @param template  The template used for matching. Matching is done against the template with
     *                  <code>null</code> fields being. wildcards ("match anything") other fields
     *                  being values ("match exactly on the serialized form"). The template can also
     *                  be one of the different {@link com.gigaspaces.query.ISpaceQuery} classes
     * @param changeSet Changes to apply to the matched entry.
     * @param modifiers one or a union of {@link ChangeModifiers}
     * @param listener  A listener to be notified when a result arrives.
     * @return A future containing the details of the change operation affect which arrived
     * asynchronously.
     * @throws ChangeException Arrived asynchronously in event of a change error, via future or
     *                         listener.
     * @since 9.1
     */
    <T> Future<ChangeResult<T>> asyncChange(T template, ChangeSet changeSet, ChangeModifiers modifiers, AsyncFutureListener<ChangeResult<T>> listener);

    /**
     * Changes existing objects in space in an asynchronous manner, returning immidiately with a
     * future. The future can be used to check the change operation results which provides details
     * of the operation affect. The change operation is designed for performance optimization, By
     * allowing to change an existing object unlike with regular updating write operation which
     * usually requires reading the object before applying to update to it. As part of the
     * optimization, when the operation is replicated, on a best effort it will try to replicate
     * only the required data which is needed to apply the changes on the entry in the replicated
     * target. <p/> <p>Modifiers can be used to specify behavior of the change operation, by default
     * uses the {@link ChangeModifiers#NONE} modifier.
     *
     * @param template  The template used for matching. Matching is done against the template with
     *                  <code>null</code> fields being. wildcards ("match anything") other fields
     *                  being values ("match exactly on the serialized form"). The template can also
     *                  be one of the different {@link com.gigaspaces.query.ISpaceQuery} classes
     * @param changeSet Changes to apply to the matched entry.
     * @param modifiers one or a union of {@link ChangeModifiers}
     * @param timeout   The timeout of the operation, in <b>milliseconds</b>. If the entry is locked
     *                  by another transaction wait for the specified number of milliseconds for it
     *                  to be released.
     * @return A future containing the details of the change operation affect which arrived
     * asynchronously.
     * @throws ChangeException Arrived asynchronously in event of a change error, via future or
     *                         listener.
     * @since 9.1
     */
    <T> Future<ChangeResult<T>> asyncChange(T template, ChangeSet changeSet, ChangeModifiers modifiers, long timeout);

    /**
     * Changes existing objects in space in an asynchronous manner, returning immidiately with a
     * future. The future can be used to check the change operation results which provides details
     * of the operation affect. The change operation is designed for performance optimization, By
     * allowing to change an existing object unlike with regular updating write operation which
     * usually requires reading the object before applying to update to it. As part of the
     * optimization, when the operation is replicated, on a best effort it will try to replicate
     * only the required data which is needed to apply the changes on the entry in the replicated
     * target. <p/> <p>Modifiers can be used to specify behavior of the change operation, by default
     * uses the {@link ChangeModifiers#NONE} modifier.
     *
     * @param template  The template used for matching. Matching is done against the template with
     *                  <code>null</code> fields being. wildcards ("match anything") other fields
     *                  being values ("match exactly on the serialized form"). The template can also
     *                  be one of the different {@link com.gigaspaces.query.ISpaceQuery} classes
     * @param changeSet Changes to apply to the matched entry.
     * @param modifiers one or a union of {@link ChangeModifiers}
     * @param timeout   The timeout of the operation, in <b>milliseconds</b>. If the entry is locked
     *                  by another transaction wait for the specified number of milliseconds for it
     *                  to be released.
     * @param listener  A listener to be notified when a result arrives.
     * @return A future containing the details of the change operation affect which arrived
     * asynchronously.
     * @throws ChangeException Arrived asynchronously in event of a change error, via future or
     *                         listener.
     * @since 9.1
     */
    <T> Future<ChangeResult<T>> asyncChange(T template, ChangeSet changeSet, ChangeModifiers modifiers, long timeout, AsyncFutureListener<ChangeResult<T>> listener);

    /**
     * Creates a new DataEventSession with the default configuration.
     *
     * @return a new data event session.
     * @since 9.7.0
     */
    DataEventSession newDataEventSession();

    /**
     * Creates a new DataEventSession with the specified configuration.
     *
     * @param config The configuration settings for the new data event session.
     * @return a new data event session.
     * @since 9.7.0
     */
    DataEventSession newDataEventSession(EventSessionConfig config);

    void setQuiesceToken(QuiesceToken token);

    /**
     * Translates the template object into pre-processed packet which will be sent to the space. The
     * process of serializing an entry for transmission to the space will be identical if the same
     * entry is used twice. The prepareTemplate method gives a way to reduce the impact of repeated
     * use of the same entry therefore it is extremely useful when dealing with many template
     * matching operations (i.e read and take). This method will not access the space, all
     * processing is done in client side only.
     *
     * @param template to be translated into {@link com.gigaspaces.query.ISpaceQuery}
     * @return pre-processed {@link com.gigaspaces.query.ISpaceQuery} which represents the
     * translated template
     * @see #snapshot(Object)
     * @since 10.1.0
     */
    <T> ISpaceQuery<T> prepareTemplate(Object template);
}
