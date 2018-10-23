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


package com.j_spaces.core.client;

import com.gigaspaces.client.iterator.GSIteratorConfig;
import com.gigaspaces.client.iterator.IteratorScope;
import com.gigaspaces.client.iterator.internal.MultipleUidsPerPartitionList;
import com.gigaspaces.events.AbstractDataEventSession;
import com.gigaspaces.events.DataEventSessionFactory;
import com.gigaspaces.events.EventSessionConfig;
import com.gigaspaces.events.NotifyActionType;
import com.gigaspaces.events.NotifyInfo;
import com.gigaspaces.events.batching.BatchRemoteEvent;
import com.gigaspaces.events.batching.BatchRemoteEventListener;
import com.gigaspaces.internal.client.QueryResultTypeInternal;
import com.gigaspaces.internal.client.ReadTakeEntriesUidsResult;
import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.client.spaceproxy.metadata.ISpaceProxyTypeManager;
import com.gigaspaces.internal.client.spaceproxy.metadata.ObjectType;
import com.gigaspaces.internal.collections.CollectionsFactory;
import com.gigaspaces.internal.collections.ObjectShortMap;
import com.gigaspaces.internal.query.IPartitionResultMetadata;
import com.gigaspaces.internal.query.PartitionResultMetadata;
import com.gigaspaces.internal.transport.AbstractProjectionTemplate;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.internal.transport.ITransportPacket;
import com.gigaspaces.internal.transport.MutliProjectionByUids;
import com.gigaspaces.internal.utils.concurrent.GSThread;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.UidQueryPacket;
import com.j_spaces.core.client.sql.IQueryManager;
import com.j_spaces.jdbc.builder.SQLQueryTemplatePacket;

import net.jini.core.entry.Entry;
import net.jini.core.entry.UnusableEntryException;
import net.jini.core.event.EventRegistration;
import net.jini.core.event.RemoteEvent;
import net.jini.core.lease.Lease;
import net.jini.core.lease.LeaseDeniedException;
import net.jini.core.transaction.TransactionException;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Creates an iterator that can be used to exhaustively read through all of the visible matching
 * entities in the space and any additional entities that match. <p> The <code>templates</code>
 * parameter must be a {@link Collection} of entity instances ({@link Entry} or plain java objects)
 * to be used as templates, but can't coexist with an {@link ExternalEntry} as a template. All of
 * the entities iterated will match one or more of these templates. <code>templates</code> may
 * contain <code>null</code> values and may contain duplicates. An entity is said to be visible to
 * an invocation of this iterator if the entity could have been returned by a singleton read using a
 * <code>null</code> transaction. <p> The resulting iterator must initially contain all of the
 * visible matching entities in the space (note the <code>iteratorScope</code> parameter). During
 * the lifetime of the iterator an entity may be added to the iterator if it becomes visible. If the
 * iterator's <code>leaseDuration</code> expires, no more entries can be added. <p> Iteration is
 * done by polling results into a buffer. The buffer size can be configured using
 * <code>GSIteratorConfig</code>. The buffer serves as a static snapshot of the entities in space
 * and remains unchanged during the life of its iteration. Modifications only affect unbuffered
 * entities. Once the buffer has been iterated over, a new buffer is polled from the space.
 * Generally speaking, it is impossible to make any hard guarantees in the presence of concurrent
 * modifications. <p> The iterator's lease duration can be configured using
 * <code>GSIteratorConfig</code>. If the iteration is leased the lease duration must be positive. If
 * <code>leaseDuration</code> is {@link Lease#ANY}, the initial duration of the lease can be any
 * positive value desired by the implementation - in this case {@link Lease#FOREVER}. <p> If there
 * are remote method constraints associated with an invocation of this iterator, any remote
 * communications performed by or on behalf of the iterator's <code>next</code> methods will be
 * performed in compliance with these constraints, not with the constraints (if any) associated with
 * next. The {@linkplain #next(long) next with timeout} blocks until a next entity is available or
 * timeout expires. <p> <p> <b>Sample usage:</b> Here is an example of using an iterator for
 * iterating over matching entities:
 *
 * <pre><code>
 * GSIteratorConfig config = new GSIteratorConfig();
 * config.setBufferSize(100);
 * config.setIteratorScope(IteratorScope.CURRENT_AND_FUTURE);
 * Collection&lt;Entry&gt; templates = new LinkedList&lt;Entry&gt;();
 * templates.add(new MyEntry());
 * GSIterator iterator = new GSIterator(space, templates, config);
 *
 * long LATENCY = 30000;	//30 seconds
 *
 * //may return false due to communication constraints
 * while (iterator.hasNext())
 * {
 * 	Entry next = (Entry) iterator.next();
 * 	Thread.sleep(LATENCY);
 * }
 * </code></pre>
 *
 * <p> Here is an example of using a <b>blocking iterator</b> for iterating over matching entities:
 *
 * <pre><code>
 * Collection&lt;Entry&gt; templates = new LinkedList&lt;Entry&gt;();
 * templates.add(new MyEntry());
 *
 * GSIterator iterator = new GSIterator(space, templates, config);
 *
 * long NEXT_TIMEOUT = 30000; //30 seconds
 *
 * try
 * {
 * 	while (true)
 *    {
 * 		Entry next = (Entry) iterator.next(NEXT_TIMEOUT);
 *    }
 * }
 * catch (NoSuchElementException nsee)
 * {
 * 	System.out.println(&quot;iteration presumably complete.&quot;);
 * }
 * </code></pre>
 *
 * <p><b>Note that this implementation is not synchronized.</b> If multiple threads access this
 * iterator concurrently, it <i>must</i> be synchronized externally.
 *
 *
 * <p>The GSIterator also implements <code>Iterable</code> allowing to use the iterator within a
 * <code>foreach</code> element. Note, when using the iterator within a foreach element, at the end
 * of the foreach, the iterator will be <b>canceled</b>.
 *
 * @author moran
 * @version 2.0
 * @since 5.0
 * @deprecated since 14.0 - use {@link com.gigaspaces.client.iterator.SpaceIterator} instead.
 */
@Deprecated
@com.gigaspaces.api.InternalApi
public class GSIterator implements Iterator, Iterable {
    private static final Logger _logger = Logger.getLogger(Constants.LOGGER_GSITERATOR);

    private final ISpaceProxy _spaceProxy;
    private final GSIteratorConfig _config;

    // Lease management
    private final ILeasedIterator _leasedIterator;    // timer for lease expiration of this iterator

    private List<Lease> _notifyLeases;                // notification registrars
    private IEntryPacket _snapshot;                    // last entry returned by next call

    // Internal Structures Guarded By Lock
    private final Object _lock = new Object();        //lock guarding access to inner structures.
    private Set<String> _historyUids;
    private final MultipleUidsPerPartitionList _uidsPartitionedList;    // uids notified from space

    // Iteration structures
    private final List<IEntryPacket> _buffer;        // entry Buffer list of entries read from space
    private Iterator<IEntryPacket> _bufferIterator;    // iterator over the entryBuffer set
    private final QueryResultTypeInternal _queryResultType;
    private final boolean _returnEntryPacket;

    //projection related vars
    private List<ITemplatePacket> _templatesWithProjection; //list of templates
    private AbstractProjectionTemplate[] _projectionTemplates; //list of projection templates, will be sent to server
    private ConcurrentMap<String, Short> _uidsToProjection;  //for each uid that has projection- the pos of its template
    private boolean _oneOverallProjection;  //one projection for all uids, no map needed

    private static final NotifyActionType NOTIFY_MASK =
            NotifyActionType.NOTIFY_WRITE.or(
                    NotifyActionType.NOTIFY_TAKE).or(
                    NotifyActionType.NOTIFY_LEASE_EXPIRATION);

    /**
     * Equivalent to calling {@link #GSIterator(IJSpace, Collection, com.gigaspaces.client.iterator.GSIteratorConfig)}.
     * <p> <b>i.e.</b> <code>new GSIterator(spaceProxy, templates, new GSIteratorConfig());</code>
     *
     * @param spaceProxy a Space instance to iterate over.
     * @param templates  a Collection of entity instances, each representing a template. All the
     *                   entities added to the resulting match set will match one or more elements
     *                   of templates.
     * @throws RemoteException          if a communication error occurs.
     * @throws UnusableEntryException   if history contained an unusable entity.
     * @throws IllegalArgumentException if any non-null element of <code>templates</code> is a mix
     *                                  of {@link ExternalEntry} and {@link Entry}/POJO instance, if
     *                                  <code>templates</code> is empty or <code>null</code>, if
     *                                  <code>bufferSize</code> is non-positive, or if
     *                                  <code>leaseDuration</code> is neither positive nor {@link
     *                                  Lease#ANY}.
     * @see #GSIterator(IJSpace, Collection, GSIteratorConfig)
     */
    public GSIterator(IJSpace spaceProxy, Collection<?> templates)
            throws RemoteException, UnusableEntryException {
        this(spaceProxy, templates, new GSIteratorConfig());
    }

    /**
     * Equivalent to calling {@link #GSIterator(IJSpace, Collection, GSIteratorConfig)}, translating
     * withHistory to CURRENT_AND_FUTURE or FUTURE.
     *
     * @param spaceProxy    a Space instance to iterate over.
     * @param templates     a Collection of entity instances, each representing a template. All the
     *                      entities added to the resulting match set will match one or more
     *                      elements of templates.
     * @param bufferSize    the maximum number of entities to pool each time from the space.
     * @param withHistory   set <code>true</code> to initially contain all of the visible matching
     *                      entities in the space. Otherwise, will contain only visible matching
     *                      entities thereafter.
     * @param leaseDuration the requested initial lease time on the resulting match set.
     * @throws RemoteException          if a communication error occurs.
     * @throws UnusableEntryException   if history contained an unusable entity.
     * @throws IllegalArgumentException if any non-null element of <code>templates</code> is a mix
     *                                  of {@link ExternalEntry} and {@link Entry}/POJO instance, if
     *                                  <code>templates</code> is empty or <code>null</code>, if
     *                                  <code>bufferSize</code> is non-positive, or if
     *                                  <code>leaseDuration</code> is neither positive nor {@link
     *                                  Lease#ANY}.
     * @deprecated Use {@link #GSIterator(IJSpace, Collection, GSIteratorConfig)} instead.
     */
    @Deprecated
    public GSIterator(IJSpace spaceProxy, Collection<?> templates, int bufferSize, boolean withHistory, long leaseDuration)
            throws RemoteException, UnusableEntryException {
        this(spaceProxy, templates, new GSIteratorConfig()
                .setBufferSize(bufferSize)
                .setLeaseDuration(leaseDuration)
                .setIteratorScope(withHistory ? IteratorScope.CURRENT_AND_FUTURE : IteratorScope.FUTURE));
    }

    /**
     * Constructs an iterator over a space using a collection of entity instances to be
     * incrementally returned. Some operations on a space must return more entities than can be
     * conveniently returned by a single call, generally because returning all the entries in one
     * result would consume too many resources in the client or introduce too much latency before
     * the first entity could be processed. In these cases, iterators are used to incrementally
     * return the necessary entries. A <code>GSIterator</code> instance is used to access a set of
     * matches returned by <code>next</code>. <p> The iterator will initially contain some
     * population of entities. These entities can be retrieved by calling {@link #next next}. A
     * successful call to <code>next</code> will remove the returned entity from the iteration. An
     * iterator can end up in one of two terminal states, <em>invalidated</em> or
     * <em>exhausted</em>. <p> A leased iterator which expires is considered as
     * <em>invalidated</em>. A canceled iterator is an exhausted iterator and will have no more
     * entities added to it. Calling <code>next</code> on an iterator with either state always
     * returns <code>null</code> or it may throw one of the allowed exceptions. In particular
     * <code>next(timeout)</code> may throw {@link java.rmi.NoSuchObjectException} to indicate that
     * no entity has been found during the allowed timeout. There is no guarantee that once
     * <code>next(timeout)</code> throws a <code>NoSuchObjectException</code>, or <code>next</code>
     * returns <code>null</code>, all future calls on that instance will do the same. <p> Between
     * the time an iterator is created and the time it reaches a terminal state, entities may be
     * added by the space. However, an entity that is removed by a <code>next</code> call may be
     * added back to the iterator if its uniqueness is equivalent. The space may also update or
     * remove entities that haven't yet been returned by a <code>next</code> call, and are not part
     * of the buffered set. <p> If there is a possibility that an iterator may become invalidated,
     * it must be leased. If there is no possibility that the iterator will become invalidated,
     * implementations should not lease it (i.e. use {@link Lease#FOREVER}). If there is no further
     * interest an iterator may be <code>canceled</code>. <p> An active lease on an iterator serves
     * as a hint to the space that the client is still interested in matching entities, and as a
     * hint to the client that the iterator is still functioning. There are cases, however, where
     * this may not be possible in particular, it is not expected that iteration will maintain
     * across crashes. If the lease expires or is canceled, the iterator is invalidated. Clients
     * should <em>not</em> assume that the resources associated with a leased match set will be
     * freed if the match set reaches the exhausted state, and should instead cancel the lease. <p>
     *
     * @param spaceProxy a Space instance to iterate over.
     * @param templates  a Collection of entity instances, each representing a template. All the
     *                   entities added to the resulting match set will match one or more elements
     *                   of templates.
     * @param config     a set of configurations to determine the iterator's behaviour.
     * @throws RemoteException          if a communication error occurs.
     * @throws UnusableEntryException   if history contained an unusable entity.
     * @throws IllegalArgumentException if any non-null element of <code>templates</code> is a mix
     *                                  of {@link ExternalEntry} and {@link Entry}/POJO instance, if
     *                                  <code>templates</code> is empty or <code>null</code>, if
     *                                  <code>bufferSize</code> is non-positive, or if
     *                                  <code>leaseDuration</code> is neither positive nor {@link
     *                                  Lease#ANY}.
     */
    public GSIterator(IJSpace spaceProxy, Collection<?> templates, GSIteratorConfig config)
            throws RemoteException, UnusableEntryException {
        validateArgs(spaceProxy, templates, config);

        if (_logger.isLoggable(Level.FINE))
            _logger.log(Level.FINE, "GSIterator initialized with scope=" + config.getIteratorScope() +
                    ", bufferSize=" + config.getBufferSize() +
                    ", leaseDuration=" + config.getLeaseDuration());


        this._spaceProxy = (ISpaceProxy) spaceProxy;
        this._config = config;
        this._uidsPartitionedList = new MultipleUidsPerPartitionList();
        this._buffer = new LinkedList<IEntryPacket>();

        /*
        final int initialSize = (int)(config.getBufferSize() / (float)0.75) + 1;
        final int initialCapacity = Math.max(initialSize, 100);
         */

        final Object firstTemplate = templates.iterator().next();
        this._returnEntryPacket = firstTemplate instanceof ITransportPacket;

        _templatesWithProjection = null; //list of templates
        _projectionTemplates = null; //list of projection templates, will be sent to server
        _uidsToProjection = null;  //for each uid that has projection- the pos of its template

        final ITemplatePacket[] templatesPackets = extractTemplatePacketsFromCollection(templates);
        this._queryResultType = templatesPackets[0].getQueryResultType();

        this._leasedIterator = _config.getIteratorScope().hasFuture()
                ? new TimedLeasedIterator(_config.getLeaseDuration())
                : new InfiniteLeasedIterator();

        try {
            synchronized (_lock) {
                if (_config.getIteratorScope().hasFuture())
                    registerForNotifications(templatesPackets);
                extractHistoryIfNeeded(templatesPackets, _config.getIteratorScope());
            }
        } catch (TransactionException e) {
            // can never happen - we don't use transactions
        }
    }

    private void validateArgs(IJSpace space, Collection<?> collectionTemplates, GSIteratorConfig config) {
        if (space == null)
            throw new IllegalArgumentException("space argument must not be null.");

        if (collectionTemplates == null || collectionTemplates.isEmpty())
            throw new IllegalArgumentException("templates argument must contain at least one element.");

        if (config == null)
            throw new IllegalArgumentException("config argument must not be null.");

        if (config.getBufferSize() <= 0)
            throw new IllegalArgumentException("bufferSize argument must be greater than zero.");

        validateDuration(config.getLeaseDuration());
    }

    private ITemplatePacket[] extractTemplatePacketsFromCollection(Collection<?> templates)
            throws NullPointerException, IllegalArgumentException {
        ITemplatePacket[] templatesPackets = new ITemplatePacket[templates.size()];

        ISpaceProxyTypeManager typeManager = _spaceProxy.getDirectProxy().getTypeManager();
        IQueryManager queryManager = _spaceProxy.getDirectProxy().getQueryManager();

        int index = 0;
        for (Object template : templates) {
            ObjectType objectType = ObjectType.fromObject(template);
            ITemplatePacket templatePacket = typeManager.getTemplatePacketFromObject(template, objectType);

            if (templatePacket instanceof SQLQueryTemplatePacket)
                templatePacket = queryManager.getSQLTemplate((SQLQueryTemplatePacket) templatePacket, null);

            templatesPackets[index++] = templatePacket;
            if (templatePacket.getProjectionTemplate() != null) {
                if (_templatesWithProjection == null)
                    _templatesWithProjection = new ArrayList<ITemplatePacket>(templates.size());
                _templatesWithProjection.add(templatePacket);
            }
        }
        if (_templatesWithProjection != null) {
            if (templates.size() == 1)
                _oneOverallProjection = true;
            else {//need mapping
                _uidsToProjection = new ConcurrentHashMap<String, Short>();
                _projectionTemplates = new AbstractProjectionTemplate[_templatesWithProjection.size()];
                int pos = 0;
                for (ITemplatePacket p : _templatesWithProjection)
                    _projectionTemplates[pos++] = p.getProjectionTemplate();
            }

        }


        return templatesPackets;
    }

    private void registerForNotifications(ITemplatePacket[] templates)
            throws RemoteException, TransactionException {
        _notifyLeases = new ArrayList<Lease>(templates.length);
        boolean isFifoPerTemplate = false;

        // Get event session configuration from iterator configuration:
        EventSessionConfig sessionConfig = _config.getEventSessionConfig();
        // If not configured:
        if (sessionConfig == null) {
            // Create a default configuration:
            sessionConfig = new EventSessionConfig();
            if (_spaceProxy.isFifo() || ReadModifiers.isFifo(_spaceProxy.getReadModifiers()))
                sessionConfig.setFifo(true);
            else {
                // Turn on fifo per template:
                // (this is probably pointless, but we want to preserve the old behavior)
                isFifoPerTemplate = true;
            }
        }

        // Initialize a data event session:
        AbstractDataEventSession session = (AbstractDataEventSession) DataEventSessionFactory.create
                (_spaceProxy, sessionConfig);
        // Initialize a fifo data event session if needed:
        AbstractDataEventSession fifoSession = null;
        if (isFifoPerTemplate) {
            EventSessionConfig fifoSessionConfig = new EventSessionConfig();
            fifoSessionConfig.setFifo(true);
            fifoSession = (AbstractDataEventSession) DataEventSessionFactory.create(_spaceProxy, fifoSessionConfig);
        }
        // Determine listener lease duration:
        long leaseDuration = sessionConfig.isAutoRenew()
                ? sessionConfig.getRenewDuration()
                : Lease.FOREVER;
        // Initialize event listener:
        NotificationListener defaultListener = new NotificationListener(-1);
        // Subscribe a listener for each template:
        int projectionNum = 0;
        for (ITemplatePacket template : templates) {
            // Select data event session for this template:
            AbstractDataEventSession currSession = isFifoPerTemplate && template.getTypeDescriptor() != null && template.getTypeDescriptor().isFifoDefault()
                    ? fifoSession : session;
            // Add a notification listener for this template:
            NotificationListener curListener = (template.getProjectionTemplate() != null && !_oneOverallProjection) ? new NotificationListener(projectionNum++) : defaultListener;
            NotifyInfo notifyInfo = currSession.createNotifyInfo(curListener, NOTIFY_MASK)
                    .setReturnOnlyUids(true);

            //notifyInfo.setTemplateUID(UUID.randomUUID().toString());
            //template.setReturnOnlyUIDs(true);
            EventRegistration event = currSession.addListener(template, leaseDuration, notifyInfo);
            // Keep track of event registration leases for later use:
            _notifyLeases.add(event.getLease());
        }
    }

    private void extractHistoryIfNeeded(ITemplatePacket[] templates, IteratorScope iteratorScope)
            throws RemoteException, TransactionException, UnusableEntryException {
        if (!iteratorScope.hasCurrent())
            return;

        if (iteratorScope.hasFuture())
            this._historyUids = new HashSet<String>();

        for (final ITemplatePacket template : templates) {
            final ReadTakeEntriesUidsResult uidsResult = _spaceProxy.readEntriesUids(template, null, Integer.MAX_VALUE, _config.getReadModifiers());
            final String[] uids = uidsResult.getUids();

            int uidIndex = 0;
            for (IPartitionResultMetadata partitionData : uidsResult.getPartitionsMetadata()) {
                //mark the beginning of new partition data
                _uidsPartitionedList.startPartition(partitionData);
                int projectionpos = (_templatesWithProjection != null && !_oneOverallProjection) ?
                        indexOfByRef(_templatesWithProjection, template) : -1;
                for (int i = 0; i < partitionData.getNumOfResults(); i++, uidIndex++) {
                    if (_historyUids != null)
                        _historyUids.add(uids[uidIndex]);
                    _uidsPartitionedList.addIfNew(uids[uidIndex]);
                    if (projectionpos != -1)
                        _uidsToProjection.putIfAbsent(uids[uidIndex], (short) projectionpos);
                }
            }
        }
        // add a dummy partition for the new object that arrive through notifications
        _uidsPartitionedList.startPartition(new PartitionResultMetadata(null, 0));

        if (_logger.isLoggable(Level.FINE))
            _logger.log(Level.FINE, "extractHistoryIfNeeded found " + _uidsPartitionedList.size() + " matching uids.");
    }

    private int indexOfByRef(List l, Object o) {
        int size = l.size();
        for (int i = 0; i < size; i++) {
            if (l.get(i) == o)
                return i;
        }
        return -1;
    }

    /**
     * Returns a snapshot, as defined in section JS.2.6 of the JavaSpaces specification, of the
     * entry returned by the last next call. If the last next call returned null or failed with an
     * exception or error, snapshot will throw an IllegalStatException
     *
     * @return Entry snapshot of last next call
     * @throws IllegalStateException when last next call failed
     */
    public EntrySnapshot snapshot() throws IllegalStateException {
        if (_snapshot == null)
            throw new IllegalStateException("Last next call either returned null or failed.");

        try {
            return (EntrySnapshot) _spaceProxy.snapshot(_snapshot.toObject(_queryResultType));
        } catch (RemoteException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Returns <code>true</code> if the iterator has more elements to iterate over. If the iterator
     * was constructed to initially contain matching entities, then <code>hasNext()</code> will
     * return <code>true</code> until the entire initial set has been iterated over. Still,
     * <code>hasNext()</code> may return <code>false</code> iterating over any additional matching
     * entities due to communication constraints - thus, it does not guarantee that the iterator is
     * in one of its terminal states. It is advised to use {@link Thread#sleep(long)} between
     * subsequent calls, to allow the iterator to receive possible additional matches.
     *
     * @return true if the iterator has more matches; false if currently there are no more available
     * matches to return from a call to {@link #next()}.
     */
    public boolean hasNext() {
        boolean result;

        if (_leasedIterator.isTerminated())
            result = false;
        else {
            // If null, we either reached end of iterator or this is the first time.
            if (_bufferIterator == null)
                _bufferIterator = getNextBatch();

            // If still null, there's no pending entries:
            if (_bufferIterator == null)
                result = false;
            else {
                // otherwise, we use the iterator's hasNext method.
                if (_bufferIterator.hasNext())
                    result = true;
                else {
                    // Reset and call recursively:
                    _bufferIterator = null;
                    result = hasNext();
                }
            }
        }

        if (_logger.isLoggable(Level.FINER))
            _logger.log(Level.FINER, "hasNext() returned " + result);

        return result;
    }

    /**
     * Removes number of entities from the iterated buffered set and returns a copy to the caller.
     * <p> A given invocation of this method may perform remote communications, retrieving the next
     * buffer set, but generally the <code>nextBatch</code> method is not expected to have remote
     * method constraints that can vary from invocation to invocation. <p>
     *
     * @param limit a limit on the number of entities to be returned. Use {@link Integer#MAX_VALUE}
     *              for the uppermost limit.
     * @return an entity from the iterated buffered set, or <code>null</code> if the iterated set is
     * empty or presumably complete.
     */
    public Object[] nextBatch(int limit) throws NoSuchElementException {
        ArrayList<Object> list = new ArrayList<Object>(limit);
        for (int i = 0; i < limit; ++i) {
            Object next = next();
            if (next == null)
                break;

            list.add(next);
        }
        return list.toArray();
    }

    /**
     * Removes one entity from the iterated buffered set and returns a copy to the caller. Returns
     * <code>null</code> if a call to <code>hasNext</code> would have returned <code>false</code>.
     * <p> A given invocation of this method may perform remote communications, retrieving the next
     * buffer set, but generally the <code>next</code> method is not expected to have remote method
     * constraints that can vary from invocation to invocation. <p>
     *
     * @return an entity from the iterated buffered set, or <code>null</code> if the iterated set is
     * empty or presumably complete.
     */
    public Object next() {
        Object result = null;

        _snapshot = null;

        if (hasNext()) {
            _snapshot = _bufferIterator.next();
            result = _returnEntryPacket ? _snapshot : _snapshot.toObject(_queryResultType);
        } else
            _snapshot = null;

        if (_logger.isLoggable(Level.FINER))
            _logger.log(Level.FINER, "next() returned " + (_snapshot == null ? "null" : "object with uid=" + _snapshot.getUID()));

        return result;
    }

    /**
     * Same as {@link #next()} but with blocking behavior, blocking until a matched entity is
     * available or until timeout expires.
     *
     * @param timeout time to wait until a next element is available.
     * @return an entity from the iterated buffered set.
     * @throws NoSuchElementException when timeout expires and no available match was found.
     */
    public Object next(long timeout) throws NoSuchElementException {
        Object nextEntry = next();
        if (nextEntry != null)
            return nextEntry;

        //else try blocking next...
        synchronized (_lock) {
            if (!_leasedIterator.isTerminated()) {
                try {
                    _lock.wait(timeout);
                } catch (InterruptedException e) {
                    //fall through
                }
            }
        }//synchronized(lock)

        // either we found before timeout or we try at end of timeout
        nextEntry = next();
        if (nextEntry == null)
            throw new NoSuchElementException();

        //return result
        return nextEntry;
    }

    private Iterator<IEntryPacket> getNextBatch() {
        Iterator<IEntryPacket> result = null;
        _buffer.clear();

        UidQueryPacket template;
        synchronized (_lock) {
            template = _uidsPartitionedList.buildQueryPacket(_config.getBufferSize(), _queryResultType);
            if (template != null && _templatesWithProjection != null) {
                if (_oneOverallProjection) {
                    template.setProjectionTemplate(_templatesWithProjection.get(0).getProjectionTemplate());
                } else {//build the uids
                    ObjectShortMap<String> puids = null;
                    String[] uids = template.getMultipleUIDs();

                    if (uids != null && uids.length > 0) {
                        for (String uid : uids) {
                            Short pos = _uidsToProjection.remove(uid);
                            if (pos != null) {
                                if (puids == null)
                                    puids = CollectionsFactory.getInstance().createObjectShortMap();
                                puids.put(uid, pos);
                            }
                        }
                    } else {//single UID in read
                        if (template.getUID() != null) {
                            Short pos = _uidsToProjection.remove(template.getUID());
                            if (pos != null) {
                                if (puids == null)
                                    puids = CollectionsFactory.getInstance().createObjectShortMap();
                                puids.put(template.getUID(), pos);
                            }
                        }
                    }
                    if (puids != null) {//we have projection mappings
                        MutliProjectionByUids mp = new MutliProjectionByUids(_projectionTemplates, puids);
                        template.setProjectionTemplate(mp);
                    }
                }

            }

        }

        if (template != null) {
            try {
                Object[] entries = _spaceProxy.readMultiple(template, null, template.getMultipleUIDs().length, _config.getReadModifiers());

                _buffer.clear();
                for (Object entry : entries)
                    _buffer.add((IEntryPacket) entry);

                result = _buffer.iterator();
            } catch (RemoteException e) {
                if (_logger.isLoggable(Level.SEVERE))
                    _logger.log(Level.SEVERE, "Failed to build iterator data", e);
            } catch (UnusableEntryException e) {
                if (_logger.isLoggable(Level.SEVERE))
                    _logger.log(Level.SEVERE, "Failed to build iterator data", e);
            } catch (TransactionException e) {
                if (_logger.isLoggable(Level.SEVERE))
                    _logger.log(Level.SEVERE, "Failed to build iterator data", e);
            }
        }

        if (_logger.isLoggable(Level.FINE))
            _logger.log(Level.FINE, "getNextBatch returns with a buffer of " + _buffer.size() + " entries.");
        return result;
    }

    /**
     * Returns the absolute time that the lease will expire, represented as milliseconds from the
     * beginning of the epoch, relative to the local clock.
     *
     * @return a long value in milliseconds from the beginning of the epoch relative to the local
     * clock that represents the absolute time that the lease will expire.
     */
    public long getExpiration() {
        return _leasedIterator.getExpiration();
    }

    /**
     * Used to renew the iterator's lease for an additional period of time, specified in
     * milliseconds. This duration is not added to the original lease, but is used to determine a
     * new expiration time for the existing lease. If the renewal is granted this is reflected in
     * value returned by getExpiration. If the renewal fails, the lease is left intact for the same
     * duration that was in force prior to the call to renew. <p> An expired iterator is an
     * <em>invalidated</em> iterator and will have no more entities added to it. Subsequent calls to
     * <code>next</code> on this iterator will always return <code>null</code>.
     *
     * @param duration the requested duration in milliseconds
     * @throws IllegalArgumentException if invalid lease expiration time
     * @throws LeaseDeniedException     if the iterator's lease has already expired
     */
    public void renew(long duration) throws IllegalArgumentException, LeaseDeniedException {
        if (_leasedIterator.isTerminated())
            throw new LeaseDeniedException("Lease has already expired or been cancelled.");

        _leasedIterator.renew(duration);
    }

    /**
     * Used by the iterator holder to indicate that there is no further interest in this iterator.
     * The overall effect of a cancel call is the same as lease expiration, but instead of happening
     * at the end of a pre-agreed duration it happens immediately. <p> A cancelled iterator is an
     * <em>exhausted</em> iterator and will have no more entities added to it. Subsequent calls to
     * <code>next</code> on this iterator will always return <code>null</code>.
     */
    public void cancel() {
        synchronized (_lock) {
            boolean alreadyCancelled = _leasedIterator.cancel();

            // wake up blocking next (even if canceled)
            _lock.notifyAll();

            // if already canceled then there is no more to handle
            if (alreadyCancelled)
                return;

            _uidsPartitionedList.clear();
            if (_historyUids != null)
                _historyUids.clear();
        }

        if (_notifyLeases != null) {
            for (Lease lease : _notifyLeases) {
                try {
                    lease.cancel();
                } catch (Exception e) { /*ignore exception */}
            }
        }

        if (_bufferIterator != null) {
            while (_bufferIterator.hasNext()) {
                _bufferIterator.next();
                _bufferIterator.remove();
            }
        }

        _buffer.clear();
    }

    /**
     * Not supported operation. Here because of <code>Iterator</code> implementation.
     */
    public void remove() {
        throw new UnsupportedOperationException("remove is not supported on GSIterator");
    }

    public Iterator<?> iterator() {
        return new GSIteratorIterable();
    }

    private long validateDuration(long duration) throws IllegalArgumentException {
        if (duration == Lease.ANY)
            return Lease.FOREVER;

        if (duration <= 0)
            throw new IllegalArgumentException("duration argument must be greater than zero.");

        return duration; //untouched
    }

    private final class NotificationListener implements BatchRemoteEventListener {
        private final int _projectionTemplateNumber;  //-1 means no such for this listener

        public NotificationListener(int projectionTemplateNumber) {
            _projectionTemplateNumber = projectionTemplateNumber;
        }

        public void notify(RemoteEvent event) {
            synchronized (_lock) {
                if (_leasedIterator.isTerminated())
                    return;

                processEvent((EntryArrivedRemoteEvent) event);
            }
        }

        public void notifyBatch(BatchRemoteEvent batchEvent) {
            synchronized (_lock) {
                if (_leasedIterator.isTerminated())
                    return;

                for (RemoteEvent event : batchEvent.getEvents())
                    processEvent((EntryArrivedRemoteEvent) event);
            }
        }

        private void processEvent(final EntryArrivedRemoteEvent spaceEvent) {
            final NotifyActionType notifyType = spaceEvent.getNotifyActionType();
            final String uid = spaceEvent.getEntryPacket().getUID();

            if (notifyType.isWrite()) {
                boolean isDuplicate = _historyUids != null && _historyUids.remove(uid);
                if (!isDuplicate) {
                    if (_uidsToProjection != null && _projectionTemplateNumber != -1)
                        _uidsToProjection.putIfAbsent(uid, (short) _projectionTemplateNumber);

                    final boolean isNew = _uidsPartitionedList.addIfNew(uid);
                    if (isNew)
                        _lock.notifyAll(); //wake up blocking next

                }
            } else if (notifyType.isTake() || notifyType.isLeaseExpiration()) {
                _uidsPartitionedList.remove(uid);
                if (_historyUids != null)
                    _historyUids.remove(uid);
                if (_uidsToProjection != null && _projectionTemplateNumber != -1)
                    _uidsToProjection.remove(uid);
            }
        }
    }

    private interface ILeasedIterator {
        boolean isTerminated();

        long getExpiration();

        boolean cancel();

        void renew(long newDuration);
    }

    private static final class InfiniteLeasedIterator implements ILeasedIterator {
        private boolean _cancelled;

        public synchronized boolean cancel() {
            if (_cancelled)
                return false;

            _cancelled = true;
            return true;
        }

        public long getExpiration() {
            return Lease.FOREVER;
        }

        public synchronized boolean isTerminated() {
            return _cancelled;
        }

        public void renew(long newDuration) {
            // Renew is not supported on a StubLeasedIterator.
        }
    }

    /**
     * Leased iterator daemon thread, responsible for renewing and/or canceling of this iterator
     * according to its lease.
     */
    private final class TimedLeasedIterator extends GSThread implements ILeasedIterator {
        private long _duration;
        private long _expirationTime;

        private boolean _cancelled;
        private boolean _renewed;
        private boolean _expired;

        /**
         * @param duration duration (from now) until this leased iteration expired.
         */
        public TimedLeasedIterator(long duration) {
            super("<" + _spaceProxy.getName() + ">-Leased-Iterator");
            this.setDaemon(true);

            this._duration = validateDuration(duration);

            this.start();
        }

        @Override
        public void run() {
            try {
                while (stillLeased() && !isInterrupted()) ;
            } catch (InterruptedException ie) {
                if (_logger.isLoggable(Level.SEVERE))
                    _logger.log(Level.SEVERE, this.getName() + " interrupted.\n", ie);

                //Restore the interrupted status
                interrupt();
                //fall through
            }

            // cancel when lease expires (if canceled will return)
            GSIterator.this.cancel();
        }

        /**
         * waits until this lease expires.
         *
         * @return true if this lease is still leased; false if expired or canceled.
         */
        private synchronized boolean stillLeased() throws InterruptedException {
            if (_cancelled)
                return false;

            _expirationTime = SystemTime.timeMillis() + _duration;
            wait(_duration);

            if (_renewed) {
                _renewed = false;
                return true;
            }

            _expired = true;
            return false;
        }

        /**
         * cancel this leased iteration
         *
         * @return true if already canceled; false if this is first cancellation.
         */
        public synchronized boolean cancel() {
            if (_cancelled)
                return true; //already canceled

            _cancelled = true;
            notify(); //wake up if asleep

            return false; //first cancellation
        }

        /**
         * renew this leased iteration with a new duration.
         *
         * @param newDuration duration until leased iteration expires.
         * @throws IllegalArgumentException if new duration is invalid.
         */
        public synchronized void renew(long newDuration) {
            _duration = validateDuration(newDuration);

            _renewed = true;
            notify(); //wake up if asleep
        }

        /**
         * @return true if leased iteration has expired or been canceled.
         */
        public synchronized boolean isTerminated() {
            return _expired || _cancelled;
        }

        /**
         * @return the absolute expiration time of this leased iteration.
         */
        public synchronized long getExpiration() {
            return _expirationTime;
        }
    }

    private final class GSIteratorIterable implements Iterator<Object> {
        public boolean hasNext() {
            if (GSIterator.this.hasNext())
                return true;

            cancel();
            return false;
        }

        public Object next() {
            return GSIterator.this.next();
        }

        public void remove() {
            throw new UnsupportedOperationException("remove is not supported with GSIterator");
        }
    }
}
