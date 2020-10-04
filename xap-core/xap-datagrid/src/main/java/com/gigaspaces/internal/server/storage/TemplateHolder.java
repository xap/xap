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

package com.gigaspaces.internal.server.storage;

import com.gigaspaces.client.mutators.SpaceEntryMutator;
import com.gigaspaces.internal.client.QueryResultTypeInternal;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.query.EntryHolderAggregatorContext;
import com.gigaspaces.internal.query.ICustomQuery;
import com.gigaspaces.internal.query.RegexCache;
import com.gigaspaces.internal.query.explainplan.SingleExplainPlan;
import com.gigaspaces.internal.query.explainplan.ExplainPlanUtil;
import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.space.BatchQueryOperationContext;
import com.gigaspaces.internal.server.space.FifoSearch;
import com.gigaspaces.internal.server.space.MatchResult;
import com.gigaspaces.internal.server.space.MultipleIdsContext;
import com.gigaspaces.internal.server.space.iterator.ServerIteratorInfo;
import com.gigaspaces.internal.transport.AbstractProjectionTemplate;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.lrmi.nio.IResponseContext;
import com.j_spaces.core.AnswerHolder;
import com.j_spaces.core.OperationID;
import com.j_spaces.core.PendingFifoSearch;
import com.j_spaces.core.SpaceContext;
import com.j_spaces.core.SpaceOperations;
import com.j_spaces.core.UpdateOrWriteContext;
import com.j_spaces.core.XtnEntry;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.cache.TerminatingFifoXtnsInfo;
import com.j_spaces.core.cache.TypeData;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.client.Modifiers;
import com.j_spaces.core.client.ReadModifiers;
import com.j_spaces.core.client.SQLQuery;
import com.j_spaces.core.client.TakeModifiers;
import com.j_spaces.core.filters.FilterManager;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import com.j_spaces.kernel.locks.ILockObject;

import net.jini.core.transaction.server.ServerTransaction;

import java.util.*;
import org.slf4j.Logger;

/**
 * This class represents a template in a J-Space. Each instance of this class contains a reference
 * to the template value plus any other necessary info about the template. <p> This class extends
 * entryHolder object.
 */
@com.gigaspaces.api.InternalApi
public class TemplateHolder extends AbstractSpaceItem implements ITemplateHolder {
    private final int _templateOperation; // of type SpaceOperations
    private boolean _pendingRemoteException;   //for notify template- other threads will not try to xmit

    //does entry has WF array ?  
    private volatile boolean _hasWaitingFor;
    private Collection<IEntryHolder> _waitingFor;

    private final AbstractProjectionTemplate _projectionTemplate;
    // if this field is not null and the template is a
    // null template perform the designated operation using the uid as an entry uid
    private String _uidToOperateBy;
    //for read/take multiple, to select by
    private final String[] _multipleUids;
    //for read-take to return only UID(s)
    private final boolean _returnOnlyUid;
    //true if template defines a fifo operation
    private final boolean _fifoTemplate;
    // for update operations only
    private IEntryHolder _updatedEntry;
    private boolean _isReRegisterLeaseOnUpdate;

    //answer holder (instead of answer table)
    private AnswerHolder _answerHolder;
    /**
     * indication whether this template is inserted to the ~cache
     */
    private volatile boolean _inCache;
    // The following field contains operation modifiers. relevant for write, update & read-take.
    private final int _operationModifiers;
    //if true the initial ifExist scan is progressing so template
    //should not be  given-up (deleted)
    private boolean _initialIfExistSearchActive;
    // object used to accumulate fifo events while template is in active search
    private PendingFifoSearch _pendingFifoSearch;
    //if true the initial fifo search is progressing
    private boolean _initialFifoSearchActive;
    //the fifo serial xtn number when search started
    private long _fifoXtnNumberOnSearchStart = TerminatingFifoXtnsInfo.UNKNOWN_FIFO_XTN;
    //the fifo thread partition- several fifo threads are handling incoming events
    private int _fifoThreadPartition;

    private final IResponseContext _respContext;
    private boolean _secondPhase = false;
    private boolean _inExpirationManager = false;
    private final QueryResultTypeInternal _queryResultType;
    private String _externalEntryImplClassName = null;
    private final OperationID _operationID;
    // Transaction that created the template
    private XtnEntry _templateXtnOriginated;
    //when true- search for entries only in memory and not in DB/EDS
    private boolean _memoryOnlySearch;
    //when true-its NBR
    private boolean _nonBlockingRead;

    private Object _id;
    private final TemplateEntryData _templateData;
    private transient int _previousVersion;
    //for in-place update
    private Collection<SpaceEntryMutator> _mutators;
    private long _inPlaceUpdateExpiration;
    private boolean _ifExistForInPlaceUpdate;   //ifExist semantics

    //extended info regarding rejected operation. currently used only by singe op' change by ID
    private Throwable _cause;
    private IEntryData _rejectedEntry;

    //the following fields are used in after-operation filter
    private int _afterOpFilterCode = -1;
    private IEntryPacket _updateOperationEntry;  //for update-the entry to update with
    private FilterManager _filterManager;
    private SpaceContext _spaceContext;

    //the following fields handle multiple-operation templates (read/take/change multiple)
    private BatchQueryOperationContext _batchOerationContext;
    //the following fields handle byIds template (update/updateOrWrite Multiple)
    private MultipleIdsContext _multipleIdsContexct;
    private int _ordinalForMultipleIdsOperation;  //ordinal within the MultipleIdsContext
    private UpdateOrWriteContext _updateOrWriteContext;
    private EntryHolderAggregatorContext aggregatorContext;

    //the following is used by blob store
    private transient Boolean _optimizedForBlobStoreOp;
    //all the query values are indexes- used in blob store (count) optimizations
    private final boolean _allValuesIndexSqlQuery;
    private SingleExplainPlan _singleExplainPlan = null;
    private ServerIteratorInfo _serverIteratorInfo;


    public TemplateHolder(IServerTypeDesc typeDesc, ITemplatePacket packet, String uid,
                          long expirationTime, XtnEntry xidOriginated, long scn, int templateOperation,
                          IResponseContext respContext, boolean returnOnlyUid,
                          int operationModifiers, boolean isfifo, boolean fromReplication) {
        this(typeDesc, packet, packet.getProjectionTemplate(), uid, scn, expirationTime, xidOriginated,
                templateOperation, respContext, returnOnlyUid, operationModifiers, isfifo, packet.getQueryResultType(), fromReplication, packet.isAllIndexValuesSqlQuery());
    }

    public TemplateHolder(IServerTypeDesc typeDesc, ITemplatePacket packet, String uid,
                          long expirationTime, XtnEntry xidOriginated, long scn, int templateOperation,
                          IResponseContext respContext, boolean returnOnlyUid,
                          int operationModifiers, boolean isfifo) {
        this(typeDesc, packet, packet.getProjectionTemplate(), uid, scn, expirationTime, xidOriginated,
                templateOperation, respContext, returnOnlyUid, operationModifiers, isfifo, packet.getQueryResultType());
    }

    public TemplateHolder(IServerTypeDesc typeDesc, IEntryPacket packet, String uid,
                          long expirationTime, XtnEntry xidOriginated, long scn, int templateOperation,
                          IResponseContext respContext, int operationModifiers) {
        this(typeDesc, packet, null, uid, scn, expirationTime, xidOriginated,
                templateOperation, respContext, false /*returnOnlyUid*/, operationModifiers, false /*isfifo*/, QueryResultTypeInternal.getUpdateResultType(packet));
    }

    private TemplateHolder(IServerTypeDesc typeDesc, IEntryPacket packet, AbstractProjectionTemplate projectionTemplate,
                           String uid, long scn, long expirationTime, XtnEntry xidOriginated,
                           int templateOperation, IResponseContext respContext,
                           boolean returnOnlyUid, int operationModifiers, boolean isfifo, QueryResultTypeInternal queryResultType, boolean fromReplication, boolean isAllIndexValuesSqlQuery) {
        super(typeDesc, uid, scn, packet.isTransient());
        _projectionTemplate = projectionTemplate;
        _templateOperation = templateOperation;
        _returnOnlyUid = returnOnlyUid;
        _operationModifiers = operationModifiers;
        _externalEntryImplClassName = packet.getExternalEntryImplClassName();
        _respContext = respContext;
        _templateXtnOriginated = xidOriginated;
        _operationID = packet.getOperationID();
        _multipleUids = packet.getMultipleUIDs();
        _uidToOperateBy = packet.getUID();
        _allValuesIndexSqlQuery = isAllIndexValuesSqlQuery;

        // Disable FIFO if not required:
        if (isInitiatedEvictionOperation())
            isfifo = false;
        _fifoTemplate = isfifo;

        _queryResultType = queryResultType;
        _templateData = new TemplateEntryData(typeDesc.getTypeDesc(), packet, expirationTime, fromReplication);

        // Set the previous entry version if relevant (used for update operation version validation)
        if (packet.hasPreviousVersion()) {
            _previousVersion = packet.getPreviousVersion();
        }
        // If there's no previous version set previous version to the current version - 1
        else {
            int version = packet.getVersion();
            if (version != 0)
                _previousVersion = version - 1;
        }

        setMemoryOnlySearch(Modifiers.contains(_operationModifiers, Modifiers.MEMORY_ONLY_SEARCH));
        if (Modifiers.contains(_operationModifiers, Modifiers.EXPLAIN_PLAN)) {
            QueryTemplatePacket templatePacket = (QueryTemplatePacket) packet;
            SingleExplainPlan plan = new SingleExplainPlan();
            if (HasMatchCodes(packet)) {
                plan.setRoot(ExplainPlanUtil.BuildMatchCodes(templatePacket));
                if (templatePacket.getCustomQuery() != null) {
                    plan.getRoot().getChildren().add(ExplainPlanUtil.buildQueryTree(templatePacket.getCustomQuery()));
                }
            } else if (templatePacket.getCustomQuery() != null) {
                plan.setRoot(ExplainPlanUtil.buildQueryTree(templatePacket.getCustomQuery()));
            }
            this._singleExplainPlan = plan;
        }
    }

    private boolean HasMatchCodes(IEntryPacket packet) {
        Object[] fieldValues = packet.getFieldValues();
        for (Object fieldValue : fieldValues) {
            if(fieldValue != null)
                return  true;
        }
        return false;
    }

    private TemplateHolder(IServerTypeDesc typeDesc, IEntryPacket packet, AbstractProjectionTemplate projectionTemplate,
                           String uid, long scn, long expirationTime, XtnEntry xidOriginated,
                           int templateOperation, IResponseContext respContext,
                           boolean returnOnlyUid, int operationModifiers, boolean isfifo, QueryResultTypeInternal queryResultType) {
        this(typeDesc, packet, projectionTemplate, uid, scn, expirationTime, xidOriginated, templateOperation, respContext, returnOnlyUid,
                operationModifiers, isfifo, queryResultType, false, false);
    }

    public boolean isHasWaitingFor() {
        return _hasWaitingFor;
    }

    public void setHasWaitingFor(boolean value) {
        this._hasWaitingFor = value;
    }

    public boolean isNotifyTemplate() {
        return false;
    }

    @Override
    public void setExpirationTime(long expirationTime) {
        this._templateData.setExpirationTime(expirationTime);
    }

    public void setExpirationTime(long expirationTime, boolean createSnapshot) {//dummy
        setExpirationTime(expirationTime);
    }

    public boolean isExpired() {
        return _templateData.isExpired();
    }

    public boolean isExpired(long limit) {
        return _templateData.isExpired(limit);
    }

    private void setVersionID(int versionID) {
        _templateData.setVersion(versionID);
    }

    public void updateVersionAndExpiration(int versionID, long expiration) {
        setExpirationTime(expiration);
        setVersionID(versionID);
    }

    public int getTemplateOperation() {
        return _templateOperation;
    }

    public boolean hasPendingRemoteException() {
        return _pendingRemoteException;
    }

    public void setPendingRemoteException(boolean value) {
        this._pendingRemoteException = value;
    }

    public QueryResultTypeInternal getQueryResultType() {
        return _queryResultType;
    }


    public IResponseContext getResponseContext() {
        return _respContext;
    }

    public int getTokenFieldNumber() {
        return -1;
    }

    /**
     * is the initial if-exist search active ?
     *
     * @return _initialIfExistSearchActive indicator
     */
    public boolean isInitialIfExistSearchActive() {
        return _initialIfExistSearchActive;
    }

    /**
     * set the _initialIfExistSearchActive indicator
     */
    public void setInitialIfExistSearchActive() {
        _initialIfExistSearchActive = true;
    }

    /**
     * reset the _initialIfExistSearchActive indicator
     */
    public void resetInitialIfExistSearchActive() {
        _initialIfExistSearchActive = false;
    }

    /**
     * returns true if this template represents an empty template. i.e. this template's type is
     * Object.
     */
    public boolean isEmptyTemplate() {
        return getServerTypeDesc().isRootType();
    }

    @Override
    public boolean hasExtendedMatchCodes() {
        return _templateData.getExtendedMatchCodes() != null;
    }

    @Override
    public short[] getExtendedMatchCodes() {
        return _templateData.getExtendedMatchCodes();
    }

    /**
     * return indication if the template is exclusive read-lock operation
     *
     * @return true if exclusive read-lock
     */
    public boolean isExclusiveReadLockOperation() {
        return ((_templateOperation == SpaceOperations.READ || _templateOperation == SpaceOperations.READ_IE) &&
                ReadModifiers.isExclusiveReadLock(_operationModifiers) && getXidOriginatedTransaction() != null);

    }

    /**
     * is template relevant for fifo blocking in initial search ?
     */
    public boolean isFifoSearch() {
        return
                _fifoTemplate && (_uidToOperateBy == null &&
                        (_multipleUids == null || _multipleUids.length == 0));
    }

    public boolean isFifoTemplate() {
        return _fifoTemplate;
    }

    public boolean isIfExist() {
        return
                _templateOperation == SpaceOperations.READ_IE ||
                        _templateOperation == SpaceOperations.TAKE_IE ||
                        (_templateOperation == SpaceOperations.UPDATE && !isChange()) ||
                        _ifExistForInPlaceUpdate;
    }

    public boolean isInCache() {
        if (getExpirationTime() == 0 && !isNotifyTemplate())
            return false;  //save touching volatile
        return _inCache;
    }

    public void setInCache() {
        if (getExpirationTime() == 0 && !isNotifyTemplate())
            throw new UnsupportedOperationException();
        this._inCache = true;
    }

    public boolean isExpirationTimeSet() {
        return getExpirationTime() != 0;
    }

    /**
     * remove the pending search object & if requested disable initial search indicator NOTE- should
     * be done when template is locked
     */
    public void removePendingFifoSearchObject(boolean disableInitialSearch) {
        _pendingFifoSearch = null;
        if (disableInitialSearch)
            resetInitialFifoSearchActive();
    }

    public void setPendingFifoSearchObject(PendingFifoSearch pobj) {
        _pendingFifoSearch = pobj;
    }

    public PendingFifoSearch getPendingFifoSearchObject() {
        return _pendingFifoSearch;
    }

    public boolean isInitialFifoSearchActive() {
        return _initialFifoSearchActive;
    }

    public void setInitialFifoSearchActive() {
        _initialFifoSearchActive = true;
    }

    public void resetInitialFifoSearchActive() {
        _initialFifoSearchActive = false;
    }

    public long getFifoXtnNumberOnSearchStart() {
        return _fifoXtnNumberOnSearchStart;
    }

    public void setFifoXtnNumberOnSearchStart(long xtnnum) {
        _fifoXtnNumberOnSearchStart = xtnnum;
    }

    public void resetFifoXtnNumberOnSearchStart() {
        _fifoXtnNumberOnSearchStart = TerminatingFifoXtnsInfo.UNKNOWN_FIFO_XTN;
    }

    public boolean isWriteLockOperation() {
        return
                _templateOperation == SpaceOperations.WRITE ||
                        _templateOperation == SpaceOperations.TAKE ||
                        _templateOperation == SpaceOperations.TAKE_IE ||
                        _templateOperation == SpaceOperations.UPDATE ||
                        isExclusiveReadLockOperation();

    }

    public boolean isReadOperation() {
        return
                _templateOperation == SpaceOperations.READ ||
                        _templateOperation == SpaceOperations.READ_IE;

    }

    public boolean isTakeOperation() {
        return
                _templateOperation == SpaceOperations.TAKE ||
                        _templateOperation == SpaceOperations.TAKE_IE;

    }

    public boolean isUpdateOperation() {
        return
                _templateOperation == SpaceOperations.UPDATE;

    }

    public boolean isInitiatedEvictionOperation() {
        return TakeModifiers.isEvictOnly(_operationModifiers) && isTakeOperation();
    }

    public boolean isReadCommittedRequested() {
        return isReadOperation() && ReadModifiers.isReadCommitted(_operationModifiers) &&
                !isExclusiveReadLockOperation();

    }

    public boolean isDirtyReadRequested() {
        return isReadOperation() && ReadModifiers.isDirtyRead(_operationModifiers);
    }

    @Override
    public int getFifoThreadPartition() {
        return _fifoThreadPartition;
    }

    @Override
    public void setFifoThreadPartition(int nThread) {
        _fifoThreadPartition = nThread;
    }

    /**
     * the second phase is reached if the templateHolder enters the cache and is being matched by a
     * different (later) call (thread) this flag is used to avoid sending a response twice to the
     * client.
     *
     * @return the current phase.
     * @see com.gigaspaces.internal.server.space.SpaceEngine#notifyReceiver(ITemplateHolder,
     * IEntryPacket, Exception, boolean)
     */
    public boolean isSecondPhase() {
        return _secondPhase;
    }

    /**
     * @see #isSecondPhase()
     */
    public void setSecondPhase() {
        _secondPhase = true;
    }

    public boolean hasAnswer() {
        AnswerHolder answerHolder = _answerHolder;
        return answerHolder != null && (!answerHolder.m_AnswerPacket.isDummy() || answerHolder.m_Exception != null);
    }

    public AnswerHolder getAnswerHolder() {
        return _answerHolder;
    }

    public void setAnswerHolder(AnswerHolder answerHolder) {
        if (_singleExplainPlan != null) {
            answerHolder.setExplainPlan(_singleExplainPlan);
        }
        this._answerHolder = answerHolder;
    }

    /**
     * the template MUST be locked when calling this method.
     */
    public void setInExpirationManager(boolean inManager) {
        _inExpirationManager = inManager;
    }

    /**
     * the template MUST be locked when calling this method.
     */
    public boolean isInExpirationManager() {
        return _inExpirationManager;
    }

    @Override
    public boolean isExplicitInsertionToExpirationManager() {
        return isChange();
    }


    public String getExternalEntryImplClassName() {
        return _externalEntryImplClassName;
    }

    public OperationID getOperationID() {
        return _operationID;
    }

    /**
     * @param m_XidOriginated the m_XidOriginated to set
     */
    public void resetXidOriginated() {
        _templateXtnOriginated = null;
    }

    public XtnEntry getXidOriginated() {
        return _templateXtnOriginated;
    }

    /**
     * @return the m_XidOriginated transaction
     */
    public ServerTransaction getXidOriginatedTransaction() {
        return _templateXtnOriginated == null ? null : _templateXtnOriginated.m_Transaction;
    }

    @Override
    public boolean isMaybeUnderXtn() {
        return getXidOriginated() != null;
    }

    public void setNonBlockingRead(boolean val) {
        _nonBlockingRead = val;
    }

    /**
     * return true if this template should perform in non-blocking read
     */
    public boolean isNonBlockingRead() {
        return _nonBlockingRead;
    }

    @Override
    public Object getRangeValue(int index) {
        return _templateData.getRangeValue(index);
    }

    public boolean getRangeInclusion(int index) {
        return _templateData.getRangeInclusion(index);
    }

    @Override
    public void dump(Logger logger, String msg) {
        super.dump(logger, msg);

        logger.info("TemplateOperation : " + _templateOperation);
    }

    /**
     * Return true whether this template should match by uid only (if such provided) or with full
     * match regardless present if uid.
     */
    public boolean isMatchByID() {
        return (_uidToOperateBy != null &&
                ( ( _templateOperation == SpaceOperations.UPDATE ) ||
                        (ReadModifiers.isMatchByID(_operationModifiers))));

    }

    @Override
    public Object getEntryId() {
        if (getEntryData().getEntryTypeDesc().getTypeDesc().isAutoGenerateId())
            return getUidToOperateBy();

        return super.getEntryId();
    }

    public String getUidToOperateBy() {
        return _uidToOperateBy;
    }

    public void setUidToOperateBy(String uid) {
        this._uidToOperateBy = uid;
    }

    public String[] getMultipleUids() {
        return _multipleUids;
    }

    public boolean isReturnOnlyUid() {
        return _returnOnlyUid;
    }

    public IEntryHolder getUpdatedEntry() {
        return _updatedEntry;
    }

    public void setUpdatedEntry(IEntryHolder updatedEntry) {
        this._updatedEntry = updatedEntry;
    }

    @Override
    public void setReRegisterLeaseOnUpdate(boolean value) {
        _isReRegisterLeaseOnUpdate = value;
    }

    @Override
    public boolean isReRegisterLeaseOnUpdate() {
        return _isReRegisterLeaseOnUpdate;
    }


    public int getOperationModifiers() {
        return _operationModifiers;
    }

    public IEntryData getEntryData() {
        return _templateData;
    }

    public boolean isMemoryOnlySearch() {
        return _memoryOnlySearch;
    }

    public void setMemoryOnlySearch(boolean memoryOnly) {
        _memoryOnlySearch = memoryOnly;
    }

    @Override
    public ICustomQuery getCustomQuery() {
        return _templateData.getCustomQuery();
    }

    /**
     * This method is required for SF case 7017 (https://na6.salesforce.com/5008000000GijmA?srPos=1&srKp=500).
     * The customer is using this internal API with our approval, until we provide our own
     * solution.
     */
    public void setCustomQuery(ICustomQuery customQuery) {
        _templateData.setCustomQuery(customQuery);
    }

    public SQLQuery<?> toSQLQuery(ITypeDesc typeDesc) {
        return _templateData.toSQLQuery(typeDesc);
    }

    public void setID(Object id) {
        _id = id;
    }

    public Object getID() {
        return _id;
    }

    @Override
    public MatchResult match(CacheManager cacheManager, IEntryHolder entry, int skipAlreadyMatchedFixedPropertyIndex, String skipAlreadyMatchedIndexPath, boolean safeEntry, Context context, RegexCache regexCache) {
        context.incrementNumOfEntriesMatched();
        MatchResult res = MatchResult.NONE;
        ITransactionalEntryData masterEntryData = null;
        IEntryData shadowEntryData = null;
        if (getCustomQuery() != null && context != null)
            context.setOnMatchUid(entry.getUID());

        //first- screen by uid if relevant , for in-place-update by id only current class is relevant (no inheritance)
        if (isChangeById() && !getServerTypeDesc().getTypeName().equals(entry.getServerTypeDesc().getTypeName()))
            res = MatchResult.NONE;
        else if (_uidToOperateBy != null && (!_uidToOperateBy.equals(entry.getUID())))
            res = MatchResult.NONE;
        else {
            //obtain the relevant field values
            masterEntryData = entry.getTxnEntryData();
            if (safeEntry) {//entry is locked (or a clone)
                if (entry.hasShadow(true /*safeEntry*/)) //use mayHaveShadow() not to touch volatile
                    shadowEntryData = masterEntryData.getOtherUpdateUnderXtnEntry().getEntryData();
            } else {
                //note that we test the pending update under same monitor as taking the values
                IEntryHolder sh = masterEntryData.getOtherUpdateUnderXtnEntry();
                shadowEntryData = sh != null ? masterEntryData.getOtherUpdateUnderXtnEntry().getEntryData() : null;
            }

            if ( ( this.isMatchByID() && !isChangeQuery() ) || this.isEmptyTemplate())
                res = shadowEntryData == null ? MatchResult.MASTER : MatchResult.MASTER_AND_SHADOW;
            else {
                boolean masterMatch = _templateData.match(cacheManager, masterEntryData, skipAlreadyMatchedFixedPropertyIndex, skipAlreadyMatchedIndexPath, regexCache, context);

                if (shadowEntryData == null)
                    res = masterMatch ? MatchResult.MASTER : MatchResult.NONE;
                else {
                    boolean shadowMatch = _templateData.match(cacheManager, shadowEntryData, skipAlreadyMatchedFixedPropertyIndex, skipAlreadyMatchedIndexPath, regexCache, context);

                    if (masterMatch)
                        res = shadowMatch ? MatchResult.MASTER_AND_SHADOW : MatchResult.MASTER;
                    else
                        res = shadowMatch ? MatchResult.SHADOW : MatchResult.NONE;
                }
            }
        }

        if (context != null) {
            if (res == MatchResult.NONE)
                context.setRawmatchResult(null, MatchResult.NONE, null, null);
            else {
                context.setRawmatchResult(masterEntryData, res, entry, this);
                //note- if entry insertion/update can be revoked in the middle (unique index
                //setting unstable should be done differently with double check
                context.setUnstableEntry(entry.isUnstable());
            }
        }
        if(_singleExplainPlan != null){
            _singleExplainPlan.incrementScanned(entry.getClassName());
            if(res != MatchResult.NONE){
                _singleExplainPlan.incrementMatched(entry.getClassName());
            }
        }
        return res;
    }

    @Override
    public boolean quickReject(Context context, FifoSearch fifoSearch) {
        if (isDeleted() || isExpired())
            return true;

        //if this is a fifo scan and the template is not a fifo, or vice versa, abort:
        boolean isOperationFifoSearch = fifoSearch == FifoSearch.YES;
        boolean isTemplateFifoSearch = isFifoSearch();
        if (isTemplateFifoSearch != isOperationFifoSearch)
            return true;

        if (context.isFifoThread() && isTemplateFifoSearch) {
            //handle the template only if rendered by the correct thread according to fifo thread number & template thrad partition
            if (context.getFifoThreadNumber() != _fifoThreadPartition)
                return true;
        }

        return false;
    }

    @Override
    public ITransactionalEntryData getTxnEntryData() {
        throw new UnsupportedOperationException("This operation is not supported for TemplateHolder");
    }

    public boolean anyReadLockXtn() {
        throw new UnsupportedOperationException("This operation is not supported for TemplateHolder");
    }

    public List<XtnEntry> getReadLockOwners() {
        throw new UnsupportedOperationException("This operation is not supported for TemplateHolder");
    }

    public void addReadLockOwner(XtnEntry xtn) {
        throw new UnsupportedOperationException("This operation is not supported for TemplateHolder");
    }

    public void removeReadLockOwner(XtnEntry xtn) {
        throw new UnsupportedOperationException("This operation is not supported for TemplateHolder");
    }

    public void clearReadLockOwners() {
        throw new UnsupportedOperationException("This operation is not supported for TemplateHolder");
    }

    public XtnEntry getWriteLockOwner() {
        throw new UnsupportedOperationException("This operation is not supported for TemplateHolder");
    }

    public boolean isEntryUnderWriteLockXtn() {
        throw new UnsupportedOperationException("This operation is not supported for TemplateHolder");
    }

    public int getWriteLockOperation() {
        throw new UnsupportedOperationException("This operation is not supported for TemplateHolder");
    }

    public ServerTransaction getWriteLockTransaction() {
        throw new UnsupportedOperationException("This operation is not supported for TemplateHolder");
    }

    public Collection<ITemplateHolder> getTemplatesWaitingForEntry() {
        throw new UnsupportedOperationException("This operation is not supported for TemplateHolder");
    }

    public Collection<ITemplateHolder> getCopyOfTemplatesWaitingForEntry() {
        throw new UnsupportedOperationException("This operation is not supported for TemplateHolder");

    }

    public void addTemplateWaitingForEntry(ITemplateHolder template) {
        throw new UnsupportedOperationException("This operation is not supported for TemplateHolder");
    }

    public void removeTemplateWaitingForEntry(ITemplateHolder template) {
        throw new UnsupportedOperationException("This operation is not supported for TemplateHolder");
    }

    public IEntryHolder getMaster() {
        throw new UnsupportedOperationException("This operation is not supported for TemplateHolder");
    }

    public void setFieldsValues(Object[] fieldsValues, boolean createSnapsht) {
        throw new UnsupportedOperationException("This operation is not supported for TemplateHolder");
    }

    public void updateEntryData(IEntryData newEntryData, long expirationTime) {
        throw new UnsupportedOperationException("This operation is not supported for TemplateHolder");
    }

    public void resetEntryXtnInfo() {
        throw new UnsupportedOperationException("This operation is not supported for TemplateHolder");
    }

    public void resetWriteLockOwner() {
        throw new UnsupportedOperationException("This operation is not supported for TemplateHolder");
    }

    public void setWriteLockOperation(int writeLockOperation, boolean createSnapshot) {
        throw new UnsupportedOperationException("This operation is not supported for TemplateHolder");
    }

    public void setWriteLockOwnerAndOperation(XtnEntry writeLockOwner, int writeLockOperation) {
        throw new UnsupportedOperationException("This operation is not supported for TemplateHolder");
    }

    public void setWriteLockOwnerAndOperation(XtnEntry writeLockOwner, int writeLockOperation, boolean createSnapshot) {
        throw new UnsupportedOperationException("This operation is not supported for TemplateHolder");
    }

    public void setWriteLockOwnerOperationAndShadow(XtnEntry writeLockOwner, int writeLockOperation, IEntryHolder otherEh) {
        throw new UnsupportedOperationException("This operation is not supported for TemplateHolder");
    }

    public boolean hasShadow(boolean safeEntry) {
        throw new UnsupportedOperationException("This operation is not supported for TemplateHolder");
    }

    public ShadowEntryHolder getShadow() {
        throw new UnsupportedOperationException("This operation is not supported for TemplateHolder");
    }

    public void restoreUpdateXtnRollback(IEntryData entryData) {
        throw new UnsupportedOperationException("This operation is not supported for TemplateHolder");
    }

    public void setOtherUpdateUnderXtnEntry(IEntryHolder eh) {
        throw new UnsupportedOperationException("This operation is not supported for TemplateHolder");
    }

    public IEntryHolder createCopy() {
        throw new UnsupportedOperationException("This operation is not supported for TemplateHolder");
    }

    public IEntryHolder createDummy() {
        throw new UnsupportedOperationException("This operation is not supported for TemplateHolder");
    }

    public boolean isUnstable() {
        throw new UnsupportedOperationException("This operation is not supported for TemplateHolder");
    }

    public void setunStable(boolean value) {
        throw new UnsupportedOperationException("This operation is not supported for TemplateHolder");
    }

    @Override
    public int getPreviousVersion() {
        return _previousVersion;
    }

    public int getAfterOpFilterCode() {
        return _afterOpFilterCode;
    }

    public IEntryPacket getUpdateOperationEntry() {
        return _updateOperationEntry;
    }


    public SpaceContext getSpaceContext() {
        return _spaceContext;
    }

    public FilterManager getFilterManager() {
        return _filterManager;
    }

    public void setForAfterOperationFilter(int afterOpFilterCode, SpaceContext sc, FilterManager fm, IEntryPacket updateOperationEntry) {
        _afterOpFilterCode = afterOpFilterCode;
        _spaceContext = sc;
        _filterManager = fm;
        if (updateOperationEntry != null)
            _updateOperationEntry = updateOperationEntry;
    }

    //is this template a fifo-group poll template ?
    public boolean isFifoGroupPoll() {
        return ReadModifiers.isFifoGroupingPoll(_operationModifiers);
    }

    @Override
    public boolean isChange() {
        return _mutators != null;
    }

    @Override
    public boolean isChangeById() {
        return isChange() && (getUidToOperateBy() != null || getID() != null);
    }

    @Override
    public void setMutators(Collection<SpaceEntryMutator> mutators) {
        _mutators = mutators;
    }

    @Override
    public Collection<SpaceEntryMutator> getMutators() {
        return _mutators;
    }

    public void setChangeExpiration(long expirationTime) {
        _inPlaceUpdateExpiration = expirationTime;
    }


    @Override
    public EntryHolderAggregatorContext getAggregatorContext() {
        return aggregatorContext;
    }

    public void setAggregatorContext(EntryHolderAggregatorContext aggregatorContext) {
        this.aggregatorContext = aggregatorContext;
    }

    public long getChangeExpiration() {
        return _inPlaceUpdateExpiration;
    }

    public void setIfExistForChange() {
        _ifExistForInPlaceUpdate = true;
    }

    @Override
    public Throwable getRejectedOpOriginalException() {
        return _cause;
    }

    @Override
    //note- must be called when template is locked or in NO_WAIT situation
    public void setRejectedOpOriginalExceptionAndEntry(Throwable cause, IEntryData rejectedEntry) {
        _cause = cause;
        _rejectedEntry = rejectedEntry;
    }

    @Override
    public IEntryData getRejectedOperationEntry() {
        return _rejectedEntry;
    }


    @Override
    public boolean isSetSingleOperationExtendedErrorInfo() {
        return isChangeById();
    }


    @Override
    //NOTE- call only when template is locked
    public Collection<IEntryHolder> getEntriesWaitingForTemplate() {
        return _waitingFor;
    }

    //NOTE- call only when template is locked
    public void addEntryWaitingForTemplate(IEntryHolder entry) {
        if (_waitingFor == null)
            _waitingFor = new HashSet<IEntryHolder>();

        if (!_waitingFor.contains(entry))
            _waitingFor.add(entry);

        if (_waitingFor.size() == 1 && !isHasWaitingFor())
            setHasWaitingFor(true);
    }

    //NOTE- call only when template is locked
    public void removeEntryWaitingForTemplate(IEntryHolder entry) {
        if (_waitingFor != null)
            _waitingFor.remove(entry);

        if (_waitingFor != null && _waitingFor.isEmpty() && isHasWaitingFor())
            setHasWaitingFor(false);

    }

    //batch op related methods
    public boolean isBatchOperation() {
        return _batchOerationContext != null;
    }

    public boolean isReadMultiple() {
        return isBatchOperation() && isReadOperation();
    }

    public boolean isTakeMultiple() {
        return isBatchOperation() && isTakeOperation();

    }

    @Override
    public BatchQueryOperationContext getBatchOperationContext() {
        return _batchOerationContext;
    }

    @Override
    public void setBatchOperationContext(BatchQueryOperationContext batchOpContext) {
        _batchOerationContext = batchOpContext;
    }

    @Override
    public boolean canFinishBatchOperation() {
        return ((_batchOerationContext.reachedMaxEntries()) ||
                (isInCache() &&
                        ((isIfExist() && !isInitialIfExistSearchActive() && !isHasWaitingFor()) ||
                                (_batchOerationContext.reachedMinEntries() && (!isIfExist() || !isInitialIfExistSearchActive())))));
    }

    @Override
    public boolean isChangeMultiple() {
        return isBatchOperation() && isChange();
    }

    @Override
    public boolean isChangeQuery() {
        return isChange() && ( getCustomQuery() != null || getExtendedMatchCodes() != null );
    }

    @Override
    public AbstractProjectionTemplate getProjectionTemplate() {
        return _projectionTemplate;
    }


    @Override
    public boolean isIdQuery() {
        return _templateData.isIdQuery();
    }

    //by Ids related methods
    public boolean isMultipleIdsOperation() {
        return _multipleIdsContexct != null;
    }

    public MultipleIdsContext getMultipleIdsContext() {
        return _multipleIdsContexct;
    }

    public void setMultipleIdsContext(MultipleIdsContext byIdsContext) {
        _multipleIdsContexct = byIdsContext;
    }

    public boolean isUpdateMultiple() {
        return isMultipleIdsOperation() && isUpdateOperation() && !isChange();
    }

    public void setOrdinalForEntryByIdMultipleOperation(int ordinal) {
        _ordinalForMultipleIdsOperation = ordinal;
    }

    public int getOrdinalForEntryByIdMultipleOperation() {
        return _ordinalForMultipleIdsOperation;
    }

    public UpdateOrWriteContext getUpfdateOrWriteContext() {
        return _updateOrWriteContext;
    }

    public void setUpdateOrWriteContext(UpdateOrWriteContext ctx) {
        _updateOrWriteContext = ctx;
    }

    @Override
    public boolean isSameEntryInstance(IEntryHolder other) {
        return this == other;
    }

    @Override
    public boolean isBlobStoreEntry() {
        return false;
    }

    @Override
    public IEntryHolder getOriginalEntryHolder() {
        return this;
    }

    @Override
    public boolean isAllValuesIndexSqlQuery() {
        return _allValuesIndexSqlQuery;
    }

    @Override
    public boolean isSqlQuery() {
        return (getCustomQuery() != null || getExtendedMatchCodes() != null);
    }

    @Override
    public boolean isServerIterator(){
        return isReadMultiple() && getServerIteratorInfo() != null;
    }

    @Override
    public ServerIteratorInfo getServerIteratorInfo(){
        return _serverIteratorInfo;
    }

    @Override
    public void setServerIteratorInfo(ServerIteratorInfo serverIteratorInfo) {
        this._serverIteratorInfo = serverIteratorInfo;
    }

    //+++++++++++++ ILockObject methods
    @Override
    public ILockObject getExternalLockObject() {
        return null;
    }

    //blob store
    @Override
    public boolean isOptimizedForBlobStoreOp(CacheManager cacheManager) {
        if (_optimizedForBlobStoreOp != null)
            return _optimizedForBlobStoreOp;
        //first time check for class- set it
        if (isSqlQuery())
            _optimizedForBlobStoreOp = isAllValuesIndexSqlQuery();
        else
            _optimizedForBlobStoreOp = isOptimizedForBlobStoreNonSql(cacheManager,(TemplateEntryData)getEntryData(),getClassName());
        return _optimizedForBlobStoreOp;
   }


    public static boolean isOptimizedForBlobStoreClear(CacheManager cacheManager,ITemplatePacket templatePacket, TemplateEntryData templateEntryData) {

        if (templatePacket.getCustomQuery() != null)
            return templatePacket.isAllIndexValuesSqlQuery();

        return isOptimizedForBlobStoreNonSql(cacheManager,templateEntryData,templatePacket.getTypeName());
    }

    private static boolean isOptimizedForBlobStoreNonSql(CacheManager cacheManager, TemplateEntryData templateEntryData, String typeName)
    {
        boolean optimized = false;
        if (templateEntryData != null) {
            if (templateEntryData.getFixedPropertiesValues() == null) {
                optimized = true; //null template
            } else {
                TypeData typeData = cacheManager.getTypeData(cacheManager.getEngine().getTypeManager().getServerTypeDesc(typeName));
                optimized = true;
                for (int i = 0; i < templateEntryData.getFixedPropertiesValues().length; i++)
                {
                    if (templateEntryData.getFixedPropertiesValues()[i] != null && !typeData.getIndexesRelatedFixedProperties()[i]) {
                        optimized = false;
                        break;
                    }
                }
            }
        }
        else
            return true; //null template
        return optimized;

    }

    public SingleExplainPlan getExplainPlan() {
        return _singleExplainPlan;
    }
}
