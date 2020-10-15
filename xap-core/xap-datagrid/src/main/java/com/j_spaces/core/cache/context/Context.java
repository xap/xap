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

package com.j_spaces.core.cache.context;

import com.gigaspaces.client.EntryLockedException;
import com.gigaspaces.client.mutators.SpaceEntryMutator;
import com.gigaspaces.internal.client.spaceproxy.operations.ChangeEntriesSpaceOperationResult;
import com.gigaspaces.internal.client.spaceproxy.operations.ReadTakeEntriesSpaceOperationResult;
import com.gigaspaces.internal.client.spaceproxy.operations.ReadTakeEntrySpaceOperationResult;
import com.gigaspaces.internal.client.spaceproxy.operations.WriteEntriesSpaceOperationResult;
import com.gigaspaces.internal.client.spaceproxy.operations.WriteEntrySpaceOperationResult;
import com.gigaspaces.internal.cluster.node.IReplicationOutContext;
import com.gigaspaces.internal.exceptions.BatchQueryException;
import com.gigaspaces.internal.query.explainplan.ExplainPlanContext;
import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.space.MatchResult;
import com.gigaspaces.internal.server.space.ReadByIdsInfo;
import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.server.space.operations.ChangeEntriesSpaceOperation;
import com.gigaspaces.internal.server.space.operations.WriteEntryResult;
import com.gigaspaces.internal.server.storage.*;
import com.gigaspaces.internal.sync.hybrid.SyncHybridOperationDetails;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.transport.TemplatePacketFactory;
import com.gigaspaces.lrmi.nio.IResponseContext;
import com.j_spaces.core.AnswerHolder;
import com.j_spaces.core.AnswerPacket;
import com.j_spaces.core.ExtendedAnswerHolder;
import com.j_spaces.core.OperationID;
import com.j_spaces.core.cache.IEntryCacheInfo;
import com.j_spaces.core.cache.InitialLoadInfo;
import com.j_spaces.core.cache.TypeDataIndex;
import com.j_spaces.core.cache.blobStore.storage.bulks.BlobStoreBulkInfo;
import com.j_spaces.core.cache.blobStore.storage.preFetch.BlobStorePreFetchBatchResult;
import com.j_spaces.core.client.Modifiers;
import com.j_spaces.core.client.OperationTimeoutException;
import com.j_spaces.core.fifo.FifoBackgroundRequest;
import com.j_spaces.core.filters.FilterOperationCodes;
import com.j_spaces.kernel.list.MultiIntersectedStoredList;

import net.jini.core.lease.Lease;
import net.jini.core.transaction.server.ServerTransaction;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

@com.gigaspaces.api.InternalApi
public class Context {

    public static OperationTimeoutException _operationTimeoutExecption = new OperationTimeoutException();
    private static EntryLockedException _entryLockedException = new EntryLockedException();

    // The two markers are used to optimize checking the flag by the same thread.
    private volatile boolean _active = false;
    private boolean _localActive = false;

    /**
     * recent fifo object- not null if fifo handling needed
     */
    private FifoBackgroundRequest _recentFifoObject;
    /**
     * if not null perform fifo notify for new entries if fifo-notify templates exist
     */
    private IEntryHolder _notifyNewEntry;

    /**
     * sync packets collected from xxxxMultiple operation.
     */
    private boolean m_SyncReplFromMultipleOperation;

    /**
     * set to <code>true</code> in some regions in the code in order to disable sync-replication
     * when objects are locked.
     */
    private boolean _disableSyncReplication;

    //this field is true is template is in initial search for entries
    private boolean _templateInitialSearchThread;

    private boolean _isFifoThread;

    //the number of the fifo thread owning this context
    private int _fifoThreadNumber;

    private boolean _fromReplication;

    // the time when original space operation (write/update/take/..) became visible
    // - finished the initial processing and result return to the user
    private long _operationVisibilityTime;

    /**
     * set to true (in read/take/update) if this context is held by main thread handling the op'
     */
    private boolean _mainThread;
    /**
     * set to true if the thread holding the context has set the result in the template
     */
    private boolean _opResultByThread;

    /**
     * Set to true if the operation is updateMultiple/writeMultiple/takeMultiple used for
     * transactional operations to avoid double locking of the transaction readLock
     */
    private boolean _isTransactionalMultipleOperation;

    private OperationID _operationID;
    private OperationID[] _operationIDs;

    //true if NBR currently executing
    private boolean _isNonBlockingReadOp;

    //after rawmatch the following fields are updated with the match snapshot (if matched) or null (unmatched)
    // and the result + the instance of enreyHolder & templateHolder
    private MatchResult _matchResult;
    private ITransactionalEntryData _lastRawMatchSnapshot;
    private IEntryHolder _lastRawmatchEntry;
    private ITemplateHolder _lastRawmatchTemplate;
    private boolean _unstableEntry; //entry being inserted/updated

    //in Non blocking IfExist we perform a NO_WAIT  scan first, and do a 
    // rescan if we encounter a possible xtn conflict
    private boolean _possibleIEBlockingMatch;

    //true if this is a multiple (read-take-update-write) op
    private boolean _isMultipleOperation;

    //for partial update operations only,created when update op is performed-
    //in order to enable partial update replication
    private boolean[] _partialUpdatedValuesIndicators;

    //for lease registration/creation
    private WriteEntryResult _writeResult;
    private Lease _notifyLease;
    //valid only for update op'
    private boolean _reRegisterLease;

    private IReplicationOutContext _replicationContext;
    //Is this operation originated from this space (not replicated or recovery)
    private boolean _origin = true;

    private boolean _updateLastSentKey = true;

    private boolean _fromGateway;

    private ITemplateHolder _take_template;

    private IServerTypeDesc _take_typeDesc;

    private ReadByIdsInfo _readByIdsInfo;

    private Map<String, IEntryHolder> _prefetchedNonBlobStoreEntries;

    /**
     * during a read operation  we encountered pending expired entries. we may call lease-manager to
     * force a reap cycle
     */
    private boolean _pendingExpiredEntriesExist;

    //the following propery is not null if the current thread is performing prepare/commit
    //of a xtn
    private ServerTransaction _committingXtn;

    //the following properties are handling fifo-group scanning 
    private boolean _anyFifoGroupIndex;    //is there a f-g index in any subclass ?
    private HashMap<Object, TypeDataIndex> _fifoGroupIndexResultsUsedInFifoGroupScan;  //results from the index used in fg scans- the result mapped to its TypeDataIndex
    private boolean _fifoGroupScanEncounteredXtnConflict;
    private HashSet<Object> _fifoGroupValuesForMiltipleOperations; //for readmultiple/takemultiple, aloow those values
    private boolean _exhaustedFifoGroupsTemplatesSearch; // search for templates exhausted
    private boolean _fifoGroupQueryContainsOrCondition;  //in this scan non f-g index is disabled

    //the following field is used is multiple-ids operation
    private int _ordinalForMultipleIdsOperation = -1;

    //the following field is used in change api
    private List<Object> _changeResultsForCurrentEntry;

    //used in insertEntryToCache error handling
    private int _numOfIndexesInserted;

    private int _numOfEntriesMatched;

    //used in index intersection
    private boolean _indicesIntersectionEnabled;
    private MultiIntersectedStoredList<IEntryCacheInfo> _chosenIntersectedList;   //if index intersection desired
    private boolean _usingIntersectedListForScanning;  //it was selected

    //the following are used in blobStore<->SA
    private IEntryCacheInfo _blobStoreEntryCacheInfo;
    private IEntryHolder _blobStoreOpEntryHolder;
    private boolean _blobStoreOpPin;

    //used in general search as indicator of index usage
    private boolean _indexUsed;

    //used in initial load
    private InitialLoadInfo _initialLoadInfo;

    //used in blob-store bulking
    private BlobStoreBulkInfo _blobStoreBulkInfo;

    //used in blob-store to avoid access to ssd and use only intersection of indexes
    private boolean _blobStoreUsePureIndexesAccess;
    private boolean _blobStoreTryNonPersistentOp;
    //the following relates to blob-store prefetch
    private BlobStorePreFetchBatchResult _blobStorePreFetchBatchResult;
    private boolean _disableBlobStorePreFetching;
    //the following fields is blob-store info used in reverting non transactional update
    private short _originalBlobStoreVersion;

    //the following are used in blob-store bulk delayed replication for embedded list
    private boolean _delayedReplicationForbulkOpUsed;
    private IEntryHolder _blobStoreEntry;
    private IEntryData _originalData; //for update
    private Collection<SpaceEntryMutator> _mutators; //for change
    private SpaceEngine.EntryRemoveReasonCodes _removeReason; //remove

    //the following are used in hybrid sync storageAdapter
    private SyncHybridOperationDetails[] _syncHybridOperationsDetails;

    private String _owningThreadName;

    //used to accumulate index information for SQLquery explain plan
    private ExplainPlanContext _explainPlanContext;

    private Boolean _optimizedBlobStoreReadEnabled;
    private boolean _inInitialLoad;

    private String   _onMatchUid;

    private  boolean _inMemoryRecovery;

    private IndexMetricsContext indexMetricsContext;
    private boolean _backupOnly;

    private ViewEntryData viewEntryData;

    public Context() {
    }


    public void setOwningThreadName(String name) {
        _owningThreadName = name;
    }

    public String getOwningThreadName() {
        return _owningThreadName;
    }


    public OperationID getOperationID() {
        return _operationID;
    }

    public void setOperationID(OperationID operationID) {
        this._operationID = operationID;
    }


    public boolean isTemplateInitialSearch() {
        return _templateInitialSearchThread;
    }

    public void setTemplateInitialSearchThread() {
        _templateInitialSearchThread = true;
    }

    public void resetTemplateInitialSearchThread() {
        _templateInitialSearchThread = false;
    }


    public void setFifoThread() {
        _isFifoThread = true;
    }

    public boolean isFifoThread() {
        return _isFifoThread;
    }


    public FifoBackgroundRequest getRecentFifoObject() {
        return _recentFifoObject;
    }

    public void setRecentFifoObject(FifoBackgroundRequest fb) {
        _recentFifoObject = fb;
    }

    public void resetRecentFifoObject() {
        if (_recentFifoObject != null)
            _recentFifoObject = null;
    }

    public int getFifoThreadNumber() {
        return _fifoThreadNumber;
    }

    public void setFifoThreadNumber(int fifoThreadNumber) {
        setFifoThread();
        _fifoThreadNumber = fifoThreadNumber;
    }


    public boolean isFromReplication() {
        return _fromReplication;
    }

    public void setFromReplication(boolean fromReplication) {
        this._fromReplication = fromReplication;
    }

    public void setMainThread(boolean mainThread) {
        _mainThread = mainThread;
    }

    public boolean isMainThread() {
        return _mainThread;
    }

    public void setOpResultByThread(boolean opResultByThread) {
        _opResultByThread = opResultByThread;
    }

    public boolean isOpResultByThread() {
        return _opResultByThread;
    }

    public ITransactionalEntryData getLastRawMatchSnapshot() {
        return _lastRawMatchSnapshot;
    }

    public void setLastRawMatchSnapshot(ITransactionalEntryData entryData) {
        _lastRawMatchSnapshot = entryData;
    }

    public void setLastMatchResult(MatchResult res) {
        _matchResult = res;
    }

    public MatchResult getLastMatchResult() {
        return _matchResult;
    }

    public IEntryHolder getLastRawmatchEntry() {
        return _lastRawmatchEntry;
    }

    public void setLastRawmatchEntry(IEntryHolder entry) {
        _lastRawmatchEntry = entry;
    }

    public ITemplateHolder getLastRawmatchTemplate() {
        return _lastRawmatchTemplate;
    }

    public void setLastRawmatchTemplate(ITemplateHolder template) {
        _lastRawmatchTemplate = template;
    }


    public void setRawmatchResult(ITransactionalEntryData entryData, MatchResult res, IEntryHolder entry, ITemplateHolder template) {
        _matchResult = res;
        _lastRawMatchSnapshot = entryData;
        _lastRawmatchEntry = entry;
        _lastRawmatchTemplate = template;
    }


    public ExplainPlanContext getExplainPlanContext() {
        return _explainPlanContext;
    }

    public void setExplainPlanContext(ExplainPlanContext _explainPlanContext) {
        this._explainPlanContext = _explainPlanContext;
    }

    public boolean isPossibleIEBlockingMatch() {
        return _possibleIEBlockingMatch;
    }

    public void setPossibleIEBlockingMatch(boolean val) {
        _possibleIEBlockingMatch = val;
    }

    /**
     * clean all context.
     */
    public void clean() {
        _recentFifoObject = null;
        _notifyNewEntry = null;
        setSyncReplFromMultipleOperation(false);
        _disableSyncReplication = false;
        resetTemplateInitialSearchThread();
        _isFifoThread = false;
        _fifoThreadNumber = 0;
        _mainThread = false;
        _opResultByThread = false;

        _operationID = null;
        _operationIDs = null;

        _fromReplication = false;
        _origin = true;
        _isTransactionalMultipleOperation = false;
        _operationVisibilityTime = 0;
        _lastRawMatchSnapshot = null;
        _matchResult = MatchResult.NONE;
        _lastRawmatchEntry = null;
        _lastRawmatchTemplate = null;
        _possibleIEBlockingMatch = false;
        _isMultipleOperation = false;
        _unstableEntry = false;
        _isNonBlockingReadOp = false;
        _partialUpdatedValuesIndicators = null;
        _writeResult = null;
        _notifyLease = null;

        _reRegisterLease = false;
        if (_replicationContext != null) {
            _replicationContext.release();
            _replicationContext = null;
        }
        _updateLastSentKey = true;
        _fromGateway = false;
        _take_template = null;
        _take_typeDesc = null;
        _readByIdsInfo = null;
        _prefetchedNonBlobStoreEntries = null;
        _pendingExpiredEntriesExist = false;
        _committingXtn = null;
        _anyFifoGroupIndex = false;
        _fifoGroupIndexResultsUsedInFifoGroupScan = null;
        _fifoGroupScanEncounteredXtnConflict = false;
        _fifoGroupValuesForMiltipleOperations = null;
        _exhaustedFifoGroupsTemplatesSearch = false;
        _fifoGroupQueryContainsOrCondition = false;
        _ordinalForMultipleIdsOperation = -1;
        _changeResultsForCurrentEntry = null;
        _numOfIndexesInserted = 0;
        _numOfEntriesMatched = 0;
        _indicesIntersectionEnabled = false;
        _chosenIntersectedList = null;
        _usingIntersectedListForScanning = false;
        _blobStoreEntryCacheInfo = null;
        _blobStoreOpEntryHolder = null;
        _blobStoreOpPin = false;
        _indexUsed = false;
        _initialLoadInfo = null;
        _blobStoreBulkInfo = null;
        _blobStoreUsePureIndexesAccess = false;
        _blobStoreTryNonPersistentOp = false;
        _blobStorePreFetchBatchResult = null;
        _disableBlobStorePreFetching = false;
        _blobStoreEntry = null;
        _originalBlobStoreVersion = 0;
        _originalData = null; //for update
        _mutators = null; //for change
        _removeReason = null; //remove
        _delayedReplicationForbulkOpUsed = false;
        _explainPlanContext = null;
        _syncHybridOperationsDetails = null;
        _owningThreadName = null;
        _optimizedBlobStoreReadEnabled = null;
        _inInitialLoad = false;
        _onMatchUid = null;
        _inMemoryRecovery = false;
        indexMetricsContext = null;
        _backupOnly=false;
        viewEntryData = null;
    }

    public void setOnMatchUid(String uid)
    {
        _onMatchUid = uid;
    }

    public String getOnMatchUid()
    {
        return _onMatchUid;
    }
    /**
     * @param syncReplFromMultipleOperation the m_SyncReplFromMultipleOperation to set
     */
    public void setSyncReplFromMultipleOperation(boolean syncReplFromMultipleOperation) {
        this.m_SyncReplFromMultipleOperation = syncReplFromMultipleOperation;

        setUpdateLastSentKey(!syncReplFromMultipleOperation);
    }

    public boolean isSyncReplFromMultipleOperation() {
        return m_SyncReplFromMultipleOperation;
    }

    /**
     * @return the isMultipleOperation
     */
    public boolean isTransactionalMultipleOperation() {
        return _isTransactionalMultipleOperation;
    }

    /**
     * @param isMultipleOperation the isMultipleOperation to set
     */
    public void setTransactionalMultipleOperation(boolean isMultipleOperation) {
        _isTransactionalMultipleOperation = isMultipleOperation;
    }


    public void setOperationIDs(OperationID[] opIDs) {
        _operationIDs = opIDs;
    }

    public OperationID[] getOperationIDs() {
        return _operationIDs;
    }

    public long getOperationVisibilityTime() {
        return _operationVisibilityTime;
    }

    public void setOperationVisibilityTime(long operationVisibilityTime) {
        _operationVisibilityTime = operationVisibilityTime;
    }

    public boolean isMultipleOperation() {
        return _isMultipleOperation;
    }

    public void setMultipleOperation() {
        _isMultipleOperation = true;
    }

    public boolean isNonBlockingReadOp() {
        return _isNonBlockingReadOp;
    }

    public void setNonBlockingReadOp(boolean value) {
        _isNonBlockingReadOp = value;
    }

    public boolean isUnstableEntry() {
        return _unstableEntry;
    }

    public void setUnstableEntry(boolean value) {
        _unstableEntry = value;
    }

    public void setActive(boolean _active) {
        this._localActive = _active;
        this._active = _active;
    }

    /**
     * A thread safe check.
     */
    public boolean isActive() {
        return _active;
    }

    /**
     * Not a thread safe check, an optimized call for the local thread only.
     */
    public boolean isLocalActive() {
        return _localActive;
    }

    public boolean[] getPartialUpdatedValuesIndicators() {
        return _partialUpdatedValuesIndicators;

    }

    public void setPartialUpdatedValuesIndicators(boolean[] partialUpdatedValuesIndicators) {
        _partialUpdatedValuesIndicators = partialUpdatedValuesIndicators;

    }

    public WriteEntryResult getWriteResult() {
        return _writeResult;
    }

    public void setWriteResult(WriteEntryResult result) {
        _writeResult = result;
    }

    public Lease getNotifyLease() {
        return _notifyLease;
    }

    public void setNotifyLease(Lease lease) {
        _notifyLease = lease;
    }

    public void setReRegisterLeaseOnUpdate(boolean value) {
        _reRegisterLease = value;
    }

    public boolean isReRegisterLeaseOnUpdate() {
        return _reRegisterLease;
    }

    public void setReplicationContext(IReplicationOutContext replicationContext) {
        this._replicationContext = replicationContext;
    }

    public IReplicationOutContext getReplicationContext() {
        return _replicationContext;
    }

    public void setOrigin(boolean origin) {
        this._origin = origin;
    }

    public boolean isOrigin() {
        return _origin;
    }

    public boolean isFromGateway() {
        return _fromGateway;
    }

    public void setFromGateway(boolean fromGateway) {
        _fromGateway = fromGateway;
    }

    public void setUpdateLastSentKey(boolean _updateLastSentKey) {
        this._updateLastSentKey = _updateLastSentKey;
    }

    public boolean isUpdateLastSentKey() {
        return _updateLastSentKey;
    }


    public void setNotifyNewEntry(IEntryHolder fifoNotifyNewEntry) {
        this._notifyNewEntry = fifoNotifyNewEntry;
    }

    public IEntryHolder getNotifyNewEntry() {
        return _notifyNewEntry;
    }

    public void setDisableSyncReplication(boolean disableSyncReplication) {
        this._disableSyncReplication = disableSyncReplication;
    }

    public boolean isDisableSyncReplication() {
        return _disableSyncReplication;
    }

    public ITemplateHolder get_take_template() {
        return _take_template;
    }

    public void set_take_template(ITemplateHolder _take_template) {
        this._take_template = _take_template;
    }

    public IServerTypeDesc get_take_typeDesc() {
        return _take_typeDesc;
    }

    public void set_take_typeDesc(IServerTypeDesc _take_typeDesc) {
        this._take_typeDesc = _take_typeDesc;
    }

    public void setReadByIdsInfo(ReadByIdsInfo readByIdsInfo) {
        _readByIdsInfo = readByIdsInfo;
    }

    public ReadByIdsInfo getReadByIdsInfo() {
        return _readByIdsInfo;
    }

    public Map<String, IEntryHolder> getPrefetchedNonBlobStoreEntries() {
        return _prefetchedNonBlobStoreEntries;
    }

    public void setPrefetchedNonBlobStoreEntries(Map<String, IEntryHolder> prefetchedEntries) {
        _prefetchedNonBlobStoreEntries = prefetchedEntries;
    }

    public void setPendingExpiredEntriesExist(boolean val) {
        _pendingExpiredEntriesExist = val;
    }

    public boolean isPendingExpiredEntriesExist() {
        return _pendingExpiredEntriesExist;
    }

    public ServerTransaction getCommittingXtn() {
        return _committingXtn;
    }

    public void setCommittingXtn(ServerTransaction xtn) {
        _committingXtn = xtn;
    }

    public boolean isAnyFifoGroupIndex() {
        return _anyFifoGroupIndex;
    }

    public void setAnyFifoGroupIndex() {
        _anyFifoGroupIndex = true;
    }

    public TypeDataIndex getIndexUsedInFifoGroupScan(Object res) {
        return _fifoGroupIndexResultsUsedInFifoGroupScan.get(res);
    }

    public void setFifoGroupIndexUsedInFifoGroupScan(Object res, TypeDataIndex usedIndex) {
        if (_fifoGroupIndexResultsUsedInFifoGroupScan == null)
            _fifoGroupIndexResultsUsedInFifoGroupScan = new HashMap<Object, TypeDataIndex>();
        _fifoGroupIndexResultsUsedInFifoGroupScan.put(res, usedIndex);
    }

    public void resetFifoGroupIndexUsedInFifoGroupScan() {
        _fifoGroupIndexResultsUsedInFifoGroupScan = null;
    }

    public HashMap<Object, TypeDataIndex> getFfoGroupIndexResultsUsedInFifoGroupScan() {
        return _fifoGroupIndexResultsUsedInFifoGroupScan;
    }


    public boolean isFifoGroupScanEncounteredXtnConflict() {
        return _fifoGroupScanEncounteredXtnConflict;
    }

    public void setFifoGroupScanEncounteredXtnConflict(boolean val) {
        _fifoGroupScanEncounteredXtnConflict = val;
    }


    public boolean isFifoGroupValueForMiltipleOperations(Object val) {
        return (_fifoGroupValuesForMiltipleOperations != null && _fifoGroupValuesForMiltipleOperations.contains(val));
    }

    public void setToFifoGroupValueForMiltipleOperations(Object val) {
        if (_fifoGroupValuesForMiltipleOperations == null)
            _fifoGroupValuesForMiltipleOperations = new HashSet();
        _fifoGroupValuesForMiltipleOperations.add(val);
    }

    public boolean isExhaustedFifoGroupsTemplatesSearch() {
        return _exhaustedFifoGroupsTemplatesSearch;
    }

    public void setExhaustedFifoGroupsTemplatesSearch(boolean val) {
        _exhaustedFifoGroupsTemplatesSearch = val;
    }

    public boolean isFifoGroupQueryContainsOrCondition() {
        return _fifoGroupQueryContainsOrCondition;
    }

    public void setFifoGroupQueryContainsOrCondition(boolean val) {
        _fifoGroupQueryContainsOrCondition = val;
    }

    public void setOrdinalForMultipleIdsOperation(int ordinal) {
        _ordinalForMultipleIdsOperation = ordinal;
    }

    public int getOrdinalForMultipleIdsOperation() {
        return _ordinalForMultipleIdsOperation;
    }

    public List<Object> getChangeResultsForCurrentEntry() {
        return _changeResultsForCurrentEntry;
    }

    public void setChangeResultsForCurrentEntry(List<Object> results) {
        _changeResultsForCurrentEntry = results;
    }


    public int getNumOfIndexesInserted() {
        return _numOfIndexesInserted;
    }

    public void clearNumOfIndexesInserted() {
        _numOfIndexesInserted = 0;
    }

    public void incrementNumOfIndexesInserted() {
        ++_numOfIndexesInserted;
    }


    public void incrementNumOfEntriesMatched() {
        ++_numOfEntriesMatched;
    }

    public int getNumberOfEntriesMatched() {
        return _numOfEntriesMatched;
    }


    //++++++++++++++++++intersected indices related methods
    public void setIntersectionEnablment(boolean val) {
        _indicesIntersectionEnabled = val;
        _chosenIntersectedList = null;
        _usingIntersectedListForScanning = false;
    }

    public boolean isIndicesIntersectionEnabled() {
        return _indicesIntersectionEnabled;
    }

    public MultiIntersectedStoredList<IEntryCacheInfo> getChosenIntersectedList(boolean finalChoice) {
        if (finalChoice && _chosenIntersectedList != null)
            _usingIntersectedListForScanning = true;
        return _chosenIntersectedList;
    }

    public void setChosenIntersectedList(MultiIntersectedStoredList<IEntryCacheInfo> chosenIntersectedList) {
        _chosenIntersectedList = chosenIntersectedList;
    }

    public boolean isUsingIntersectedListForScanning() {
        return _usingIntersectedListForScanning;
    }
    //-----------------------------------------------------------------------------------------------------


    //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++blobStore related ++++++++++++++++++

    public IEntryCacheInfo getBlobStoreOpEntryCacheInfo() {
        return _blobStoreEntryCacheInfo;
    }

    public IEntryHolder getBlobStoreOpEntryHolder() {
        return _blobStoreOpEntryHolder;
    }

    public boolean getBlobStoreOpPin() {
        return _blobStoreOpPin;
    }

    public void setBlobStoreOpParams(IEntryCacheInfo blobStoreEntryCacheInfo, IEntryHolder blobStoreOpEntryHolder, boolean blobStoreOpPin) {
        _blobStoreEntryCacheInfo = blobStoreEntryCacheInfo;
        _blobStoreOpEntryHolder = blobStoreOpEntryHolder;
        _blobStoreOpPin = blobStoreOpPin;
    }
    //---------------------------------------------------------------------------------------blobStore related ------------------

    public boolean isIndexUsed() {
        return _indexUsed;
    }

    public void setIndexUsed(boolean val) {
        _indexUsed = val;
    }

    public void setInitialLoadInfo(InitialLoadInfo ili) {
        _initialLoadInfo = ili;
    }

    public InitialLoadInfo getInitialLoadInfo() {
        return _initialLoadInfo;
    }

    public BlobStoreBulkInfo getBlobStoreBulkInfo() {
        return _blobStoreBulkInfo;
    }

    public boolean isActiveBlobStoreBulk() {
        return (_blobStoreBulkInfo != null && _blobStoreBulkInfo.isActive());
    }

    public void setBlobStoreBulkInfo(BlobStoreBulkInfo bi) {
        _blobStoreBulkInfo = bi;
    }

    public void setBlobStoreUsePureIndexesAccess(boolean val) {
        _blobStoreUsePureIndexesAccess = val;
    }

    public boolean isBlobStoreUsePureIndexesAccess() {
        return _blobStoreUsePureIndexesAccess;
    }

    public boolean isBlobStoreTryNonPersistentOp() {
        return _blobStoreTryNonPersistentOp;
    }

    public void setBlobStoreTryNonPersistentOp(boolean val) {
        _blobStoreTryNonPersistentOp = val;
    }

    public BlobStorePreFetchBatchResult getBlobStorePreFetchBatchResult() {
        return _blobStorePreFetchBatchResult;

    }

    public void setBlobStorePreFetchBatchResult(BlobStorePreFetchBatchResult r) {
        _blobStorePreFetchBatchResult = r;
    }

    public boolean isDisableBlobStorePreFetching() {
        return _disableBlobStorePreFetching;
    }

    public void setDisableBlobStorePreFetching(boolean val) {
        _disableBlobStorePreFetching = val;
    }


    public boolean isDelayedReplicationForbulkOpUsed() {
        return _delayedReplicationForbulkOpUsed;
    }

    public void setDelayedReplicationForbulkOpUsed(boolean delayedReplicationForbulkOpUsed) {
        _delayedReplicationForbulkOpUsed = delayedReplicationForbulkOpUsed;
    }


    public IEntryHolder getBlobStoreEntry() {
        return _blobStoreEntry;
    }

    public IEntryData getOriginalData() {
        return _originalData;
    }

    public Collection<SpaceEntryMutator> getMutators() {
        return _mutators;
    }

    public SpaceEngine.EntryRemoveReasonCodes getRemoveReason() {
        return _removeReason;
    }

    public void setForBulkInsert(IEntryHolder eh) {
        _blobStoreEntry = eh;
    }

    public void setForBulkUpdate(IEntryHolder eh, IEntryData originalData, Collection<SpaceEntryMutator> mutators) {
        _blobStoreEntry = eh;
        _originalData = originalData;
        _mutators = mutators;
    }

    public void setForBulkRemove(IEntryHolder eh, SpaceEngine.EntryRemoveReasonCodes removeReason) {
        _blobStoreEntry = eh;
        _removeReason = removeReason;
    }

    public void setBlobStoreOriginalEntryInfo(IEntryData originalData, short originalBlobStoreVersion) {
        _originalData = originalData;
        _originalBlobStoreVersion = originalBlobStoreVersion;
    }

    public short getOriginalBlobStoreVersion() {
        return _originalBlobStoreVersion;
    }

    /**
     * fills the template with the answer to be sent to the client. if template is in callbackmode
     * and main thread has returned then performs a callback to the client. <p/> this method MUST be
     * called under template lock. after this method is called the template should be removed from
     * the cache.
     */
    public void setOperationAnswer(ITemplateHolder template, IEntryPacket entryPacket, Exception ex) {
        setOpResultByThread(true);

        if (!isMultipleOperation() && !template.isMultipleIdsOperation() && template.getXidOriginated() != null)
            template.getXidOriginated().decrementUsed(template.isUpdateOperation()); //xtn not used by this thread


        // determine callback mode - we first try returning the result
        // through the waiting Receiver thread, if it is still waiting
        IEntryPacket epToReturn = entryPacket;
        if (entryPacket != null && template.isReturnOnlyUid())
            epToReturn = TemplatePacketFactory.createUidResponsePacket(entryPacket.getUID(), entryPacket.getVersion());

        boolean immidiateAwnswerSet = template.getExpirationTime() == 0 || !template.isInCache();

        //IF-EXISTS blocking execption
        if (ex == null && entryPacket == null && Modifiers.contains(template.getOperationModifiers(), Modifiers.IF_EXISTS)) {//note- currently only readIE and takeIE (single-ops)
            boolean setBlockingException = false;
            if (!immidiateAwnswerSet) {
                if (template.getEntriesWaitingForTemplate() != null && !template.getEntriesWaitingForTemplate().isEmpty())
                    setBlockingException = true;
            } else
                setBlockingException = isPossibleIEBlockingMatch();
            if (setBlockingException)
                ex = _entryLockedException;
        }


        AnswerHolder answer = template.getAnswerHolder();
        answer.m_Exception = ex;
        if (!template.isBatchOperation()) {
            if (template.isUpdateOperation())
                answer.m_AnswerPacket = new AnswerPacket(epToReturn, _writeResult);
            else
                answer.m_AnswerPacket = new AnswerPacket(epToReturn);
        } else
            answer.m_AnswerPacket = AnswerPacket.NullPacket;

        if (template.isBatchOperation() && ex == null && !template.isChange())
            answer.setEntryPackets(template.getBatchOperationContext().processReturnedValueForBatchOperation(template));


        //call after-op filter if applicable
        if (template.getAfterOpFilterCode() >= 0 && template.getFilterManager()._isFilter[template.getAfterOpFilterCode()]) {
            if (entryPacket != null) {
                //protect against nullifying of properties by filters
                if (entryPacket.hasFixedPropertiesArray() && !entryPacket.allNullFieldValues()) {
                    Object[] src = entryPacket.getFieldValues();
                    Object[] target = new Object[src.length];
                    System.arraycopy(src, 0, target, 0, src.length);
                    entryPacket.setFieldsValues(target);
                }
                try {
                    if (template.isUpdateOperation()) {
                        if (template.isChange()) {
                            template.getFilterManager().invokeFilters(template.getAfterOpFilterCode(), template.getSpaceContext(), template);
                        } else {
                            IEntryPacket[] toFilterObjects = new IEntryPacket[2];
                            toFilterObjects[0] = entryPacket; //returned (old) value
                            toFilterObjects[1] = template.getUpdateOperationEntry();
                            template.getFilterManager().invokeFilters(FilterOperationCodes.AFTER_UPDATE, template.getSpaceContext(), toFilterObjects);
                        }
                    } else
                        template.getFilterManager().invokeFilters(template.getAfterOpFilterCode(), template.getSpaceContext(), entryPacket);
                } catch (Exception exp) {//filter execption-throw to user instead of answer
                    answer.m_Exception = exp;
                    answer.m_AnswerPacket.m_EntryPacket = null;
                }
            } else {//batch op filter
                if (template.isBatchOperation() && ex == null && template.getBatchOperationContext().hasAnyEntries() && !template.isInitiatedEvictionOperation() && template.getBatchOperationContext().getResults() != null) {
                    for (IEntryPacket curep : template.getBatchOperationContext().getResults()) {
                        if (curep != null && curep.hasFixedPropertiesArray() && !curep.allNullFieldValues()) {
                            Object[] src = curep.getFieldValues();
                            Object[] target = new Object[src.length];
                            System.arraycopy(src, 0, target, 0, src.length);
                            curep.setFieldsValues(target);
                        }
                        if (!template.isChange())
                            template.getFilterManager().invokeFilters(template.getAfterOpFilterCode(), template.getSpaceContext(), curep);
                        else {
                            //Hack in order to pass into the filter the mutators and the previous entry, we set the template answer holder
                            //so it will propogate into the filter manager, and restore it afterwards
                            AnswerPacket prevAnswerPacket = answer.m_AnswerPacket;
                            AnswerHolder prevAnswerHolder = template.getAnswerHolder();
                            answer.m_AnswerPacket = new AnswerPacket(curep);
                            template.setAnswerHolder(answer);
                            try {
                                template.getFilterManager().invokeFilters(template.getAfterOpFilterCode(), template.getSpaceContext(), template);
                            } finally {
                                answer.m_AnswerPacket = prevAnswerPacket;
                                template.setAnswerHolder(prevAnswerHolder);
                            }
                        }
                    }
                }
            }
        }


        if (template.isMultipleIdsOperation()) {//update multiple/ updateOrWrite multiple
            if (!template.getMultipleIdsContext().isAnswerReady()) {
                //should we retry for this entry (done in case of EntryNotInSpaceException)
                if (ex != null && template.getMultipleIdsContext().isUpdateOrWriteMultiple() && template.getMultipleIdsContext().shouldRetryUpdateOrWriteForEntry(ex, template, template.getUpfdateOrWriteContext(), template.isInCache()/*spawnThread*/))
                    return;
                if (!template.getMultipleIdsContext().setAnswer(answer, template.getOrdinalForEntryByIdMultipleOperation()))
                    return;
                if (template.getXidOriginated() != null)
                    template.getXidOriginated().decrementUsed(true); //xtn not used
            }
        }

        if (isActiveBlobStoreBulk() && !_fromReplication)
        //if off-heap in case of bulk we first need to flush to the blob-store
        {
            try {
                getBlobStoreBulkInfo().bulk_flush(this, false/*only_if_chunk_reached*/, true);
            } catch (Exception ex1) {
                ex = ex1;
            }
        }


        if (template.isMultipleIdsOperation()) {//finished with all updates, set answer and template
            template = template.getMultipleIdsContext().getConcentratingTemplate();
            answer = template.getAnswerHolder();
            immidiateAwnswerSet = template.getExpirationTime() == 0;
        }

        if (entryPacket == null && template.isSetSingleOperationExtendedErrorInfo()) {//In case of failures (locked entry or failed change op) set it in the answer-holder
            ExtendedAnswerHolder eah = (ExtendedAnswerHolder) template.getAnswerHolder();
            if (template.getRejectedOpOriginalException() != null) {
                eah.m_Exception = (Exception) template.getRejectedOpOriginalException();
                eah.setRejectedEntry(template.getRejectedOperationEntry());
            } else if (ex == null) {//check if its a situation of expiration because of transaction blocking
                if (template.getEntriesWaitingForTemplate() != null && !template.getEntriesWaitingForTemplate().isEmpty()) {
                    for (IEntryHolder eh : template.getEntriesWaitingForTemplate()) {//currently only one
                        //a blocking xtn caused time expiration. set execption
                        eah.setRejectedEntry(eh.getEntryData());
                        eah.m_Exception = _operationTimeoutExecption;
                        break;
                    }
                }
            }
        }
        //cater for batch-change- build execptions for waiting entries
        if (ex == null && template.isChangeMultiple() && template.getExpirationTime() != 0) {
            if (template.getEntriesWaitingForTemplate() != null && !template.getEntriesWaitingForTemplate().isEmpty()) {
                ExtendedAnswerHolder eah = (ExtendedAnswerHolder) template.getAnswerHolder();
                for (IEntryHolder eh : template.getEntriesWaitingForTemplate()) {//currently only one
                    //a blocking xtn caused time expiration. set execption
                    eah.addRejectedEntriesInfo(_operationTimeoutExecption, eh.getEntryData(), eh.getUID());
                }
            }

        }


        if (!immidiateAwnswerSet) {
            IResponseContext respContext = template.getResponseContext();
            if (respContext != null) {
                // to avoid sending the response twice
                // response is sent only if not from first phase i.e.
                // not user calling thread.
                if (template.isSecondPhase()) {
                    if (answer.m_AnswerPacket.m_EntryPacket != null)
                        SpaceImpl.applyEntryPacketOutFilter(answer.m_AnswerPacket.m_EntryPacket, template.getOperationModifiers(), template.getProjectionTemplate());
                    if (template.isBatchOperation() && !template.isChange()) {
                        if (ex == null && template.getBatchOperationContext().hasAnyEntries()) {
                            IEntryPacket[] entryPackets = template.getAnswerHolder().getEntryPackets();
                            for (IEntryPacket packet : entryPackets) {
                                SpaceImpl.applyEntryPacketOutFilter(packet, template.getOperationModifiers(), template.getProjectionTemplate());
                            }
                        }
                        if (ex != null && ex instanceof BatchQueryException) {
                            BatchQueryException batchEx = (BatchQueryException) ex;
                            if (batchEx.getResults() != null) {
                                for (Object result : batchEx.getResults()) {
                                    if (result != null && result instanceof IEntryPacket)
                                        SpaceImpl.applyEntryPacketOutFilter((IEntryPacket) result,
                                                template.getOperationModifiers(), null);
                                }
                            }
                        }
                    }

                    if (respContext.isInvokedFromNewRouter()) {
                        if (template.isChange()) {
                            ChangeEntriesSpaceOperationResult result = new ChangeEntriesSpaceOperationResult();
                            if (template.isChangeById()) {
                                ChangeEntriesSpaceOperation.setResult(result,
                                        (ExtendedAnswerHolder) answer,
                                        template.getOperationModifiers(),
                                        template.getUidToOperateBy());
                            } else
                            //batch change
                            {
                                ChangeEntriesSpaceOperation.setMultipleEntriesResult(result, (ExtendedAnswerHolder) answer,
                                        template.getOperationModifiers());
                            }

                            respContext.sendResponse(result, null);
                        } else if (template.isUpdateOperation()) {
                            if (!template.isMultipleIdsOperation()) {
                                WriteEntryResult result = answer.m_AnswerPacket != null ? answer.m_AnswerPacket.getWriteEntryResult() : null;
                                respContext.sendResponse(new WriteEntrySpaceOperationResult(result, ex), null);
                            } else {   //updateMultiple/UpdateOrWriteMultiple
                                respContext.sendResponse(new WriteEntriesSpaceOperationResult(answer.getUpdateMultipleResult(), ex), null);
                            }
                        } else if (template.isBatchOperation()) {
                            respContext.sendResponse(new ReadTakeEntriesSpaceOperationResult(template.getAnswerHolder().getEntryPackets(), ex), null);
                        } else {
                            ReadTakeEntrySpaceOperationResult response = new ReadTakeEntrySpaceOperationResult(answer.m_AnswerPacket, ex);
                            respContext.sendResponse(response, null);
                            if (getReplicationContext() != null) {
                                response.setSyncReplicationLevel(getReplicationContext().getCompleted());
                            }
                        }
                    } else
                        respContext.sendResponse(answer.m_AnswerPacket, ex);
                }
            } else {
                if (!isMainThread()) {
                    synchronized (answer) {
                        // now we have the answer lock and the ,
                        // so the main thread is waiting for the result
                        answer.notify();
                    }
                }
            }

        }//if (!skipAnswerTable)
    }

    public void setSyncHybridOperationDetails(SyncHybridOperationDetails[] syncHybridOperationsDetails) {
        _syncHybridOperationsDetails = syncHybridOperationsDetails;
    }

    public SyncHybridOperationDetails[] getSyncHybridOperationDetails() {
        return _syncHybridOperationsDetails;
    }

    public Boolean getOptimizedBlobStoreReadEnabled() {
        return _optimizedBlobStoreReadEnabled;
    }

    public void setOptimizedBlobStoreReadEnabled(boolean optimizedBlobStoreReadEnabled) {
        this._optimizedBlobStoreReadEnabled = optimizedBlobStoreReadEnabled;
    }

    public boolean isInInitialLoad()
    {
        return _inInitialLoad;
    }
    public void setInInitialLoad(boolean val)
    {
        _inInitialLoad = val;
    }

    public boolean isInMemoryRecovery() {
        return _inMemoryRecovery;
    }

    public void setInMemoryRecovery(boolean inMemoryRecovery) {
        this._inMemoryRecovery = inMemoryRecovery;
    }

    public IndexMetricsContext getIndexMetricsContext() {
        return indexMetricsContext;
    }

    public void setIndexMetricsContext(IndexMetricsContext indexMetricsContext) {
        this.indexMetricsContext = indexMetricsContext;
    }

    public void setBackupOnly() {
        this._backupOnly = true;
    }

    public boolean isBackupOnly(){
        return this._backupOnly;
    }

    public IEntryData getViewEntryData(IEntryData entryData) {
        if(entryData instanceof BinaryEntryData){
            if(viewEntryData == null){
                viewEntryData = new ViewEntryData();
            }
            viewEntryData.view(entryData);
            return viewEntryData;
        } else {
            return entryData;
        }
    }

    public IEntryData getCacheViewEntryDataIfNeeded(IEntryData entryData) {
        if(entryData instanceof BinaryEntryData && viewEntryData != null && viewEntryData.isViewOf(entryData)){
            return viewEntryData;
        }
        return entryData;
    }

    public void cacheViewEntryDataIfNeeded(IEntryData entryData, Object[] fieldsValues) {
        if(entryData instanceof BinaryEntryData){
            if(viewEntryData == null){
                viewEntryData = new ViewEntryData();
            }
            viewEntryData.view(entryData, fieldsValues);
        }
    }
}