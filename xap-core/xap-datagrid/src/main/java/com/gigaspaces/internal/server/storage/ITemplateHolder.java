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
import com.gigaspaces.internal.server.space.BatchQueryOperationContext;
import com.gigaspaces.internal.server.space.FifoSearch;
import com.gigaspaces.internal.server.space.MatchResult;
import com.gigaspaces.internal.server.space.MultipleIdsContext;
import com.gigaspaces.internal.server.space.iterator.ServerIteratorInfo;
import com.gigaspaces.internal.transport.AbstractProjectionTemplate;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.lrmi.nio.IResponseContext;
import com.j_spaces.core.AnswerHolder;
import com.j_spaces.core.OperationID;
import com.j_spaces.core.PendingFifoSearch;
import com.j_spaces.core.SpaceContext;
import com.j_spaces.core.UpdateOrWriteContext;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.cache.TypeData;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.client.SQLQuery;
import com.j_spaces.core.filters.FilterManager;

import java.util.Collection;

/**
 * @author Niv Ingberg
 * @since 7.0
 */
public interface ITemplateHolder extends ISpaceItem, IEntryHolder {

    void setUidToOperateBy(String uid);

    String[] getMultipleUids();

    int getTokenFieldNumber();

    int getTemplateOperation();

    boolean isFifoTemplate();

    boolean isEmptyTemplate();

    boolean isReturnOnlyUid();

    boolean isInCache();

    void setInCache();

    boolean isSecondPhase();

    void setSecondPhase();

    boolean hasAnswer();

    AnswerHolder getAnswerHolder();

    void setAnswerHolder(AnswerHolder answer);

    int getOperationModifiers();

    boolean isMatchByID();

    boolean isReadOperation();

    boolean isUpdateOperation();

    boolean isFifoSearch();

    boolean isIfExist();

    boolean isNotifyTemplate();

    boolean isTakeOperation();

    boolean isInitiatedEvictionOperation();

    boolean isFifoGroupPoll();

    //batch op related methods
    boolean isBatchOperation();

    boolean isReadMultiple();

    boolean isTakeMultiple();

    boolean isChangeMultiple();

    boolean isChangeQuery();

    BatchQueryOperationContext getBatchOperationContext();

    void setBatchOperationContext(BatchQueryOperationContext batchOpContext);

    boolean canFinishBatchOperation();

    //by Ids related methods
    boolean isMultipleIdsOperation();

    MultipleIdsContext getMultipleIdsContext();

    void setMultipleIdsContext(MultipleIdsContext multipleIdsContext);

    boolean isUpdateMultiple();

    void setOrdinalForEntryByIdMultipleOperation(int ordinal);

    int getOrdinalForEntryByIdMultipleOperation();

    UpdateOrWriteContext getUpfdateOrWriteContext();

    void setUpdateOrWriteContext(UpdateOrWriteContext ctx);


    boolean isIdQuery();

    int getFifoThreadPartition();

    void setFifoThreadPartition(int tnum);

    boolean hasExtendedMatchCodes();

    short[] getExtendedMatchCodes();

    Object getRangeValue(int index);

    boolean getRangeInclusion(int index);

    boolean isInitialIfExistSearchActive();

    void setInitialIfExistSearchActive();

    void resetInitialIfExistSearchActive();

    long getFifoXtnNumberOnSearchStart();

    void setFifoXtnNumberOnSearchStart(long latestTTransactionTerminationNum);

    void resetFifoXtnNumberOnSearchStart();

    boolean isInExpirationManager();

    void setInExpirationManager(boolean value);

    boolean isExplicitInsertionToExpirationManager();

    OperationID getOperationID();

    IEntryHolder getUpdatedEntry();

    void setUpdatedEntry(IEntryHolder entryHolder);

    void setReRegisterLeaseOnUpdate(boolean value);

    boolean isReRegisterLeaseOnUpdate();

    QueryResultTypeInternal getQueryResultType();

    String getExternalEntryImplClassName();

    boolean isInitialFifoSearchActive();

    boolean isExclusiveReadLockOperation();

    boolean isWriteLockOperation();

    IResponseContext getResponseContext();

    boolean isDirtyReadRequested();

    boolean isReadCommittedRequested();

    void setInitialFifoSearchActive();

    PendingFifoSearch getPendingFifoSearchObject();

    void setPendingFifoSearchObject(PendingFifoSearch pendingFifoSearch);

    void removePendingFifoSearchObject(boolean value);

    public void setNonBlockingRead(boolean val);

    boolean isNonBlockingRead();

    public boolean isMemoryOnlySearch();

    public void setMemoryOnlySearch(boolean memoryOnly);

    ICustomQuery getCustomQuery();

    long getExpirationTime();

    Object getID();

    void setID(Object id);

    MatchResult match(CacheManager cacheManager, IEntryHolder entry, int skipAlreadyMatchedFixedPropertyIndex, String skipAlreadyMatchedIndexPath, boolean safeEntry, Context context, RegexCache regexCache);

    SQLQuery<?> toSQLQuery(ITypeDesc typeDesc);

    boolean quickReject(Context context, FifoSearch fifoSearch);

    int getPreviousVersion();

    int getAfterOpFilterCode();

    IEntryPacket getUpdateOperationEntry();

    SpaceContext getSpaceContext();

    FilterManager getFilterManager();

    void setForAfterOperationFilter(int afterOpFilterCode, SpaceContext sc, FilterManager fm, IEntryPacket updateOperationEntry);

    boolean isExpirationTimeSet();

    boolean isChange();

    boolean isChangeById();

    void setMutators(Collection<SpaceEntryMutator> mutators);

    Collection<SpaceEntryMutator> getMutators();

    EntryHolderAggregatorContext getAggregatorContext();

    void setAggregatorContext(EntryHolderAggregatorContext aggregatorContext);

    void setChangeExpiration(long expirationTime);

    long getChangeExpiration();

    void setIfExistForChange();

    Throwable getRejectedOpOriginalException();

    void setRejectedOpOriginalExceptionAndEntry(Throwable cause, IEntryData rejectedEntry);

    IEntryData getRejectedOperationEntry();

    boolean isSetSingleOperationExtendedErrorInfo();

    void addEntryWaitingForTemplate(IEntryHolder entry);

    void removeEntryWaitingForTemplate(IEntryHolder entry);

    Collection<IEntryHolder> getEntriesWaitingForTemplate();

    AbstractProjectionTemplate getProjectionTemplate();

    boolean isOptimizedForBlobStoreOp(CacheManager cacheManager);

    boolean isAllValuesIndexSqlQuery();

    boolean isSqlQuery();

    boolean isServerIterator();

    ServerIteratorInfo getServerIteratorInfo();

    void setServerIteratorInfo(ServerIteratorInfo serverIteratorInfo);

    boolean isClear();

    TemplateEntryData getTemplateEntryData();
}
