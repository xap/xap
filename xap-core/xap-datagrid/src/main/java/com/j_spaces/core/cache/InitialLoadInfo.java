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

//
package com.j_spaces.core.cache;

import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.core.cache.blobStore.sadapter.BlobStoreFifoInitialLoader;

import java.util.LinkedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * cache-manager information about initial load performed
 *
 * @author yechiel
 * @since 10.0
 */

@com.gigaspaces.api.InternalApi
public class InitialLoadInfo {

    private int _foundInDatabase;
    private int _insertedToCache;
    private int _insertedToHotTier;
    private long _recoveryStartTime;
    private long _lastLoggedTime;
    private final Logger _logger;
    private final boolean _logRecoveryProcess;
    private final long _recoveryLogInterval;

    private final LinkedList<String> _initialLoadErrors;

    private volatile BlobStoreFifoInitialLoader _blobStoreFifoInitialLoader;  //used in off heap
    private IServerTypeDesc _curDesc; //used in off heap
    private TypeData _curTypeData;  //used  in off heap

    public InitialLoadInfo(Logger logger, boolean logRecoveryProcess, long recoveryLogInterval) {
        _initialLoadErrors = new LinkedList<String>();
        _recoveryStartTime = SystemTime.timeMillis();
        _lastLoggedTime = _recoveryStartTime;
        _logger = logger;
        _logRecoveryProcess = logRecoveryProcess;
        _recoveryLogInterval = recoveryLogInterval;

    }

    public int getFoundInDatabase() {
        return _foundInDatabase;
    }

    public void setFoundInDatabase(int foundInDatabase) {
        this._foundInDatabase = foundInDatabase;
    }

    public void incrementFoundInDatabase() {
        this._foundInDatabase++;
    }

    public int getInsertedToCache() {
        return _insertedToCache;
    }

    public void setInsertedToCache(int insertedToCache) {
        this._insertedToCache = insertedToCache;
    }

    public void incrementInsertedToCache() {
        this._insertedToCache++;
    }

    public void incrementInsertedToHotTier() {
        this._insertedToHotTier++;
    }

    public int getInsertedToHotTier() {
        return _insertedToHotTier;
    }

    public long getRecoveryStartTime() {
        return _recoveryStartTime;
    }

    public void setRecoveryStartTime(long _recoveryStartTime) {
        this._recoveryStartTime = _recoveryStartTime;
    }

    public long getLastLoggedTime() {
        return _lastLoggedTime;
    }

    public void setLastLoggedTime(long _lastLoggedTime) {
        this._lastLoggedTime = _lastLoggedTime;
    }

    public BlobStoreFifoInitialLoader getBlobStoreFifoInitialLoader() {
        return _blobStoreFifoInitialLoader;
    }

    public void setBlobStoreFifoInitialLoader(BlobStoreFifoInitialLoader il) {
        _blobStoreFifoInitialLoader = il;
    }

    public IServerTypeDesc getCurDesc() {
        return _curDesc;
    }

    public void setCurDesc(IServerTypeDesc _curDesc) {
        this._curDesc = _curDesc;
    }

    public TypeData getCurTypeData() {
        return _curTypeData;
    }

    public void setCurTypeData(TypeData _curTypeData) {
        this._curTypeData = _curTypeData;
    }

    public LinkedList<String> getInitialLoadErrors() {
        return _initialLoadErrors;
    }

    public Logger getLogger() {
        return _logger;
    }

    public boolean isLogRecoveryProcess() {
        return _logRecoveryProcess;
    }

    public long getRecoveryLogInterval() {
        return _recoveryLogInterval;
    }


}
