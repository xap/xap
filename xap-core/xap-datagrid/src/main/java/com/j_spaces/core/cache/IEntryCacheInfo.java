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

package com.j_spaces.core.cache;

/*******************************************************************************
 * Copyright (c) 2010 GigaSpaces Technologies Ltd. All rights reserved
 *
 * The software source code is proprietary and confidential information of GigaSpaces. You may use
 * the software source code solely under the terms and limitations of The license agreement granted
 * to you by GigaSpaces.
 *******************************************************************************/
//

import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.server.eviction.EvictableServerEntry;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.kernel.IObjectInfo;
import com.j_spaces.kernel.IStoredList;
import com.j_spaces.kernel.IStoredListIterator;
import com.j_spaces.kernel.list.IScanListIterator;

import java.util.ArrayList;


public interface IEntryCacheInfo extends EvictableServerEntry, IObjectInfo<IEntryCacheInfo>
        , IStoredList<IEntryCacheInfo>, ILeasedEntryCacheInfo, IStoredListIterator<IEntryCacheInfo>, IScanListIterator<IEntryCacheInfo> {
//	static final long serialVersionUID = 3521908341294522947L;

    public static final int UID_HASH_INDICATOR = -1;

    IEntryHolder getEntryHolder(CacheManager cacheManager);

    IEntryHolder getEntryHolder(CacheManager cacheManager, Context context);

    boolean isDeleted();

    public ArrayList<IObjectInfo<IEntryCacheInfo>> getBackRefs();

    void setBackRefs(ArrayList<IObjectInfo<IEntryCacheInfo>> backRefs);

    IObjectInfo<IEntryCacheInfo> getMainListBackRef();

    void setMainListBackRef(IObjectInfo<IEntryCacheInfo> mainListBackref);

    boolean indexesBackRefsKept();

    void setLeaseManagerListRefAndPosition(IStoredList<Object> entriesList, IObjectInfo<Object> entryPos);

    IStoredList<Object> getLeaseManagerListRef();

    IObjectInfo<Object> getLeaseManagerPosition();

    boolean isConnectedToLeaseManager();

    boolean isSameLeaseManagerRef(ILeasedEntryCacheInfo other);

    int getLatestIndexCreationNumber();

    void setLatestIndexCreationNumber(int val);

    String getClassName();

    void setInCache(boolean checkPendingPin);

    boolean setPinned(boolean value, boolean waitIfPendingInsertion);

    boolean setPinned(boolean value);

    boolean isPinned();

    boolean setRemoving(boolean isPinned);

    boolean isRemoving();

    void setRemoved();

    boolean isRemoved();

    boolean isRemovingOrRemoved();

    boolean wasInserted();

    boolean isRecentDelete();

    void setRecentDelete();

    boolean preMatch(Context context, ITemplateHolder template);

    IServerTypeDesc getServerTypeDesc();

    @Override
    default boolean hasSize() { return true; }
    @Override
    default int size() { return 1; }
}