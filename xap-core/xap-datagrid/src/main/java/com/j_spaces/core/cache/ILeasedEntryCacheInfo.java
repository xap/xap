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

import com.j_spaces.kernel.IObjectInfo;
import com.j_spaces.kernel.IStoredList;

/*
 * TODO add Javadoc
 * @author Yechiel Fefer
 * @version 1.0
 * @since 5.0
 */

/**
 * methods for storing-retrieving lease cache info of entry
 */
public interface ILeasedEntryCacheInfo {
    public void setLeaseManagerListRefAndPosition(IStoredList<Object> entriesList, IObjectInfo<Object> entryPos);

    public IStoredList<Object> getLeaseManagerListRef();

    public IObjectInfo<Object> getLeaseManagerPosition();

    public boolean isConnectedToLeaseManager();

    public boolean isSameLeaseManagerRef(ILeasedEntryCacheInfo other);

    public boolean isBlobStoreEntry();

    public Object getObjectStoredInLeaseManager();
}