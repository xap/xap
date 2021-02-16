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


package com.j_spaces.kernel.locks;

import com.gigaspaces.internal.server.space.SpaceConfigReader;

import static com.j_spaces.core.Constants.TieredStorage.CACHE_MANAGER_TIERED_STORAGE_LOCKS_SIZE_DEFAULT;
import static com.j_spaces.core.Constants.TieredStorage.CACHE_MANAGER_TIERED_STORAGE_LOCKS_SIZE_PROP;


@com.gigaspaces.api.InternalApi
public class TieredStorageLockManager<T extends ISelfLockingSubject>
        implements IBasicLockManager<T> {
    private static class LockObject implements ILockObject {
        /**
         * returns true if the lock object is the subject itself (i.e. entry or template) or a
         * representing object
         *
         * @return true if is the subject itself
         */
        public boolean isLockSubject() {
            return false;
        }
    }

    private final LockObject[] _locks;

    public TieredStorageLockManager(SpaceConfigReader configReader) {
        int size = configReader.getIntSpaceProperty(CACHE_MANAGER_TIERED_STORAGE_LOCKS_SIZE_PROP,
                CACHE_MANAGER_TIERED_STORAGE_LOCKS_SIZE_DEFAULT);

        _locks = new LockObject[size];
        for (int i = 0; i < size; i++)
            _locks[i] = new LockObject();
    }

    /*
     * @see com.j_spaces.kernel.locks.IBasicLockManager#getLockObject(java.lang.Object)
     */
    public ILockObject getLockObject(T subject) {
        return getLockObject_impl(subject.getUID());
    }

    /*
     * @see com.j_spaces.kernel.locks.IBasicLockManager#getLockObject(java.lang.Object, java.lang.Object, boolean)
     */
    public ILockObject getLockObject(T subject, boolean isEvictable) {
        if (!isEvictable)
            return subject; //nothing to do, return the subject

        return getLockObject_impl(subject.getUID());
    }

    /**
     * based only on subject's uid, return a lock object in order to lock the represented subject
     * this method is relevant only for evictable objects
     *
     * @return the lock object
     */
    public ILockObject getLockObject(String subjectUid) {
        return getLockObject_impl(subjectUid);
    }


    private ILockObject getLockObject_impl(String subjectUid) {
        return _locks[Math.abs(subjectUid.hashCode() % _locks.length)];
    }

    /*
     * @see com.j_spaces.kernel.locks.IBasicLockManager#freeLockObject(com.j_spaces.kernel.locks.ILockObject)
     */
    public void freeLockObject(ILockObject lockObject) {
        return;
    }

    /**
     * do we use subject for locking itself ?
     *
     * @param isEvictable - is subject evictable
     * @return true if we use subject
     */
    public boolean isPerLogicalSubjectLockObject(boolean isEvictable) {
        return !isEvictable;
    }
}
