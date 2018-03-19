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

package com.gigaspaces.internal.server.space;

import com.gigaspaces.internal.utils.collections.CopyOnUpdateMap;
import com.gigaspaces.time.SystemTime;

import java.util.concurrent.atomic.AtomicLong;

@com.gigaspaces.api.InternalApi
public class SpaceUidFactory {
    private static final boolean DISABLE_UID_SUFFIX = Boolean.getBoolean("com.gs.disable-uid-suffix");
    public static final String SUFFIX = DISABLE_UID_SUFFIX ? "" : "^0^0";
    private static final int MAX_NUM_CLASSES_CACHED = 500;
    public static final char SEPARATOR = '^';
    public static final String PREFIX_AUTO = "A";
    public static final String PREFIX_MANUAL = "M";

    private final String _memberId;
    private final AtomicLong _counter;
    private String _timeStamp;
    private String _prefix;

    private final CopyOnUpdateMap<String, String> _typeUidFactoryCache;

    public SpaceUidFactory() {
        this(null);
    }

    public SpaceUidFactory(String memberId) {
        _memberId = memberId != null ? memberId : "0";
        _counter = new AtomicLong();
        _typeUidFactoryCache = new CopyOnUpdateMap<String, String>();
        reset();
    }

    private void reset() {
        synchronized (this) {
            if (_counter.get() > 0)
                return;

            _timeStamp = Long.toString(SystemTime.timeMillis());
            _prefix = PREFIX_AUTO + _memberId + SEPARATOR + _timeStamp + SEPARATOR;
            _counter.set(0);
        }
    }

    private long getNextId() {
        long id = _counter.incrementAndGet();
        if (id > 0)
            return id;

        reset();
        return getNextId();
    }

    public String getTimeStamp() {
        return _timeStamp;
    }

    public String createUIDFromCounter() {
        return Long.toString(getNextId());
    }

    public String generateUid() {
        final long id = getNextId();
        // NOTE: This is currently the most efficient way known to concatenate two strings. 
        return _prefix.concat(Long.toString(id));
    }

    public String createUidFromTypeAndId(String typeName, String id, boolean validate) {
        if (validate && id.indexOf(SEPARATOR) != -1)
            throw new RuntimeException("Invalid UID creation request: UID can not contains the character '" + SEPARATOR + "'.");

        String prefix = _typeUidFactoryCache.get(typeName);
        if (prefix == null) {
            prefix = generateTypePrefix(typeName);
            if (_typeUidFactoryCache.size() > MAX_NUM_CLASSES_CACHED)
                _typeUidFactoryCache.clear();
            _typeUidFactoryCache.put(typeName, prefix);
        }

        return generateUid(prefix, id);
    }

    public static Integer extractPartitionId(String uid) {
        if (uid == null || uid.length() < 2)
            return null;

        int result = 0;
        int pos = 1;
        for (char c = uid.charAt(pos++); c >= '0' && c <= '9'; c = uid.charAt(pos++))
            result = result * 10 + c - '0';

        return result == 0 ? null : result - 1;
    }

    public static String generateUid(String typePrefix, String id) {
        // NOTE: This is currently the most efficient way known to concatenate two strings.
        return typePrefix.concat(id).concat(SUFFIX);
    }

    public static String generateTypePrefix(String typeName) {
        return "" + typeName.hashCode() + SEPARATOR + typeName.length() + SEPARATOR;
    }
}
