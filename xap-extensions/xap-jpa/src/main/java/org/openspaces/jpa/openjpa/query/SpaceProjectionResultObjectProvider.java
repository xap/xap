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

package org.openspaces.jpa.openjpa.query;

import org.apache.openjpa.lib.rop.ResultObjectProvider;


/**
 * A wrapper for holding JPQL projection query result set (aggregation...)
 *
 * @author idan
 * @since 8.0
 */
public class SpaceProjectionResultObjectProvider implements ResultObjectProvider {
    //
    private Object[][] _result;
    private int _currentIndex;

    public SpaceProjectionResultObjectProvider(Object[][] result) {
        _result = result;
        _currentIndex = -1;
    }

    public boolean absolute(int pos) throws Exception {
        if (pos >= 0 && pos < _result.length) {
            _currentIndex = pos;
            return true;
        }
        return false;
    }

    public void close() throws Exception {
        reset();
    }

    /**
     * Gets the current result as a Pojo initiated with a state manager.
     */
    public Object getResultObject() throws Exception {
        return _result[_currentIndex];
    }

    public void handleCheckedException(Exception e) {
        // openjpa: shouldn't ever happen
        throw new RuntimeException(e);
    }

    public boolean next() throws Exception {
        return absolute(_currentIndex + 1);
    }

    public void open() throws Exception {
    }

    public void reset() throws Exception {
        _currentIndex = -1;
    }

    public int size() throws Exception {
        return _result.length;
    }

    public boolean supportsRandomAccess() {
        return true;
    }

}
