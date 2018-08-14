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

package com.j_spaces.core.filters;

/*******************************************************************************
 * Copyright (c) 2010 GigaSpaces Technologies Ltd. All rights reserved
 *
 * The software source code is proprietary and confidential information of GigaSpaces. You may use
 * the software source code solely under the terms and limitations of The license agreement granted
 * to you by GigaSpaces.
 *******************************************************************************/

@com.gigaspaces.api.InternalApi
public class FilterHolder {

    private boolean _initialized;

    final private FilterProvider _filterProvider;

    /**
     * @param filterProvider
     */
    public FilterHolder(FilterProvider filterProvider) {
        super();
        _filterProvider = filterProvider;
    }

    public ISpaceFilter getFilter() {
        return _filterProvider.getFilter();
    }

    public String getName() {
        return _filterProvider.getName();
    }

    public int[] getOpCodes() {
        return _filterProvider.getOpCodes();
    }

    public int getPriority() {
        return _filterProvider.getPriority();
    }

    public boolean isPrimaryOnly() {
        return !_filterProvider.isActiveWhenBackup();
    }

    public boolean isEnabled() {
        return _filterProvider.isEnabled();
    }

    public boolean isShutdownSpaceOnInitFailure() {
        return _filterProvider.isShutdownSpaceOnInitFailure();
    }

    public String getFilterParam() {
        return _filterProvider.getFilterParam();
    }

    public void setInitialized() {
        _initialized = true;
    }

    /**
     * @return the initialized
     */
    public boolean isInitialized() {
        return _initialized;
    }

    public void close() throws RuntimeException {
        _initialized = false;
        getFilter().close();
    }


    public boolean isPassFilterEntry() {
        return _filterProvider.isPassFilterEntry();
    }


}