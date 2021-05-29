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
package com.gigaspaces.internal.quiesce;

import com.gigaspaces.admin.quiesce.QuiesceToken;

/**
 * @author yohanakh
 * @since 14.0.0
 */
@com.gigaspaces.api.InternalApi
public class QuiesceTokenProviderImpl implements QuiesceTokenProvider {

    private volatile QuiesceToken token;

    public QuiesceTokenProviderImpl() {
    }

    public QuiesceToken getToken() {
        return token;
    }

    public void setToken(QuiesceToken token) {
        this.token = token;
    }
}
