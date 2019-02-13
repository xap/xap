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
package org.openspaces.pu.container.servicegrid;

import com.gigaspaces.api.InternalApi;
import com.j_spaces.core.client.FinderException;
import com.j_spaces.core.client.LookupFinder;
import com.j_spaces.core.client.LookupRequest;

/**
 * @author Niv Ingberg
 * @since 14.2
 */
@InternalApi
public class PUFinder {

    public static LookupRequest puLookupRequest() {
        return new LookupRequest(PUServiceBean.class);
    }

    public static LookupRequest puLookupRequest(String name) {
        return puLookupRequest().setServiceName(name);
    }

    public static PUServiceBean find(String name) throws FinderException {
        return find(puLookupRequest(name));
    }

    public static PUServiceBean find(LookupRequest request) throws FinderException {
        return (PUServiceBean) LookupFinder.find(request);
    }
}
