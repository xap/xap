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

/**
 *
 */
package com.gigaspaces.internal.server.space;

import com.gigaspaces.client.TakeMultipleException;
import com.gigaspaces.internal.transport.ITemplatePacket;

import java.util.Collections;

/**
 * Context for takeMultiple operation - accumulates operation results and exceptions
 *
 * @author anna
 * @since 7.1
 */
@com.gigaspaces.api.InternalApi
public class TakeMultipleContext
        extends BatchQueryOperationContext {

    public TakeMultipleContext(ITemplatePacket template, int maxEntries, int minEntries) {
        super(template, maxEntries, minEntries);

    }

    /**
     * @param t
     */
    public void onException(Throwable t) {
        // create a multiple failure exception
        throw new TakeMultipleException(getResults(), Collections.singletonList(t));
    }

}
