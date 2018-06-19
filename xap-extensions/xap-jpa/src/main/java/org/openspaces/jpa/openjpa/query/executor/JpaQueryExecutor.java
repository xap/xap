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

package org.openspaces.jpa.openjpa.query.executor;

import org.apache.openjpa.lib.rop.ResultObjectProvider;
import org.openspaces.jpa.StoreManager;

/**
 * An interface for JPA's translated expression tree executor.
 *
 * @author idan
 * @since 8.0
 */
public interface JpaQueryExecutor {
    /**
     * Execute the JPA translated expression tree.
     *
     * @param store The store manager.
     * @return Read entries from space.
     */
    public ResultObjectProvider execute(StoreManager store) throws Exception;

    /**
     * Gets the executor's generated SQL buffer.
     */
    public StringBuilder getSqlBuffer();

}
