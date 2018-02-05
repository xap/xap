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
package com.j_spaces.core.cache.offHeap.storage;

/**
 * @author kobi on 29/12/16.
 * @since 12.1
 */
public enum InternalCacheControl {
    INSERT_IF_NEEDED_BY_OP,
    INSERT_TO_INTERNAL_CACHE_FROM_INITIAL_LOAD,
    DONT_INSERT_TO_INTERNAL_CACHE_FROM_INITIAL_LOAD
}