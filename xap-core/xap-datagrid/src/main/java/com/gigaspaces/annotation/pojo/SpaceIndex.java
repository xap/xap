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


package com.gigaspaces.annotation.pojo;

import com.gigaspaces.metadata.index.SpaceIndexType;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * <pre>
 * Defines a space index.
 * Can be defined on the property getter or as part of @SpaceIndexes when multiple indexes are
 * used.
 *
 * Indexes on nested object properties are defined on the nested object getter.
 *
 * For example:
 *
 * <code>
 *  To index the 'socialSecurity' property using extended indexing:
 *  1. @SpaceIndex(type = IndexType.EXTENDED)
 *     public long getSocialSecurity() {
 * return socialSecurity;
 * }
 *
 * To index 'personalInfo.name':
 *  2. @SpaceIndex(path = "name")
 *     public Info getPersonalInfo() {
 * return personalInfo;
 * }
 *
 * </code>
 * </pre>
 *
 * @author Anna Pavtulov
 * @since 7.1
 */

@Target(METHOD)
@Retention(RUNTIME)
public @interface SpaceIndex {
    public static final String EMPTY = "";

    /**
     * Defines the index property path. The path specifies which property path is indexed. If none
     * is defined - the property itself is indexed.
     */
    String path() default EMPTY;

    /**
     * The type of the index - default is EQUAL index
     */
    SpaceIndexType type() default SpaceIndexType.EQUAL;

    /**
     * Indicates if unique constraint is applied for this index. The constraint is per partition.
     * default is false
     */
    boolean unique() default false;


}
