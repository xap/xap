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

import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * <pre>
 * Defines a compound space index.
 *
 * Should be defined on the Pojo level
 * Segements of the compound index should be specified. Each segment must be a property or a path
 * within a property
 *
 * Only basic indices are supported
 *
 * For example:
 * The Pojo has 2 properties 'socialSecurity' property and 'personalInfo' which has a 'name'
 * property
 * A compound index can be defined using both properties
 * <code>
 *
 * @CompoundSpaceIndex (paths = {"socialSecurity","personalInfo.name"})
 *
 * </code>
 * </pre>
 *
 * @author Yechiel Fefer
 * @since 9.5
 */
@Target(TYPE)
@Retention(RUNTIME)
@Repeatable(value = CompoundSpaceIndexes.class)
public @interface CompoundSpaceIndex {
    String[] paths();

    /**
     * Indicates if unique constraint is applied for this index. The constraint is per partition.
     * default is false
     */
    boolean unique() default false;

}
