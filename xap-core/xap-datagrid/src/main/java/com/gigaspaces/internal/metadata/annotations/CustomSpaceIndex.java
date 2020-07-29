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

package com.gigaspaces.internal.metadata.annotations;

import com.gigaspaces.annotation.pojo.SpaceIndexes;
import com.gigaspaces.internal.query.valuegetter.ISpaceValueGetter;
import com.gigaspaces.metadata.index.SpaceIndexType;
import com.gigaspaces.server.ServerEntry;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Defines an annotation for defining custom space indexes. Can be defined on the pojo class or as
 * part of the @SpaceClass(customIndexes) For example:
 *
 * When only one custom index is needed:
 *
 * @author Anna Pavtulov
 * @CustomSpaceIndex(name="index1",type=Basic,indexValueGetter=MySpaceValueGetter.class,unique=false)
 * or
 *
 * When more than one custom index needed:
 * @SpaceClass(customIndexes = {
 * @CustomSpaceIndex(name = "myIndex1", indexValueGetter = MyValueGetter.class),
 * @CustomSpaceIndex(name = "myIndex2", indexValueGetter = MyValueGetter2.class) })
 * @since 7.1
 */

@Target(ElementType.TYPE)
@Retention(RUNTIME)
@Repeatable(value = CustomSpaceIndexes.class)
public @interface CustomSpaceIndex {
    public final static String EMPTY_VALUE = "";

    /**
     * Defines index name. Used to find the right index duting query matching.
     */
    String name() default EMPTY_VALUE;

    /**
     * <pre>
     * Reference to a class that implements {@link ISpaceValueGetter} .
     * The implementation defines how the index value is extracted
     * from the IServerEntry - the space internal object representation
     * </pre>
     */
    Class<? extends ISpaceValueGetter<ServerEntry>> indexValueGetter();

    /**
     * Defines whether this index is unique or not
     */
    boolean unique() default false;

    /**
     * Defines the index type.
     */
    SpaceIndexType type() default SpaceIndexType.EQUAL;
}
