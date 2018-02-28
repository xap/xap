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

import com.gigaspaces.metadata.SpaceDocumentSupport;
import com.gigaspaces.metadata.index.SpaceIndexType;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Defines a method annotation type with full meta-data of POJO fields. The getter method of the
 * POJO object provides a matchable fields for the space. Per getter method you are able to provide
 * the desired annotation.
 *
 * @author Lior Ben Yizhak
 * @since 5.1
 */

@Target(METHOD)
@Retention(RUNTIME)
public @interface SpaceProperty {
    public final static String EMPTY_VALUE = "";

    /**
     * Determines the index type for a specified property.
     */
    @Deprecated
    public enum IndexType {
        /**
         * Indicates no index type was set, and default will be used.
         *
         * @since 7.0.1
         */
        NOT_SET,
        /**
         * Indicates no index should be used.
         */
        NONE,
        /**
         * Indicates basic index should be used. This index supports equality.
         */
        BASIC,
        /**
         * Indicates extended index should be used. This index supports comparison.
         *
         * @since 7.0.1
         */
        EXTENDED;

        /**
         * Returns true if any index other than NONE is set, false otherwise.
         *
         * @return <code>true</code> if any index other than NONE
         * @since 7.0.1
         */
        public boolean isIndexed() {
            return this != NONE;
        }

        public SpaceIndexType toSpaceIndexType() {
            switch (this) {
                case NOT_SET:
                    return null;
                case NONE:
                    return SpaceIndexType.NONE;
                case BASIC:
                    return SpaceIndexType.EQUAL;
                case EXTENDED:
                    return SpaceIndexType.EQUAL_AND_ORDERED;
                default:
                    return null;
            }
        }
    }

    /**
     * Define if this field data is indexed. Querying indexed fields speed up read and take
     * operations.
     *
     * @return index type
     * @deprecated since 7.1 . Use @SpaceIndex instead
     */
    @Deprecated IndexType index() default IndexType.NOT_SET;

    /**
     * To designate primitive types null value a matchable fields for the space. Per getter method
     * you are able to provide the desired annotation Specify a value to be treaded as null value
     */
    String nullValue() default EMPTY_VALUE;

    /**
     * Defines the document support of the annotated property.
     *
     * @since 8.0.1
     */
    SpaceDocumentSupport documentSupport() default SpaceDocumentSupport.DEFAULT;
}
