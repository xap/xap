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

package com.gigaspaces.internal.metadata;

import com.gigaspaces.metadata.index.SpaceIndexType;

@com.gigaspaces.api.InternalApi
public class SpaceIndexTypeHelper {
    private static final byte NOT_SET_CODE = -1;
    private static final byte NONE_CODE = 0;
    private static final byte EQAL_CODE = 1;
    private static final byte EQUAL_AND_ORDERED_CODE = 2;
    private static final byte GEOSPATIAL_CODE = 3;
    private static final byte ORDERED_CODE = 4;

    public static byte toCode(SpaceIndexType spaceIndexType) {
        if (spaceIndexType == null)
            return NOT_SET_CODE;

        switch (spaceIndexType) {
            case NONE:
                return NONE_CODE;
            case BASIC:
            case EQUAL:
                return EQAL_CODE;
            case EXTENDED:
            case EQUAL_AND_ORDERED:
                return EQUAL_AND_ORDERED_CODE;
            case ORDERED:
                return ORDERED_CODE;
            default:
                throw new IllegalArgumentException("Unsupported space index type: " + spaceIndexType);
        }
    }

    public static SpaceIndexType fromCode(byte code) {
        switch (code) {
            case NOT_SET_CODE:
                return null;
            case NONE_CODE:
                return SpaceIndexType.NONE;
            case EQAL_CODE:
                return SpaceIndexType.EQUAL;
            case EQUAL_AND_ORDERED_CODE:
                return SpaceIndexType.EQUAL_AND_ORDERED;
            case ORDERED_CODE:
                return SpaceIndexType.ORDERED;
            default:
                throw new IllegalArgumentException("Unsupported space index type code: " + code);
        }
    }
}
