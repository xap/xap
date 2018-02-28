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

import com.gigaspaces.annotation.pojo.SpaceProperty.IndexType;
import com.gigaspaces.metadata.index.SpaceIndexType;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public abstract class IndexTypeHelper {
    private static final byte CODE_NOT_SET = -1;
    private static final byte CODE_NONE = 0;
    private static final byte CODE_BASIC = 1;
    private static final byte CODE_EXTENDED = 2;

    public static void writeExternal(ObjectOutput out, IndexType value)
            throws IOException {
        byte code = CODE_NOT_SET;

        if (value != null) {
            switch (value) {
                case NONE:
                    code = CODE_NONE;
                    break;
                case BASIC:
                    code = CODE_BASIC;
                    break;
                case EXTENDED:
                    code = CODE_EXTENDED;
                    break;
                default:
                    code = CODE_NOT_SET;
                    break;
            }
        }
        out.writeByte(code);
    }

    public static IndexType readExternal(ObjectInput in)
            throws IOException {
        byte code = in.readByte();
        switch (code) {
            case CODE_NOT_SET:
                return IndexType.NOT_SET;
            case CODE_NONE:
                return IndexType.NONE;
            case CODE_BASIC:
                return IndexType.BASIC;
            case CODE_EXTENDED:
                return IndexType.EXTENDED;
            default:
                return IndexType.NOT_SET;
        }
    }

    public static IndexType fromOld(boolean isIndex) {
        return isIndex ? IndexType.BASIC : IndexType.NONE;
    }

    public static SpaceIndexType[] fromOld(boolean[] indexIndicators) {
        if (indexIndicators == null)
            return null;

        SpaceIndexType[] indexTypes = new SpaceIndexType[indexIndicators.length];
        for (int i = 0; i < indexTypes.length; i++)
            indexTypes[i] = indexIndicators[i] ? SpaceIndexType.EQUAL : SpaceIndexType.NONE;

        return indexTypes;
    }
}
