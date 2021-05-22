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

package com.gigaspaces.metadata.index;

import com.gigaspaces.internal.server.space.redolog.storage.bytebuffer.ISwapExternalizable;
import com.gigaspaces.serialization.SmartExternalizable;
import com.gigaspaces.server.ServerEntry;

/**
 * <p> Encapsulates information about an index segment of a compound index.
 *
 * @author Yechiel Fefer
 * @since 9.5
 */

public interface ISpaceCompoundIndexSegment extends SmartExternalizable, ISwapExternalizable {
    /**
     * @return the value that will be used for this segment using a given entry
     */
    Object getSegmentValue(ServerEntry entry);

    /**
     * @return the value that will be used for this segment using a given template
     */
    Object getSegmentValueForTemplate(ServerEntry entry);


    /**
     * @return the positio0n of this segment in the compound index
     */
    int getSegmentPosition();

    /**
     * @return the path name of this segment
     */
    String getName();

    /**
     * @return true if its a property segment
     */
    boolean isPropertySegment();
}
