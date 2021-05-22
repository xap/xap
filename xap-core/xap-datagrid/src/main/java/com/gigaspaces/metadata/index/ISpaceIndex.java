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
 * <p> Encapsulates information about an index in a space type.
 *
 * Index defines how the objects of this class are stored: Index can be: <li>basic - supports
 * efficiently equality queries. <li>extended - supports range queries
 *
 *
 * <p>Index also defines how the index value is extracted from the given entry.
 *
 * <p>Usage example: <br>
 *
 * <code>@SpaceIndex(path="personalInfo.address") </code>
 *
 * @author Niv Ingberg
 * @since 7.1
 */
public interface ISpaceIndex extends SpaceIndex, SmartExternalizable, ISwapExternalizable {

    //multi-value per entry index types
    public static enum MultiValuePerEntryIndexTypes {
        COLLECTION,
        ARRAY,
        GENERAL //supports per-instance collection or aray (instanceof is used on each op')
    }


    //fifo groups index support type
    public static enum FifoGroupsIndexTypes {
        MAIN,   //main f-g index
        AUXILIARY,   //used in as a leading segment in compound index + the main f-g index
        COMPOUND,   //main + auxiliary will create compound index
        NONE
    }

    //index origin type
    public static enum IndexOriginTypes {
        PROPERTY,
        PATH,
        CUSTOM,
        COLLECTION
    }

    /**
     * sets the unique indicator to the desired value.
     */
    void setUnique(boolean val);


    /**
     * @return the value that will be used to index the data
     */
    Object getIndexValue(ServerEntry entry);

    /**
     * @return the value that will be used for single template value indexing
     */
    Object getIndexValueForTemplate(ServerEntry entry);

    /**
     * @return true if the index is a multi-value index
     */
    boolean isMultiValuePerEntryIndex();

    /**
     * @return the  index origin
     */
    IndexOriginTypes getIndexOriginType();


    /**
     * @return the type of multi-value index
     */

    MultiValuePerEntryIndexTypes getMultiValueIndexType();


    /**
     * @return true if its a compound index
     */
    boolean isCompoundIndex();


    /**
     * @return segments of compound index
     */
    public ISpaceCompoundIndexSegment[] getCompoundIndexSegments();

}
