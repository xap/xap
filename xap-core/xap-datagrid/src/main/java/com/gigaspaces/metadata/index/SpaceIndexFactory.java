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

import com.gigaspaces.internal.metadata.SpaceCollectionIndex;

import java.util.HashSet;

/**
 * Factory which provides methods to create space indexes.
 *
 * @author Niv Ingberg
 * @see com.gigaspaces.metadata.index.SpaceIndex
 * @see com.gigaspaces.metadata.SpaceTypeDescriptor
 * @see com.gigaspaces.metadata.SpaceTypeDescriptorBuilder
 * @since 8.0
 */

public class SpaceIndexFactory {
    /**
     * Creates a space index for the specified property with the specified index type.
     *
     * @param propertyName Name of property to index.
     * @param indexType    type of index.
     * @return A space index for the specified property.
     */
    public static SpaceIndex createPropertyIndex(String propertyName, SpaceIndexType indexType) {
        return createPropertyIndex(propertyName, indexType, false /*unique*/);
    }

    /**
     * Creates a space index for the specified property with the specified index type.
     *
     * @param propertyName Name of property to index.
     * @param indexType    type of index.
     * @param unique       tre if unique index.
     * @return A space index for the specified property.
     */
    public static SpaceIndex createPropertyIndex(String propertyName, SpaceIndexType indexType, boolean unique) {
        return createPathIndex_Impl(propertyName, indexType, unique);
    }

    /**
     * Creates a space index for the specified path with the specified index type.
     *
     * @param path      Path to index.
     * @param indexType type of index.
     * @return A space index for the specified path.
     */
    public static SpaceIndex createPathIndex(String path, SpaceIndexType indexType) {
        return createPathIndex(path, indexType, false /*unique*/);

    }

    /**
     * Creates a space index for the specified path with the specified index type.
     *
     * @param path      Path to index.
     * @param indexType type of index.
     * @param unique    tre if unique index.
     * @return A space index for the specified path.
     */
    public static SpaceIndex createPathIndex(String path, SpaceIndexType indexType, boolean unique) {
        return createPathIndex_Impl(path, indexType, unique);

    }

    public static SpaceIndex createCompoundIndex(String[] paths) {
        return createCompoundIndex(paths, SpaceIndexType.EQUAL, null, false);
    }

    /**
     * Creates a space compound index from  the specified paths with the specified index type.
     *
     * @param indexName - null or a preset name
     */
    public static SpaceIndex createCompoundIndex(String[] paths, SpaceIndexType indexType, String indexName, boolean unique) {
        return createCompoundIndex_Impl(paths, indexType, indexName, unique);
    }


    private static SpaceIndex createPathIndex_Impl(String path, SpaceIndexType indexType, boolean unique) {
        if (path == null || path.length() == 0)
            throw new IllegalArgumentException("Argument cannot be null or empty - 'path'.");
        if (indexType == null)
            throw new IllegalArgumentException("Argument cannot be null - 'indexType'.");

        if (path.indexOf(SpaceCollectionIndex.COLLECTION_INDICATOR) != -1) {
            return new SpaceCollectionIndex(path, indexType, unique);
        }
        return new SpacePathIndex(path, indexType, unique);
    }

    private static SpaceIndex createCompoundIndex_Impl(String[] paths, SpaceIndexType indexType, String indexName, boolean unique) {
        if (indexType == null)
            throw new IllegalArgumentException("Argument cannot be null - 'indexType'.");
        ISpaceCompoundIndexSegment[] segments = createCompoundSegmentsDefinition(paths);
        return new CompoundIndex(indexName != null ? indexName : createCompoundIndexName(paths), indexType, segments, unique);
    }


    static ISpaceCompoundIndexSegment[] createCompoundSegmentsDefinition(String[] paths) {
        if (paths == null || paths.length < 2)
            throw new IllegalArgumentException("Argument cannot be null or less than 2 elements - 'paths'.");

        ISpaceCompoundIndexSegment[] segments = new ISpaceCompoundIndexSegment[paths.length];
        HashSet<String> pathsNames = new HashSet<String>();
        for (int i = 0; i < paths.length; i++) {
            if (paths[i] == null || paths[i].length() == 0)
                throw new IllegalArgumentException("Argument cannot be null or empty - path number " + (i + 1));
            if (paths[i].indexOf(SpaceCollectionIndex.COLLECTION_INDICATOR) != -1)
                throw new IllegalArgumentException("Argument cannot be a collection - path number " + (i + 1));

            if (pathsNames.contains(paths[i]))
                throw new IllegalArgumentException("Duplicate path name in compound index");
            pathsNames.add(paths[i]);
            segments[i] = (paths[i].indexOf(".") != -1) ? new PathCompoundIndexSegment(i + 1, paths[i]) :
                    new PropertyCompoundIndexSegment(i + 1, paths[i]);
        }

        return segments;
    }

    static String createCompoundIndexName(String[] paths) {
        StringBuffer sb = new StringBuffer();
        boolean first = true;
        for (String path : paths) {
            if (!first)
                sb.append("+");
            else
                first = false;
            sb.append(path);
        }
        return sb.toString();
    }

}
