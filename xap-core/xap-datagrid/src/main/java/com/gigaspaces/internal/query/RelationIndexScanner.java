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

package com.gigaspaces.internal.query;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.j_spaces.core.cache.QueryExtensionIndexManagerWrapper;
import com.j_spaces.core.cache.TypeData;
import com.j_spaces.core.cache.TypeDataIndex;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.kernel.list.IObjectsList;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Scans the index using an external index using one of the relation that the external index is know
 * to handle.
 *
 * @author barak
 * @since 11.0
 */
@com.gigaspaces.api.InternalApi
public class RelationIndexScanner extends AbstractQueryIndex {
    private static final long serialVersionUID = 1L;
    private String typeName;
    private String path;
    private String namespace;
    private String relation;
    private Object subject;


    public RelationIndexScanner() {
        super();
    }

    public RelationIndexScanner(String typeName, String path, String namespace, String relation, Object subject) {
        super(path);
        this.typeName = typeName;
        this.path = path;
        this.namespace = namespace;
        this.relation = relation;
        this.subject = subject;
    }

    @Override
    public IObjectsList getIndexedEntriesByType(Context context, TypeData typeData,
                                                ITemplateHolder template, int latestIndexToConsider) {
        return getEntriesByIndex(context, typeData, null /*index*/, false /*fifoGroupsScan*/);
    }


    @Override
    protected IObjectsList getEntriesByIndex(Context context, TypeData typeData, TypeDataIndex<Object> index, boolean fifoGroupsScan) {
        QueryExtensionIndexManagerWrapper handler = typeData.getCacheManager().getQueryExtensionManager(namespace);
        if (handler != null)
            return handler.scanIndex(typeData.getClassName(), path, relation, subject);
        return IQueryIndexScanner.RESULT_IGNORE_INDEX;
    }

    public boolean requiresOrderedIndex() {
        return false;
    }

    @Override
    protected boolean hasIndexValue() {
        return true;
    }

    public Object getIndexValue() {
        return null;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readExternal(in);
        typeName = IOUtils.readString(in);
        path = IOUtils.readString(in);
        namespace = IOUtils.readString(in);
        relation = IOUtils.readString(in);
        subject = IOUtils.readObject(in);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        IOUtils.writeString(out, typeName);
        IOUtils.writeString(out, path);
        IOUtils.writeString(out, namespace);
        IOUtils.writeString(out, relation);
        IOUtils.writeObject(out, subject);
    }

    public boolean supportsTemplateIndex() {
        return false;
    }
}
