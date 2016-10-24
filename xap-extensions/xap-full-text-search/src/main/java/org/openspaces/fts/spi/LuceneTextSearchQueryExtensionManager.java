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

package org.openspaces.fts.spi;

import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.query.extension.QueryExtensionEntryIterator;
import com.gigaspaces.query.extension.QueryExtensionManager;
import com.gigaspaces.query.extension.QueryExtensionRuntimeInfo;
import com.gigaspaces.server.SpaceServerEntry;

import org.openspaces.spatial.lucene.common.spi.BaseLuceneQueryExtensionManager;

import java.util.logging.Logger;

/**
 * @author yechielf
 * @since 11.0
 */
public class LuceneTextSearchQueryExtensionManager extends BaseLuceneQueryExtensionManager {
    private static final Logger _logger = Logger.getLogger(LuceneTextSearchQueryExtensionManager.class.getName());

    protected LuceneTextSearchQueryExtensionManager(LuceneTextSearchQueryExtensionProvider provider, QueryExtensionRuntimeInfo info) {
        super(provider, info);
    }


    @Override
    public boolean accept(String operation, Object leftOperand, Object rightOperand) {
        return false;
    }

    @Override
    public boolean insertEntry(SpaceServerEntry entry, boolean hasPrevious) {
        return false;
    }

    @Override
    public void removeEntry(SpaceTypeDescriptor typeDescriptor, String uid, int version) {

    }

    @Override
    public QueryExtensionEntryIterator queryByIndex(String typeName, String path, String operation, Object operand) {
        return null;
    }
}
