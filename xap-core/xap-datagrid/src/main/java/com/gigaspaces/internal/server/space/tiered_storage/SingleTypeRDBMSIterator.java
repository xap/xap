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
package com.gigaspaces.internal.server.space.tiered_storage;

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.j_spaces.core.sadapter.ISAdapterIterator;
import com.j_spaces.core.sadapter.SAException;

import java.sql.ResultSet;
import java.sql.SQLException;

public class SingleTypeRDBMSIterator implements ISAdapterIterator<IEntryHolder> {

    private final ResultSet resultSet;
    private final String typeName;
    private final SpaceTypeManager typeManager;
    private final ITypeDesc typeDesc;

    public SingleTypeRDBMSIterator(ResultSet resultSet, ITypeDesc typeDesc, SpaceTypeManager typeManager) {
        this.resultSet = resultSet;
        this.typeManager = typeManager;
        this.typeDesc = typeDesc;
        this.typeName = typeDesc.getTypeName();
    }

    @Override
    public IEntryHolder next() throws SAException {
        try {
            if (resultSet.next()) {
                return TieredStorageUtils.getEntryHolderFromRow(typeManager.getServerTypeDesc(typeName), resultSet);
            } else {
                return null;
            }
        } catch (SQLException e) {
            throw new SAException("failed to read row from result set for type " + typeName, e);
        }
    }

    @Override
    public void close() throws SAException {
        try {
            resultSet.close();
        } catch (SQLException e) {
            throw new SAException("failed to close result set for type " + typeName, e);
        }
    }
}
