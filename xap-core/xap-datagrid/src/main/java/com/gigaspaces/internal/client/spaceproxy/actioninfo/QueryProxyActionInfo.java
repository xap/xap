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

package com.gigaspaces.internal.client.spaceproxy.actioninfo;

import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.client.spaceproxy.metadata.ObjectType;
import com.gigaspaces.internal.transport.AbstractProjectionTemplate;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.logger.Constants;
import com.j_spaces.core.client.ClientUIDHandler;
import com.j_spaces.core.client.Modifiers;
import com.j_spaces.core.client.ReadModifiers;
import com.j_spaces.core.client.SQLQuery;
import com.j_spaces.jdbc.builder.SQLQueryTemplatePacket;
import com.j_spaces.map.Envelope;
import net.jini.core.transaction.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SubAction that contains most Action parameters and implements ParamProxyAction for access.
 *
 * @author Mordy Arnon Gigaspaces.com
 * @version 1.0
 * @since 6.0
 */
public abstract class QueryProxyActionInfo extends CommonProxyActionInfo {
    protected static final Logger _devLogger = LoggerFactory.getLogger(Constants.LOGGER_DEV);

    protected Object _query;
    protected final ObjectType _queryObjectType;
    public ITemplatePacket queryPacket;
    public final boolean isSqlQuery;

    protected QueryProxyActionInfo(ISpaceProxy spaceProxy, Transaction txn, int modifiers) {
        super(txn, modifiers);
        this._query = null;
        this._queryObjectType = null;
        this.isSqlQuery = false;
    }

    protected QueryProxyActionInfo(ISpaceProxy spaceProxy, Object query, Transaction txn,
                                   int modifiers, boolean requiresOperationId) {
        this(spaceProxy, query, txn, modifiers, requiresOperationId, false);
    }

    protected QueryProxyActionInfo(ISpaceProxy spaceProxy, Object query, Transaction txn,
                                   int modifiers, boolean requiresOperationId, boolean supportIdsQuery) {
        super(txn, modifiers);
        this._query = query;
        if (query instanceof SQLQuery && ((SQLQuery)query).getExplainPlan() != null) {
            super.modifiers = Modifiers.add(super.modifiers, Modifiers.EXPLAIN_PLAN);
        }

        this.isSqlQuery = preProcessQuery();
        this._queryObjectType = ObjectType.fromObject(_query, supportIdsQuery);
        this.queryPacket = spaceProxy.getDirectProxy().getTypeManager().getTemplatePacketFromObject(_query, _queryObjectType);
        if (query instanceof Envelope) {
            Object id = queryPacket.getID();
            if (id != null)
                this.queryPacket.setUID(ClientUIDHandler.createUIDFromName(id, Envelope.class.getName()));
        }
        if (requiresOperationId)
            initOperationId(spaceProxy, queryPacket);
    }

    @Override
    public QueryProxyActionInfo clone() {
        QueryProxyActionInfo copy = (QueryProxyActionInfo) super.clone();
        copy.queryPacket = this.queryPacket.clone();
        return copy;
    }

    private boolean preProcessQuery() {
        if (_query == null)
            return false;

        if (_query instanceof SQLQuery<?>) {
            SQLQuery<?> sqlQuery = (SQLQuery<?>) _query;

            // Use the query is is was defined or no template was provided
            if (sqlQuery.isNullExpression() && sqlQuery.getObject() != null) {
                // Handle SQLQuery with null expression - use the entry instead
                _query = sqlQuery.getObject();
                return false;
            }
            return true;
        }
        if (_query instanceof SQLQueryTemplatePacket)
            return true;

        return false;
    }

    protected void setFifoIfNeeded(ISpaceProxy spaceProxy) {
        boolean isFifo = ReadModifiers.isFifo(modifiers) ||
                spaceProxy.isFifo() ||
                (queryPacket.getTypeDescriptor() != null && queryPacket.getTypeDescriptor().isFifoDefault());
        if (isFifo)
            modifiers = Modifiers.add(modifiers, Modifiers.FIFO);
    }

    public AbstractProjectionTemplate clearProjectionTemplate() {
        AbstractProjectionTemplate projectionTemplate = queryPacket.getProjectionTemplate();
        if (projectionTemplate != null)
            queryPacket.setProjectionTemplate(null);
        return projectionTemplate;
    }

    public Object getQuery() {
        return _query;
    }

    protected boolean verifyLogScannedEntriesCountParams(long timeout) {
        if (timeout > 0 & _devLogger.isTraceEnabled()) {
            _devLogger.trace("Logging scanned entries count is not available for blocking operations, 0 timeout must be used");
            return false;
        }

        return true;
    }
}
