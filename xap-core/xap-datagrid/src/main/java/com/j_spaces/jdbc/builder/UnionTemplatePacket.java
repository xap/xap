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

package com.j_spaces.jdbc.builder;

import com.gigaspaces.internal.client.QueryResultTypeInternal;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.query.*;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.j_spaces.jdbc.JoinedEntry;
import com.j_spaces.jdbc.query.QueryTableData;

import java.util.LinkedList;
import java.util.List;


/**
 * Represents query that is translated to several template packets The result is the union of all
 * results
 *
 * @author anna
 * @since 7.0
 */
@com.gigaspaces.api.InternalApi
public class UnionTemplatePacket extends QueryTemplatePacket {
    private static final long serialVersionUID = 1L;

    private transient List<QueryTemplatePacket> _packets;
    private transient List<Object> _routingValues;

    public UnionTemplatePacket() {
        super();
    }

    /**
     * @param table
     */
    public UnionTemplatePacket(QueryTableData table, QueryResultTypeInternal queryResultType) {
        super(table, queryResultType);
        _packets = new LinkedList<QueryTemplatePacket>();
        _routingValues = new LinkedList<Object>();
    }

    /**
     * @return packets list
     */
    public List<QueryTemplatePacket> getPackets() {
        return _packets;
    }

    /**
     * @param packets the packets to set
     */
    public void setPackets(List<QueryTemplatePacket> packets) {
        _packets = packets;
    }

    public void add(QueryTemplatePacket packet) {
        _packets.add(packet);
    }

    @Override
    public QueryTemplatePacket buildOrPacket(QueryTemplatePacket packet) {
        return packet.union(this);
    }

    @Override
    public QueryTemplatePacket union(QueryTemplatePacket packet) {
        add(packet);
        return this;
    }

    @Override
    public QueryTemplatePacket union(UnionTemplatePacket composite) {
        _packets.addAll(composite.getPackets());
        return this;
    }


    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.QueryTemplatePacket#isComplex()
     */
    @Override
    public boolean isComplex() {
        return false;
    }

    @Override
    public QueryTemplatePacket buildAndPacket(QueryTemplatePacket packet) {
        return packet.and(this);
    }

    /**
     * Merge given templates into one template.
     *
     * for example: x=3 and (y=5 or z=8) ==> (x=3 and y=5) or (x=3 and z=8)
     *
     * @return QueryTemplatePacket
     */
    @Override
    public QueryTemplatePacket and(QueryTemplatePacket template) {

        LinkedList<QueryTemplatePacket> optimizedPackets = new LinkedList<QueryTemplatePacket>();
        for (QueryTemplatePacket packet : getPackets()) {
            QueryTemplatePacket result = new QueryTemplatePacket(packet);

            result.intersectRanges(template);

            if (!result.isAlwaysEmpty())
                optimizedPackets.add(result);
        }

        setPackets(optimizedPackets);

        if (optimizedPackets.isEmpty())
            setAlwaysEmpty(true);
        return this;
    }

    /**
     * Merge given templates into one template. for example: (w=3 or x=5) and (y=5 or z=8) ==> (w=3
     * and (y=5 or z=8)) or (x=5 and (y=5 or z=8)) ==> ((w=3 and y=5) or (w=3 and z=8)) or ((x=5 and
     * y=5) or (x=5 or z=8))
     *
     * @return QueryTemplatePacket
     */
    @Override
    public QueryTemplatePacket and(UnionTemplatePacket template) {
        LinkedList<QueryTemplatePacket> optimizedPackets = new LinkedList<QueryTemplatePacket>();

        for (QueryTemplatePacket packet : getPackets()) {
            for (QueryTemplatePacket packet2 : template.getPackets()) {
                QueryTemplatePacket newPacket = new QueryTemplatePacket(packet);

                newPacket.intersectRanges(packet2);

                if (!newPacket.isAlwaysEmpty())
                    optimizedPackets.add(newPacket);
            }
        }
        setPackets(optimizedPackets);

        if (optimizedPackets.isEmpty())
            setAlwaysEmpty(true);
        return this;
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.QueryTemplatePacket#prepareForSpace(com.j_spaces.core.ITypeDescriptor)
     */
    @Override
    public void prepareForSpace(ITypeDesc typeDesc) {

        // check if the template was already prepared for space format
        if (_preparedForSpace)
            return;

        // no need to prepare if the query is allways empty
        if (isAlwaysEmpty())
            return;
        _allIndexValuesQuery = true;

        List<ICustomQuery> subQueries = new LinkedList<ICustomQuery>();
        CompoundOrIndexScanner indexScanner = new CompoundOrIndexScanner();
        for (QueryTemplatePacket packet : _packets) {
            packet.prepareForUnion(typeDesc);
            _allIndexValuesQuery &= packet.isAllIndexValuesSqlQuery();

            addRouting(packet);

            if (packet.getCustomQuery() != null) {
                subQueries.add(packet.getCustomQuery());

                List<IQueryIndexScanner> customIndexes = packet.getCustomQuery()
                        .getCustomIndexes();
                if (customIndexes != null && !customIndexes.isEmpty())
                    indexScanner.add(new CompoundAndIndexScanner(customIndexes));
            }
        }

        //calc the routing of this packet - routing is set only if all the sub packets use the same routing
        // otherwise broadcast is required
        if (getTypeDescriptor().getRoutingPropertyName() != null) {
            //if not all sub packets have routing - the union packet won't have routing either
            if (_routingValues.size() == _packets.size()) {
                Object lastRouting = _routingValues.get(0);
                for (Object routing : _routingValues) {
                    if (lastRouting != routing)
                        lastRouting = null;
                }
                setPropertyValue(getTypeDescriptor().getRoutingPropertyName(), lastRouting);
            } else {
                _routingValues.clear();
            }

        }

        if (subQueries.size() > 0) {
            CompoundOrCustomQuery orQuery = new CompoundOrCustomQuery(subQueries);

            // add the custom index only if all the sub queries are indexes,
            // otherwise don't use indexes at all since all the entries need to be scanned anyway
            if (indexScanner.getIndexScanners().size() == subQueries.size())
                orQuery.addCustomIndex(indexScanner); //TODO support CombinedOrCustomQuery
            setCustomQuery(orQuery);
        }
        _preparedForSpace = true;
    }

    private void addRouting(QueryTemplatePacket packet) {
        Object routing = packet.getRoutingFieldValue();
        if (routing != null)
            _routingValues.add(routing);
    }

    @Override
    protected boolean matches(IEntryPacket entryPacket) {
        for (QueryTemplatePacket packet : _packets) {
            boolean isInRange = packet.matches(entryPacket);

            if (isInRange)
                return true;
        }
        return false;

    }

    @Override
    public boolean matches(JoinedEntry joinedEntry) {
        for (QueryTemplatePacket packet : _packets) {
            boolean isInRange = packet.matches(joinedEntry);

            if (isInRange)
                return true;
        }
        return false;
    }

//    @Override
//    public void validate() {
//        for(QueryTemplatePacket packet : _packets) {
//            packet.validate();
//        }
//    }
}
