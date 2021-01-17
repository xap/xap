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


import com.gigaspaces.client.protective.ProtectiveMode;
import com.gigaspaces.client.protective.ProtectiveModeException;
import com.gigaspaces.internal.client.QueryResultTypeInternal;
import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.metadata.EntryType;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.query.CompoundAndCustomQuery;
import com.gigaspaces.internal.query.CompoundContainsItemsCustomQuery;
import com.gigaspaces.internal.query.ExacValueCompoundIndexScanner;
import com.gigaspaces.internal.query.IContainsItemsCustomQuery;
import com.gigaspaces.internal.query.ICustomQuery;
import com.gigaspaces.internal.query.IQueryIndexScanner;
import com.gigaspaces.internal.query.NullValueIndexScanner;
import com.gigaspaces.internal.query.RangeCompoundIndexScanner;
import com.gigaspaces.query.explainplan.ExplainPlan;
import com.gigaspaces.internal.transport.AbstractProjectionTemplate;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.metadata.index.CompoundIndex;
import com.gigaspaces.metadata.index.ISpaceCompoundIndexSegment;
import com.gigaspaces.metadata.index.SpaceIndex;
import com.gigaspaces.metadata.index.SpaceIndexType;
import com.gigaspaces.query.aggregators.AggregationSet;
import com.j_spaces.core.ExternalTemplatePacket;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.client.TemplateMatchCodes;
import com.j_spaces.jdbc.AbstractDMLQuery;
import com.j_spaces.jdbc.AggregationsUtil;
import com.j_spaces.jdbc.JoinedEntry;
import com.j_spaces.jdbc.SQLUtil;
import com.j_spaces.jdbc.builder.range.ContainsItemIntersectionBase;
import com.j_spaces.jdbc.builder.range.ContainsItemValueRange;
import com.j_spaces.jdbc.builder.range.EmptyRange;
import com.j_spaces.jdbc.builder.range.Range;
import com.j_spaces.jdbc.builder.range.RelationRange;
import com.j_spaces.jdbc.query.ArrayListResult;
import com.j_spaces.jdbc.query.IQueryResultSet;
import com.j_spaces.jdbc.query.QueryTableData;

import net.jini.core.entry.UnusableEntryException;
import net.jini.core.transaction.Transaction;
import net.jini.core.transaction.TransactionException;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;


@com.gigaspaces.api.InternalApi
public class QueryTemplatePacket extends ExternalTemplatePacket {
    private static final long serialVersionUID = 1L;

    private QueryTableData _table;
    // template ranges per field
    private HashMap<String, Range> _ranges = new HashMap<String, Range>();
    private boolean _isAlwaysEmpty;
    private Set<String> _multipleUids;
    protected boolean _preparedForSpace;
    private transient Object _routing = null;

    private QueryResultTypeInternal _queryResultType;

    private AbstractProjectionTemplate _projectionTemplate;

    protected AggregationSet _aggregationSet;

    private transient List<IContainsItemsCustomQuery> _containsItemsQueries;
    //true if all the values in the query are indexes, currently used to optimize blob-store count()
    protected boolean _allIndexValuesQuery;

    public static final IQueryIndexScanner _dummyNullIndexScanner = new NullValueIndexScanner();

    private transient ExplainPlan explainPlan;

    public QueryTemplatePacket() {
    }

    public QueryTemplatePacket(QueryTableData table, QueryResultTypeInternal queryResultType) {
        this._table = table;
        super._typeName = table.getTypeDesc() == null ? table.getTableName() : table.getTypeDesc().getTypeName();
        this._queryResultType = queryResultType;
        init(table.getTypeDesc());
    }

    public QueryTemplatePacket(QueryTableData table, QueryResultTypeInternal queryResultType, String fieldName, Range range) {
        this(table, queryResultType);
        _ranges.put(fieldName, range);
    }

    public QueryTemplatePacket(QueryTableData table, QueryResultTypeInternal queryResultType, IContainsItemsCustomQuery containsItemsQuery) {
        this(table, queryResultType);
        if (_containsItemsQueries == null)
            _containsItemsQueries = new ArrayList<IContainsItemsCustomQuery>();
        _containsItemsQueries.add(containsItemsQuery);
    }


    public QueryTemplatePacket(QueryTemplatePacket template) {
        super._typeName = template.getTypeName();
        this._table = template._table;
        this._queryResultType = template.getQueryResultType();
        this._isAlwaysEmpty = template.isAlwaysEmpty();
        this._ranges = new HashMap<String, Range>(template.getRanges());

        if (template.getMultipleUids() != null)
            this._multipleUids = new HashSet<String>(template.getMultipleUids());

        uniteContainsItems(template);
        _typeDesc = template.getTypeDescriptor();
        if (_typeDesc != null)
            init(_typeDesc);
    }

    private void init(ITypeDesc typeDesc) {
        super._typeDesc = typeDesc;
        super._typeDescChecksum = typeDesc.getChecksum();
        super._entryType = typeDesc.getObjectType();
        setFieldsValues(new Object[typeDesc.getNumOfFixedProperties()]);
        _extendedMatchCodes = new short[typeDesc.getNumOfFixedProperties()];
    }


    @Override
    public boolean isTransient() {
        //noinspection SimplifiableConditionalExpression,unchecked
        return _typeDesc == null ? false : _typeDesc.getIntrospector(null).isTransient(null);
    }

    /**
     * Convert the ranges to a single space template
     */
    public void prepareForSpace(ITypeDesc typeDesc) {
        // check if the template was already prepared for space format
        if (_preparedForSpace)
            return;

        if (_typeDesc == null)
            init(typeDesc);
        _allIndexValuesQuery = true;

        if (_multipleUids != null) {
            setMultipleUIDs(_multipleUids.toArray(new String[_multipleUids.size()]));
            _allIndexValuesQuery = false;
        }

        boolean anyCompoundIndexCreated = false;
        List<IQueryIndexScanner> queryIndexes = new LinkedList<IQueryIndexScanner>();

        List<ICustomQuery> customQueries = null;
        HashMap<String, List<Range>> containsSamePathRanges = null;
        HashMap<ContainsItemIntersectionBase, ContainsItemValueRange> containsIntersections;
        if (_containsItemsQueries != null && !_containsItemsQueries.isEmpty()) {
            customQueries = new ArrayList<ICustomQuery>(_containsItemsQueries);
            if (_ranges == null)
                _ranges = new HashMap<String, Range>();
            containsSamePathRanges = new HashMap<String, List<Range>>();
            containsIntersections = new HashMap<ContainsItemIntersectionBase, ContainsItemValueRange>();
            insertContainsItemsRanges(_containsItemsQueries, containsSamePathRanges, null, containsIntersections);  //just for indexing sake, not used as independent ranges
        }


        //the following are used for compound indexes
        Map<Range, IQueryIndexScanner> usedByRanges = null;
        Map<String, Range> possibleCompoundSegments = typeDesc.anyCompoundIndex() && _ranges != null ? new HashMap<String, Range>() : null;


        if (_ranges != null) {
            if (customQueries == null)
                customQueries = new ArrayList<ICustomQuery>();

            for (Map.Entry<String, Range> mapEntry : _ranges.entrySet()) {
                String fieldName = mapEntry.getKey();
                Range range = mapEntry.getValue();

                int propertyIndex = _typeDesc.getFixedPropertyPosition(fieldName);
                boolean addedRange = false;
                if (propertyIndex != -1 || range.isUidsRange()) {
                    if (range.getFunctionCallDescription() == null) {
                        range.toEntryPacket(this, propertyIndex);
                        addedRange = true;
                        if (!range.isRelevantForAllIndexValuesOptimization() || !_typeDesc.getPropertiesIndexTypes()[propertyIndex])
                            _allIndexValuesQuery = false;
                        if (possibleCompoundSegments != null && range.suitableAsCompoundIndexSegment()) {
                            if (usedByRanges == null)
                                usedByRanges = new HashMap<Range, IQueryIndexScanner>();
                            usedByRanges.put(range, _dummyNullIndexScanner); //property range, no index scanner created
                        }
                    }
                    else
                        _allIndexValuesQuery = false;
                }


                if (range.isNestedQuery() || range.isComplex() || (propertyIndex == -1 && _typeDesc.supportsDynamicProperties())) {
                    if (!range.isInternalRange()) {
                        customQueries.add(mapEntry.getValue());
                        addedRange = true;
                    }

                    // path is the index name
                    if (range.isIndexed(_typeDesc) || range.isUidsRange()) {
                        if (!range.isRelevantForAllIndexValuesOptimization()  && !range.isUidsRange())
                            _allIndexValuesQuery = false;
                        IQueryIndexScanner indexScanner = range.getIndexScanner();
                        if (indexScanner != null) {
                            queryIndexes.add(indexScanner);
                            if (possibleCompoundSegments != null && range.suitableAsCompoundIndexSegment()) {
                                if (usedByRanges == null)
                                    usedByRanges = new HashMap<Range, IQueryIndexScanner>();
                                usedByRanges.put(range, indexScanner);
                            }
                        }
                        //in contains item there may be several unrelated ranges with same path
                        //try to derive indices
                        if (containsSamePathRanges != null && containsSamePathRanges.containsKey(range.getPath())) {
                            List<Range> l = containsSamePathRanges.get(range.getPath());
                            for (Range r : l) {
                                if (r == range)
                                    continue;
                                IQueryIndexScanner is = r.getIndexScanner();
                                if (indexScanner != null)
                                    queryIndexes.add(is);
                            }
                        }
                    } else
                        _allIndexValuesQuery = false;
                } else {
                    if (propertyIndex == -1)
                        _allIndexValuesQuery = false;
                }

                if (addedRange && possibleCompoundSegments != null && range.suitableAsCompoundIndexSegment())
                    possibleCompoundSegments.put(mapEntry.getKey(), range);
            }

            //do we have possible compound indices ?
            if (possibleCompoundSegments != null && possibleCompoundSegments.size() > 1) {
                List<Range> possibleSegments = new ArrayList<Range>();
                //go over all compound indices in type and check if we can use some
                for (SpaceIndex idx : typeDesc.getCompoundIndexes()) {
                    possibleSegments.clear();
                    CompoundIndex index = (CompoundIndex) idx;
                    for (ISpaceCompoundIndexSegment seg : index.getCompoundIndexSegments()) {
                        Range checkedRange = possibleCompoundSegments.get(seg.getName());
                        if (checkedRange != null && checkedRange.getFunctionCallDescription() == null) {
                            possibleSegments.add(possibleCompoundSegments.get(seg.getName()));
                        } else {
                            possibleSegments.clear();
                            break;  //not relevant
                        }
                    }
                    if (!possibleSegments.isEmpty())
                    //noinspection SpellCheckingInspection
                    {//maybe we can derive a compound indexscanner out of this.
                        IQueryIndexScanner indexScanner = buildCompoundIndexScannerFromSegmentRanges(index, possibleSegments);
                        if (indexScanner != null) {
                            queryIndexes.add(indexScanner);
                            anyCompoundIndexCreated = true;
                            if (usedByRanges != null && !usedByRanges.isEmpty()) {
                                //use the ranges used to build the compound in order to remove unneeded indexScanners
                                for (Range r : possibleSegments) {
                                    IQueryIndexScanner s = usedByRanges.get(r);
                                    if (s != null) {
                                        if (s == _dummyNullIndexScanner) {//set null to the properties array so no index retrieval + add the range
                                            if (r.isIndexed(_typeDesc)) {
                                                int propertyIndex = _typeDesc.getFixedPropertyPosition(r.getPath());
                                                setFieldValue(propertyIndex, null);
                                                //noinspection deprecation
                                                setExtendedMatchCode(propertyIndex, TemplateMatchCodes.EQ);
                                                customQueries.add(r);
                                            }
                                        } else
                                            queryIndexes.remove(s);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            if (!customQueries.isEmpty() || ((anyCompoundIndexCreated && !queryIndexes.isEmpty()))) {
                CompoundAndCustomQuery customQuery = new CompoundAndCustomQuery(customQueries);

                customQuery.getCustomIndexes().addAll(queryIndexes);
                setCustomQuery(customQuery);
            }
        }

        _preparedForSpace = true;
    }

    private boolean isRangeIndexed(Range range) {
        if (range.isUidsRange())
            return true;
        if (range.getClass().equals(RelationRange.class)) {
            return range.isIndexed(_typeDesc);
        } else //noinspection SimplifiableIfStatement
            if (range.getFunctionCallDescription() != null) {
                return false;
            } else {
                return _typeDesc.getIndexes().containsKey(range.getPath());
            }
    }

    private IQueryIndexScanner buildCompoundIndexScannerFromSegmentRanges(CompoundIndex index, List<Range> possibleSegments) {
        if (possibleSegments.get(0).isEqualValueRange())
            return ExacValueCompoundIndexScanner.build(index.getName(), possibleSegments);
        if (possibleSegments.get(0).isSegmentRange()) {
            if (!index.getIndexType().isOrdered())
                return null;
            Range first = possibleSegments.get(0);
            return RangeCompoundIndexScanner.build(index.getName(), possibleSegments, first.isIndexed(_typeDesc) && _typeDesc.getIndexes().get(first.getPath()).getIndexType().isOrdered());
        }
        return null;
    }

    private void insertContainsItemsRanges(List<IContainsItemsCustomQuery> lq, HashMap<String, List<Range>> containsSamePathRanges, IContainsItemsCustomQuery root, HashMap<ContainsItemIntersectionBase, ContainsItemValueRange> containsIntersections) {//insert contains-items ranges. Not used as independent ranges but just for indexing
        for (IContainsItemsCustomQuery q : lq) {
            if (q.isRootHandler()) {
                insertContainsItemsRanges(((CompoundContainsItemsCustomQuery) q).getSubQueries(), containsSamePathRanges, q, containsIntersections);
            } else {
                ContainsItemValueRange r = (ContainsItemValueRange) q;
                r.setRoot(root);

                //can we intersect for indexing sake ? currently only extended matching is relevant
                if (r.supportsIntersection()) {
                    ContainsItemIntersectionBase ib = new ContainsItemIntersectionBase(r);
                    if (containsIntersections.containsKey(ib)) {
                        if (containsIntersections.get(ib).intersectIfPossible(r))
                            return;   //no need for this range for indexing sake- its intersected
                    } else
                        containsIntersections.put(ib, r);
                }

                if (!_ranges.containsKey(r.getPath()))
                    _ranges.put(r.getPath(), r);

                if (containsSamePathRanges.containsKey(r.getPath())) {
                    List<Range> lr = containsSamePathRanges.get(r.getPath());
                    lr.add(r);
                } else {
                    List<Range> lr = new ArrayList<Range>(2);
                    lr.add(r);
                    containsSamePathRanges.put(r.getPath(), lr);
                }
            }
        }
    }


    /**
     * Temporary method used only for "OR" queries. Should be united with the prepareForSpace
     */
    public void prepareForUnion(ITypeDesc typeDesc) {
        _allIndexValuesQuery = true;
        if (_multipleUids != null) {
            setMultipleUIDs(_multipleUids.toArray(new String[_multipleUids.size()]));
            _allIndexValuesQuery = false;
        }
        List<IQueryIndexScanner> queryIndexes = new LinkedList<IQueryIndexScanner>();

        List<ICustomQuery> customQueries = null;
        HashMap<String, List<Range>> containsSamePathRanges = null;
        HashMap<ContainsItemIntersectionBase, ContainsItemValueRange> containsIntersections;
        if (_containsItemsQueries != null && !_containsItemsQueries.isEmpty()) {
            customQueries = new ArrayList<ICustomQuery>(_containsItemsQueries);
            if (_ranges == null)
                _ranges = new HashMap<String, Range>();
            containsSamePathRanges = new HashMap<String, List<Range>>();
            containsIntersections = new HashMap<ContainsItemIntersectionBase, ContainsItemValueRange>();
            insertContainsItemsRanges(_containsItemsQueries, containsSamePathRanges, null, containsIntersections);  //just for indexing sake, not used as independent ranges
        }

        //the following are used for compound indexes
        Map<Range, IQueryIndexScanner> usedByRanges = null;
        Map<String, Range> possibleCompoundSegments = typeDesc.anyCompoundIndex() && _ranges != null ? new HashMap<String, Range>() : null;


        if (_ranges != null) {
            if (customQueries == null)
                customQueries = new ArrayList<ICustomQuery>();


            //assign routing property so it can be used by the union packet
            String routingPropertyName = typeDesc.getRoutingPropertyName();
            Range routingRange;
            if (routingPropertyName != null) {
                routingRange = _ranges.get(routingPropertyName);

                if (routingRange != null) {
                    int propertyIndex = _typeDesc.getFixedPropertyPosition(routingPropertyName);
                    routingRange.toEntryPacket(this, propertyIndex);
                }
            }


            for (Map.Entry<String, Range> mapEntry : _ranges.entrySet()) {
                Range range = mapEntry.getValue();

                if (!range.isInternalRange())
                    customQueries.add(mapEntry.getValue());

                if (possibleCompoundSegments != null && range.suitableAsCompoundIndexSegment())
                    possibleCompoundSegments.put(mapEntry.getKey(), range);


                // path is the index name
                if (isRangeIndexed(range))
                //noinspection SpellCheckingInspection
                {
                    if (!range.isRelevantForAllIndexValuesOptimization())
                        _allIndexValuesQuery = false;
                    IQueryIndexScanner indexScanner = range.getIndexScanner();
                    if (indexScanner != null) {
                        queryIndexes.add(indexScanner);
                        if (possibleCompoundSegments != null && range.suitableAsCompoundIndexSegment()) {
                            if (usedByRanges == null)
                                usedByRanges = new HashMap<Range, IQueryIndexScanner>();
                            usedByRanges.put(range, indexScanner);
                        }
                    }
                    //in containsitem there may be several unrelated ranges with same path
                    //try to derive indices
                    if (containsSamePathRanges != null && containsSamePathRanges.containsKey(range.getPath())) {
                        List<Range> l = containsSamePathRanges.get(range.getPath());
                        for (Range r : l) {
                            if (r == range)
                                continue;
                            IQueryIndexScanner is = r.getIndexScanner();
                            if (indexScanner != null)
                                queryIndexes.add(is);
                        }
                    }
                } else
                    _allIndexValuesQuery = false;

            }


            //do we have possible compound indices ?
            if (!customQueries.isEmpty() && possibleCompoundSegments != null && possibleCompoundSegments.size() > 1) {
                List<Range> possibleSegments = new ArrayList<Range>();
                //go over all compound indices in type and check if we can use some
                for (SpaceIndex idx : typeDesc.getCompoundIndexes()) {
                    possibleSegments.clear();
                    CompoundIndex index = (CompoundIndex) idx;
                    for (ISpaceCompoundIndexSegment seg : index.getCompoundIndexSegments()) {
                        Range checkedRange = possibleCompoundSegments.get(seg.getName());
                        if (checkedRange != null && checkedRange.getFunctionCallDescription() == null) {
                            possibleSegments.add(possibleCompoundSegments.get(seg.getName()));
                        } else {
                            possibleSegments.clear();
                            break;  //not relevant
                        }
                    }
                    if (!possibleSegments.isEmpty())
                    //noinspection SpellCheckingInspection
                    {//maybe we can derive a compound indexscanner out of this.
                        IQueryIndexScanner indexScanner = buildCompoundIndexScannerFromSegmentRanges(index, possibleSegments);
                        if (indexScanner != null) {
                            queryIndexes.add(indexScanner);
                            if (usedByRanges != null && !usedByRanges.isEmpty()) {
                                //use the ranges used to build the compound in order to remove unneeded indexScanners
                                for (Range r : possibleSegments) {
                                    IQueryIndexScanner s = usedByRanges.get(r);
                                    if (s != null)
                                        queryIndexes.remove(s);
                                }
                            }
                        }
                    }
                }
            }
            if (!customQueries.isEmpty()) {
                if (customQueries.size() == 1) {
                    customQueries.get(0).getCustomIndexes().addAll(queryIndexes);
                    setCustomQuery(customQueries.get(0));
                } else {
                    CompoundAndCustomQuery customQuery = new CompoundAndCustomQuery(customQueries);
                    customQuery.getCustomIndexes().addAll(queryIndexes);
                    setCustomQuery(customQuery);
                }
            }
        }

        _preparedForSpace = true;
    }

    /**
     * Converts the external entry to a map of ranges
     */
    public HashMap<String, Range> getRanges() {
        return _ranges;
    }

    public boolean isAlwaysEmpty() {
        return _isAlwaysEmpty;
    }

    public void setAlwaysEmpty(boolean isAlwaysEmpty) {
        _isAlwaysEmpty = isAlwaysEmpty;
    }

    /**
     * Method to indicate whether this query is complex - false only if the template can be
     * translated to a space query without post processing some space operations don't support
     * complex queries
     */
    public boolean isComplex() {
        return false;
    }

    public QueryTemplatePacket buildAndPacket(QueryTemplatePacket packet) {
        return packet.and(this);
    }

    /**
     * Merge given templates into one template.
     */
    public QueryTemplatePacket and(QueryTemplatePacket template) {

        QueryTemplatePacket result = new QueryTemplatePacket(this);

        result.intersectRanges(template);
        result.intersectUids(template);
        result.uniteContainsItems(template);
        return result;
    }

    /**
     * Merge given templates into one template. for example: x=3 and (y=5 or z=8) ==> (x=3 and y=5)
     * or (x=3 and z=8)
     */
    public QueryTemplatePacket and(UnionTemplatePacket template) {
        LinkedList<QueryTemplatePacket> optimizedPackets = new LinkedList<QueryTemplatePacket>();
        for (QueryTemplatePacket packet : template.getPackets()) {
            QueryTemplatePacket result = new QueryTemplatePacket(this);

            result.intersectRanges(packet);
            result.intersectUids(packet);

            if (!result.isAlwaysEmpty())
                optimizedPackets.add(result);
        }

        if (optimizedPackets.isEmpty())
            template.setAlwaysEmpty(true);

        template.setPackets(optimizedPackets);
        return template;

    }

    protected void intersectUids(QueryTemplatePacket template) {
        if (template.getMultipleUids() == null)
            return;

        if (_multipleUids == null) {
            _multipleUids = template.getMultipleUids();
            return;
        }

        // merge the uids
        _multipleUids.retainAll(template.getMultipleUids());
    }

    /**
     * Merge given templates into one template. If merge fails - the templates are attached to the
     * original exp nodes
     */
    public void intersectRanges(QueryTemplatePacket template) {
        for (Map.Entry<String, Range> entry : template.getRanges().entrySet()) {
            String fieldName = entry.getKey();
            Range newRange = entry.getValue();

            Range prevRange = _ranges.get(fieldName);

            // Check for collisions on the same field
            if (prevRange == null) {
                _ranges.put(fieldName, newRange);
            } else {
                // Merge two expressions on the same field
                Range intersection = prevRange.intersection(newRange);

                _ranges.put(fieldName, intersection);

                if (intersection instanceof EmptyRange) {
                    setAlwaysEmpty(true);
                    return;
                }
            }
        }
    }

    public Set<String> getMultipleUids() {
        return _multipleUids;
    }

    public void setMultipleUids(Set<String> multipleUids) {
        _multipleUids = multipleUids;
    }

    public void setExtendedMatchCode(int index, short eq) {
        _extendedMatchCodes[index] = eq;
    }

    public void setRangeValue(int index, Object range) {
        if (_rangeValues == null)
            _rangeValues = new Object[_typeDesc.getNumOfFixedProperties()];

        _rangeValues[index] = range;
    }

    public void setRangeValueInclusion(int index, boolean include) {
        if (_rangeValuesInclusion == null)
            _rangeValuesInclusion = new boolean[_typeDesc.getNumOfFixedProperties()];

        _rangeValuesInclusion[index] = include;
    }

    public void setEntryType(EntryType objectType) {
        _entryType = objectType;
    }

    @Override
    public QueryResultTypeInternal getQueryResultType() {
        return _queryResultType;
    }

    public void setQueryResultType(QueryResultTypeInternal queryResultType) {
        _queryResultType = queryResultType;
    }

    public QueryTemplatePacket buildOrPacket(QueryTemplatePacket packet) {
        return packet.union(this);
    }

    public QueryTemplatePacket union(UnionTemplatePacket composite) {
        composite.add(this);
        return composite;
    }

    public QueryTemplatePacket union(QueryTemplatePacket packet) {
        UnionTemplatePacket composite = new UnionTemplatePacket(_table, packet.getQueryResultType());
        composite.add(this);
        composite.add(packet);
        return composite;
    }

    public IQueryResultSet<IEntryPacket> readMultiple(ISpaceProxy space, Transaction txn, int maxResults, int modifiers)
            throws Exception {
        return read(space, txn, 0, modifiers, false, maxResults, 0);
    }

    @Override
    public Object getRoutingFieldValue() {
        return (_routing != null) ? _routing : super.getRoutingFieldValue();
    }

    /**
     * Sets the routing value for the template.
     */
    public void setRouting(Object routing) {
        // Its important to first set routing anyway for overriding
        // cached routing value in template level
        this._routing = routing;

        // If null, super routing will be used.
        if(ProtectiveMode.isAmbiguousQueryRoutingUsageProtectionEnabled() && routing != null) {
            Object superRouting = super.getRoutingFieldValue();
            if (superRouting != null && !superRouting.equals(routing))
                throw new ProtectiveModeException(
                        "Ambiguous routing: Routing in SQL expression is <" + superRouting
                                + "> and the SQLQuery's routing was explicitly set to <"
                                + routing + "> (you can disable this protection, though it is not recommended, by setting the following system property: " + ProtectiveMode.AMBIGUOUS_QUERY_ROUTING_USAGE + "=false)");

        }
    }

    public AbstractProjectionTemplate getProjectionTemplate() {
        return _projectionTemplate;
    }

    public void setProjectionTemplate(AbstractProjectionTemplate projectionTemplate) {
        this._projectionTemplate = projectionTemplate;
    }

    public AggregationSet getAggregationSet() {
        return _aggregationSet;
    }

    public void setAggregationSet(AggregationSet aggregationSet) {
        _aggregationSet = aggregationSet;
    }

    public IQueryResultSet<IEntryPacket> read(ISpaceProxy space, AbstractDMLQuery query, Transaction txn, int modifiers, int maxResults)
            throws Exception {
        setRouting(query.getRouting());
        setProjectionTemplate(query.getProjectionTemplate());
        setQueryResultType(query.getQueryResultType());
        return read(space, txn, query.getTimeout(), modifiers, query.getIfExists(), maxResults, query.getMinEntriesToWaitFor());
    }

    public IQueryResultSet<IEntryPacket> read(ISpaceProxy space, Transaction txn,
                                              long timeout, int modifiers, boolean ifExists, int maxResults, int minEntriesToWaitFor)
            throws Exception {
        if (isAlwaysEmpty())
            return new ArrayListResult();


        if (getAggregationSet() != null) {
            //execute aggregation query
            return AggregationsUtil.aggregate(this, getAggregationSet(), space, txn, modifiers);
        }
        if (maxResults == 1) {
            IEntryPacket result = (IEntryPacket) space.read(this, txn, timeout, modifiers, ifExists);
            if (result != null)
                return new ArrayListResult(result);
        } else {
            IEntryPacket[] result;
            if (timeout != 0) {
                result = (IEntryPacket[]) space.readMultiple(this, txn, timeout, maxResults, minEntriesToWaitFor, modifiers, false, ifExists);

            } else {
                //noinspection deprecation
                result = (IEntryPacket[]) space.readMultiple(this, txn, maxResults, modifiers);
            }
            if (result != null)
                return new ArrayListResult(result);
        }
        return new ArrayListResult();
    }

    public ArrayList<IEntryPacket> take(IJSpace space, Object routing, AbstractProjectionTemplate projectionTemplate, Transaction txn,
                                        long timeout, int modifiers, boolean ifExists, int maxResults, int minEntriesToWaitFor, QueryResultTypeInternal resultType)
            throws RemoteException, TransactionException, UnusableEntryException, InterruptedException {
        ArrayList<IEntryPacket> entries = new ArrayList<IEntryPacket>();

        setRouting(routing);
        setQueryResultType(resultType);
        setProjectionTemplate(projectionTemplate);

        if (maxResults == 1) {
            ISpaceProxy spaceProxy = (ISpaceProxy) space;
            IEntryPacket result = (IEntryPacket) spaceProxy.take(this, txn, timeout, modifiers, ifExists);
            if (result != null)
                entries.add(result);
        } else {
            IEntryPacket[] result;
            if (timeout != 0) {
                ISpaceProxy spaceProxy = (ISpaceProxy) space;
                result = (IEntryPacket[]) spaceProxy.takeMultiple(this, txn, timeout, maxResults, minEntriesToWaitFor, modifiers, false /*boolean returnOnlyUids*/, ifExists);
            } else
                //noinspection deprecation
                result = (IEntryPacket[]) space.takeMultiple(this, txn, maxResults, modifiers);
            if (result != null)
                Collections.addAll(entries, result);
        }

        return entries;
    }

    protected boolean matches(IEntryPacket entryPacket) {
        for (Map.Entry<String, Range> entry : _ranges.entrySet()) {
            String fieldName = entry.getKey();
            Range range = entry.getValue();

            Object fieldValue;
            if (range.isNestedQuery()) {
                fieldValue = SQLUtil.getFieldValue(entryPacket, _typeDesc, fieldName);
            } else {
                fieldValue = entryPacket.getPropertyValue(fieldName);
            }

            if (!range.getPredicate().execute(fieldValue))
                return false;

        }
        return true;
    }

    public boolean matches(JoinedEntry joinedEntry) {
        IEntryPacket entry = joinedEntry.getEntry(_table.getTableIndex());
        return matches(entry);
    }

    @Override
    public boolean isAllIndexValuesSqlQuery() {
        return _allIndexValuesQuery;
    }

    @Override
    public void writeToSwap(ObjectOutput out) throws IOException {
        super.writeToSwap(out);
        serialize(out, PlatformLogicalVersion.getLogicalVersion());
    }

    @Override
    public void readFromSwap(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readFromSwap(in);
        deserialize(in, PlatformLogicalVersion.getLogicalVersion());
    }

    @Override
    protected void writeExternal(ObjectOutput out, PlatformLogicalVersion version) throws IOException {
        super.writeExternal(out, version);
        serialize(out, version);
    }

    @Override
    protected void readExternal(ObjectInput in, PlatformLogicalVersion version) throws IOException, ClassNotFoundException {
        super.readExternal(in, version);
        deserialize(in, version);
    }

    private void serialize(ObjectOutput out, PlatformLogicalVersion version) throws IOException {
        if (_containsItemsQueries != null && version.lessThan(PlatformLogicalVersion.v9_6_0) && !_containsItemsQueries.isEmpty())
            throw new RuntimeException("cannot execute query- server version does not support contains-item");
        out.writeByte(_queryResultType.getCode());

        if (version.greaterOrEquals(PlatformLogicalVersion.v9_5_0))
            IOUtils.writeObject(out, _projectionTemplate);
        if (version.greaterOrEquals(PlatformLogicalVersion.v11_0_0))
            out.writeBoolean(_allIndexValuesQuery);
        if (version.greaterOrEquals(PlatformLogicalVersion.v15_8_0))
            if (_table != null){
                IOUtils.writeObject(out, _table);
                IOUtils.writeObject(out, _ranges);
                IOUtils.writeObject(out, _aggregationSet);
            } else {
                IOUtils.writeObject(out, null);
            }
    }

    private void deserialize(ObjectInput in, PlatformLogicalVersion version) throws IOException, ClassNotFoundException {
        byte code = in.readByte();
        _queryResultType = QueryResultTypeInternal.fromCode(code);
        if (_queryResultType == null) throw new IllegalStateException("code=" + code);

        if (version.greaterOrEquals(PlatformLogicalVersion.v9_5_0))
            _projectionTemplate = IOUtils.readObject(in);
        if (version.greaterOrEquals(PlatformLogicalVersion.v11_0_0))
            _allIndexValuesQuery = in.readBoolean();
        if (version.greaterOrEquals(PlatformLogicalVersion.v15_8_0)) {
            _table = IOUtils.readObject(in);
            if (_table != null) {
                _ranges = IOUtils.readObject(in);
                _aggregationSet = IOUtils.readObject(in);
            }
        }
    }

    private void uniteContainsItems(QueryTemplatePacket template) {
        if (template._containsItemsQueries != null && !template._containsItemsQueries.isEmpty()) {
            if (_containsItemsQueries != null)
                _containsItemsQueries.addAll(template._containsItemsQueries);
            else
                _containsItemsQueries = template._containsItemsQueries;
        }

    }

    public void setExplainPlan(ExplainPlan explainPlan) {
        this.explainPlan = explainPlan;
    }

    public ExplainPlan getExplainPlan() {
        return explainPlan;
    }
}
