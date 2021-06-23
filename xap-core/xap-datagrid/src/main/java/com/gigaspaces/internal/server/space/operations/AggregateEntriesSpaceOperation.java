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

package com.gigaspaces.internal.server.space.operations;

import com.gigaspaces.internal.client.spaceproxy.operations.AggregateEntriesSpaceOperationRequest;
import com.gigaspaces.internal.client.spaceproxy.operations.AggregateEntriesSpaceOperationResult;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.query.aggregators.*;
import com.gigaspaces.security.authorities.SpaceAuthority;
import com.gigaspaces.utils.CodeChangeUtilities;
import com.j_spaces.core.AnswerHolder;

import java.util.List;

/**
 * @author Niv Ingberg
 * @since 10.0
 */
@com.gigaspaces.api.InternalApi
public class AggregateEntriesSpaceOperation extends AbstractSpaceOperation<AggregateEntriesSpaceOperationResult, AggregateEntriesSpaceOperationRequest> {

    @Override
    public void execute(AggregateEntriesSpaceOperationRequest request, AggregateEntriesSpaceOperationResult result, SpaceImpl space, boolean oneway)
            throws Exception {
        try{
            SpaceAuthority.SpacePrivilege requiredPrivilege = AggregationInternalUtils.containsCustomAggregators(request.getAggregators())
                    ? SpaceAuthority.SpacePrivilege.EXECUTE
                    : SpaceAuthority.SpacePrivilege.READ;

            space.beginPacketOperation(true, request.getSpaceContext(), requiredPrivilege, request.getQueryPacket());

            AnswerHolder answerHolder = space.getEngine().aggregate(request.getQueryPacket(), request.getAggregators(), request.getReadModifiers(), request.getSpaceContext());

            Object[] intermediateResults = new Object[request.getAggregators().size()];
            for (int i = 0; i < intermediateResults.length; i++)
                intermediateResults[i] = request.getAggregators().get(i).getIntermediateResult();
            result.setIntermediateResults(intermediateResults);
            if (answerHolder != null && answerHolder.getExplainPlan() != null) {
                for(SpaceEntriesAggregator aggregator : request.getAggregators()) {
                    if (aggregator instanceof OrderByAggregator) { // for now support only orderBy
                        OrderByAggregator orderByAggregator = (OrderByAggregator) aggregator;
                        List<OrderByPath> orderByPaths = orderByAggregator.getOrderByPaths();
                        for (OrderByPath orderByPath : orderByPaths) {
                            answerHolder.getExplainPlan().addAggregatorsInfo("OrderBy", orderByPath.toString());
                        }
                    }
                    else if (aggregator instanceof DistinctAggregator){
                        DistinctAggregator distinctAggregator = (DistinctAggregator)aggregator;
                        String[] distinctPaths = distinctAggregator.getDistinctPaths();
                        if(distinctAggregator.isGroupByAggregator()){ //group by
                            for(int i=0; i < distinctPaths.length ; i++) {
                                answerHolder.getExplainPlan().addAggregatorsInfo("Group By", distinctPaths[i]);
                            }
                        }
                        else{ //distinct
                            for(int i=0; i < distinctPaths.length ; i++) {
                                answerHolder.getExplainPlan().addAggregatorsInfo("Distinct", distinctPaths[i]);
                            }
                        }
                    }
                    else if (aggregator instanceof GroupByAggregator){
                        GroupByAggregator groupByAggregator = (GroupByAggregator)aggregator;
                        String[] groupByPaths = groupByAggregator.getGroupByPaths();
                        for(int i=0; i < groupByPaths.length ; i++) {
                            answerHolder.getExplainPlan().addAggregatorsInfo("Group By", groupByPaths[i]);
                        }
                    }
                }
                result.setExplainPlan(answerHolder.getExplainPlan());
            }
        }
        finally {
            CodeChangeUtilities.removeOneTimeClassLoaderIfNeeded(request.getAggregators());
        }
    }

    @Override
    public String getLogName(AggregateEntriesSpaceOperationRequest request, AggregateEntriesSpaceOperationResult result) {
        return "scan";
    }
}
