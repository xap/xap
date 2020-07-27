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

package com.j_spaces.jdbc;

/**
 *
 * @since 15.5.0
 * @author Evgeny Fisher
 */
@com.gigaspaces.api.InternalApi
public class ExplainPlanResponsePacket extends ResponsePacket{

    private static final long serialVersionUID = 1L;

    private String explainPlan;

    public ExplainPlanResponsePacket(){

    }

    public ExplainPlanResponsePacket( ResponsePacket responsePacket, String explainPlan ) {
        setIntResult( responsePacket.getIntResult() );
        setResultEntry( responsePacket.getResultEntry() );
        setResultSet( responsePacket.getResultSet() );
        if( responsePacket.getResultSet() != null ) {
            setResultArray(responsePacket.getArray());
        }
        this.explainPlan = explainPlan;
    }

    public String getExplainPlan() {
        return explainPlan;
    }
}