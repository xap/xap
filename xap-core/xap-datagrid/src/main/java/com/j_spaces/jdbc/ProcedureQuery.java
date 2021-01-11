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

/*
 * Created on 03/05/2005
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package com.j_spaces.jdbc;

import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.j_spaces.core.ExternalEntryPacket;
import com.j_spaces.jdbc.driver.GConnection;
import com.j_spaces.jdbc.driver.GDriver;
import com.j_spaces.kernel.ClassLoaderHelper;

import net.jini.core.transaction.Transaction;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;

/**
 * @author alex
 *
 *         TODO To change the template for this generated type comment go to Window - Preferences -
 *         Java - Code Style - Code Templates
 */
@com.gigaspaces.api.InternalApi
public class ProcedureQuery extends AbstractDMLQuery {

    private String procName;
    private ArrayList paramList;


    public ProcedureQuery() {
        this.paramList = new ArrayList();
    }


    public void setProcName(String procName) {
        this.procName = procName;
    }

    public void addParamList(Object param) {
        paramList.add(param);
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.Query#executeOnSpace(com.j_spaces.core.IJSpace, net.jini.core.transaction.Transaction, java.util.Hashtable)
     */
    public ResponsePacket executeOnSpace(ISpaceProxy space, Transaction txn) throws SQLException {
        ResponsePacket packet = new ResponsePacket();

        Connection con = null;
        ResultEntry result = null;
        IProcedure runObj = null;
        ArrayList param = null;


        try {
            // First try to use query processor embedded connection
            con = GConnection.getInstance(space);

            runObj = getProcedure(procName);


            if (isPrepared) {
                param = new ArrayList();
                param.addAll(0, Arrays.asList(preparedValues));
            } else {
                param = paramList;
            }

            result = runObj.execute(con, space, param);

            if (result == null) {
                result = new ResultEntry(
                        new String[]{procName},
                        new String[]{procName},
                        new String[]{""},
                        new Object[0][0]);
            }

            if (isConvertResultToArray()) {
                packet.setResultEntry(result);
            } else {
                // create a result set out of the field values
                Collection<IEntryPacket> resultSet = new ArrayList<IEntryPacket>(result.getFieldValues().length);

                ITypeDesc typeDesc = getTypeInfo();

                // Since no classname is provided in result
                // - the original template class is used.
                // Note - class inheritance is not handled
                for (Object[] value : result.getFieldValues())
                    resultSet.add(new ExternalEntryPacket(typeDesc, value));

                packet.setResultSet(resultSet);
            }

            con.close();
        } catch (Exception ex) {


            SQLException se = new SQLException("Failed to execute stored procedure; Cause: " + ex, "GSP", -222);
            se.initCause(ex);
            throw se;
        }

        return packet;
    }

    public IProcedure getProcedure(String procName) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        IProcedure runObj = null;
        HashMap pTable = GDriver.getProcTable();

        if (pTable.containsKey(procName)) {
            runObj = (IProcedure) pTable.get(procName);
        } else {
            runObj = (IProcedure) ClassLoaderHelper.loadClass(procName).newInstance();
            pTable.put(procName, runObj);
        }

        return runObj;
    }

    @Override
    public ProcedureQuery clone() {
        ProcedureQuery query = new ProcedureQuery();

        query.paramList = this.paramList;
        query.procName = this.procName;
        query.isPrepared = this.isPrepared;
        query._tablesData = this._tablesData;
        query.tables = tables;
        return query;
    }
}
