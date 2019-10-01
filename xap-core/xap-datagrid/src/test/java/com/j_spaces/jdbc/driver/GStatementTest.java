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
package com.j_spaces.jdbc.driver;

import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;

public class GStatementTest {

    /*
    By default unsupported sql operation should throw an exception

    This behavior can be relaxed by setting a system property

    Operations tested:
    setFetchSize,setMaxRows
     */

    final private static int INT_VALUE = 0;

    @Test
    public void testUnsupportedSqlOperationsThrowException(){

        System.setProperty(GStatement.IGNORE_UNSUPPORTED_OPTIONS_PROP,"false");

        GStatement statement = new GStatement(null);

        try{

            statement.setFetchSize(INT_VALUE);

        }catch (Exception e){
            Assert.assertTrue(e.getMessage().equals("Command not Supported!"));

            Assert.assertTrue("Unsupported sql operations should throw an SQLException ", e.getClass().getSimpleName().equals("SQLException"));
        }

        try{

            statement.setMaxRows(INT_VALUE);

        }catch (Exception e){
            Assert.assertTrue(e.getMessage().equals("Command not Supported!"));

            Assert.assertTrue("Unsupported sql operations should throw an SQLException ", e.getClass().getSimpleName().equals("SQLException"));
        }


    }

    @Test
    public void testUnsupportedSqlOperationsOnlyWarn() {

        System.setProperty(GStatement.IGNORE_UNSUPPORTED_OPTIONS_PROP,"true");

        GStatement statement = new GStatement(null);

        try{

            statement.setFetchSize(INT_VALUE);

        }catch (SQLException e){
            Assert.fail("Unsupported sql operations should not throw an exception " + e);
        }

        try{

            statement.setMaxRows(INT_VALUE);

        }catch (SQLException e){
            Assert.fail("Unsupported sql operations should not throw an exception " + e);
        }
    }
}

