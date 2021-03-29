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
package com.gigaspaces.jdbc;

import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;

public class ConnectionStringTest {

    @Test
    public void test() {

        //TODO - currently there is no default so v3 is mandatory
//        testConnectionString("jdbc:gigaspaces://localhost:4174;localhost2:1234/demo", "jini://*/*/demo?locators=localhost:4174,localhost2:1234");
        testFailure("jdbc:gigaspaces://localhost:4174;localhost2:1234/demo");
//        testConnectionString("jdbc:gigaspaces://localhost:4174/demo", "jini://*/*/demo?locators=localhost:4174");
        testFailure("jdbc:gigaspaces://localhost:4174/demo");
//        testConnectionString("jdbc:gigaspaces://localhost/demo", "jini://*/*/demo?locators=localhost");
        testFailure("jdbc:gigaspaces://localhost/demo");



        testConnectionString("jdbc:gigaspaces:v3://localhost:4174;localhost2:1234/demo", "jini://*/*/demo?locators=localhost:4174,localhost2:1234");
        testConnectionString("jdbc:gigaspaces:v3://localhost:4174/demo", "jini://*/*/demo?locators=localhost:4174");
        testConnectionString("jdbc:gigaspaces:v3://localhost/demo", "jini://*/*/demo?locators=localhost");

        testFailure("jdbc:gigaspaces:aaa://localhost:4174;localhost2:1234/demo");
        testFailure("jdbc:gigaspaces:aaa://localhost:4174/demo");
        testFailure("jdbc:gigaspaces:aaa://localhost/demo");

        testFailure("jdbc:gigaspaces:v1://localhost:4174;localhost2:1234/demo");
        testFailure("jdbc:gigaspaces:v1://localhost:4174/demo");
        testFailure("jdbc:gigaspaces:v1://localhost/demo");

        testFailure("jdbc:gigaspaces:v3://localhost:/demo");
        testFailure("jdbc:gigaspaces:v3://localhost:12/demo");
        testFailure("jdbc:gigaspaces:v3://localhost:123456/demo");

        testFailure("jdbc:gigaspaces:v3://localhost:1234;localhost2:/demo");

    }

    private void testFailure(String connString) {
        try {
            String spaceUrl = new GSConnection("", null).validateAndGetSpaceUrl(connString);
            Assert.fail("Should fail");
        } catch (SQLException e) {
            Assert.assertEquals(getExpectedErrorMessage(connString), e.getMessage());
        }

    }

    private void testConnectionString(String connString, String expectedSpaceUrl) {
        try {
            String spaceUrl = new GSConnection("", null).validateAndGetSpaceUrl(connString);
            Assert.assertEquals("Unexpected space url for connection string ["+connString+"]", expectedSpaceUrl, spaceUrl);
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail("Failed to validate and get space url from connection string: " + connString);
        }

    }


    private String getExpectedErrorMessage(String connString) {
        return "Invalid Url ["+connString+"] - does not match jdbc:gigaspaces:v3://hostname[:port]/spaceName";
    }
}
