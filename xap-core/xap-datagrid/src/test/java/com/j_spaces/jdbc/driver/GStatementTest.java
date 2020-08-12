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

