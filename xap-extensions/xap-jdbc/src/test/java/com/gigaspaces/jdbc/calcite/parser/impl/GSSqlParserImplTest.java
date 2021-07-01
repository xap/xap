package com.gigaspaces.jdbc.calcite.parser.impl;

import com.gigaspaces.jdbc.calcite.parser.GSSqlParserFactoryWrapper;
import com.gigaspaces.jdbc.calcite.sql.extension.SqlShowOption;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlShuttle;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;

public class GSSqlParserImplTest {
    @Test
    public void testParseSetRegular() throws Exception {
        String sql = "SET CLIENT_ENCODING = 'UTF-8'";
        SqlNode parseResult = parse(sql);
        Assert.assertNotNull(parseResult);
        Assert.assertTrue(parseResult instanceof SqlSetOption);
        SqlSetOption set = (SqlSetOption) parseResult;
        String name = set.getName().toString();
        Assert.assertEquals("CLIENT_ENCODING", name);
        SqlNode valueNode = set.getValue();
        Assert.assertTrue(valueNode instanceof SqlLiteral);
        SqlLiteral literal = (SqlLiteral) valueNode;
        String value = literal.getValueAs(String.class);
        Assert.assertEquals("UTF-8", value);
    }

    @Test
    public void testParseSetTo() throws Exception {
        String sql = "SET CLIENT_ENCODING TO 'UTF-8'";
        SqlNode parseResult = parse(sql);
        Assert.assertNotNull(parseResult);
        Assert.assertTrue(parseResult instanceof SqlSetOption);
        SqlSetOption set = (SqlSetOption) parseResult;
        String name = set.getName().toString();
        Assert.assertEquals("CLIENT_ENCODING", name);
        SqlNode valueNode = set.getValue();
        Assert.assertTrue(valueNode instanceof SqlLiteral);
        SqlLiteral literal = (SqlLiteral) valueNode;
        String value = literal.getValueAs(String.class);
        Assert.assertEquals("UTF-8", value);
    }

    @Test
    public void testSetTransactionIsolation() throws Exception {
        String sql = "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE";
        SqlNode parseResult = parse(sql);
        Assert.assertNotNull(parseResult);
        Assert.assertTrue(parseResult instanceof SqlSetOption);
        SqlSetOption set = (SqlSetOption) parseResult;
        String name = set.getName().toString();
        Assert.assertEquals("TRANSACTION_ISOLATION", name);
        SqlNode valueNode = set.getValue();
        Assert.assertTrue(valueNode instanceof SqlLiteral);
        SqlLiteral literal = (SqlLiteral) valueNode;
        String value = literal.getValueAs(String.class);
        Assert.assertEquals("SERIALIZABLE", value);
    }

    @Test
    public void testParseReset() throws Exception {
        String sql = "RESET CLIENT_ENCODING";
        SqlNode parseResult = parse(sql);
        Assert.assertNotNull(parseResult);
        Assert.assertTrue(parseResult instanceof SqlSetOption);
        SqlSetOption set = (SqlSetOption) parseResult;
        String name = set.getName().toString();
        Assert.assertEquals("CLIENT_ENCODING", name);
        SqlNode valueNode = set.getValue();
        Assert.assertNull(valueNode);
    }

    @Test
    public void testParseShow() throws Exception {
        String sql = "SHOW CLIENT_ENCODING";
        SqlNode parseResult = parse(sql);
        Assert.assertNotNull(parseResult);
        Assert.assertTrue(parseResult instanceof SqlShowOption);
        SqlShowOption show = (SqlShowOption) parseResult;
        String name = show.getName().toString();
        Assert.assertEquals("CLIENT_ENCODING", name);
    }

    @Test
    public void testParseLiteral() throws Exception {
        String sql = "select 1::int2";
        SqlNode parseResult = parse(sql);
        Assert.assertNotNull(parseResult);
    }

    @Test
    public void testParsePgParameter() throws Exception {
        String sql = "select * from test where id = $2";
        SqlNode parseResult = parse(sql);
        Assert.assertNotNull(parseResult);

        AtomicReference<SqlDynamicParam> valueContainer = new AtomicReference<>();
        parseResult.accept(
            new SqlShuttle() {
                @Override public SqlNode visit(SqlDynamicParam param) {
                    valueContainer.set(param);
                    return param;
                }
            });

        SqlDynamicParam param = valueContainer.get();
        Assert.assertNotNull(param);
        Assert.assertEquals(param.getIndex(), 1);
    }

    private SqlNode parse(String sql) throws Exception {
        SqlParser.Config config = SqlParser.configBuilder()
                .setParserFactory(GSSqlParserFactoryWrapper.FACTORY)
                .setQuotedCasing(Casing.UNCHANGED)
                .setUnquotedCasing(Casing.UNCHANGED)
                .build();

        SqlParser parser = SqlParser.create(sql, config);

        SqlNodeList nodeList = parser.parseStmtList();

        return nodeList.size() == 1 ? nodeList.get(0) : nodeList;
    }
}