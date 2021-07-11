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
package com.gigaspaces.sql.aggregatornode.netty.server;

import com.gigaspaces.sql.aggregatornode.netty.server.output.ConsoleOutput;
import com.gigaspaces.sql.aggregatornode.netty.server.output.Output;
import org.junit.Assert;

import java.io.BufferedReader;
import java.io.StringReader;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class DumpUtils {
    public static List<Row> dump(ResultSet rs) throws SQLException {
        List<Row> result = new ArrayList<>();
        ResultSetMetaData metaData = rs.getMetaData();
        Output out = new Output();
        String[] columnNames = getHeader(metaData);
        out.addColumns(columnNames);

        while (rs.next()) {
            Object[] rowValues = getRow(rs, metaData);
            out.addRow(Arrays.stream(rowValues).map(String::valueOf).collect(Collectors.toList()));
            result.add(new Row(columnNames, rowValues));
        }

        ConsoleOutput.newline();
        ConsoleOutput.println(out);

        return result;
    }

    public static void checkResult(ResultSet rs, String expected) throws Exception {
        List<Row> result = new ArrayList<>();
        ResultSetMetaData metaData = rs.getMetaData();
        Output out = new Output();
        String[] columnNames = getHeader(metaData);
        out.addColumns(columnNames);

        while (rs.next()) {
            Object[] rowValues = getRow(rs, metaData);
            out.addRow(Arrays.stream(rowValues).map(String::valueOf).collect(Collectors.toList()));
            result.add(new Row(columnNames, rowValues));
        }

        ConsoleOutput.newline();
        ConsoleOutput.println(out);

        BufferedReader reader = new BufferedReader(new StringReader(expected));
        List<String> lines = reader.lines().collect(Collectors.toList());
        assert lines.size() >= 2;

        checkColumns(lines.get(0), columnNames);
        Assert.assertEquals("Unexpected rows count", lines.size() - 2, result.size());

        for (int i = 0; i < result.size(); i++)
            checkValues(lines.get(i + 2), result.get(i), i + 1);
    }

    private static void checkColumns(String header, String[] columnNames) {
        List<String> expected = splitValues(header);
        Assert.assertEquals("Unexpected columns count", expected.size(), columnNames.length);
        for (int i = 0; i < columnNames.length; i++)
            Assert.assertEquals("Unexpected column at position " + (i + 1), expected.get(i), columnNames[i]);
    }

    private static void checkValues(String line, Row row, int lineNum) {
        List<String> expected = splitValues(line);
        assert expected.size() == row.columnValues.length;
        Object[] columnValues = row.columnValues;
        for (int i = 0; i < columnValues.length; i++) {
            Assert.assertEquals("Unexpected value at line " + lineNum + " column " + (i + 1), expected.get(i), String.valueOf(columnValues[i]));
        }
    }

    private static List<String> splitValues(String line) {
        List<String> result = new ArrayList<>();
        StringBuilder b = new StringBuilder();
        boolean escaped = false;
        boolean quoted = false;
        for (int i = 0; i < line.length(); i++) {
            if ('\\' == line.charAt(i)) {
                escaped = true;
                continue;
            }
            if (!escaped && '\'' == line.charAt(i)) {
                quoted = !quoted;
                continue;
            }
            if (!quoted && Character.isWhitespace(line.charAt(i)))
                continue;

            if (!escaped && '|' == line.charAt(i)) {
                if (b.length() == 0)
                    continue;
                result.add(b.toString());
                b.setLength(0);

                continue;
            }

            b.append(line.charAt(i));
            escaped = false;
        }
        return result;
    }

    public static String[] getHeader(ResultSetMetaData metaData) throws SQLException {
        int columnCount = metaData.getColumnCount();
        String[] result = new String[columnCount];
        int Cs=1;
        for (int i = 1; i <= columnCount; i++) {
            String label = metaData.getColumnLabel(i);
            if (label.endsWith(")")) { //replace aggr column without alias to CX
                label = "C"+(Cs++);
            }
            result[i-1] = label;
        }
        return result;
    }

    public static Object[] getRow(ResultSet rs, ResultSetMetaData metaData) throws SQLException {
        Object[] result = new Object[metaData.getColumnCount()];
        for (int i = 0; i < result.length; i++) {
            result[i] = rs.getObject(i + 1);
        }
        return result;
    }

    public static class Row {
        private final String[] columnNames;
        private final Object[] columnValues;

        public Row(String[] columnNames, Object[] columnValues) {
            this.columnNames = columnNames;
            this.columnValues = columnValues;
        }

        public Object getColumnValue(String columnName) {
            int pos = -1;
            for (int i = 0; i < columnNames.length; i++) {
                String column = columnNames[i];
                if (column.equalsIgnoreCase(columnName)) {
                    pos = i;
                    break;
                }
            }
            return pos == -1 ? null : columnValues[pos];
        }

        @Override
        public boolean equals(Object o) {
            Row otherRow = (Row) o;
            for (String column : columnNames) {
                Object value = getColumnValue(column);
                Object otherValue = otherRow.getColumnValue(column);
                if (!value.equals(otherValue))
                    return false;
            }
            return true;
        }
    }

}
